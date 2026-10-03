"""The classification derivation's Dagster side: three calls in order, and when they fire.

The rules are SQL, tested against real Postgres in muffin-deployment
(`stack/supabase/tests/a-weighted-classification-is-not-a-label.sql` and its neighbours, plus
`classification-is-the-worker-s-to-derive.sql` for `ingest_rw`'s EXECUTE). What this side owns:
- the three run in the edge resource's order;
- each commits on its own, so a late failure keeps the earlier work;
- the counts and durations reach the materialization;
- the asset fires on a new fund holding, and daily, while the N-PORT lane always has filings
  that are named but not yet fetched.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar

import dagster as dg

from muffin_ingest_dagster.lib.resources import Postgres

#: What the three returned on production, 2026-10-03, rolled back.
RETURNS = {
    "select market.derive_classifications()": 515,
    "select market.derive_segment_classification()": 174,
    "select market.derive_sic_classification()": 2,
}


class _Cursor:
    def __init__(self, db: type[FakePostgres]) -> None:
        self._db = db
        self._row: tuple[Any, ...] | None = None

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        self._db.executed.append(text)
        if text == self._db.fail_on:
            raise RuntimeError("canceling statement due to statement timeout")
        if text not in RETURNS:
            raise AssertionError(f"unexpected query: {text[:120]}")
        self._row = (RETURNS[text],)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class _Conn:
    def __init__(self, db: type[FakePostgres]) -> None:
        self._db = db

    def cursor(self) -> _Cursor:
        return _Cursor(self._db)

    def commit(self) -> None:
        self._db.commits += 1


class FakePostgres(Postgres):
    executed: ClassVar[list[str]] = []
    commits: ClassVar[int] = 0
    fail_on: ClassVar[str | None] = None

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield _Conn(type(self))


def _reset(fail_on: str | None = None) -> None:
    FakePostgres.executed = []
    FakePostgres.commits = 0
    FakePostgres.fail_on = fail_on


def test_the_three_derivations_run_in_order_each_committed_and_recorded() -> None:
    """THE EDGE RESOURCE'S ORDER, ONE TRANSACTION EACH. Its three RPCs were three transactions, so
    `derive_classifications`' rows survived a timeout in the segment call; one transaction here
    would turn a late failure into a lost day of the cheap work too."""
    from muffin_ingest_dagster.defs.discovery.derived import security_classification

    _reset()
    result = dg.materialize([security_classification], resources={"postgres": FakePostgres()})

    assert result.success
    assert FakePostgres.executed == list(RETURNS)
    assert FakePostgres.commits == 3
    events = result.asset_materializations_for_node("security_classification")
    assert len(events) == 1
    recorded = {k: v.value for k, v in events[0].metadata.items()}
    assert {k: recorded[k] for k in ("classified", "weighted", "sic")} == {
        "classified": 515,
        "weighted": 174,
        "sic": 2,
    }
    assert all(isinstance(recorded[f"{k}_ms"], int) for k in ("classified", "weighted", "sic"))


def test_a_failure_keeps_what_the_earlier_derivations_wrote() -> None:
    """The segment call is the one that timed out at the 8 s ceiling. Should it fail here, the
    first derivation is already committed and the run fails loudly, rather than succeeding with
    a partial answer or discarding the part that worked."""
    from muffin_ingest_dagster.defs.discovery.derived import security_classification

    _reset(fail_on="select market.derive_segment_classification()")
    result = dg.materialize(
        [security_classification], resources={"postgres": FakePostgres()}, raise_on_error=False
    )

    assert not result.success
    assert FakePostgres.executed == list(RETURNS)[:2], "a failure must stop the run there"
    assert FakePostgres.commits == 1, "the first derivation must be committed before the second"


#: Two evaluations 30 s apart at midday, so no tick of the 05:44 floor falls between them.
MIDDAY = (datetime(2026, 10, 3, 12, 0, 0, tzinfo=UTC), datetime(2026, 10, 3, 12, 0, 30, tzinfo=UTC))
#: The same, either side of the floor's 05:44 tick.
ACROSS_THE_FLOOR = (
    datetime(2026, 10, 3, 5, 42, tzinfo=UTC),
    datetime(2026, 10, 3, 5, 46, tzinfo=UTC),
)


def _requested_before_and_after(
    classification: dg.AssetsDefinition,
    *,
    update: bool = True,
    times: tuple[datetime, datetime] = MIDDAY,
) -> tuple[bool, bool]:
    """Evaluate the condition over the N-PORT lane at two times, optionally with one filing's
    holdings landing between them, while a second filing stays unfetched as production's do."""
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw
    from muffin_ingest_dagster.defs.discovery.partitions import NPORT_PARTITIONS
    from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore

    # Resources only so the definitions validate; nothing is executed.
    defs = dg.Definitions(
        assets=[
            discovery_raw.raw_nport_filing,
            discovery_core.discovered_security,
            discovery_core.fund_holding,
            classification,
        ],
        resources={
            "postgres": Postgres(),
            "parquet_io": ParquetIOManager("/nonexistent"),
            "postgres_io": ParquetIOManager("/nonexistent"),
            "raw_store": RawStore(base_path="/nonexistent"),
        },
    )
    only = dg.AssetSelection.assets("security_classification")
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(NPORT_PARTITIONS, ["1:0001-26-1", "2:0002-26-2"])
        # Derived once already, so a request below can only come from the update or the floor.
        instance.report_runless_asset_event(dg.AssetMaterialization("security_classification"))
        first = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, asset_selection=only, evaluation_time=times[0]
        )
        if update:
            instance.report_runless_asset_event(
                dg.AssetMaterialization("fund_holding", partition="1:0001-26-1")
            )
        second = dg.evaluate_automation_conditions(
            defs=defs,
            instance=instance,
            asset_selection=only,
            cursor=first.cursor,
            evaluation_time=times[1],
        )

    def requested(result: Any) -> bool:
        return any(
            r.key == dg.AssetKey("security_classification") and r.true_subset.size > 0
            for r in result.results
        )

    return requested(first), requested(second)


def test_the_derivation_fires_when_a_filing_s_holdings_land_while_others_are_missing() -> None:
    """The N-PORT lane always holds filings the directory named and nobody has fetched yet, so
    plain `eager()` would never fire. The control makes the two rules disagree on the same graph."""
    from muffin_ingest_dagster.defs.discovery.derived import security_classification

    condition = next(iter(security_classification.automation_conditions_by_key.values()), None)
    assert condition is not None and condition.is_serializable, "the daemon must be able to read it"

    assert _requested_before_and_after(security_classification) == (False, True)

    plain = dg.map_asset_specs(
        lambda spec: spec.replace_attributes(automation_condition=dg.AutomationCondition.eager()),
        [security_classification],
    )[0]
    assert isinstance(plain, dg.AssetsDefinition)
    assert _requested_before_and_after(plain) == (False, False), (
        "plain eager() fired too, so this test no longer shows why the gate was removed"
    )


def test_a_daily_floor_derives_with_no_upstream_change() -> None:
    """MOST INPUTS ARE NOT ASSETS. yfinance sectors, SIC codes and segment rows are written by edge
    resources, so without the floor a segment filing parsed today would wait for the next fund
    filing to be classified. The control without the floor proves the tick is what fires."""
    from muffin_ingest_dagster.defs.discovery.derived import security_classification

    assert _requested_before_and_after(
        security_classification, update=False, times=ACROSS_THE_FLOOR
    ) == (False, True)
    assert _requested_before_and_after(security_classification, update=False) == (False, False)

    without_floor = dg.map_asset_specs(
        lambda spec: spec.replace_attributes(
            automation_condition=dg.AutomationCondition.eager().without(
                ~dg.AutomationCondition.any_deps_missing()
            )
        ),
        [security_classification],
    )[0]
    assert isinstance(without_floor, dg.AssetsDefinition)
    assert _requested_before_and_after(without_floor, update=False, times=ACROSS_THE_FLOOR) == (
        False,
        False,
    )
