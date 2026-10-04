"""The symbol map's Dagster side: a thin caller of `market.refresh_symbol_map()`, and when it fires.

The refresh itself is SQL, tested against real Postgres in muffin-deployment
(`stack/supabase/tests/the-symbol-map-follows-its-writers.sql`). What this side owns is that the
function's report reaches the materialization, and that the asset fires after each of the three
assets that write the map's inputs, while their partitions are, as always, partly unmaterialised.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar

import dagster as dg
import pytest

from muffin_ingest_dagster.lib.resources import Postgres

#: What `market.refresh_symbol_map()` answered on production, 2026-10-04.
REPORT: dict[str, int] = {"rows": 12402, "duration_ms": 312}

MIDDAY = (datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC), datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC))
#: Either side of the floor's 05:47 tick.
ACROSS_THE_FLOOR = (
    datetime(2026, 10, 5, 5, 45, tzinfo=UTC),
    datetime(2026, 10, 5, 5, 50, tzinfo=UTC),
)


class _Cursor:
    def __init__(self, db: type[FakePostgres]) -> None:
        self._db = db
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        self._db.executed.append(text)
        if text != "select market.refresh_symbol_map()":
            raise AssertionError(f"unexpected query: {text[:120]}")
        # psycopg decodes a jsonb result to a dict.
        self._rows = [(dict(self._db.report),)]

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, db: type[FakePostgres]) -> None:
        self._db = db

    def cursor(self) -> _Cursor:
        return _Cursor(self._db)


class FakePostgres(Postgres):
    executed: ClassVar[list[str]] = []
    report: ClassVar[dict[str, int]] = {}

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield _Conn(type(self))


def test_the_asset_refreshes_the_map_and_records_its_size_and_duration() -> None:
    """ONE STATEMENT, AND ITS REPORT IS THE MATERIALIZATION: the duration is what shows a refresh
    walking toward a ceiling before it hits one."""
    from muffin_ingest_dagster.defs.discovery.derived import symbol_security

    FakePostgres.executed = []
    FakePostgres.report = dict(REPORT)
    result = dg.materialize([symbol_security], resources={"postgres": FakePostgres()})

    assert result.success
    assert FakePostgres.executed == ["select market.refresh_symbol_map()"]
    events = result.asset_materializations_for_node("symbol_security")
    assert len(events) == 1
    assert {k: v.value for k, v in events[0].metadata.items()} == REPORT


def _requested_before_and_after(
    asset: dg.AssetsDefinition,
    *,
    landed: tuple[str, str | None] | None,
    times: tuple[datetime, datetime] = MIDDAY,
) -> tuple[bool, bool]:
    """Evaluate `asset`'s condition over the real graph at two times, with `landed` — an upstream
    asset key and partition — materialising between them, or nothing.

    The graph keeps every partitioned upstream incomplete, as production's always are: a venue
    never swept, a filing never fetched, a subject whose rungs never ran.
    """
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import derived as discovery_derived
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw
    from muffin_ingest_dagster.defs.discovery.partitions import NPORT_PARTITIONS, SWEEP_PARTITIONS
    from muffin_ingest_dagster.defs.symbology import core as symbology_core
    from muffin_ingest_dagster.defs.symbology import raw as symbology_raw
    from muffin_ingest_dagster.defs.symbology.partitions import SYMBOLOGY_PARTITIONS
    from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore

    # Resources only so the definitions validate; nothing is executed.
    defs = dg.Definitions(
        assets=[
            discovery_raw.raw_exchange_sweep,
            discovery_core.venue_listing,
            discovery_derived.venue_listing_absence,
            discovery_raw.raw_nport_filing,
            discovery_core.discovered_security,
            symbology_raw.raw_figi_ticker,
            symbology_raw.raw_figi_local_symbol,
            symbology_raw.raw_yahoo_symbol,
            symbology_core.security_symbology,
            discovery_derived.security_listing,
            asset,
        ],
        resources={
            "postgres": Postgres(),
            "parquet_io": ParquetIOManager("/nonexistent"),
            "postgres_io": ParquetIOManager("/nonexistent"),
            "raw_store": RawStore(base_path="/nonexistent"),
        },
    )
    only = dg.AssetSelection.assets("symbol_security")
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(SWEEP_PARTITIONS, ["AU.common", "LN.common"])
        instance.add_dynamic_partitions(NPORT_PARTITIONS, ["1:0001", "2:0002"])
        instance.add_dynamic_partitions(
            SYMBOLOGY_PARTITIONS,
            ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"],
        )
        # Refreshed once already, so a request below can only come from what lands.
        instance.report_runless_asset_event(dg.AssetMaterialization("symbol_security"))
        first = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, asset_selection=only, evaluation_time=times[0]
        )
        if landed is not None:
            key, partition = landed
            instance.report_runless_asset_event(dg.AssetMaterialization(key, partition=partition))
        second = dg.evaluate_automation_conditions(
            defs=defs,
            instance=instance,
            asset_selection=only,
            cursor=first.cursor,
            evaluation_time=times[1],
        )

    def requested(result: Any) -> bool:
        return any(
            r.key == dg.AssetKey("symbol_security") and r.true_subset.size > 0
            for r in result.results
        )

    return requested(first), requested(second)


@pytest.mark.parametrize(
    "landed",
    [
        ("security_symbology", "11111111-1111-1111-1111-111111111111"),
        ("discovered_security", "1:0001"),
        ("security_listing", None),
    ],
    ids=["a-symbol-adopted", "a-security-minted", "a-primary-moved"],
)
def test_the_map_is_rebuilt_after_each_writer_of_its_inputs(landed: tuple[str, str | None]) -> None:
    """EACH OF THE THREE WRITERS, while the others hold unmaterialised partitions. The control is
    plain `eager()`, which waits for every upstream partition and so never fires here: the state
    `security_return` sat in through a whole history load."""
    from muffin_ingest_dagster.defs.discovery.derived import symbol_security

    condition = next(iter(symbol_security.automation_conditions_by_key.values()), None)
    assert condition is not None and condition.is_serializable, "the daemon must be able to read it"

    assert _requested_before_and_after(symbol_security, landed=landed) == (False, True)

    plain = dg.map_asset_specs(
        lambda spec: spec.replace_attributes(automation_condition=dg.AutomationCondition.eager()),
        [symbol_security],
    )[0]
    assert isinstance(plain, dg.AssetsDefinition)
    assert _requested_before_and_after(plain, landed=landed) == (False, False), (
        "plain eager() fired too, so this test no longer shows why the gate was removed"
    )


def test_a_daily_floor_refreshes_with_no_upstream_change() -> None:
    """A SAFETY NET for a writer nobody listed: the Track button refreshes the map itself, and the
    three assets are upstreams. The control without the floor proves the tick is what fires."""
    from muffin_ingest_dagster.defs.discovery.derived import symbol_security

    assert _requested_before_and_after(symbol_security, landed=None, times=ACROSS_THE_FLOOR) == (
        False,
        True,
    )
    assert _requested_before_and_after(symbol_security, landed=None) == (False, False)

    without_floor = dg.map_asset_specs(
        lambda spec: spec.replace_attributes(
            automation_condition=dg.AutomationCondition.eager().without(
                ~dg.AutomationCondition.any_deps_missing()
            )
        ),
        [symbol_security],
    )[0]
    assert isinstance(without_floor, dg.AssetsDefinition)
    assert _requested_before_and_after(without_floor, landed=None, times=ACROSS_THE_FLOOR) == (
        False,
        False,
    )
