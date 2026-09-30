"""The listing derivation's Dagster side: a thin caller, the check that gates the swap, and when it
fires.

The rules themselves are SQL, tested against real Postgres in muffin-deployment
(`stack/supabase/tests/a-listing-is-derived-from-the-directory.sql`) and mutation-proven there on
2026-09-30. What this side owns is that the function's counts reach the materialization, that the
legacy-coverage check fails on exactly the state Stage 2c would break, and that the asset fires at
all while its upstreams hold unmaterialised partitions.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar

import dagster as dg

from muffin_ingest_dagster.lib.resources import Postgres

#: What `market.derive_security_listing()` returned on production, 2026-09-26, rolled back.
COUNTS: dict[str, int] = {
    "lines": 33859,
    "securities": 11476,
    "inserted": 33859,
    "changed": 0,
    "retracted": 0,
    "primary_by_symbol": 10227,
    "primary_by_legacy_venue": 972,
    "primary_by_home_venue": 16,
    "without_primary": 261,
    "primaries_demoted": 0,
    "primaries_promoted": 11215,
    "with_currency": 10626,
}


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
        if text == "select market.derive_security_listing()":
            # psycopg decodes a jsonb result to a dict.
            self._rows = [(dict(self._db.counts),)]
        elif "from market.listing l where l.is_primary" in text:
            self._rows = list(self._db.legacy_rows)
        else:
            raise AssertionError(f"unexpected query: {text[:120]}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, db: type[FakePostgres]) -> None:
        self._db = db

    def cursor(self) -> _Cursor:
        return _Cursor(self._db)


class FakePostgres(Postgres):
    executed: ClassVar[list[str]] = []
    counts: ClassVar[dict[str, int]] = {}
    #: (legacy venue, same venue, other venue, lost, legacy only) — the check's query, per venue.
    legacy_rows: ClassVar[list[tuple[Any, ...]]] = []

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield _Conn(type(self))


def test_the_asset_calls_the_derivation_and_records_what_it_did() -> None:
    """ONE STATEMENT, AND ITS COUNTS ARE THE MATERIALIZATION. The rules live in the database, so a
    run that recorded nothing would be a derivation nobody can read the outcome of."""
    from muffin_ingest_dagster.defs.discovery.derived import security_listing

    FakePostgres.executed = []
    FakePostgres.counts = dict(COUNTS)
    result = dg.materialize([security_listing], resources={"postgres": FakePostgres()})

    assert result.success
    assert FakePostgres.executed == ["select market.derive_security_listing()"]
    events = result.asset_materializations_for_node("security_listing")
    assert len(events) == 1
    recorded = {k: v.value for k, v in events[0].metadata.items()}
    assert recorded == COUNTS


def _check(rows: list[tuple[Any, ...]]) -> dg.AssetCheckResult:
    from muffin_ingest_dagster.defs.discovery.checks import listing_covers_legacy

    FakePostgres.executed = []
    FakePostgres.legacy_rows = rows
    result = listing_covers_legacy(FakePostgres())
    assert isinstance(result, dg.AssetCheckResult)
    return result


def test_the_check_fails_on_a_security_that_would_lose_its_primary_and_names_the_venue() -> None:
    """LOST IS THE ONE STATE THE SWAP BREAKS: the security has derived lines and none became
    primary, so once `market.listing` is a view over `security_listing` its display symbol and
    currency go. The production shape was 177 of 182 on the US venue, past the directory's cap."""
    result = _check([("US", 900, 5, 177, 30), ("AT", 100, 2, 0, 0), ("HK", 50, 0, 5, 0)])

    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert result.metadata["lost"].value == 182
    assert result.metadata["lost_by_legacy_venue"].value == "US 177, HK 5"
    assert result.metadata["legacy_primaries"].value == 1269


def test_another_venue_or_no_line_at_all_does_not_fail_it() -> None:
    """THE OTHER TWO STATES ARE NOT THE SWAP'S PROBLEM, and failing on them would make the gate
    unpassable. A primary on another venue is the held symbol naming a sibling line (AT and AU both
    spell `.AX`); a security with no derived line at all keeps its legacy row through 2c."""
    result = _check([("US", 900, 54, 0, 899)])

    assert result.passed is True
    assert result.metadata["other_venue"].value == 54
    assert result.metadata["legacy_only"].value == 899
    assert result.metadata["lost_by_legacy_venue"].value == "none"


#: Two evaluations 30 s apart at midday: no cron tick of the daily floor falls between them, so
#: nothing but an upstream update can make the second one request. Pinned rather than "now", or a
#: run of the suite across 05:43 UTC would request for the floor's reason.
MIDDAY = (datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC), datetime(2026, 9, 30, 12, 0, 30, tzinfo=UTC))
#: The same, either side of the floor's 05:43 tick.
ACROSS_THE_FLOOR = (
    datetime(2026, 9, 30, 5, 40, tzinfo=UTC),
    datetime(2026, 9, 30, 5, 46, tzinfo=UTC),
)


def _requested_before_and_after(
    listing: dg.AssetsDefinition,
    *,
    update: bool = True,
    times: tuple[datetime, datetime] = MIDDAY,
) -> tuple[bool, bool]:
    """Evaluate `listing`'s condition over the real graph slice at two times, optionally with one
    venue landing between them.

    The slice keeps both upstreams incomplete, as production's are: a second venue that was never
    swept, and a symbology subject whose rungs never ran.
    """
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw
    from muffin_ingest_dagster.defs.discovery.partitions import SWEEP_PARTITIONS
    from muffin_ingest_dagster.defs.symbology import core as symbology_core
    from muffin_ingest_dagster.defs.symbology import raw as symbology_raw
    from muffin_ingest_dagster.defs.symbology.partitions import SYMBOLOGY_PARTITIONS
    from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore

    # Resources only so the definitions validate; nothing is executed.
    defs = dg.Definitions(
        assets=[
            discovery_raw.raw_exchange_sweep,
            discovery_core.venue_listing,
            symbology_raw.raw_figi_ticker,
            symbology_raw.raw_figi_local_symbol,
            symbology_raw.raw_yahoo_symbol,
            symbology_core.security_symbology,
            listing,
        ],
        resources={
            "postgres": Postgres(),
            "parquet_io": ParquetIOManager("/nonexistent"),
            "postgres_io": ParquetIOManager("/nonexistent"),
            "raw_store": RawStore(base_path="/nonexistent"),
        },
    )
    only = dg.AssetSelection.assets("security_listing")
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(SWEEP_PARTITIONS, ["AU", "LN"])
        instance.add_dynamic_partitions(
            SYMBOLOGY_PARTITIONS, ["11111111-1111-1111-1111-111111111111"]
        )
        # Derived once already, so the request below can only come from the upstream update.
        instance.report_runless_asset_event(dg.AssetMaterialization("security_listing"))
        first = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, asset_selection=only, evaluation_time=times[0]
        )
        if update:
            instance.report_runless_asset_event(
                dg.AssetMaterialization("venue_listing", partition="AU")
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
            r.key == dg.AssetKey("security_listing") and r.true_subset.size > 0
            for r in result.results
        )

    return requested(first), requested(second)


def test_the_derivation_fires_when_a_venue_lands_while_other_partitions_are_missing() -> None:
    """AN UNPARTITIONED ASSET DEPENDS ON EVERY UPSTREAM PARTITION, and both of these upstreams
    always have some unmaterialised: a venue enabled but not yet swept, a subject whose rungs have
    not run. Plain `eager()` waits for all of them, which is the state `security_return` sat in
    through a whole history load. The control makes the two rules disagree on the same graph."""
    from muffin_ingest_dagster.defs.discovery.derived import security_listing

    condition = next(iter(security_listing.automation_conditions_by_key.values()), None)
    assert condition is not None and condition.is_serializable, "the daemon must be able to read it"

    # NOTHING BEFORE THE UPDATE, so the request after it can only be the update's doing.
    assert _requested_before_and_after(security_listing) == (False, True)

    plain = dg.map_asset_specs(
        lambda spec: spec.replace_attributes(automation_condition=dg.AutomationCondition.eager()),
        [security_listing],
    )[0]
    assert isinstance(plain, dg.AssetsDefinition)
    assert _requested_before_and_after(plain) == (False, False), (
        "plain eager() fired too, so this test no longer shows why the gate was removed"
    )


def test_a_daily_floor_derives_with_no_upstream_change() -> None:
    """NOT EVERY WRITER OF THE INPUTS IS AN ASSET. `promote_listing` (the Track button) writes a
    share class outside Dagster, so without a floor a tracked security's listings would wait for
    some unrelated upstream change. The control without the floor proves the tick is what fires."""
    from muffin_ingest_dagster.defs.discovery.derived import security_listing

    assert _requested_before_and_after(security_listing, update=False, times=ACROSS_THE_FLOOR) == (
        False,
        True,
    )
    assert _requested_before_and_after(security_listing, update=False) == (False, False)

    without_floor = dg.map_asset_specs(
        lambda spec: spec.replace_attributes(
            automation_condition=dg.AutomationCondition.eager().without(
                ~dg.AutomationCondition.any_deps_missing()
            )
        ),
        [security_listing],
    )[0]
    assert isinstance(without_floor, dg.AssetsDefinition)
    assert _requested_before_and_after(without_floor, update=False, times=ACROSS_THE_FLOOR) == (
        False,
        False,
    )
