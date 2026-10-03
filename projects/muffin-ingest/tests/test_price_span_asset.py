"""The price span's Dagster side: which securities a run asks about, and when it fires.

The rule itself is SQL, tested against real Postgres in muffin-deployment
(`stack/supabase/tests/a-price-span-follows-the-bars.sql`, mutation-proven on 2026-10-03). What
this side owns is the cursor: a run asks for exactly the history partitions materialised since the
last run, every one of them on the first, and nothing when nothing is new.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar

import dagster as dg
import pytest

from muffin_ingest_dagster.lib.resources import Postgres

A = "11111111-1111-1111-1111-111111111111"
B = "22222222-2222-2222-2222-222222222222"
C = "33333333-3333-3333-3333-333333333333"


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
        if text != "select market.derive_security_price_span(%s::uuid[])":
            raise AssertionError(f"unexpected query: {text[:120]}")
        chunk = list(params[0])
        self._db.calls.append(chunk)
        # psycopg decodes a jsonb result to a dict.
        self._row = ({"asked": len(chunk), "written": len(chunk), "without_bars": 0},)

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
    calls: ClassVar[list[list[str]]] = []
    commits: ClassVar[int] = 0

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield _Conn(type(self))


def _history_landed(instance: dg.DagsterInstance, *securities: str) -> None:
    for security in securities:
        instance.report_runless_asset_event(
            dg.AssetMaterialization("price_bar_history", partition=security)
        )


def _run(instance: dg.DagsterInstance) -> dict[str, Any]:
    from muffin_ingest_dagster.defs.prices.derived import security_price_span

    FakePostgres.calls = []
    FakePostgres.commits = 0
    result = dg.materialize(
        [security_price_span], instance=instance, resources={"postgres": FakePostgres()}
    )
    assert result.success
    events = result.asset_materializations_for_node("security_price_span")
    assert len(events) == 1
    return {k: v.value for k, v in events[0].metadata.items()}


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION

    with dg.instance_for_test() as inst:
        inst.add_dynamic_partitions(SECURITY_PARTITION, [A, B, C])
        yield inst


def test_the_first_run_asks_for_every_materialised_history_partition_in_committed_chunks(
    instance: dg.DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE BOOTSTRAP. With no earlier run there is no watermark, so the run asks for every security
    the history lane has reached, and only those: C is in the grid and was never fetched, so it
    gets no row, which is the difference between "not reached" and "reached, holds nothing"."""
    from muffin_ingest_dagster.defs.prices import derived

    monkeypatch.setattr(derived, "SPAN_CHUNK", 1)
    _history_landed(instance, A, B)

    recorded = _run(instance)

    assert sorted(s for chunk in FakePostgres.calls for s in chunk) == [A, B]
    assert all(len(chunk) == 1 for chunk in FakePostgres.calls), "the chunk size was not applied"
    assert FakePostgres.commits == 2, (
        "each chunk must commit, or a late failure loses the bootstrap"
    )
    assert recorded["mode"] == "bootstrap"
    assert recorded["securities"] == 2 and recorded["asked"] == 2
    latest = instance.fetch_materializations(dg.AssetKey("price_bar_history"), limit=1)
    assert recorded["watermark_storage_id"] == latest.records[0].storage_id


def test_a_later_run_asks_only_for_what_landed_since(instance: dg.DagsterInstance) -> None:
    """THE CURSOR IS THE PREVIOUS RUN'S WATERMARK. A night touches ~2,500 of ~12,000 securities, and
    re-deriving the rest costs minutes for nothing. B landing twice is asked once."""
    _history_landed(instance, A, B)
    first = _run(instance)

    _history_landed(instance, B, C, B)
    second = _run(instance)

    assert FakePostgres.calls == [[B, C]]
    assert second["mode"] == "incremental"
    assert second["securities"] == 2
    assert second["watermark_storage_id"] > first["watermark_storage_id"]


def test_a_run_with_nothing_new_asks_nothing(instance: dg.DagsterInstance) -> None:
    """AND KEEPS ITS WATERMARK, or the next run would fall back to a bootstrap."""
    _history_landed(instance, A)
    first = _run(instance)

    second = _run(instance)

    assert FakePostgres.calls == []
    assert second["mode"] == "incremental" and second["securities"] == 0
    assert second["watermark_storage_id"] == first["watermark_storage_id"]


#: Two evaluations 30 s apart. The condition has no cron floor, so the time only has to be fixed.
TIMES = (datetime(2026, 10, 3, 12, 0, 0, tzinfo=UTC), datetime(2026, 10, 3, 12, 0, 30, tzinfo=UTC))


def _requested_before_and_after(span: dg.AssetsDefinition) -> tuple[bool, bool]:
    """Evaluate `span`'s condition at two times, with one history partition landing between them,
    while another partition of the lane stays unfetched — as production's always does."""
    from muffin_ingest_dagster.defs.prices import core as prices_core
    from muffin_ingest_dagster.defs.prices import raw as prices_raw
    from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION
    from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore

    # Resources only so the definitions validate; nothing is executed.
    defs = dg.Definitions(
        assets=[prices_raw.raw_price_history, prices_core.price_bar_history, span],
        resources={
            "postgres": Postgres(),
            "parquet_io": ParquetIOManager("/nonexistent"),
            "postgres_io": ParquetIOManager("/nonexistent"),
            "raw_store": RawStore(base_path="/nonexistent"),
        },
    )
    only = dg.AssetSelection.assets("security_price_span")
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(SECURITY_PARTITION, [A, B])
        # Derived once already, so a request below can only come from the history landing.
        instance.report_runless_asset_event(dg.AssetMaterialization("security_price_span"))
        first = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, asset_selection=only, evaluation_time=TIMES[0]
        )
        _history_landed(instance, A)
        second = dg.evaluate_automation_conditions(
            defs=defs,
            instance=instance,
            asset_selection=only,
            cursor=first.cursor,
            evaluation_time=TIMES[1],
        )

    def requested(result: Any) -> bool:
        return any(
            r.key == dg.AssetKey("security_price_span") and r.true_subset.size > 0
            for r in result.results
        )

    return requested(first), requested(second)


def test_the_span_fires_when_history_lands_while_other_partitions_are_missing() -> None:
    """The history lane always holds unfetched `security` keys, so plain `eager()` would never fire.
    The control makes the two rules disagree on the same graph."""
    from muffin_ingest_dagster.defs.prices.derived import security_price_span

    condition = next(iter(security_price_span.automation_conditions_by_key.values()), None)
    assert condition is not None and condition.is_serializable, "the daemon must be able to read it"

    assert _requested_before_and_after(security_price_span) == (False, True)

    plain = dg.map_asset_specs(
        lambda spec: spec.replace_attributes(automation_condition=dg.AutomationCondition.eager()),
        [security_price_span],
    )[0]
    assert isinstance(plain, dg.AssetsDefinition)
    assert _requested_before_and_after(plain) == (False, False), (
        "plain eager() fired too, so this test no longer shows why the gate was removed"
    )
