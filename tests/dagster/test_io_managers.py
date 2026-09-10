"""The two storage seams, exercised through Dagster rather than by calling their methods.

A round-trip of `dump_to_path`/`load_from_path` would prove serialisation and nothing about what
actually breaks: which PATH a partition lands on, and whether a downstream asset reading the same
partition key gets those rows back. So the parquet tests materialise a real two-asset graph and
assert on what the DOWNSTREAM asset was handed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dagster as dg
import pytest

from muffin_ingest_dagster.io_managers import ParquetIOManager, PostgresIOManager, _updatable

DAY = dg.DailyPartitionsDefinition(start_date="2026-09-01")


def through_the_seam(
    tmp_path: Path, rows: list[dict[str, Any]], key: str = "2026-09-02"
) -> list[dict[str, Any]]:
    """Write `rows` from one partitioned asset and return what the next one loaded."""
    seen: list[dict[str, Any]] = []

    @dg.asset(partitions_def=DAY, io_manager_key="parquet_io", name="raw_thing")
    def raw_thing() -> list[dict[str, Any]]:
        return rows

    @dg.asset(partitions_def=DAY, name="downstream")
    def downstream(raw_thing: list[dict[str, Any]]) -> None:
        seen.extend(raw_thing)

    dg.materialize(
        [raw_thing, downstream],
        partition_key=key,
        resources={"parquet_io": ParquetIOManager(base_path=str(tmp_path))},
    )
    return seen


def test_a_partition_written_by_one_asset_is_read_by_the_next(tmp_path: Path) -> None:
    rows = [{"symbol": "AAPL", "close": 230.5}, {"symbol": "MSFT", "close": 410.0}]
    assert sorted(r["symbol"] for r in through_the_seam(tmp_path, rows)) == ["AAPL", "MSFT"]


def test_an_empty_partition_is_written_rather_than_skipped(tmp_path: Path) -> None:
    """A market holiday produces no bars. "We collected that day and there was nothing" and "we
    never collected that day" must not look the same on disk — that distinction is the entire
    reason the cross-section is partitioned by date."""
    assert through_the_seam(tmp_path, []) == []
    assert len(list(tmp_path.rglob("*.parquet"))) == 1, "the empty partition still has a file"


def test_two_partitions_do_not_share_a_file(tmp_path: Path) -> None:
    through_the_seam(tmp_path, [{"symbol": "AAPL", "close": 1.0}], key="2026-09-02")
    through_the_seam(tmp_path, [{"symbol": "AAPL", "close": 2.0}], key="2026-09-03")
    assert len(list(tmp_path.rglob("*.parquet"))) == 2


def test_re_materialising_replaces_rather_than_appends(tmp_path: Path) -> None:
    """Appending would make a re-run silently double the data — the upsert-cannot-retract failure
    moved to the filesystem."""
    through_the_seam(tmp_path, [{"symbol": "AAPL", "close": 1.0}])
    again = through_the_seam(tmp_path, [{"symbol": "AAPL", "close": 2.0}])
    assert again == [{"symbol": "AAPL", "close": 2.0}]


def test_a_row_missing_a_column_another_row_has_round_trips_as_null(tmp_path: Path) -> None:
    """Provider rows are not uniform — a dividend rides on the bar that has one and no other. The
    columns are the UNION, so a sparse field must not shift a row's values into the wrong column."""
    rows = [{"symbol": "A", "close": 1.0, "dividend": 0.5}, {"symbol": "B", "close": 2.0}]
    seen = {r["symbol"]: r for r in through_the_seam(tmp_path, rows)}
    assert seen["A"]["dividend"] == 0.5
    assert seen["B"]["dividend"] is None
    assert seen["B"]["close"] == 2.0


# --- the core seam -----------------------------------------------------------------------------


def test_the_core_manager_refuses_an_asset_that_does_not_say_where_its_rows_go() -> None:
    """Guessing the table or the conflict key is how a write lands in the wrong place, or collides
    on the wrong column and quietly overwrites. Refusing names the asset."""

    @dg.asset(io_manager_key="postgres_io", name="undeclared")
    def undeclared() -> list[dict[str, Any]]:
        return [{"a": 1}]

    with pytest.raises(Exception) as caught:
        dg.materialize([undeclared], resources={"postgres_io": PostgresIOManager()})
    assert "declares no" in str(caught.value) or "declares no" in str(caught.value.__cause__)


def test_the_updatable_columns_are_derived_from_the_rows() -> None:
    """Declared by hand they drift: a facet that starts producing a new column would keep writing
    the old value on conflict, silently. The same reason a hand-maintained retraction list let a
    tenth column go unlisted for weeks."""
    rows = [{"a": 1, "b": 2}, {"a": 3, "c": 4}]
    assert _updatable(rows, ["a"]) == ["b", "c"]


def test_every_column_being_part_of_the_key_leaves_nothing_to_update() -> None:
    """`writers.upsert` then raises rather than emitting a DO NOTHING nobody asked for."""
    assert _updatable([{"a": 1, "b": 2}], ["a", "b"]) == []
