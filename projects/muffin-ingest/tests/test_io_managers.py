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

from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, PostgresIOManager, _updatable
from muffin_ingest_dagster.lib.partitioned import Complete
from muffin_ingest_dagster.lib.resources import Postgres

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

    # A REAL `Postgres` IS SAFE HERE AND THE POINT OF THE TEST. The refusal must happen BEFORE a
    # connection is opened — an asset that cannot say where its rows go should cost nothing — so
    # if this ever starts needing a fake, the check has moved to the wrong side of the connect.
    with pytest.raises(Exception) as caught:
        dg.materialize(
            [undeclared], resources={"postgres_io": PostgresIOManager(postgres=Postgres())}
        )
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


# ── merging partitions: extend what is stored rather than replace it ───────────────────────────


def merging_seam(
    tmp_path: Path,
    runs: list[list[dict[str, Any]]],
    merge_on: list[str],
    key: str = "2026-09-02",
) -> list[dict[str, Any]]:
    """Materialise ONE merging asset once per entry in `runs`, and return what downstream last saw.

    The same asset twice, not two assets: a merging partition is only interesting across
    re-materialisations of itself, which is exactly what a lane extending a subject does.
    """
    seen: list[dict[str, Any]] = []
    fetched: list[list[dict[str, Any]]] = list(runs)

    @dg.asset(
        partitions_def=DAY,
        io_manager_key="parquet_io",
        name="raw_thing",
        metadata={"merge_on": merge_on},
    )
    def raw_thing() -> list[dict[str, Any]]:
        return fetched.pop(0)

    @dg.asset(partitions_def=DAY, name="downstream")
    def downstream(raw_thing: list[dict[str, Any]]) -> None:
        seen.clear()
        seen.extend(raw_thing)

    manager = ParquetIOManager(base_path=str(tmp_path))
    for _ in runs:
        dg.materialize(
            [raw_thing, downstream], partition_key=key, resources={"parquet_io": manager}
        )
    return seen


def test_a_merging_partition_keeps_what_an_earlier_run_stored(tmp_path: Path) -> None:
    """The whole point: a run that fetched only the extension must not lose the history.

    Without the merge this returns the second run's one row, which is a lane re-asking for ten
    years to gain a day, or a resumed night deleting the half that succeeded.
    """
    seen = merging_seam(
        tmp_path,
        [
            [{"symbol": "AAPL", "date": "2026-09-01", "close": 1.0}],
            [{"symbol": "AAPL", "date": "2026-09-02", "close": 2.0}],
        ],
        merge_on=["symbol", "date"],
    )
    assert sorted(r["date"] for r in seen) == ["2026-09-01", "2026-09-02"]


def test_a_re_asked_row_is_superseded_rather_than_duplicated(tmp_path: Path) -> None:
    """Newest wins per key, and "newest" is this run — a re-ask is the provider restating itself."""
    seen = merging_seam(
        tmp_path,
        [
            [{"symbol": "AAPL", "date": "2026-09-01", "close": 1.0}],
            [{"symbol": "AAPL", "date": "2026-09-01", "close": 9.0}],
        ],
        merge_on=["symbol", "date"],
    )
    assert seen == [{"symbol": "AAPL", "date": "2026-09-01", "close": 9.0}]


def test_an_empty_fetch_keeps_the_history_rather_than_erasing_it(tmp_path: Path) -> None:
    """For a MERGING asset an empty answer means "no extension", never "there is nothing".

    A replacing asset writes the zero-row marker here, deliberately, to record that it collected
    and found nothing. Doing that on a merging partition would delete the history to record a
    market holiday.
    """
    seen = merging_seam(
        tmp_path,
        [[{"symbol": "AAPL", "date": "2026-09-01", "close": 1.0}], []],
        merge_on=["symbol", "date"],
    )
    assert seen == [{"symbol": "AAPL", "date": "2026-09-01", "close": 1.0}]


def test_a_complete_answer_replaces_a_stored_shape_that_predates_the_key(tmp_path: Path) -> None:
    """The merge cannot converge on rows it cannot key, and keeping them for ever is not a fix.

    THIS IS THE PRODUCTION CASE, measured 2026-09-20. Every `raw_price_history` partition was
    written before raw stopped adding `trade_date`, so none carried the `date` the key names. The
    watermark then read nothing, the asset fetched the subject's WHOLE history, and the merge kept
    all 4,496 unkeyable stored rows beside the 4,502 just fetched: the same history twice, in a
    file that would have doubled for each of 12,016 partitions.

    Without `Complete` this returns SIX rows — the three stored and the three fetched.
    """
    seen = merging_seam(
        tmp_path,
        [
            [{"symbol": "AAPL", "trade_date": f"2026-09-0{n}", "close": float(n)} for n in (1, 2)],
            Complete(
                [{"symbol": "AAPL", "date": f"2026-09-0{n}", "close": float(n)} for n in (1, 2, 3)]
            ),
        ],
        merge_on=["symbol", "date"],
    )
    assert sorted(r["date"] for r in seen) == ["2026-09-01", "2026-09-02", "2026-09-03"]
    assert not [r for r in seen if r.get("date") is None], "a superseded shape survived"


def test_an_empty_complete_answer_replaces_nothing(tmp_path: Path) -> None:
    """A dead symbol, a refusal and a market holiday all return no rows.

    Letting an empty claim replace would delete a stored history to record a quiet day — the very
    failure the merge exists to prevent, reached through the mechanism that bypasses it. The rule
    lives in the manager, not in each caller, because the callers are the ones who forget.
    """
    seen = merging_seam(
        tmp_path,
        [[{"symbol": "AAPL", "date": "2026-09-01", "close": 1.0}], Complete([])],
        merge_on=["symbol", "date"],
    )
    assert seen == [{"symbol": "AAPL", "date": "2026-09-01", "close": 1.0}]


def test_a_complete_answer_without_merge_on_is_an_ordinary_replacement(tmp_path: Path) -> None:
    """The marker only means anything to a merging asset; it must not start a new behaviour of its
    own on a replacing one, which already replaces."""
    seen = merging_seam(
        tmp_path,
        [
            [{"symbol": "AAPL", "date": "2026-09-01", "close": 1.0}],
            Complete([{"symbol": "AAPL", "date": "2026-09-02", "close": 2.0}]),
        ],
        merge_on=[],
    )
    assert seen == [{"symbol": "AAPL", "date": "2026-09-02", "close": 2.0}]


def test_merging_on_a_column_the_rows_do_not_carry_is_refused(tmp_path: Path) -> None:
    """`row.get` returns None for a missing column, so an unkeyable set collapses onto one key and
    one row survives — silently, on data that is perfectly good. Refuse rather than drop."""
    with pytest.raises(dg.DagsterInvariantViolationError, match="do not carry"):
        merging_seam(
            tmp_path,
            [
                [{"symbol": "AAPL", "date": "2026-09-01"}],
                [{"symbol": "AAPL", "close": 2.0}],
            ],
            merge_on=["symbol", "date"],
        )


def test_an_asset_without_merge_on_still_replaces(tmp_path: Path) -> None:
    """The default is unchanged, so no existing lane silently starts accumulating."""
    assert through_the_seam(tmp_path, [{"symbol": "AAPL", "n": 1}]) == [{"symbol": "AAPL", "n": 1}]
    assert through_the_seam(tmp_path, [{"symbol": "MSFT", "n": 2}]) == [{"symbol": "MSFT", "n": 2}]
