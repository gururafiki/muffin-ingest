"""The two storage seams, exercised through Dagster rather than by calling their methods.

A round-trip of `dump_to_path`/`load_from_path` would prove serialisation and nothing about what
actually breaks: which PATH a partition lands on, and whether a downstream asset reading the same
partition key gets those rows back. So the parquet tests materialise a real two-asset graph and
assert on what the DOWNSTREAM asset was handed.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import dagster as dg
import pytest

from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, PostgresIOManager, _updatable
from muffin_ingest_dagster.lib.partitioned import Complete, Unchanged
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
    metadata: dict[str, Any] | None = None,
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
        metadata={"merge_on": merge_on, **(metadata or {})},
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


# --- A RANGE RUN, WHICH IS THE ONE SHAPE EVERY OTHER TEST HERE CANNOT REACH -----------------


def test_a_range_run_over_merging_partitions_writes_them_all(tmp_path: Path) -> None:
    """`add_output_metadata` MAY BE CALLED ONCE PER OUTPUT, and a range writes many partitions.

    `dump_to_path` runs once per partition and each call was emitting the same seven merge keys,
    so the SECOND partition to take a metadata-emitting path raised

        DagsterInvalidMetadata: Tried to add metadata for key(s) that already have metadata:
        {'merged_on', 'rows_fetched', 'rows_kept', 'rows_superseded', 'rows_unkeyable',
         'rows_total', 'replaced_because'}

    Every other test in this file materialises ONE partition, and a single-partition run is the
    only shape that cannot reach it — so the suite was green while, measured 2026-09-22, all 66 of
    `nightly_prices`' bounded runs failed this way and `market.price_bar` published nothing for
    four days.

    IT TAKES TWO PASSES, which is the part a smaller fixture would miss. On a FIRST write there is
    nothing stored, so `_merge_with_stored` returns early and records nothing — only one partition
    (the `Complete` one) emits, and one emit never collides. The shape that broke production is the
    SECOND night: every partition in the range has history, so every one of them emits.
    """
    manager = ParquetIOManager(base_path=str(tmp_path))
    keys = ["2026-09-02", "2026-09-03", "2026-09-04"]
    mode: dict[str, str] = {"how": "complete"}

    @dg.asset(
        partitions_def=DAY,
        io_manager_key="parquet_io",
        name="raw_thing",
        backfill_policy=dg.BackfillPolicy.single_run(),
        metadata={"merge_on": ["symbol", "date"]},
    )
    # UNANNOTATED ON PURPOSE: this module is `from __future__ import annotations` and the asset is
    # defined inside a function, so Dagster cannot resolve a stringified `AssetExecutionContext`
    # from the local scope. Blank is explicitly allowed.
    def raw_thing(context) -> dict[str, Any]:  # type: ignore[no-untyped-def]
        rows = {k: [{"symbol": "A", "date": k, "close": 1.0}] for k in context.partition_keys}
        if mode["how"] == "complete":
            return {k: Complete(v) for k, v in rows.items()}
        # The extension a nightly sweep fetches: a NEW bar per subject, beside stored history.
        return {
            k: [{"symbol": "A", "date": k, "close": 2.0}, {"symbol": "B", "date": k, "close": 9.0}]
            for k in rows
        }

    def run() -> Any:
        return dg.materialize(
            [raw_thing],
            resources={"parquet_io": manager},
            # A RANGE IS DRIVEN BY THE TWO TAGS, which is also how a schedule asks for one —
            # `materialize()` has no `partition_key_range` parameter in 1.13.
            tags={
                "dagster/asset_partition_range_start": keys[0],
                "dagster/asset_partition_range_end": keys[-1],
            },
        )

    assert run().success, "the first range run populates the partitions"
    mode["how"] = "extend"
    result = run()
    assert result.success, "a range run where EVERY partition merges must not fail at the write"

    for k in keys:
        assert (tmp_path / "raw_thing" / f"{k}.parquet").exists(), k

    meta = result.asset_materializations_for_node("raw_thing")[0].metadata
    assert meta["partitions_written"].value == 3
    # THE COUNTS ARE SUMS ACROSS THE RANGE, and a summary that reported only the last partition's
    # numbers would read as a third of the work with nothing to indicate it.
    assert meta["partitions_merged"].value == 3
    assert meta["partitions_replaced"].value == 0
    assert meta["rows_fetched"].value == 6, "two fresh rows per partition, three partitions"
    assert meta["rows_superseded"].value == 3, "each partition's stored A row was re-asked"
    assert meta["rows_total"].value == 6


def test_a_caller_can_ask_for_columns_without_reading_the_bodies(tmp_path: Path) -> None:
    """The resume sensor and the finished check read one or two columns of every directory walk —
    237 files, US.common alone 150 pages of JSON. Asking for those columns must return exactly
    them, a column the file lacks must be skipped rather than raise, and the empty marker must
    still read as nothing held."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from muffin_ingest_dagster.lib.io_managers import RawStore

    folder = tmp_path / "raw_exchange_sweep"
    folder.mkdir()
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"body": b"{...}", "cursor_at": "c1", "page": 0},
                {"body": b"{...}", "cursor_at": None, "page": 1},
            ]
        ),
        str(folder / "AU.common.parquet"),
    )
    pq.write_table(pa.table({"collected_nothing": [True]}), str(folder / "LN.common.parquet"))
    store = RawStore(base_path=str(tmp_path))
    key = dg.AssetKey("raw_exchange_sweep")

    assert store.stored_rows_for(key, "AU.common", columns=["cursor_at", "page", "absent"]) == [
        {"cursor_at": "c1", "page": 0},
        {"cursor_at": None, "page": 1},
    ]
    assert len(store.stored_rows_for(key, "AU.common")[0]) == 3, "no columns named: every column"
    assert store.stored_rows_for(key, "LN.common", columns=["cursor_at"]) == []
    assert store.stored_rows_for(key, "US.common", columns=["cursor_at"]) == [], "no file: nothing"


def visiting_seam(
    tmp_path: Path, runs: list[Any], *, metadata: dict[str, Any] | None = None
) -> tuple[list[Any], list[dict[str, Any]]]:
    """Materialise one asset once per entry in `runs`; return each run's result and what the last
    downstream read was handed."""
    seen: list[dict[str, Any]] = []
    answers = list(runs)

    @dg.asset(
        partitions_def=DAY, io_manager_key="parquet_io", name="raw_thing", metadata=metadata or {}
    )
    def raw_thing() -> list[dict[str, Any]]:
        answer: list[dict[str, Any]] = answers.pop(0)
        return answer

    @dg.asset(partitions_def=DAY, name="downstream")
    def downstream(raw_thing: list[dict[str, Any]]) -> None:
        seen.clear()
        seen.extend(raw_thing)

    manager = ParquetIOManager(base_path=str(tmp_path))
    results = [
        dg.materialize(
            [raw_thing, downstream],
            partition_key="2026-09-02",
            resources={"parquet_io": manager},
            raise_on_error=False,
        )
        for _ in runs
    ]
    return results, seen


def test_an_unchanged_partition_is_left_alone_and_counted(tmp_path: Path) -> None:
    """A filer with nothing due must not have megabytes of company facts rewritten, nor replaced
    by the "collected nothing" marker an empty list would write."""
    stored = [{"cik": 320193, "body": b"{...}"}]
    results, seen = visiting_seam(tmp_path, [stored, Unchanged()])
    assert all(r.success for r in results)
    assert seen == stored, "downstream reads the stored rows, not nothing"
    meta = results[1].asset_materializations_for_node("raw_thing")[0].metadata
    assert meta["partitions_unchanged"].value == 1


def test_an_unchanged_partition_is_not_rewritten(tmp_path: Path) -> None:
    stored = [{"cik": 320193, "body": b"{...}"}]
    file = tmp_path / "raw_thing" / "2026-09-02.parquet"
    visiting_seam(tmp_path, [stored])
    before = (file.stat().st_mtime_ns, file.read_bytes())
    os.utime(file, ns=(1, 1))
    visiting_seam(tmp_path, [Unchanged()])
    assert file.stat().st_mtime_ns == 1, "the file was rewritten"
    assert file.read_bytes() == before[1]


@pytest.mark.parametrize(
    ("runs", "says"),
    [
        ([Unchanged()], "no file is stored"),
        ([[{"cik": 1}], Unchanged([{"cik": 1}])], "with rows"),
    ],
    ids=["never-collected", "with-rows"],
)
def test_unchanged_is_refused_where_it_would_claim_something_false(
    tmp_path: Path, runs: list[Any], says: str
) -> None:
    results, _ = visiting_seam(tmp_path, runs)
    failed = results[-1]
    assert not failed.success
    [event] = [e for e in failed.all_events if e.event_type_value == "STEP_FAILURE"][:1]
    assert says in str(event.event_specific_data)


def test_a_range_run_counts_unchanged_partitions_apart_from_written_ones(tmp_path: Path) -> None:
    manager = ParquetIOManager(base_path=str(tmp_path))
    keys = ["2026-09-02", "2026-09-03", "2026-09-04"]
    first = {"pass": True}

    @dg.asset(
        partitions_def=DAY,
        io_manager_key="parquet_io",
        name="raw_thing",
        backfill_policy=dg.BackfillPolicy.single_run(),
        # MERGING, as SEC's submissions asset is: a merged partition records its own tally, so
        # this is the shape where the unchanged count must be added beside the merge counts.
        metadata={"merge_on": ["cik", "day"]},
    )
    def raw_thing(context) -> dict[str, Any]:  # type: ignore[no-untyped-def]
        if first["pass"]:
            return {k: [{"cik": 1, "day": k}] for k in context.partition_keys}
        return {keys[0]: [{"cik": 1, "day": "new"}], keys[1]: Unchanged(), keys[2]: Unchanged()}

    def run() -> Any:
        return dg.materialize(
            [raw_thing],
            resources={"parquet_io": manager},
            tags={
                "dagster/asset_partition_range_start": keys[0],
                "dagster/asset_partition_range_end": keys[-1],
            },
        )

    assert run().success
    first["pass"] = False
    result = run()
    assert result.success
    meta = result.asset_materializations_for_node("raw_thing")[0].metadata
    assert meta["partitions_written"].value == 1
    assert meta["partitions_merged"].value == 1
    assert meta["partitions_unchanged"].value == 2
    store = manager.stored_rows_for(dg.AssetKey("raw_thing"), keys[1])
    assert store == [{"cik": 1, "day": keys[1]}], "an unchanged partition keeps its rows"


def test_an_asset_may_name_its_compression_and_the_default_is_unchanged(tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    rows = [{"cik": 320193, "body": b"x" * 1000}]
    visiting_seam(tmp_path / "zstd", [rows], metadata={"compression": "zstd"})
    visiting_seam(tmp_path / "default", [rows])

    def codec(root: Path) -> str:
        meta = pq.ParquetFile(str(root / "raw_thing" / "2026-09-02.parquet")).metadata
        return str(meta.row_group(0).column(0).compression)

    assert codec(tmp_path / "zstd") == "ZSTD"
    assert codec(tmp_path / "default") == "SNAPPY"
    _, seen = visiting_seam(tmp_path / "zstd", [rows], metadata={"compression": "zstd"})
    assert seen == rows


def test_a_codec_pyarrow_does_not_write_is_refused(tmp_path: Path) -> None:
    results, _ = visiting_seam(tmp_path, [[{"cik": 1}]], metadata={"compression": "zip"})
    assert not results[0].success


# --- ONE DOCUMENT PER ROW GROUP (2026-11-14) ------------------------------------------------------
#
# Principal Financial's SEC instances (21 filings, 413 MB of bodies, 14 MB on disk) were one row
# group, so reading any one body decoded all of them: 2,143 MB read whole, 1,073 MB streamed, and
# stage 2 of the segment lane was OOM-killed in a 2.5 GB code location.

DOCS = ["accession", "document"]


def _doc(accession: str, document: str, body: bytes) -> dict[str, Any]:
    return {"accession": accession, "document": document, "body": body}


def _file(root: Path) -> Path:
    return root / "raw_thing" / "2026-09-02.parquet"


def _seed(root: Path, rows: list[dict[str, Any]]) -> None:
    """A file as an asset wrote it before it named a row group: one group."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    _file(root).parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), str(_file(root)))


def _by_doc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: (r["accession"], str(r.get("document"))))


def test_a_merge_by_row_group_keeps_what_the_whole_file_merge_keeps(tmp_path: Path) -> None:
    """The same runs over the same stored file, merged both ways, hold the same rows: newest wins
    per key, a stored row the key cannot read (no `document` column) is kept, and an empty fetch
    keeps everything."""
    legacy = [{"accession": "a0", "body": b"zero"}]
    runs = [
        [_doc("a1", "instance", b"one"), _doc("a2", "instance", b"two")],
        [_doc("a2", "instance", b"TWO"), _doc("a3", "index", b"three")],
        [],
    ]
    for root in (tmp_path / "whole", tmp_path / "grouped"):
        _seed(root, legacy)
    whole = merging_seam(tmp_path / "whole", [list(r) for r in runs], DOCS)
    grouped = merging_seam(
        tmp_path / "grouped", [list(r) for r in runs], DOCS, metadata={"row_group_rows": 1}
    )
    assert _by_doc(grouped) == _by_doc(whole)
    assert {r["accession"]: r["body"] for r in grouped} == {
        "a0": b"zero",
        "a1": b"one",
        "a2": b"TWO",
        "a3": b"three",
    }


def test_a_merge_writes_one_group_per_row_and_rewrites_a_file_that_was_one(
    tmp_path: Path,
) -> None:
    import pyarrow.parquet as pq

    _seed(tmp_path, [_doc("a1", "instance", b"one"), _doc("a2", "instance", b"two")])
    assert pq.ParquetFile(str(_file(tmp_path))).num_row_groups == 1
    merging_seam(
        tmp_path, [[_doc("a3", "instance", b"three")]], DOCS, metadata={"row_group_rows": 1}
    )
    stored = pq.ParquetFile(str(_file(tmp_path)))
    assert stored.num_row_groups == 3
    assert stored.metadata.num_rows == 3
    assert not list(_file(tmp_path).parent.glob("*.merging")), "the temporary file was left"


def test_an_empty_fetch_leaves_a_grouped_file_untouched(tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    first = [_doc("a1", "instance", b"one"), _doc("a1", "index", b"idx")]
    merging_seam(tmp_path, [first], DOCS, metadata={"row_group_rows": 1})
    assert pq.ParquetFile(str(_file(tmp_path))).num_row_groups == 2, "a first write is grouped"
    before = _file(tmp_path).read_bytes()
    seen = merging_seam(tmp_path, [[]], DOCS, metadata={"row_group_rows": 1})
    assert _file(tmp_path).read_bytes() == before
    assert _by_doc(seen) == _by_doc(first)


def test_a_replacing_asset_may_name_its_row_group_too(tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    rows = [_doc("a1", "instance", b"one"), _doc("a2", "instance", b"two")]
    _, seen = visiting_seam(tmp_path, [rows], metadata={"row_group_rows": 1})
    assert pq.ParquetFile(str(_file(tmp_path))).num_row_groups == 2
    assert _by_doc(seen) == _by_doc(rows)


def test_an_empty_first_fetch_writes_the_empty_marker(tmp_path: Path) -> None:
    seen = merging_seam(tmp_path, [[]], DOCS, metadata={"row_group_rows": 1})
    assert seen == []
    assert _file(tmp_path).exists(), "collected and found nothing is written"


@pytest.mark.parametrize("bad", [0, -1, "1"])
def test_a_row_group_that_is_not_a_positive_count_is_refused(tmp_path: Path, bad: Any) -> None:
    results, _ = visiting_seam(
        tmp_path, [[{"cik": 1}]], metadata={"merge_on": ["cik"], "row_group_rows": bad}
    )
    assert not results[0].success


def _six(root: Path, *, group_rows: int | None) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [_doc(f"a{n}", d, f"{n}{d}".encode()) for n in (1, 2, 3) for d in ("index", "instance")]
    _file(root).parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), str(_file(root)), row_group_size=group_rows)


def test_a_where_read_decodes_only_the_groups_that_can_hold_the_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The point of the layout: one filing's documents are read without decoding the others."""
    import pyarrow.parquet as pq

    from muffin_ingest_dagster.lib.io_managers import RawStore

    _six(tmp_path, group_rows=1)
    decoded: list[int] = []
    original = pq.ParquetFile.read_row_group

    def spy(self: Any, group: int, *args: Any, **kwargs: Any) -> Any:
        decoded.append(group)
        return original(self, group, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", spy)
    store = RawStore(base_path=str(tmp_path))
    got = store.stored_rows_where(dg.AssetKey("raw_thing"), "2026-09-02", {"accession": "a2"})
    assert _by_doc([dict(r) for r in got]) == [
        _doc("a2", "index", b"2index"),
        _doc("a2", "instance", b"2instance"),
    ]
    assert decoded == [2, 3], "a group that cannot hold the filing was decoded"


def test_a_where_read_of_a_one_group_file_still_finds_exactly_the_rows(tmp_path: Path) -> None:
    from muffin_ingest_dagster.lib.io_managers import RawStore

    _six(tmp_path, group_rows=None)
    store = RawStore(base_path=str(tmp_path))
    key = dg.AssetKey("raw_thing")
    got = store.stored_rows_where(key, "2026-09-02", {"accession": "a3"}, columns=["document"])
    assert sorted(r["document"] for r in got) == ["index", "instance"]
    assert all(set(r) == {"document"} for r in got), "only the columns asked for"
    assert store.stored_rows_where(key, "2026-09-02", {"accession": "a9"}) == []
    assert store.stored_rows_where(key, "2026-09-02", {"absent": "x"}) == []
    assert store.stored_rows_where(key, "2026-09-03", {"accession": "a1"}) == [], "no file"
