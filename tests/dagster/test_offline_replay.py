"""The whole price graph, offline, over CAPTURED provider bytes — asserted in SQL.

WHY THIS EXISTS ALONGSIDE THE OTHER ASSET TESTS. Those drive shapes someone wrote out, and every
shape that has actually cost this pipeline something was one nobody would have written: a
single-symbol response with no `symbol` column, an unknown symbol RAISING instead of answering
empty, a provider returning bars outside the range it was asked for. `price_history.json` is what
yfinance really said on 2026-09-11, so these assertions run against evidence.

AND DUCKDB IS THE ASSERTION ENGINE, NOT AN I/O MANAGER. `dagster-duckdb` pins `dagster==1.13.21`
against our `<1.13`, and it is not needed anyway: raw already lands as Parquet and DuckDB reads
Parquet directly. So the real graph runs with the real Parquet I/O manager into a tmp directory, and
the questions are asked of the files it wrote — which is exactly "was the data pulled correctly",
rather than "did the function return what I told it to".
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest

from muffin_ingest.providers import openbb
from muffin_ingest.providers.openbb import Answer
from muffin_ingest_dagster.assets import prices as asset_prices
from muffin_ingest_dagster.io_managers import ParquetIOManager

from . import test_price_assets as fakes

CAPTURED = json.loads((Path(__file__).parents[1] / "fixtures" / "price_history.json").read_text())

#: The captured window. The fixture was taken over 2026-09-08..09, so the partition asserted here
#: has to be one of those days or the window filter would correctly discard everything.
PARTITION = "2026-09-08"


def replaying(group: str) -> Any:
    """A fetcher that answers from captured bytes, raising where the provider raised."""
    captured = CAPTURED[group]

    def fetch(symbols: Sequence[str], **kwargs: Any) -> Answer:
        if captured["error"]:
            # THE PROVIDER RAISES FOR AN UNKNOWN SYMBOL rather than answering with no rows.
            # Replaying that faithfully is the point: a fake returning [] would exercise a path the
            # provider does not take.
            raise RuntimeError(captured["error"])
        return Answer(rows=list(captured["rows"]), warnings=list(captured["warnings"]))

    return fetch


def materialise(tmp_path: Path, group: str, subjects: list[tuple[str, str, float]]) -> Path:
    saved_fetch, saved_universe = openbb.price_history, list(fakes.UNIVERSE)
    openbb.price_history = replaying(group)
    fakes.UNIVERSE[:] = subjects
    try:
        dg.materialize(
            [asset_prices.raw_price_bars],
            partition_key=PARTITION,
            resources={
                "postgres": fakes.FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
        )
    finally:
        openbb.price_history = saved_fetch
        fakes.UNIVERSE[:] = saved_universe
    return tmp_path / "raw_price_bars" / f"{PARTITION}.parquet"


def sql(parquet: Path, query: str) -> list[tuple[Any, ...]]:
    return duckdb.sql(query.format(f=f"read_parquet('{parquet}')")).fetchall()


BATCH_SUBJECTS = [
    ("11111111-1111-1111-1111-111111111111", "AAPL", 5.0),
    ("22222222-2222-2222-2222-222222222222", "005930.KS", 4.0),
    ("33333333-3333-3333-3333-333333333333", "QIBK.QA", 3.0),
    ("44444444-4444-4444-4444-444444444444", "SQM-B.SN", 2.0),
]


def test_a_captured_batch_lands_one_bar_per_security_for_the_partition(tmp_path: Path) -> None:
    parquet = materialise(tmp_path, "batch_mixed_venues", BATCH_SUBJECTS)
    assert parquet.exists()

    rows = sql(
        parquet, "select count(*), count(distinct security_id), count(distinct trade_date) from {f}"
    )
    assert rows == [(4, 4, 1)], (
        "four securities, one bar each, one date — the fixture spans two days and the window "
        "filter keeps only the partition's own"
    )


def test_every_captured_bar_is_attributed_to_the_right_security(tmp_path: Path) -> None:
    """The batch response DOES carry a `symbol` column, so this is the easy direction — and it is
    still worth pinning, because the hard direction below is the same code path."""
    parquet = materialise(tmp_path, "batch_mixed_venues", BATCH_SUBJECTS)
    got = dict(sql(parquet, "select asked_symbol, security_id from {f}"))
    assert got["005930.KS"] == "22222222-2222-2222-2222-222222222222"
    assert got["SQM-B.SN"] == "44444444-4444-4444-4444-444444444444"


def test_a_single_symbol_response_carries_no_symbol_column_and_is_still_attributed(
    tmp_path: Path,
) -> None:
    """THE SHAPE NOBODY WOULD HAVE INVENTED. Captured: a one-symbol request comes back with no
    `symbol` field at all, so the rows have to be told what was asked. Getting this wrong attributes
    a whole series to the wrong company, silently."""
    assert "symbol" not in CAPTURED["single_symbol_no_symbol_column"]["rows"][0], (
        "the fixture must actually exhibit the shape this test is named for"
    )
    subjects = [("55555555-5555-5555-5555-555555555555", "NESN.SW", 1.0)]
    parquet = materialise(tmp_path, "single_symbol_no_symbol_column", subjects)
    assert sql(parquet, "select distinct security_id from {f}") == [
        ("55555555-5555-5555-5555-555555555555",)
    ]


def test_a_symbol_the_provider_raises_on_is_not_recorded_as_a_bar(tmp_path: Path) -> None:
    """Captured: an unknown symbol raises `EmptyDataError` rather than answering empty. The run must
    survive it and write nothing for that security — never a row, and never a mark, because a raise
    is not evidence about the subject until it has been asked alone with a control answering."""
    subjects = [("66666666-6666-6666-6666-666666666666", "ZZZZ.NOPE", 0.1)]
    parquet = materialise(tmp_path, "provider_has_nothing", subjects)
    # Readable by DuckDB even though it holds nothing — an empty partition is a RESULT, and one
    # that only our own loader can open is not inspectable raw.
    assert sql(parquet, "select count(*) from {f}") == [(0,)]


def test_ohlcv_survives_the_round_trip_through_parquet(tmp_path: Path) -> None:
    """Raw records what the provider said. A column silently lost in serialisation would only show
    up when a later stage wanted it."""
    parquet = materialise(tmp_path, "batch_mixed_venues", BATCH_SUBJECTS)
    cols = {r[0] for r in sql(parquet, "select column_name from (describe select * from {f})")}
    assert {"security_id", "asked_symbol", "trade_date", "close", "volume", "provider"} <= cols


@pytest.mark.parametrize("group", ["batch_mixed_venues", "single_symbol_no_symbol_column"])
def test_no_bar_escapes_the_partition_window(tmp_path: Path, group: str) -> None:
    """The fixture deliberately spans 09-08 and 09-09. A partition that kept both would be the
    defect that reached production once already."""
    subjects = (
        BATCH_SUBJECTS
        if group == "batch_mixed_venues"
        else [("55555555-5555-5555-5555-555555555555", "NESN.SW", 1.0)]
    )
    parquet = materialise(tmp_path, group, subjects)
    # A STRING, because raw records the provider's own rendering of the date rather than a parsed
    # one — interpretation belongs to stage 2.
    assert sql(parquet, "select distinct trade_date from {f}") == [(PARTITION,)]


def test_the_window_asked_for_is_half_open_and_never_degenerate(tmp_path: Path) -> None:
    """A DEGENERATE RANGE IS IGNORED BY THE PROVIDER, measured against the real hub:

        start=2026-09-01 end=2026-09-01  ->  7 rows, 2026-09-01..2026-09-10
        start=2026-09-01 end=2026-09-02  ->  2 rows, exactly those two

    `start == end` returns everything from `start` to TODAY, so asking for one old day dragged back
    every session since — 653 bars discarded for 96 securities on the first parity run. Correctness
    never depended on it, because the filter keeps only the partition's own day; the cost did.

    So the asset must hand the provider the half-open window Dagster already gives it, and the two
    dates must differ.
    """
    asked: dict[str, Any] = {}

    def recording(symbols: Sequence[str], **kwargs: Any) -> Answer:
        asked.update(kwargs)
        return Answer(rows=list(CAPTURED["batch_mixed_venues"]["rows"]))

    saved_fetch, saved_universe = openbb.price_history, list(fakes.UNIVERSE)
    openbb.price_history = recording
    fakes.UNIVERSE[:] = BATCH_SUBJECTS
    try:
        dg.materialize(
            [asset_prices.raw_price_bars],
            partition_key=PARTITION,
            resources={
                "postgres": fakes.FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
        )
    finally:
        openbb.price_history = saved_fetch
        fakes.UNIVERSE[:] = saved_universe

    assert str(asked["start"]) == PARTITION
    assert asked["end"] > asked["start"], (
        "start == end makes the provider ignore the range entirely and answer from start to today"
    )
