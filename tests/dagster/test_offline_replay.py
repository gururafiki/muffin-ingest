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
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest

from muffin_ingest.providers import openbb
from muffin_ingest.providers.openbb import Answer
from muffin_ingest_dagster.assets import prices as asset_prices
from muffin_ingest_dagster.io_managers import ParquetIOManager
from muffin_ingest_dagster.resources import Postgres

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


def columns_of(parquet: Path) -> list[str]:
    """The column names raw actually wrote — the shape, which no test asserted until raw was
    found to be dropping four of the provider's ten fields."""
    return list(duckdb.sql(f"select * from read_parquet('{parquet}') limit 1").columns)


def materialise_both(
    tmp_path: Path, group: str, subjects: list[tuple[str, str, float]]
) -> tuple[Path, list[dict[str, Any]]]:
    """Stage 1 AND stage 2 over the capture: the raw file, and what the clean stage published.

    BOTH, BECAUSE THE WINDOW MOVED. Raw keeps every bar the provider sent, out-of-window ones
    included, and the refusal happens in `price_bar`. A test that inspects only one side cannot
    tell "the rule moved" from "the rule vanished".
    """
    CAPTURED_WRITES.clear()
    saved_fetch, saved_universe = openbb.price_history, list(fakes.UNIVERSE)
    openbb.price_history = replaying(group)
    fakes.UNIVERSE[:] = subjects
    try:
        dg.materialize(
            [asset_prices.raw_price_bars, asset_prices.price_bar],
            partition_key=PARTITION,
            resources={
                "postgres": fakes.FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "postgres_io": CapturingPostgresIO(),
            },
        )
    finally:
        openbb.price_history = saved_fetch
        fakes.UNIVERSE[:] = saved_universe
    published = [row for batch in CAPTURED_WRITES for row in batch]
    return tmp_path / "raw_price_bars" / f"{PARTITION}.parquet", published


def chart_body(points: Sequence[tuple[date, float]]) -> bytes:
    """A Yahoo chart body in the provider's real shape — nested parallel arrays under
    `chart.result[0]`, each bar stamped at the London session open with `gmtoffset` saying so, and
    no live quote."""
    tz = timezone(timedelta(hours=1))
    stamps = [int(datetime(d.year, d.month, d.day, tzinfo=tz).timestamp()) for d, _ in points]
    closes = [close for _, close in points]
    quote = {
        "open": closes,
        "high": closes,
        "low": closes,
        "close": closes,
        "volume": [0] * len(closes),
    }
    meta = {"gmtoffset": 3600, "exchangeTimezoneName": "Europe/London", "regularMarketTime": 0}
    result = {"meta": meta, "timestamp": stamps, "indicators": {"quote": [quote]}}
    return json.dumps({"chart": {"error": None, "result": [result]}}).encode()


@contextmanager
def serving(body: bytes) -> Iterator[None]:
    """Yahoo answering every pair with `body`, byte for byte. Patched on the provider MODULE, which
    is the name the asset calls through."""
    from muffin_ingest.providers import yahoo_chart
    from muffin_ingest.providers.documents import Document

    def fetch(symbol: str, **kwargs: Any) -> Document:
        return Document(
            url=f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
            body=body,
            content_type="application/json",
            fetched_at=datetime.now(UTC),
        )

    saved = yahoo_chart.fetch
    yahoo_chart.fetch = fetch
    try:
        yield
    finally:
        yahoo_chart.fetch = saved


BATCH_SUBJECTS = [
    ("11111111-1111-1111-1111-111111111111", "AAPL", 5.0),
    ("22222222-2222-2222-2222-222222222222", "005930.KS", 4.0),
    ("33333333-3333-3333-3333-333333333333", "QIBK.QA", 3.0),
    ("44444444-4444-4444-4444-444444444444", "SQM-B.SN", 2.0),
]


def test_a_captured_batch_lands_one_bar_per_security_for_the_partition(tmp_path: Path) -> None:
    parquet, published = materialise_both(tmp_path, "batch_mixed_venues", BATCH_SUBJECTS)
    assert parquet.exists()

    # RAW IS THE WHOLE ANSWER — every captured row, both days, each attributed to its security.
    # The second day is another partition's to publish; it is not this one's to delete.
    sent = len(CAPTURED["batch_mixed_venues"]["rows"])
    rows = sql(
        parquet, "select count(*), count(distinct security_id), count(distinct date) from {f}"
    )
    assert rows == [(sent, 4, 2)], f"raw holds {rows}; the provider sent {sent} rows over two days"

    # AND THE CORE TABLE IS THE PARTITION'S: four securities, one bar each, one date.
    assert sorted(r["security_id"] for r in published) == sorted(s for s, _, _ in BATCH_SUBJECTS)
    assert {r["trade_date"] for r in published} == {PARTITION}


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
    """Raw records what the provider said — EVERY field, under the provider's own name. A column
    silently lost in serialisation would only show up when a later stage wanted it."""
    parquet = materialise(tmp_path, "batch_mixed_venues", BATCH_SUBJECTS)
    cols = {r[0] for r in sql(parquet, "select column_name from (describe select * from {f})")}
    sent = set(CAPTURED["batch_mixed_venues"]["rows"][0])
    assert sent <= cols, f"raw dropped the provider's {sorted(sent - cols)}"
    assert {"security_id", "asked_symbol", "provider"} <= cols
    # NOTHING DERIVED. A parsed `trade_date` was written into raw until 2026-09-12 — an
    # interpretation of the vendor's own `date`, made before anything was stored.
    assert "trade_date" not in cols


@pytest.mark.parametrize("group", ["batch_mixed_venues", "single_symbol_no_symbol_column"])
def test_no_bar_escapes_the_partition_window(tmp_path: Path, group: str) -> None:
    """The fixture deliberately spans 09-08 and 09-09. A partition that PUBLISHED both would be the
    defect that reached production once already — and the rule is enforced where publishing
    happens now, so that is where it is asserted."""
    subjects = (
        BATCH_SUBJECTS
        if group == "batch_mixed_venues"
        else [("55555555-5555-5555-5555-555555555555", "NESN.SW", 1.0)]
    )
    parquet, published = materialise_both(tmp_path, group, subjects)
    assert published, "the partition's own bars must still be published"
    assert {r["trade_date"] for r in published} == {PARTITION}
    # The other day is still on disk, exactly as sent: the rule MOVED to stage 2, it did not vanish,
    # and a correction to it costs a re-parse rather than a re-fetch. A STRING, because raw records
    # the provider's own rendering of the date.
    assert sql(parquet, "select distinct date from {f} order by 1") == [
        (PARTITION,),
        ("2026-09-09",),
    ]


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


def test_a_single_run_covering_several_partitions_writes_one_file_each(tmp_path: Path) -> None:
    """`BackfillPolicy.single_run()` IS DECORATIVE WITHOUT THIS, and that is how it reached
    production: `UPathIOManager` refuses a multi-partition output outright —

        does not support persisting an output associated with multiple partitions

    — so the first 96-security history backfill fetched every security's full history, paid the
    provider for all of it, and died at the WRITE. Nothing before the write could see it: the asset
    ran, logged "full history for 96 of 96", and failed afterwards.

    Its own suggested remedies are worse. A multi-run policy turns one backfill of 96 securities
    into 96 runs, and because this provider is asked one batch at a time that is ten calls becoming
    ninety-six.
    """
    keys = ["2026-09-08", "2026-09-09"]

    def two_days(symbols: Sequence[str], **kwargs: Any) -> Answer:
        return Answer(rows=list(CAPTURED["batch_mixed_venues"]["rows"]))

    saved_fetch, saved_universe = openbb.price_history, list(fakes.UNIVERSE)
    openbb.price_history = two_days
    fakes.UNIVERSE[:] = BATCH_SUBJECTS
    try:
        dg.materialize(
            [asset_prices.raw_price_bars],
            tags={
                "dagster/asset_partition_range_start": keys[0],
                "dagster/asset_partition_range_end": keys[-1],
            },
            resources={
                "postgres": fakes.FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
        )
    finally:
        openbb.price_history = saved_fetch
        fakes.UNIVERSE[:] = saved_universe

    written = sorted(p.stem for p in (tmp_path / "raw_price_bars").glob("*.parquet"))
    assert written == keys, "one file per partition, named for the partition it claims"

    for key in keys:
        rows = sql(tmp_path / "raw_price_bars" / f"{key}.parquet", "select distinct date from {f}")
        assert rows == [(key,)], f"{key}'s file holds only {key}"


class CapturingPostgresIO(dg.ConfigurableIOManager):
    """Stands in for `PostgresIOManager` so a clean stage can be driven without a database.

    It captures rather than writes, because the question here is what stage 2 PRODUCED — the real
    manager's own behaviour (the conflict target, `replace_scope`, the updatable column set) is
    covered by `test_io_managers.py` against a real table.
    """

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        CAPTURED_WRITES.append(obj)

    def load_input(self, context: dg.InputContext) -> Any:
        raise NotImplementedError("nothing reads back out of this one")


CAPTURED_WRITES: list[Any] = []


def test_a_single_run_covering_several_partitions_normalises_all_of_them(tmp_path: Path) -> None:
    """THE MIRROR OF THE TEST ABOVE, AND ITS ABSENCE COST A SECOND FAILED BACKFILL.

    Fixing the multi-partition WRITE made the next run get one stage further and die on the LOAD:

        Type check failed for step input "raw_price_history" - expected type "[Dict[String,Any]]"

    `UPathIOManager.load_input` hands a downstream step covering several partitions a
    `{partition_key: obj}` MAPPING, not the obj — so `single_run` changes the shape at every seam in
    the lane, and the first fix only looked at the seam that had failed. Both times the provider had
    already been paid: the raw files were on disk, 96 of them, 12 MB.

    So this drives BOTH stages over a range and asserts the clean stage saw every partition's rows.
    Driving only stage 1 is what left the gap — the write was tested and the read was not reachable,
    because no test had ever materialised stage 2 at all.
    """
    keys = ["2026-09-08", "2026-09-09"]
    rows = list(CAPTURED["batch_mixed_venues"]["rows"])

    def both_days(symbols: Sequence[str], **kwargs: Any) -> Answer:
        return Answer(rows=rows)

    CAPTURED_WRITES.clear()
    saved_fetch, saved_universe = openbb.price_history, list(fakes.UNIVERSE)
    openbb.price_history = both_days
    fakes.UNIVERSE[:] = BATCH_SUBJECTS
    try:
        result = dg.materialize(
            [asset_prices.raw_price_bars, asset_prices.price_bar],
            tags={
                "dagster/asset_partition_range_start": keys[0],
                "dagster/asset_partition_range_end": keys[-1],
            },
            resources={
                "postgres": fakes.FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "postgres_io": CapturingPostgresIO(),
            },
        )
    finally:
        openbb.price_history = saved_fetch
        fakes.UNIVERSE[:] = saved_universe

    assert result.success, "the clean stage must survive a multi-partition load"
    assert len(CAPTURED_WRITES) == 1, "single_run writes once for the whole range"

    written = CAPTURED_WRITES[0]
    dates = {r["trade_date"] for r in written}
    assert dates == set(keys), (
        f"both partitions' rows must reach the clean stage, got {sorted(dates)} — a load that "
        f"returns only one partition's rows, or the mapping itself, fails here"
    )
    assert len(written) == len(BATCH_SUBJECTS) * len(keys), (
        "four securities on each of two days, flattened into one list"
    )


def test_the_fx_spot_lane_keeps_only_its_own_partition_s_day(tmp_path: Path) -> None:
    """A PARTITION CLAIMS ITS OWN WINDOW, AND THE FIRST VERSION WROTE SOMEONE ELSE'S DAY.

    Driven against production, a run for the 2026-09-10 partition wrote **42 rates dated
    2026-09-11** — because a five-day range is requested so the partition's day is certainly inside
    what comes back, and the asset then took the NEWEST point instead of its own.

    Both halves are wrong. The partition's claim becomes false, and 09-11 was a session still in
    progress: a mid-session quote is not a close and looks exactly like one, which is the defect
    this pipeline exists to stop publishing.

    THE RULE LIVES IN STAGE 2 NOW. Raw stores Yahoo's body byte for byte — both points, the
    partition's own day and the next — and `fx_rate` publishes only the one that belongs.
    """
    from muffin_ingest_dagster.assets import fx as asset_fx

    key = "2026-09-10"
    body = chart_body([(date(2026, 9, 10), 1.16), (date(2026, 9, 11), 1.17)])

    CAPTURED_WRITES.clear()
    with serving(body):
        result = dg.materialize(
            [asset_fx.raw_fx_spot, asset_fx.fx_rate],
            partition_key=key,
            resources={
                "postgres": FxPostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "postgres_io": CapturingPostgresIO(),
            },
        )

    assert result.success
    stored = sql(
        tmp_path / "raw_fx_spot" / f"{key}.parquet",
        "select currency_code, body from {f} order by 1",
    )
    assert [(c, bytes(b)) for c, b in stored] == [("EUR", body), ("JPY", body)], (
        "raw must be each pair's response body exactly as served — no pivot, nothing dropped"
    )
    published = {(r["currency_code"], r["as_of"]) for batch in CAPTURED_WRITES for r in batch}
    assert published == {("EUR", key), ("JPY", key)}, (
        f"published {sorted(published)} — a day outside the partition's window is another "
        f"partition's claim, and today's is a session that has not closed"
    )


class FxConn:
    """Answers the one query `raw_fx_spot` makes."""

    def cursor(self) -> FxConn:
        return self

    def __enter__(self) -> FxConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql_text: str, params: Any = ()) -> None:
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return [("EUR",), ("JPY",)]

    def commit(self) -> None:
        return None


class FxPostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield FxConn()


def test_one_ETF_backing_SEVERAL_scopes_reaches_all_of_them(tmp_path: Path) -> None:
    """A DICT COMPREHENSION SILENTLY KEPT THE LAST, AND NINE SCOPES GOT NO RETURNS.

    Measured in production: 62 proxied scopes over **53 distinct symbols**. `EEM` backs
    `ftse:em-emea`, `msci:emerging` AND `msci:em-emea`; `IVV` backs `country:US`, `ftse:na` and
    `msci:na`. Written as `{symbol: code}` the map kept one code per symbol, and the run reported
    `answered=52` beside `empty=0` — two numbers that cannot both be right, and the only trace.

    It is not a modelling error. Those scopes genuinely ARE the same index, which is why the
    relation is many-to-one and has to be stored as one.
    """
    from muffin_ingest.facets import indices as facet_indices
    from muffin_ingest.providers import openbb as hub
    from muffin_ingest.providers.openbb import Answer
    from muffin_ingest_dagster.assets import indices as asset_indices

    key = "2026-09-10"
    shared = [
        ("group:ftse:em-emea", "EEM"),
        ("group:msci:emerging", "EEM"),
        ("group:msci:em-emea", "EEM"),
        ("country:BR", "EWZ"),
    ]

    def bars(symbols: Sequence[str], **kwargs: Any) -> Answer:
        return Answer(
            rows=[
                {"symbol": s, "date": "2026-09-10", "close": 40.0 + i, "volume": 1}
                for i, s in enumerate(symbols)
            ]
        )

    # Patched on the FACET module the asset imports, not through the asset's own namespace: a
    # re-exported attribute is not an export, and mypy --strict says so.
    saved_fetch = hub.price_history
    saved_scopes = facet_indices.proxied_scopes
    hub.price_history = bars
    facet_indices.proxied_scopes = lambda conn: list(shared)
    try:
        result = dg.materialize(
            [asset_indices.raw_index_bars],
            partition_key=key,
            resources={
                "postgres": FxPostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
        )
    finally:
        hub.price_history = saved_fetch
        facet_indices.proxied_scopes = saved_scopes

    assert result.success
    got = sql(
        tmp_path / "raw_index_bars" / f"{key}.parquet",
        "select distinct index_code from {f} order by 1",
    )
    assert [c for (c,) in got] == sorted(code for code, _ in shared), (
        f"only {[c for (c,) in got]} reached the file — a symbol backing several scopes must "
        f"reach every one of them"
    )


def test_the_index_lane_cuts_the_TOP_of_its_lookback_series(tmp_path: Path) -> None:
    """A LOOKBACK SERIES STILL HAS A TOP, AND THIS LANE FORGOT IT.

    `raw_index_bars` asks for 1,900 days up to the partition's `end` so the 5y anchor exists — and
    a range ending today brings TODAY's bar back with it, which is a session in progress.
    `index_return` then stamped every country and group `as_of 2026-09-11` from the 09-10 partition,
    with the newest close a mid-session price.

    THE CUT HAPPENS IN `index_return` NOW, against the file. Raw keeps the in-progress bar, because
    it is what the provider sent; what must never happen is a return STAMPED with it — so that is
    what this asserts, on the rows the core stage actually wrote.
    """
    from muffin_ingest.facets import indices as facet_indices
    from muffin_ingest.providers import openbb as hub
    from muffin_ingest.providers.openbb import Answer
    from muffin_ingest_dagster.assets import indices as asset_indices

    key = "2026-09-10"
    # A series that MOVES every day from 08-01 to 09-11, so the return rules produce periods at all:
    # a two-bar fixture can yield nothing, and an empty write would pass any assertion about dates.
    days = [date(2026, 8, 1) + timedelta(days=n) for n in range(42)]

    def with_today(symbols: Sequence[str], **kwargs: Any) -> Answer:
        return Answer(
            rows=[
                {"symbol": "EWZ", "date": d.isoformat(), "close": 30.0 + n / 10}
                for n, d in enumerate(days)
            ]
        )

    def no_sectors(provider: str = "finviz") -> Answer:
        return Answer(rows=[])

    resources = {
        "postgres": FxPostgres(),
        "parquet_io": ParquetIOManager(str(tmp_path)),
        "postgres_io": CapturingPostgresIO(),
    }
    CAPTURED_WRITES.clear()
    saved = hub.price_history, hub.sector_performance, facet_indices.proxied_scopes
    hub.price_history = with_today
    hub.sector_performance = no_sectors
    facet_indices.proxied_scopes = lambda conn: [("country:BR", "EWZ")]
    try:
        # THREE MATERIALISATIONS, because the finviz snapshot is UNPARTITIONED and the bars are not
        # — which is the design, and why one partitioned run cannot produce both inputs.
        dg.materialize([asset_indices.raw_index_bars], partition_key=key, resources=resources)
        dg.materialize([asset_indices.raw_sector_performance], resources=resources)
        result = dg.materialize(
            [
                asset_indices.raw_index_bars,
                asset_indices.raw_sector_performance,
                asset_indices.index_return,
            ],
            selection=[asset_indices.index_return],
            partition_key=key,
            resources=resources,
        )
    finally:
        hub.price_history, hub.sector_performance, facet_indices.proxied_scopes = saved

    assert result.success
    raw = sql(tmp_path / "raw_index_bars" / f"{key}.parquet", "select count(*), max(date) from {f}")
    assert raw == [(len(days), "2026-09-11")], (
        "raw keeps every bar the provider sent, the session in progress included"
    )
    stamped = {r["as_of"] for batch in CAPTURED_WRITES for r in batch}
    assert stamped == {"2026-09-10"}, (
        f"returns stamped {sorted(stamped)} — the lookback below the window is wanted, and the "
        f"session above it must never be the bar a return is computed from"
    )


def test_a_range_of_exactly_one_partition_is_written_as_one_partition(tmp_path: Path) -> None:
    """A RANGE OF ONE IS NOT A RANGE, AND THE I/O MANAGER DISAGREES ABOUT WHICH IT IS.

    `BackfillPolicy.multi_run` groups CONTIGUOUS partitions, so a backfill whose keys are scattered
    through the partition set produces ONE RUN PER PARTITION — each carrying a
    `partition_key_range` whose start equals its end. `_by_partition` saw the range and returned a
    mapping; `UPathIOManager` saw one partition and took its single-partition path, handing the
    mapping straight to the writer:

        AttributeError: 'str' object has no attribute 'get'

    — naming neither the partition nor the shape. Measured in production: 250 requested partitions
    became 250 single-partition runs and the first three failed exactly this way.

    The tags below are what Dagster itself sets for such a run, so this drives the real shape rather
    than a described one.
    """
    key = "2026-09-08"
    rows = list(CAPTURED["batch_mixed_venues"]["rows"])

    def one_day(symbols: Sequence[str], **kwargs: Any) -> Answer:
        return Answer(rows=rows)

    saved_fetch, saved_universe = openbb.price_history, list(fakes.UNIVERSE)
    openbb.price_history = one_day
    fakes.UNIVERSE[:] = BATCH_SUBJECTS
    try:
        result = dg.materialize(
            [asset_prices.raw_price_bars],
            # START AND END THE SAME KEY — a "range" of one, which is what a scattered backfill
            # produces and what `partition_key=` does not exercise.
            tags={
                "dagster/asset_partition_range_start": key,
                "dagster/asset_partition_range_end": key,
            },
            resources={
                "postgres": fakes.FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
        )
    finally:
        openbb.price_history = saved_fetch
        fakes.UNIVERSE[:] = saved_universe

    assert result.success, "a one-partition range must write as a single partition"
    written = sorted(p.stem for p in (tmp_path / "raw_price_bars").glob("*.parquet"))
    assert written == [key]
    got = sql(tmp_path / "raw_price_bars" / f"{key}.parquet", "select count(*) from {f}")
    assert got[0][0] > 0, "and it must hold rows rather than a serialised mapping"


def test_a_multi_day_fx_backfill_writes_one_file_per_day(tmp_path: Path) -> None:
    """`single_run` + a flat return is a WRITE failure, and it had never been run.

    `raw_fx_spot` declares `BackfillPolicy.single_run()`, so a backfill hands it every day at once
    and `UPathIOManager` needs one object per partition. It returned a flat list until 2026-09-12,
    which dies with

        does not support persisting an output associated with multiple partitions

    — after every provider call has been paid for. The daily schedule always covers exactly one
    partition, and a flat list is correct for that path, so the defect was invisible in production
    and in every existing test. Only a range reaches it.

    A CHART BODY COVERS A RANGE AND BELONGS TO NO SINGLE DAY, so every day's file holds the SAME
    bytes — the provider's answer covering that day — and `fx_rate` publishes each day from its own
    file. The two days carry DIFFERENT values: a stage 2 that windowed the whole run would publish
    each rate once per copy, and one that windowed the wrong file would pair a rate with the wrong
    day — the pairing below fails on both.
    """
    from muffin_ingest_dagster.assets import fx as asset_fx

    keys = ["2026-09-08", "2026-09-09"]
    body = chart_body([(date(2026, 9, 8), 1.11), (date(2026, 9, 9), 2.22)])

    CAPTURED_WRITES.clear()
    with serving(body):
        result = dg.materialize(
            [asset_fx.raw_fx_spot, asset_fx.fx_rate],
            tags={
                "dagster/asset_partition_range_start": keys[0],
                "dagster/asset_partition_range_end": keys[-1],
            },
            resources={
                "postgres": FxPostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "postgres_io": CapturingPostgresIO(),
            },
        )

    assert result.success, "a multi-day FX backfill must not die at the write"
    for key in keys:
        path = tmp_path / "raw_fx_spot" / f"{key}.parquet"
        assert path.exists(), f"no file for {key} — the range was not split per partition"
        stored = {bytes(b) for (b,) in sql(path, "select body from {f}")}
        assert stored == {body}, f"{key} must hold the body exactly as served, and nothing else"

    published = sorted(
        (r["currency_code"], r["as_of"], r["usd_per_unit"])
        for batch in CAPTURED_WRITES
        for r in batch
    )
    assert published == [
        ("EUR", "2026-09-08", 1.11),
        ("EUR", "2026-09-09", 2.22),
        ("JPY", "2026-09-08", 1.11),
        ("JPY", "2026-09-09", 2.22),
    ], f"published {published} — each day once, from its own partition, with its own rate"


def test_a_multi_day_index_backfill_writes_one_file_per_day(tmp_path: Path) -> None:
    """The same defect in the index lane, found by the same means.

    Both lanes declared `single_run()` and returned a flat list; neither had ever been backfilled.
    Fixing one and not the other is how this family has repeatedly lost a week — the helper is
    shared now so there is one place to get it right.
    """
    from muffin_ingest.facets import indices as facet_indices
    from muffin_ingest.providers import openbb as hub
    from muffin_ingest_dagster.assets import indices as asset_indices

    keys = ["2026-09-08", "2026-09-09"]

    def two_days(symbols: Sequence[str], **kwargs: Any) -> Answer:
        return Answer(
            rows=[
                {"symbol": "EWZ", "date": "2026-09-08", "close": 100.0, "volume": 1},
                {"symbol": "EWZ", "date": "2026-09-09", "close": 200.0, "volume": 1},
            ]
        )

    saved_fetch = hub.price_history
    saved_scopes = facet_indices.proxied_scopes
    hub.price_history = two_days
    facet_indices.proxied_scopes = lambda conn: [("country:BR", "EWZ")]
    try:
        result = dg.materialize(
            [asset_indices.raw_index_bars],
            tags={
                "dagster/asset_partition_range_start": keys[0],
                "dagster/asset_partition_range_end": keys[-1],
            },
            resources={
                "postgres": FxPostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
        )
    finally:
        hub.price_history = saved_fetch
        facet_indices.proxied_scopes = saved_scopes

    assert result.success, "a multi-day index backfill must not die at the write"
    for key in keys:
        path = tmp_path / "raw_index_bars" / f"{key}.parquet"
        assert path.exists(), f"no file for {key} — the range was not split per partition"
