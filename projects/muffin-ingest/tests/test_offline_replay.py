"""The FX and index graphs, offline, over CAPTURED provider shapes — asserted in SQL.

WHY THIS EXISTS ALONGSIDE THE OTHER ASSET TESTS. Those drive shapes someone wrote out, and every
shape that has actually cost this pipeline something was one nobody would have written: a body
that is the previous day's, a weekday the provider left out, a session it has not closed. The
price lane's replays over openbb's captured answers left with that lane on 2026-10-24; the chart
lane replays Yahoo's captured bodies in `test_price_chart_assets.py`.

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
from muffin_ingest.providers.openbb import Answer

from muffin_ingest_dagster.lib.io_managers import ParquetIOManager
from muffin_ingest_dagster.lib.resources import Postgres

PARTITION = "2026-09-08"


def sql(parquet: Path, query: str) -> list[tuple[Any, ...]]:
    # BY NAME, because a security the provider answered with no dividend field writes a file whose
    # columns differ from its neighbours', and a positional union would misread every row after it.
    return duckdb.sql(query.format(f=f"read_parquet('{parquet}', union_by_name = true)")).fetchall()


def chart_body(
    points: Sequence[tuple[date, float | None]], *, market_time: datetime | None = None
) -> bytes:
    """A Yahoo chart body in the provider's real shape — nested parallel arrays under
    `chart.result[0]`, each bar stamped at the London session open with `gmtoffset` saying so, and
    no live quote. `market_time` is the `regularMarketTime` the body was produced at."""
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
    produced = int(market_time.timestamp()) if market_time is not None else 0
    meta = {
        "gmtoffset": 3600,
        "exchangeTimezoneName": "Europe/London",
        "regularMarketTime": produced,
    }
    result = {"meta": meta, "timestamp": stamps, "indicators": {"quote": [quote]}}
    return json.dumps({"chart": {"error": None, "result": [result]}}).encode()


@contextmanager
def serving(body: bytes) -> Iterator[list[dict[str, Any]]]:
    """Yahoo answering every pair with `body`, byte for byte, recording what each request asked.
    Patched on the provider MODULE, which is the name the asset calls through."""
    from muffin_ingest.providers import yahoo_chart
    from muffin_ingest.providers.documents import Document

    asked: list[dict[str, Any]] = []

    def fetch(symbol: str, **kwargs: Any) -> Document:
        asked.append(kwargs)
        return Document(
            url=f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
            body=body,
            content_type="application/json",
            fetched_at=datetime.now(UTC),
        )

    saved = yahoo_chart.fetch
    yahoo_chart.fetch = fetch
    try:
        yield asked
    finally:
        yahoo_chart.fetch = saved


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
    from muffin_ingest_dagster.defs.fx import core as fx_core
    from muffin_ingest_dagster.defs.fx import raw as fx_raw

    key = "2026-09-10"
    body = chart_body([(date(2026, 9, 10), 1.16), (date(2026, 9, 11), 1.17)])

    CAPTURED_WRITES.clear()
    with serving(body) as asked:
        result = dg.materialize(
            [fx_raw.raw_fx_spot, fx_core.fx_rate],
            partition_key=key,
            resources={
                "postgres": FxPostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "postgres_io": CapturingPostgresIO(),
            },
        )

    assert result.success
    # The same pair and preset are asked again within the hour, so http-cache keeps the body.
    assert asked and all(kwargs["store"] is True for kwargs in asked)
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


def test_a_stale_body_fails_a_weekday_fx_partition_instead_of_publishing_nothing(
    tmp_path: Path,
) -> None:
    """A MATERIALISED PARTITION IS A COMPLETENESS CLAIM, AND AN EMPTY WEEKDAY IS NOT ONE.

    http-cache served the 2026-09-17 00:00 run the body it had cached a day earlier, so every point
    fell before the 09-16 window: `rows=0, outside_window=152`, and the partition materialised as a
    day with no exchange rates. FX trades every weekday, so a weekday with no rate inside its window
    from bodies that DO carry rates is a stale answer. It fails, naming the partition, so a retry is
    visible and nothing claims the day.

    A weekend is the control: no session, so the same shape is a true empty day and must succeed.
    Known edge, accepted: 25 Dec and 1 Jan have no session either and will fail, loudly.
    """
    from muffin_ingest_dagster.defs.fx import core as fx_core
    from muffin_ingest_dagster.defs.fx import raw as fx_raw

    resources = {
        "postgres": FxPostgres(),
        "parquet_io": ParquetIOManager(str(tmp_path)),
        "postgres_io": CapturingPostgresIO(),
    }
    yesterdays_cache = chart_body([(date(2026, 9, 14), 1.16), (date(2026, 9, 15), 1.17)])

    CAPTURED_WRITES.clear()
    with serving(yesterdays_cache):
        weekday = dg.materialize(
            [fx_raw.raw_fx_spot, fx_core.fx_rate],
            partition_key="2026-09-16",
            resources=resources,
            raise_on_error=False,
        )
    assert not weekday.success, "a weekday with no rate inside its window must not materialise"
    failures = [e for e in weekday.all_events if e.event_type_value == "STEP_FAILURE"]
    assert failures and "2026-09-16" in str(failures[0].event_specific_data), (
        "the failure names the partition, so the retry is obvious"
    )
    assert CAPTURED_WRITES == [], "nothing is written for a failed day"

    friday_close = chart_body([(date(2026, 9, 10), 1.16), (date(2026, 9, 11), 1.17)])
    with serving(friday_close):
        saturday = dg.materialize(
            [fx_raw.raw_fx_spot, fx_core.fx_rate],
            partition_key="2026-09-12",
            resources=resources,
        )
    assert saturday.success, "a weekend has no session, so an empty day there is the truth"


def test_a_weekday_the_provider_left_out_materialises_empty_and_says_so(tmp_path: Path) -> None:
    """A BODY PRODUCED AFTER THE DAY ENDED IS THE PROVIDER'S ANSWER FOR IT.

    Yahoo has no bar for Monday 2026-10-12 in any pair's daily series: the body fetched at
    2026-10-13 00:00 ran Thursday, Friday, then Tuesday, and a re-fetch at 19:07 was the same. The
    guard failed the partition as stale, and a re-run could never change that. The 09-17 decision
    was to fail only a body produced before the day ended, which is what a stale cache serves.
    """
    from muffin_ingest_dagster.defs.fx import core as fx_core
    from muffin_ingest_dagster.defs.fx import raw as fx_raw

    resources = {
        "postgres": FxPostgres(),
        "parquet_io": ParquetIOManager(str(tmp_path)),
        "postgres_io": CapturingPostgresIO(),
    }
    without_monday = chart_body(
        [(date(2026, 10, 8), 1.1201), (date(2026, 10, 9), 1.1213), (date(2026, 10, 13), None)],
        market_time=datetime(2026, 10, 13, 0, 0, 27, tzinfo=UTC),
    )
    CAPTURED_WRITES.clear()
    with serving(without_monday):
        result = dg.materialize(
            [fx_raw.raw_fx_spot, fx_core.fx_rate], partition_key="2026-10-12", resources=resources
        )
    assert result.success, "a day the provider left out is not a stale answer"
    meta = {
        k: v.value for k, v in result.asset_materializations_for_node("fx_rate")[0].metadata.items()
    }
    assert meta["provider_skipped_days"] == "2026-10-12"
    assert CAPTURED_WRITES == [[]], "the partition claims the day and holds no rate for it"


def test_a_night_offers_the_days_before_its_own_insert_only(tmp_path: Path) -> None:
    """A DAY YAHOO FILLS LATE ARRIVES WITH A LATER NIGHT, and never overwrites a stored one.

    The 10-09 partition's body also carries 10-07 and a 10-08 Yahoo filled late. Those go out as
    an insert that does nothing on a conflict, so a day already stated by its own partition keeps
    that partition's value; only the partition's own day is written through the manager.
    """
    from muffin_ingest_dagster.defs.fx import core as fx_core
    from muffin_ingest_dagster.defs.fx import raw as fx_raw

    resources = {
        "postgres": FxPostgres(),
        "parquet_io": ParquetIOManager(str(tmp_path)),
        "postgres_io": CapturingPostgresIO(),
    }
    filled = chart_body(
        [(date(2026, 10, 7), 1.1254), (date(2026, 10, 8), 1.1201), (date(2026, 10, 9), 1.1213)],
        market_time=datetime(2026, 10, 10, 0, 0, 30, tzinfo=UTC),
    )
    CAPTURED_WRITES.clear()
    FX_STATEMENTS.clear()
    with serving(filled):
        result = dg.materialize(
            [fx_raw.raw_fx_spot, fx_core.fx_rate], partition_key="2026-10-09", resources=resources
        )
    assert result.success
    own = {(r["currency_code"], r["as_of"]) for batch in CAPTURED_WRITES for r in batch}
    assert own == {("EUR", "2026-10-09"), ("JPY", "2026-10-09")}
    offers = [(sql, params) for sql, params in FX_STATEMENTS if sql.startswith("insert into")]
    assert len(offers) == 1
    sql, params = offers[0]
    assert sql.endswith("on conflict (currency_code, as_of) do nothing"), sql
    assert "2026-10-08" in params and "2026-10-07" in params and "2026-10-09" not in params


#: Every statement the FX lane sent its own connection, in order: the askable query, the known
#: currencies, and the re-read's insert-only offer.
FX_STATEMENTS: list[tuple[str, list[Any]]] = []


class FxConn:
    """Answers the one query `raw_fx_spot` makes, and records every statement."""

    def cursor(self) -> FxConn:
        return self

    def __enter__(self) -> FxConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql_text: str, params: Any = ()) -> None:
        FX_STATEMENTS.append((" ".join(sql_text.split()), list(params)))

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

    from muffin_ingest_dagster.defs.indices import raw as indices_raw

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
            [indices_raw.raw_index_bars],
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

    from muffin_ingest_dagster.defs.indices import core as indices_core
    from muffin_ingest_dagster.defs.indices import raw as indices_raw

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
        dg.materialize([indices_raw.raw_index_bars], partition_key=key, resources=resources)
        dg.materialize([indices_raw.raw_sector_performance], resources=resources)
        result = dg.materialize(
            [
                indices_raw.raw_index_bars,
                indices_raw.raw_sector_performance,
                indices_core.index_return,
            ],
            selection=[indices_core.index_return],
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


def test_a_session_the_provider_has_not_closed_is_not_the_newest_bar(tmp_path: Path) -> None:
    """A NaN CLOSE IS A FLOAT, AND IT TOOK SIXTY COUNTRIES' RETURNS WITH IT.

    Measured 2026-09-17: the scheduled run fired at 00:00:34 UTC, four hours after the US close, and
    yfinance returned the 09-16 session for 60 of 61 proxies with open/high/low/volume populated and
    `close: NaN`. `isinstance(close, float)` admitted it as the newest bar, `_eligible` then refused
    each whole series on `isfinite`, and the run reported `with_returns=1` while 60 scopes kept
    serving 09-15. The backfill a few hours earlier never met it: by then 09-15 had long settled.

    The unclosed session is refused like any other unusable close, so a return still comes from the
    last session that HAS one — and is stamped with that session's date, never the partition's.
    """
    from muffin_ingest.facets import indices as facet_indices
    from muffin_ingest.providers import openbb as hub
    from muffin_ingest.providers.openbb import Answer

    from muffin_ingest_dagster.defs.indices import core as indices_core
    from muffin_ingest_dagster.defs.indices import raw as indices_raw

    key = "2026-09-10"
    days = [date(2026, 8, 1) + timedelta(days=n) for n in range(41)]
    assert days[-1] == date(2026, 9, 10)

    def unclosed_today(symbols: Sequence[str], **kwargs: Any) -> Answer:
        return Answer(
            rows=[
                {
                    "symbol": "EWZ",
                    "date": d.isoformat(),
                    "open": 30.0,
                    "close": float("nan") if d == days[-1] else 30.0 + n / 10,
                }
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
    hub.price_history = unclosed_today
    hub.sector_performance = no_sectors
    facet_indices.proxied_scopes = lambda conn: [("country:BR", "EWZ")]
    try:
        dg.materialize([indices_raw.raw_index_bars], partition_key=key, resources=resources)
        dg.materialize([indices_raw.raw_sector_performance], resources=resources)
        result = dg.materialize(
            [
                indices_raw.raw_index_bars,
                indices_raw.raw_sector_performance,
                indices_core.index_return,
            ],
            selection=[indices_core.index_return],
            partition_key=key,
            resources=resources,
        )
    finally:
        hub.price_history, hub.sector_performance, facet_indices.proxied_scopes = saved

    assert result.success
    stamped = {r["as_of"] for batch in CAPTURED_WRITES for r in batch}
    assert stamped == {"2026-09-09"}, (
        f"returns stamped {sorted(stamped)} — an unclosed session must fall back to the last "
        f"close, not withhold every period for the scope"
    )
    events = result.asset_materializations_for_node(indices_core.index_return.op.name)
    metadata = {k: v.value for k, v in events[0].metadata.items()}
    assert (metadata["with_returns"], metadata["null_closes"]) == (1, 1)


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
    from muffin_ingest_dagster.defs.fx import core as fx_core
    from muffin_ingest_dagster.defs.fx import raw as fx_raw

    keys = ["2026-09-08", "2026-09-09"]
    body = chart_body([(date(2026, 9, 8), 1.11), (date(2026, 9, 9), 2.22)])

    CAPTURED_WRITES.clear()
    with serving(body):
        result = dg.materialize(
            [fx_raw.raw_fx_spot, fx_core.fx_rate],
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

    from muffin_ingest_dagster.defs.indices import raw as indices_raw

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
            [indices_raw.raw_index_bars],
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
