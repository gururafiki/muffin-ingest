"""The earnings lanes, driven end to end over captured bodies and a recording fake database.

nasdaq's bodies are the library's `tests/fixtures/nasdaq` captures, Yahoo's summaries the company
lane's (`tests/fixtures/yahoo_summary_*.json.gz`), all 2026-11-21 or earlier.
"""

from __future__ import annotations

import gzip
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.providers import nasdaq
from muffin_ingest.providers.documents import Document, DocumentRefused

from muffin_ingest_dagster.defs.companies import raw as company_raw
from muffin_ingest_dagster.defs.earnings import automation as earnings_automation
from muffin_ingest_dagster.defs.earnings import core as earnings_core
from muffin_ingest_dagster.defs.earnings import partitions as earnings_partitions
from muffin_ingest_dagster.defs.earnings import raw as earnings_raw
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

OFFERED: dict[str, list[dict[str, Any]]] = {}
DELETES: list[tuple[str, list[Any]]] = []
CALLS: list[str] = []
SYMBOLS: list[tuple[str, str]] = []


def _columns(text: str) -> list[str]:
    return [c.strip() for c in text.split("(", 1)[1].split(")", 1)[0].split(",")]


class Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        CALLS.append(text)
        self.rows, self.rowcount = [], 0
        if text.startswith("insert into"):
            table = text.split()[2]
            columns = _columns(text)
            values = list(params)
            rows = [
                dict(zip(columns, values[i : i + len(columns)], strict=True))
                for i in range(0, len(values), len(columns))
            ]
            OFFERED.setdefault(table, []).extend(rows)
            self.rowcount = len(rows)
        elif text.startswith("delete from"):
            DELETES.append((text, list(params)))
            self.rowcount = 2
        elif text.startswith("select upper(symbol)"):
            self.rows = list(SYMBOLS)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        CALLS.append("commit")


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(earnings_partitions, "SECONDS_BETWEEN_REQUESTS", 0.0)
    OFFERED.clear()
    for held in (DELETES, CALLS, SYMBOLS):
        held.clear()
    yield


def body(day: str) -> bytes:
    return (FIXTURES / "nasdaq" / f"nasdaq_earnings_{day}.json").read_bytes()


class Nasdaq:
    """Nasdaq answering by date from the captures, recording each date and whether it was fresh."""

    def __init__(self, answers: dict[str, bytes | Exception]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, bool]] = []

    def fetch(self, day: date, *, fresh: bool, **_: Any) -> Document:
        self.calls.append((day.isoformat(), fresh))
        answer = self.answers[day.isoformat()]
        if isinstance(answer, Exception):
            raise answer
        return Document(
            url=nasdaq.url_for(day),
            body=answer,
            content_type="application/json",
            fetched_at=datetime.now(UTC),
        )


@contextmanager
def answering(fake: Nasdaq) -> Iterator[Nasdaq]:
    saved = nasdaq.fetch
    nasdaq.fetch = fake.fetch
    try:
        yield fake
    finally:
        nasdaq.fetch = saved


def run_days(tmp_path: Path, first: str, last: str) -> dg.ExecuteInProcessResult:
    with dg.instance_for_test() as instance:
        return dg.materialize(
            [earnings_raw.raw_nasdaq_earnings, earnings_core.nasdaq_earnings],
            instance=instance,
            tags={ASSET_PARTITION_RANGE_START_TAG: first, ASSET_PARTITION_RANGE_END_TAG: last},
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
            },
            raise_on_error=False,
        )


def meta(result: dg.ExecuteInProcessResult, key: str) -> dict[str, Any]:
    for event in result.get_asset_materialization_events():
        materialization = event.step_materialization_data.materialization
        if materialization.asset_key == dg.AssetKey(key):
            return {k: v.value for k, v in materialization.metadata.items()}
    raise AssertionError(f"{key} did not materialize")


def outcomes_sum(m: dict[str, Any]) -> int:
    return sum(int(m[o]) for o in earnings_raw.OUTCOMES)


# --- nasdaq --------------------------------------------------------------------------------------


def _recent_days(n: int) -> list[str]:
    """`n` consecutive days ending yesterday, so they sit inside the calendar's 90 days."""
    today = datetime.now(UTC).date()
    return [(today - timedelta(days=n - i)).isoformat() for i in range(n)]


def test_a_range_of_days_is_asked_one_request_each_and_written(tmp_path: Path) -> None:
    days = _recent_days(3)
    SYMBOLS[:] = [("XOM", "x"), ("DELL", "d")]
    fake = Nasdaq(
        {days[0]: body("2026-10-30"), days[1]: body("2016-06-29"), days[2]: body("2026-11-24")}
    )
    with answering(fake):
        result = run_days(tmp_path, days[0], days[-1])
    assert result.success
    assert [d for d, _ in fake.calls] == days

    m = meta(result, "raw_nasdaq_earnings")
    assert (m["answered"], m["empty"], m["unasked"]) == (2, 1, 0)
    assert outcomes_sum(m) == m["requested"] == 3

    calendar = OFFERED["market.earnings_calendar"]
    assert len(calendar) == 66 + 52
    assert {r["source_code"] for r in calendar} == {"nasdaq"}
    history = OFFERED["market.security_eps_history"]
    assert [(r["security_id"], r["surprise_pct"]) for r in history] == [("x", 8.46)]

    # THE KEYS CARRY THE PROVIDER.
    insert = next(c for c in CALLS if c.startswith("insert into market.earnings_calendar"))
    assert "on conflict (source_code, symbol, report_date)" in insert
    insert = next(c for c in CALLS if c.startswith("insert into market.security_eps_history"))
    assert "on conflict (security_id, period_ending, source_code)" in insert


def test_each_answered_day_retracts_what_it_no_longer_lists_and_old_dates_are_pruned(
    tmp_path: Path,
) -> None:
    days = _recent_days(2)
    fake = Nasdaq({days[0]: body("2026-10-30"), days[1]: body("2026-11-21")})
    with answering(fake):
        assert run_days(tmp_path, days[0], days[-1]).success
    retractions = [d for d in DELETES if "not exists" in d[0]]
    # ONE PER DAY, SCOPED TO THAT DATE AND TO NASDAQ: Yahoo's rows for the date are not touched.
    assert [(p[0], p[1]) for _, p in retractions] == [
        (date.fromisoformat(days[0]), "nasdaq"),
        (date.fromisoformat(days[1]), "nasdaq"),
    ]
    # The empty day lists no symbol, so every nasdaq row for it is retracted.
    assert retractions[1][1][2] == []
    prunes = [d for d in DELETES if "report_date < current_date" in d[0]]
    assert len(prunes) == 1


def test_a_refused_day_fails_the_run_and_nothing_is_written(tmp_path: Path) -> None:
    days = _recent_days(3)
    refused = DocumentRefused(f"{nasdaq.url_for(date.fromisoformat(days[1]))}: HTTP 403")
    fake = Nasdaq({days[0]: body("2026-10-30"), days[1]: refused, days[2]: body("2026-11-24")})
    with answering(fake):
        result = run_days(tmp_path, days[0], days[-1])
    assert not result.success
    assert [d for d, _ in fake.calls] == days, "a refusal of one day does not stop the others"
    assert not OFFERED and not DELETES
    assert not (tmp_path / "raw_nasdaq_earnings").exists()


def test_a_429_stops_the_run(tmp_path: Path) -> None:
    days = _recent_days(3)
    throttled = DocumentRefused(f"{nasdaq.url_for(date.fromisoformat(days[1]))}: HTTP 429")
    fake = Nasdaq({days[0]: body("2026-10-30"), days[1]: throttled, days[2]: body("2026-11-24")})
    with answering(fake):
        result = run_days(tmp_path, days[0], days[-1])
    assert not result.success
    assert [d for d, _ in fake.calls] == days[:2]


def test_the_window_is_asked_past_the_cache_and_older_days_through_it(tmp_path: Path) -> None:
    today = datetime.now(UTC).date()
    old = (today - timedelta(days=200)).isoformat()
    edge = (today - timedelta(days=earnings_partitions.WINDOW_PAST_DAYS)).isoformat()
    before = (today - timedelta(days=earnings_partitions.WINDOW_PAST_DAYS + 1)).isoformat()
    for day in (old, before, edge):
        fake = Nasdaq({day: body("2016-06-29")})
        with answering(fake):
            assert run_days(tmp_path, day, day).success
        assert fake.calls == [(day, day == edge)]


def test_the_schedule_asks_the_past_week_and_the_next_three_in_one_run() -> None:
    tick = datetime(2026, 11, 21, 7, 10, tzinfo=UTC)
    with (
        dg.instance_for_test() as instance,
        dg.build_schedule_context(instance=instance, scheduled_execution_time=tick) as context,
    ):
        requests = earnings_automation.nasdaq_earnings_window(context)
    assert isinstance(requests, list)
    (request,) = requests
    assert isinstance(request, dg.RunRequest)
    assert request.run_key == "nasdaq_earnings_window:2026-11-21"
    assert request.tags[ASSET_PARTITION_RANGE_START_TAG] == "2026-11-14"
    assert request.tags[ASSET_PARTITION_RANGE_END_TAG] == "2026-12-12"
    # The grid reaches the window's last day, or the run would name a partition that does not exist.
    keys = earnings_partitions.nasdaq_days.get_partition_keys(
        current_time=datetime(2026, 11, 21, 7, 10, tzinfo=UTC)
    )
    assert keys[0] == "2012-01-01" and keys[-1] == "2026-12-12"


# --- Yahoo ---------------------------------------------------------------------------------------

AAPL = "11111111-1111-1111-1111-111111111111"
TOYOTA = "22222222-2222-2222-2222-222222222222"
NESTLE = "33333333-3333-3333-3333-333333333333"


def _store_summary(tmp_path: Path, sid: str, symbol: str) -> None:
    raw = gzip.decompress((FIXTURES / f"yahoo_summary_{symbol}.json.gz").read_bytes())
    folder = tmp_path / "raw_yahoo_summary"
    folder.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "url": [f"u/{symbol}"],
            "body": [raw],
            "content_type": ["application/json"],
            "sha256": ["x"],
            "fetched_at": [datetime(2026, 11, 21, tzinfo=UTC).isoformat()],
            "run_id": ["r"],
            "security_id": [sid],
            "asked_symbol": [symbol],
            "provider": ["yahoo"],
        }
    )
    pq.write_table(table, folder / f"{sid}.parquet")


def test_yahoo_earnings_reads_the_stored_summaries_and_replaces_each_company_s_dates(
    tmp_path: Path,
) -> None:
    for sid, symbol in ((AAPL, "AAPL"), (TOYOTA, "7203.T"), (NESTLE, "NESN.SW")):
        _store_summary(tmp_path, sid, symbol)
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(
            prices_partitions.SECURITY_PARTITION, [AAPL, TOYOTA, NESTLE]
        )
        result = dg.materialize(
            [company_raw.raw_yahoo_company, earnings_core.yahoo_earnings],
            selection=[earnings_core.yahoo_earnings],
            instance=instance,
            tags={ASSET_PARTITION_RANGE_START_TAG: AAPL, ASSET_PARTITION_RANGE_END_TAG: NESTLE},
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
            },
        )
    assert result.success
    calendar = OFFERED.get("market.earnings_calendar", [])
    history = OFFERED["market.security_eps_history"]
    assert {r["source_code"] for r in calendar + history} == {"yfinance"}
    assert len(history) == 8, "four quarters each for Apple and Toyota, none for Nestlé"

    # EVERY ANSWERED COMPANY'S YAHOO DATES ARE REPLACED, Nestlé's (which states none) included.
    retractions = [p for t, p in DELETES if "not exists" in t]
    assert sorted((p[0], p[1]) for p in retractions) == sorted(
        (sid, "yfinance") for sid in (AAPL, TOYOTA, NESTLE)
    )
