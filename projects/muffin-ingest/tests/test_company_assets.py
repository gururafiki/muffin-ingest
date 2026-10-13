"""The company lane, driven end to end over Yahoo's own bytes and a recording fake database.

Driven rather than inspected, as the price tests are: what this lane exists to get right — asking
only what is due, never reading a refusal as an absence, counting every company exactly once, and
writing each table from one document in one transaction — is invisible to a reading of the code.
The bodies are the captures in the library's `tests/fixtures` (`capture.md`, 2026-10-10/11).
"""

from __future__ import annotations

import gzip
import itertools
import json
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
    RUN_KEY_TAG,
    SCHEDULE_NAME_TAG,
)
from muffin_ingest.facets import company
from muffin_ingest.providers import yahoo_chart, yahoo_session, yahoo_summary, yahoo_timeseries
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.companies import automation as company_automation
from muffin_ingest_dagster.defs.companies import core as company_core
from muffin_ingest_dagster.defs.companies import derived as company_derived
from muffin_ingest_dagster.defs.companies import partitions as company_partitions
from muffin_ingest_dagster.defs.companies import raw as company_raw
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, PostgresIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

AAPL = ("11111111-1111-1111-1111-111111111111", "AAPL", 5.0)
VOD = ("22222222-2222-2222-2222-222222222222", "VOD.L", 4.0)
NBK = ("33333333-3333-3333-3333-333333333333", "NBK.KW", 3.0)
DEAD = ("44444444-4444-4444-4444-444444444444", "BDMS-F.BK", 2.0)


def summary_body(symbol: str) -> bytes:
    return gzip.decompress((FIXTURES / f"yahoo_summary_{symbol}.json.gz").read_bytes())


def timeseries_body(symbol: str, frequency: str) -> bytes:
    return gzip.decompress(
        (FIXTURES / f"yahoo_timeseries_{symbol}_{frequency}.json.gz").read_bytes()
    )


def stating(symbol: str, quarter: date) -> bytes:
    """A captured summary naming a newer most recent quarter, for a company that has reported."""
    parsed = json.loads(summary_body(symbol))
    stamp = int(datetime(quarter.year, quarter.month, quarter.day, tzinfo=UTC).timestamp())
    parsed["quoteSummary"]["result"][0]["defaultKeyStatistics"]["mostRecentQuarter"] = stamp
    return json.dumps(parsed).encode()


# --- Yahoo ---------------------------------------------------------------------------------------

Answer = bytes | Exception | Callable[[], "bytes | Exception"]


class Yahoo:
    """Yahoo answering from captured bodies, recording every request it was sent, in order."""

    def __init__(
        self,
        summaries: dict[str, Answer],
        statements: dict[str, Answer] | None = None,
        sessions: Sequence[Exception | None] = (),
    ) -> None:
        self.summaries = summaries
        self.statements = statements or {}
        self.sessions = list(sessions)
        self.calls: list[str] = []

    def _answer(self, answer: Answer) -> bytes:
        if callable(answer) and not isinstance(answer, Exception):
            answer = answer()
        if isinstance(answer, Exception):
            raise answer
        return answer

    def open_session(self, **_: Any) -> yahoo_session.Session:
        self.calls.append("session")
        refused = self.sessions.pop(0) if self.sessions else None
        if refused is not None:
            raise refused
        return yahoo_session.Session(cookie="A3=c", crumb="k", opened_at=datetime.now(UTC))

    def summary(self, symbol: str, session: yahoo_session.Session, **_: Any) -> Document:
        self.calls.append(f"summary {symbol}")
        return Document(
            url=f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?crumb=redacted",
            body=self._answer(self.summaries[symbol]),
            content_type="application/json",
            fetched_at=datetime.now(UTC),
        )

    def timeseries(self, symbol: str, frequency: str, **_: Any) -> Document:
        self.calls.append(f"{frequency} {symbol}")
        key = f"{frequency} {symbol}"
        answer = (
            self.statements[key] if key in self.statements else timeseries_body(symbol, frequency)
        )
        return Document(
            url=f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}",
            body=self._answer(answer),
            content_type="application/json",
            fetched_at=datetime.now(UTC),
        )


@contextmanager
def yahoo(fake: Yahoo) -> Iterator[Yahoo]:
    saved = (yahoo_session.open_session, yahoo_summary.fetch, yahoo_timeseries.fetch)
    yahoo_session.open_session = fake.open_session
    yahoo_summary.fetch = fake.summary
    yahoo_timeseries.fetch = fake.timeseries
    try:
        yield fake
    finally:
        yahoo_session.open_session, yahoo_summary.fetch, yahoo_timeseries.fetch = saved


# --- the database --------------------------------------------------------------------------------

#: The askable universe, the statements the fake database saw, and the rows it was offered.
#: MODULE LEVEL, as in the price tests: a class attribute on a `ConfigurableResource` becomes a
#: config field.
UNIVERSE: list[tuple[str, str, float]] = []
CALLS: list[tuple[str, list[Any]]] = []
OFFERED: dict[str, list[dict[str, Any]]] = {}
COMMITS: list[int] = []
NODES: dict[str, str] = {}
#: The two backlogs the derivation drains, as successive counts.
BACKLOGS: dict[str, list[int]] = {}

CURRENCIES = ["USD", "EUR", "GBP", "GBX", "KWD", "KWF"]
COUNTRIES = [("united states", "US", 0), ("united kingdom", "GB", 0), ("kuwait", "KW", 0)]
SECTORS = [
    ("information-technology", "a0000000-0000-0000-0000-000000000001"),
    ("communication-services", "a0000000-0000-0000-0000-000000000002"),
    ("financials", "a0000000-0000-0000-0000-000000000003"),
]


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
        CALLS.append((text, list(params)))
        self.rows, self.rowcount = [], 0
        if text.startswith("select code from market.currency"):
            self.rows = [(c,) for c in CURRENCIES]
        elif text.startswith("select lower(trim(name))"):
            self.rows = list(COUNTRIES)
        elif "from market.taxonomy_node where taxonomy_id = %s and level = 1" in text:
            self.rows = list(SECTORS)
        elif text.startswith("insert into market.taxonomy_node"):
            new = [c for c in params[1] if c not in NODES]
            NODES.update({c: str(uuid.uuid5(uuid.NAMESPACE_URL, c)) for c in new})
            self.rowcount = len(new)
        elif "from market.taxonomy_node" in text and "code = any" in text:
            self.rows = [(c, NODES[c]) for c in params[1] if c in NODES]
        elif text.startswith("insert into"):
            table = text.split()[2]
            columns = _columns(text)
            # THE GUARD'S VALUE FOLLOWS THE ROWS (`only_replacing`), one per guarded column.
            count = text.split(" values ", 1)[1].split(" on conflict", 1)[0].count("(")
            values = list(params)[: count * len(columns)]
            rows = [
                dict(zip(columns, values[i : i + len(columns)], strict=True))
                for i in range(0, len(values), len(columns))
            ]
            OFFERED.setdefault(table, []).extend(rows)
            self.rowcount = len(rows)
        elif text.startswith("update"):
            table = text.split()[1]
            columns = [c.strip() for c in text.split(" as v (", 1)[1].split(")", 1)[0].split(",")]
            values = list(params)
            rows = [
                dict(zip(columns, values[i : i + len(columns)], strict=True))
                for i in range(0, len(values), len(columns))
            ]
            OFFERED.setdefault(f"update {table}", []).extend(rows)
            self.rowcount = len(rows)
        elif text.startswith("delete from"):
            self.rows = []
        elif text.startswith("select count(*) from"):
            name = "underived" if "derived_at is null" in text else "pending_ttm"
            counts = BACKLOGS.get(name, [0])
            self.rows = [(counts.pop(0) if len(counts) > 1 else counts[0],)]
        elif text.startswith("select market.derive"):
            self.rows = [(100,)]
        else:
            # The askable-subjects query, answered with the universe this test set.
            self.rows = list(UNIVERSE)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        COMMITS.append(len(CALLS))


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(prices_partitions, "CHART_SECONDS_BETWEEN_REQUESTS", 0.0)
    for held in (UNIVERSE, CALLS, COMMITS):
        held.clear()
    for table in (OFFERED, NODES, BACKLOGS):
        table.clear()
    yield


def visit(
    tmp_path: Path,
    subjects: list[tuple[str, str, float]],
    *,
    assets: Sequence[Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dg.ExecuteInProcessResult:
    """One run of the company job over `subjects` as a range, the shape a day's run takes."""
    UNIVERSE[:] = subjects
    keys = [sid for sid, _, _ in subjects]
    chosen = list(assets or [company_raw.raw_yahoo_company, company_core.company_facts])
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, keys)
        single = len(keys) == 1
        return dg.materialize(
            chosen,
            instance=instance,
            partition_key=keys[0] if single else None,
            tags=None
            if single
            else {
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
            },
            run_config={"ops": {"raw_yahoo_company": {"config": config}}} if config else None,
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres_io": PostgresIOManager(postgres=FakePostgres()),
            },
        )


def meta(result: dg.ExecuteInProcessResult, key: str) -> dict[str, Any]:
    """The metadata of `key`'s first materialization (a range run puts the same on all)."""
    for event in result.get_asset_materialization_events():
        materialization = event.step_materialization_data.materialization
        if materialization.asset_key == dg.AssetKey(key):
            return {k: v.value for k, v in materialization.metadata.items()}
    raise AssertionError(f"{key} did not materialize")


def stored(tmp_path: Path, asset: str, sid: str) -> list[tuple[Any, ...]]:
    path = tmp_path / asset / f"{sid}.parquet"
    columns = (
        "frequency, fetched_at" if asset == "raw_yahoo_statements" else "asked_symbol, fetched_at"
    )
    return duckdb.sql(
        f"select {columns} from read_parquet('{path}', union_by_name=true) "
        f"where fetched_at is not null order by fetched_at"
    ).fetchall()


def sums(m: dict[str, Any], prefix: str, outcomes: Sequence[str]) -> int:
    return sum(int(m[f"{prefix}_{o}"]) for o in outcomes) + int(m["not_askable"])


def summaries_sum(m: dict[str, Any]) -> int:
    return sums(m, "summary", company_raw.SUMMARY_OUTCOMES)


def statements_sum(m: dict[str, Any]) -> int:
    return sums(m, "statements", company_raw.STATEMENT_OUTCOMES)


# --- a first visit, and the one after it ---------------------------------------------------------


def test_a_first_visit_asks_the_summary_and_then_both_statements(tmp_path: Path) -> None:
    fake = Yahoo({"AAPL": summary_body("AAPL"), "VOD.L": summary_body("VOD.L")})
    with yahoo(fake):
        result = visit(tmp_path, [AAPL, VOD])
    assert result.success

    # ONE SESSION, THEN EACH COMPANY'S SUMMARY BEFORE ITS STATEMENTS, which read the fresh summary.
    assert fake.calls == [
        "session",
        "summary AAPL",
        "annual AAPL",
        "quarterly AAPL",
        "summary VOD.L",
        "annual VOD.L",
        "quarterly VOD.L",
    ]
    m = meta(result, "raw_yahoo_summary")
    assert m["requested"] == 2 and summaries_sum(m) == 2
    assert m["summary_answered"] == 2 and m["calls"] == 6 and m["sessions"] == 1
    s = meta(result, "raw_yahoo_statements")
    assert statements_sum(s) == 2 and s["statements_fetched"] == 2
    assert s["statements_reason_never_fetched"] == 2

    assert [r[0] for r in stored(tmp_path, "raw_yahoo_summary", AAPL[0])] == ["AAPL"]
    assert sorted(r[0] for r in stored(tmp_path, "raw_yahoo_statements", VOD[0])) == [
        "annual",
        "quarterly",
    ]


def test_a_second_visit_inside_the_week_asks_nothing_and_keeps_what_is_stored(
    tmp_path: Path,
) -> None:
    with yahoo(Yahoo({"AAPL": summary_body("AAPL")})):
        assert visit(tmp_path, [AAPL]).success
    fake = Yahoo({"AAPL": RuntimeError("asked again")})
    with yahoo(fake):
        result = visit(tmp_path, [AAPL])
    assert result.success
    assert fake.calls == [], "nothing was due, so not even a session was opened"
    m = meta(result, "raw_yahoo_summary")
    assert m["summary_not_due"] == 1 and summaries_sum(m) == 1
    s = meta(result, "raw_yahoo_statements")
    assert s["statements_not_due"] == 1 and statements_sum(s) == 1
    assert len(stored(tmp_path, "raw_yahoo_summary", AAPL[0])) == 1
    assert len(stored(tmp_path, "raw_yahoo_statements", AAPL[0])) == 2


def test_a_new_period_named_by_the_summary_brings_the_statements_forward(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AAPL's captured summary names the quarter its timeseries already holds, so a visit a week
    later asks for the summary only. The same visit after Apple reports asks for the statements."""
    with yahoo(Yahoo({"AAPL": summary_body("AAPL")})):
        assert visit(tmp_path, [AAPL]).success

    later = date.today() + timedelta(days=8)

    class Later(date):
        @classmethod
        def today(cls) -> Later:
            return cls(later.year, later.month, later.day)

    monkeypatch.setattr(company_raw, "date", Later)
    fake = Yahoo({"AAPL": summary_body("AAPL")})
    with yahoo(fake):
        result = visit(tmp_path, [AAPL])
    assert fake.calls == ["session", "summary AAPL"], "the period it names is held"
    assert meta(result, "raw_yahoo_statements")["statements_not_due"] == 1

    fake = Yahoo({"AAPL": stating("AAPL", date(2026, 9, 27))})
    with yahoo(fake), pytest.MonkeyPatch.context() as m:
        m.setattr(company, "summary_due", lambda *a, **k: True)
        result = visit(tmp_path, [AAPL])
    assert fake.calls == ["session", "summary AAPL", "annual AAPL", "quarterly AAPL"]
    s = meta(result, "raw_yahoo_statements")
    assert s["statements_reason_new_period"] == 1 and s["statements_fetched"] == 1


# --- what is not an answer -----------------------------------------------------------------------


def test_an_absent_summary_skips_the_statements_and_cannot_erase_a_stored_answer(
    tmp_path: Path,
) -> None:
    answered = Yahoo(
        {"BDMS-F.BK": summary_body("AAPL")},
        {
            "annual BDMS-F.BK": timeseries_body("AAPL", "annual"),
            "quarterly BDMS-F.BK": timeseries_body("AAPL", "quarterly"),
        },
    )
    with yahoo(answered):
        assert visit(tmp_path, [DEAD]).success
    fake = Yahoo({"BDMS-F.BK": summary_body("BDMS-F.BK")})
    with yahoo(fake), pytest.MonkeyPatch.context() as m:
        m.setattr(company, "summary_due", lambda *a, **k: True)
        result = visit(tmp_path, [DEAD])
    assert fake.calls == ["session", "summary BDMS-F.BK"], "no statements for a missing symbol"
    m_ = meta(result, "raw_yahoo_summary")
    assert m_["summary_absent"] == 1 and summaries_sum(m_) == 1
    assert meta(result, "raw_yahoo_statements")["statements_skipped"] == 1
    # FILED BESIDE THE ANSWER, NOT OVER IT: stage 2 reads the newest, which is the absence, and so
    # writes nothing; the answer is still there for the day Yahoo answers again.
    assert len(stored(tmp_path, "raw_yahoo_summary", DEAD[0])) == 2


def test_a_throttle_stops_the_run_and_every_company_after_it_is_unasked(tmp_path: Path) -> None:
    refusal = yahoo_chart.refusal(429, "quoteSummary")
    fake = Yahoo({"AAPL": summary_body("AAPL"), "VOD.L": refusal, "NBK.KW": summary_body("NBK.KW")})
    with yahoo(fake):
        result = visit(tmp_path, [AAPL, VOD, NBK])
    assert "summary NBK.KW" not in fake.calls
    m = meta(result, "raw_yahoo_summary")
    assert (m["summary_answered"], m["summary_unasked"], m["throttled"]) == (1, 2, 1)
    assert summaries_sum(m) == 3
    s = meta(result, "raw_yahoo_statements")
    assert (s["statements_fetched"], s["statements_unasked"]) == (1, 2) and statements_sum(s) == 3


def test_a_throttle_between_the_two_frequencies_files_the_first_and_counts_the_company_partial(
    tmp_path: Path,
) -> None:
    fake = Yahoo(
        {"AAPL": summary_body("AAPL"), "VOD.L": summary_body("VOD.L")},
        {"quarterly AAPL": yahoo_chart.refusal(429, "timeseries")},
    )
    with yahoo(fake):
        result = visit(tmp_path, [AAPL, VOD])
    m = meta(result, "raw_yahoo_summary")
    assert (m["summary_answered"], m["summary_unasked"]) == (1, 1) and summaries_sum(m) == 2
    s = meta(result, "raw_yahoo_statements")
    assert (s["statements_partial"], s["statements_unasked"]) == (1, 1) and statements_sum(s) == 2
    assert [r[0] for r in stored(tmp_path, "raw_yahoo_statements", AAPL[0])] == ["annual"]


def test_a_transport_fault_is_counted_and_the_run_goes_on(tmp_path: Path) -> None:
    fake = Yahoo({"AAPL": yahoo_chart.YahooRefused("ReadTimeout"), "VOD.L": summary_body("VOD.L")})
    with yahoo(fake):
        result = visit(tmp_path, [AAPL, VOD])
    m = meta(result, "raw_yahoo_summary")
    assert (m["summary_transport"], m["summary_answered"]) == (1, 1) and summaries_sum(m) == 2
    s = meta(result, "raw_yahoo_statements")
    assert (s["statements_skipped"], s["statements_fetched"]) == (1, 1)


def test_a_refused_crumb_is_renewed_once_and_a_second_refusal_stops_the_run(
    tmp_path: Path,
) -> None:
    rejected = yahoo_summary.CrumbRejected("HTTP 401: the session was refused")
    answers: Iterator[bytes | Exception] = iter([rejected, summary_body("AAPL")])
    fake = Yahoo({"AAPL": lambda: next(answers), "VOD.L": rejected})
    with yahoo(fake):
        result = visit(tmp_path, [AAPL, VOD])
    assert fake.calls[:3] == ["session", "summary AAPL", "session"]
    m = meta(result, "raw_yahoo_summary")
    assert (m["sessions"], m["crumb_rejected"]) == (2, 2)
    assert (m["summary_answered"], m["summary_unasked"]) == (1, 1) and summaries_sum(m) == 2


def test_a_session_yahoo_will_not_open_stops_the_run_before_any_company(tmp_path: Path) -> None:
    fake = Yahoo(
        {"AAPL": summary_body("AAPL")},
        sessions=[yahoo_session.SessionRefused("crumb: HTTP 429")],
    )
    with yahoo(fake):
        result = visit(tmp_path, [AAPL])
    m = meta(result, "raw_yahoo_summary")
    assert (m["summary_unasked"], m["throttled"]) == (1, 1) and summaries_sum(m) == 1


def test_force_asks_everything_whatever_raw_holds(tmp_path: Path) -> None:
    with yahoo(Yahoo({"AAPL": summary_body("AAPL")})):
        assert visit(tmp_path, [AAPL]).success
    fake = Yahoo({"AAPL": summary_body("AAPL")})
    with yahoo(fake):
        result = visit(tmp_path, [AAPL], config={"force": True})
    assert fake.calls == ["session", "summary AAPL", "annual AAPL", "quarterly AAPL"]
    assert meta(result, "raw_yahoo_statements")["statements_reason_forced"] == 1


# --- stage 2 -------------------------------------------------------------------------------------


def test_stage_2_writes_each_table_from_the_newest_summary_in_one_transaction(
    tmp_path: Path,
) -> None:
    with yahoo(Yahoo({"AAPL": summary_body("AAPL"), "VOD.L": summary_body("VOD.L")})):
        result = visit(tmp_path, [AAPL, VOD])
    assert result.success

    fundamentals = {r["security_id"]: r for r in OFFERED["market.security_fundamentals"]}
    # VODAFONE QUOTES IN PENCE: its cap is labelled in pounds, its targets in pence.
    assert fundamentals[VOD[0]]["market_cap_currency"] == "GBP"
    estimates = {r["security_id"]: r for r in OFFERED["market.security_estimate"]}
    assert estimates[VOD[0]]["currency_code"] == "GBX"
    assert {r["security_id"] for r in OFFERED["market.security_profile"]} == {AAPL[0], VOD[0]}
    assert {r["security_id"] for r in OFFERED["market.security_officer"]} == {AAPL[0], VOD[0]}

    countries = {
        r["security_id"]: r["provider_country_iso2"]
        for r in OFFERED["update market.security"]
        if "provider_country_iso2" in r
    }
    assert countries == {AAPL[0]: "US", VOD[0]: "GB"}
    reporting = {
        r["security_id"]: r["reporting_currency"]
        for r in OFFERED["update market.security"]
        if "reporting_currency" in r
    }
    assert reporting == {AAPL[0]: "USD", VOD[0]: "EUR"}

    taxonomy = OFFERED["market.security_taxonomy"]
    assert {r["node_id"] for r in taxonomy if r["security_id"] == AAPL[0]} == {
        SECTORS[0][1],
        NODES["information-technology--consumer-electronics"],
    }

    # ONE COMMIT FOR THE WHOLE STEP, after its last write: a company's facts never land half from
    # one document and half from the last. Stage 1 reads the universe and commits nothing.
    writes = [i for i, (sql, _) in enumerate(CALLS, 1) if sql.split()[0] in ("insert", "update")]
    assert len(COMMITS) == 1 and COMMITS[0] >= max(writes)

    profile = meta(result, "security_profile")
    assert profile["companies"] == 2 and profile["countries_unresolved"] == 0
    assert meta(result, "yahoo_classification")["nodes_created"] == 2


def test_yahoo_statements_keep_sec_rows_and_merge_into_the_stored_lines(tmp_path: Path) -> None:
    """THE GUARD AND THE MERGE ARE METADATA ON THE ASSET, read by the Postgres manager: a dict and a
    list survive definition metadata, and the SQL the writer sends carries both."""
    with yahoo(Yahoo({"AAPL": summary_body("AAPL")})):
        result = visit(
            tmp_path,
            [AAPL],
            assets=[company_raw.raw_yahoo_company, company_core.yahoo_statement],
        )
    assert result.success
    inserts = [sql for sql, _ in CALLS if sql.startswith("insert into market.security_statement")]
    assert inserts, "no statements were written"
    assert all("stored_row.source_code = %s" in sql for sql in inserts)
    assert all("jsonb_strip_nulls(excluded.data)" in sql for sql in inserts)
    rows = OFFERED["market.security_statement"]
    assert {r["currency"] for r in rows} == {"USD"}
    assert {r["period_type"] for r in rows} == {"annual", "quarter"}


# --- stage 3 -------------------------------------------------------------------------------------


def derive(config: dict[str, Any] | None = None) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [company_derived.derived_security_metrics],
        resources={"postgres": FakePostgres()},
        run_config={"ops": {"derived_security_metrics": {"config": config}}} if config else None,
    )


def test_the_derivation_pages_until_each_backlog_drains_one_transaction_a_page() -> None:
    BACKLOGS["underived"] = [5000, 3000, 1000, 0]
    BACKLOGS["pending_ttm"] = [1500, 500, 0]
    result = derive()
    assert result.success
    m = meta(result, "derived_security_metrics")
    assert (m["metrics_pages"], m["metrics_stopped"], m["metrics_backlog_after"]) == (
        3,
        "drained",
        0,
    )
    assert (m["ttm_pages"], m["ttm_stopped"]) == (2, "drained")
    calls = [sql for sql, _ in CALLS if sql.startswith("select market.derive")]
    assert calls[:3] == ["select market.derive_security_metrics(%s)"] * 3
    assert calls[3:] == ["select market.derive_ttm(null::uuid, %s)"] * 2
    # EACH CALL COMMITS BEFORE THE NEXT: the functions' temporary tables drop on commit.
    derive_positions = [i for i, (sql, _) in enumerate(CALLS, 1) if sql.startswith("select market")]
    for here, after in itertools.pairwise(derive_positions):
        assert any(here <= c < after for c in COMMITS), "two calls in one transaction"


def test_a_page_that_does_not_shrink_the_backlog_stops_the_loop() -> None:
    BACKLOGS["underived"] = [800, 800]
    BACKLOGS["pending_ttm"] = [0]
    m = meta(derive(), "derived_security_metrics")
    assert (m["metrics_pages"], m["metrics_stopped"]) == (1, "no_progress")
    assert (m["ttm_pages"], m["ttm_stopped"]) == (0, "drained")


# --- the rotation --------------------------------------------------------------------------------


def tick(instance: dg.DagsterInstance, when: datetime) -> Any:
    return company_automation.yahoo_companies(
        dg.build_schedule_context(instance=instance, scheduled_execution_time=when)
    )


def launched(instance: dg.DagsterInstance, requests: Sequence[dg.RunRequest]) -> None:
    for request in requests:
        assert request.run_key is not None
        instance.add_run(
            dg.DagsterRun(
                job_name=company_automation.COMPANY_SCHEDULE,
                run_id=str(uuid.uuid4()),
                status=dg.DagsterRunStatus.SUCCESS,
                tags={
                    **request.tags,
                    SCHEDULE_NAME_TAG: company_automation.COMPANY_SCHEDULE,
                    RUN_KEY_TAG: request.run_key,
                },
            )
        )


def _span(request: dg.RunRequest, keys: Sequence[str]) -> tuple[int, int]:
    return (
        keys.index(request.tags[ASSET_PARTITION_RANGE_START_TAG]),
        keys.index(request.tags[ASSET_PARTITION_RANGE_END_TAG]),
    )


def test_a_day_asks_a_seventh_of_the_grid_and_the_next_day_starts_after_it() -> None:
    keys = [f"s{i:04d}" for i in range(1000)]
    width = company_partitions.COMPANY_PARTITIONS_PER_RUN
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, keys)
        first = tick(instance, datetime(2026, 10, 14, 3, 30, tzinfo=UTC))
        assert isinstance(first, list)
        spans = [_span(r, keys) for r in first]
        assert sum(hi - lo + 1 for lo, hi in spans) == 143, "ceil(1000 / 7)"
        assert all(hi - lo + 1 <= width for lo, hi in spans), "a run is at most the width"
        assert all(b[0] == (a[1] + 1) % len(keys) for a, b in itertools.pairwise(spans))
        launched(instance, first)

        second = tick(instance, datetime(2026, 10, 15, 3, 30, tzinfo=UTC))
        assert isinstance(second, list)
        assert _span(second[0], keys)[0] == (spans[-1][1] + 1) % len(keys)
        assert {r.tags[company_automation.COMPANY_DAY_TAG] for r in second} == {"2026-10-15"}


def test_the_schedule_ships_stopped() -> None:
    """The first rotation starts by decision, after the live run on a tiny subset (step 7)."""
    schedule = company_automation.yahoo_companies
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
