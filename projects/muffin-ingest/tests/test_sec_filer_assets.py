"""The SEC family's filer lane, end to end through Dagster, over bodies captured from SEC.

SEC is faked at `sec_edgar.fetch`, so each test counts the requests a run made. The database is a
recording fake that answers the lane's reads from the captured `xbrl_concept` export.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
    MAX_RUNTIME_SECONDS_TAG,
)
from muffin_ingest.facets import fiscal, sec_filings, sec_index
from muffin_ingest.providers import sec_edgar
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.sec import automation as sec_automation
from muffin_ingest_dagster.defs.sec import checks as sec_checks
from muffin_ingest_dagster.defs.sec import core as sec_core
from muffin_ingest_dagster.defs.sec import raw as sec_raw
from muffin_ingest_dagster.defs.sec.partitions import (
    FILERS_PER_RUN,
    SEC_FILER_PARTITION,
    cik_key,
    floor_day,
)
from muffin_ingest_dagster.lib import runtime
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres

FIXTURES = Path(__file__).parents[3] / "libs" / "muffin-ingest-lib" / "tests" / "fixtures" / "sec"
AAPL_SUB = (FIXTURES / "submissions_AAPL.trimmed.json").read_bytes()
TSM_SUB = (FIXTURES / "submissions_TSM.trimmed.json").read_bytes()
#: An older page's shape (Greif's): the page tests only need a body SEC would serve for one.
A_PAGE = (FIXTURES / "CIK0000043920-submissions-001.trimmed.json").read_bytes()
FACTS = json.loads((FIXTURES / "companyfacts_trimmed.json").read_bytes())
AAPL_FACTS = json.dumps(FACTS["AAPL"]).encode()
TSM_FACTS = json.dumps(FACTS["TSM"]).encode()
XBRL = json.loads((FIXTURES / "xbrl_concept.json").read_bytes())
LISTING = (FIXTURES / "daily_index_2026_QTR4_listing.json").read_bytes()
INDEX_1023 = (FIXTURES / "form.20261023.trimmed.idx").read_bytes()

AAPL, TSM, ABSENT = 320193, 1046179, 99
AAPL_PAGE, TSM_PAGE = "CIK0000320193-submissions-001.json", "CIK0001046179-submissions-001.json"


# --- SEC -----------------------------------------------------------------------------------------


class Sec:
    """Answers by URL. A URL with no answer is a test asking for something it did not expect."""

    def __init__(self) -> None:
        self.answers: dict[str, tuple[int, bytes] | Exception] = {}
        self.calls: list[str] = []

    def fetch(self, url: str, *, provider: str, timeout_s: float = 60.0) -> sec_edgar.SecAnswer:
        self.calls.append(url)
        answer = self.answers.get(url)
        if answer is None:
            raise AssertionError(f"unexpected request: {url}")
        if isinstance(answer, Exception):
            raise answer
        status, body = answer
        return sec_edgar.SecAnswer(
            document=Document(
                url=url, body=body, content_type="application/json", fetched_at=datetime.now(UTC)
            ),
            status=status,
        )

    def filer(
        self, cik: int, sub: bytes, page: tuple[str, int, bytes] | None, facts: bytes
    ) -> None:
        self.answers[sec_edgar.submissions_url(cik)] = (200, sub)
        if page is not None:
            name, status, body = page
            self.answers[sec_edgar.submissions_page_url(name)] = (status, body)
        self.answers[sec_edgar.companyfacts_url(cik)] = (200, facts)


@pytest.fixture
def sec(monkeypatch: pytest.MonkeyPatch) -> Iterator[Sec]:
    fake = Sec()
    monkeypatch.setattr(sec_edgar, "fetch", fake.fetch)
    yield fake


# --- the database --------------------------------------------------------------------------------

#: Module level: a resource's class attribute becomes a config field.
FILERS: list[tuple[int, list[str]]] = []
PROFILED: list[str] = []
CALLS: list[tuple[str, list[Any]]] = []
#: Answers for the checks' queries, by a fragment of their SQL.
ANSWERS: dict[str, list[tuple[Any, ...]]] = {}
FORMS = [
    ("10-K", True, True, False, "annual"),
    ("10-Q", True, True, False, "interim"),
    ("10-K/A", True, True, True, None),
    ("8-K", True, False, False, None),
    ("8-K/A", True, False, False, None),
    ("6-K", True, False, False, None),
    ("DEF 14A", True, False, False, None),
]
CATEGORY = {"income_statement": "income", "balance_sheet": "balance", "cash_flow": "cash"}


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
        answered = [rows for marker, rows in ANSWERS.items() if marker in text]
        if answered:
            self.rows = list(answered[0])
        elif "from market.security" in text and "group by cik" in text:
            self.rows = list(FILERS)
        elif "from market.filing_form" in text:
            self.rows = list(FORMS)
        elif "from market.xbrl_concept" in text:
            self.rows = [
                (c["metric_code"], c["taxonomy"], c["concept"], c["priority"], c["unit"])
                for c in XBRL["concepts"]
            ]
        elif "from market.metric_source_field" in text:
            self.rows = [(m, CATEGORY.get(c, "income")) for m, c in XBRL["categories"].items()]
        elif text.startswith("select code from market.currency"):
            self.rows = [("USD",), ("TWD",), ("EUR",), ("DKK",)]
        elif "from market.filer_profile" in text:
            self.rows = [(s,) for s in PROFILED]
        elif text.startswith("select market.label_fiscal_periods"):
            self.rows = [(5,)]
        elif text.startswith(("insert into", "update ", "delete from")):
            self.rowcount = 3

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        return None


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


def _filings_table(table: dict[str, Any]) -> list[dict[str, Any]]:
    """edgartools' reading of SEC's parallel arrays, in the shape `filings_table` returns. This
    job installs no extra, so it proves the code location loads without edgartools; the library's
    tests drive the real reader over the same fixtures."""
    return [
        {
            "form": table["form"][i],
            "accession_number": table["accessionNumber"][i],
            "filing_date": date.fromisoformat(table["filingDate"][i]),
            "reportDate": table["reportDate"][i],
            "isXBRL": table["isXBRL"][i],
            "primaryDocument": table["primaryDocument"][i],
        }
        for i in range(len(table["accessionNumber"]))
    ]


@pytest.fixture(autouse=True)
def _edgar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sec_edgar, "filings_table", _filings_table)


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    FILERS[:] = [(AAPL, ["s-a1", "s-a2"]), (TSM, ["s-tsm"]), (ABSENT, ["s-x"])]
    PROFILED.clear()
    CALLS.clear()
    ANSWERS.clear()
    yield


def instance_with(*keys: str) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(SEC_FILER_PARTITION, list(keys))
    return instance


def run(
    tmp_path: Path,
    instance: dg.DagsterInstance,
    keys: Sequence[str],
    *,
    stage2: bool = False,
    config: dict[str, Any] | None = None,
    facts_config: dict[str, Any] | None = None,
) -> dg.ExecuteInProcessResult:
    assets: list[Any] = [sec_raw.raw_sec_filer, *([sec_core.sec_filer_facts] if stage2 else [])]
    ops: dict[str, Any] = {"raw_sec_filer": {"config": config or {}}}
    if stage2:
        ops["sec_filer_facts"] = {"config": facts_config or {}}
    single = len(keys) == 1
    return dg.materialize(
        assets,
        instance=instance,
        partition_key=keys[0] if single else None,
        tags=None
        if single
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
        run_config={"ops": ops},
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


def stored(tmp_path: Path, asset: str, key: str) -> list[dict[str, Any]]:
    return [
        dict(r) for r in RawStore(base_path=str(tmp_path)).stored_rows_for(dg.AssetKey(asset), key)
    ]


def sums(m: dict[str, Any], document: str) -> int:
    return sum(m[f"{document}_{o}"] for o in sec_raw.FILER_OUTCOMES)


K_AAPL, K_TSM, K_ABSENT = cik_key(AAPL), cik_key(TSM), cik_key(ABSENT)


# --- when a document is due ----------------------------------------------------------------------

NOW = datetime(2026, 10, 29, 12, tzinfo=UTC)


def _row(days_ago: float, status: int = 200) -> dict[str, Any]:
    return {"fetched_at": (NOW - timedelta(days=days_ago)).isoformat(), "http_status": status}


@pytest.mark.parametrize(
    ("stored_rows", "filed_on", "force", "want"),
    [
        ([], None, False, "never"),
        ([_row(1)], None, True, "forced"),
        ([_row(29)], None, False, None),
        ([_row(31)], None, False, "floor"),
        # Fetched before the filing day ended: the filing may not be in it.
        ([_row(2)], "2026-10-27", False, "filed"),
        # Fetched after the filing day: the filing is in it already.
        ([_row(1)], "2026-10-27", False, None),
        # The NEWEST row decides: an answer after an absence is not an absence.
        ([_row(40, 404), _row(1)], None, False, None),
        ([_row(10, 404)], None, False, None),
        ([_row(31, 404)], None, False, "absent_reask"),
        # A filing does not re-ask a document SEC says is not there; the re-ask clock does.
        ([_row(10, 404)], "2026-10-28", False, None),
    ],
)
def test_a_document_is_due_for_one_reason(
    stored_rows: list[dict[str, Any]], filed_on: str | None, force: bool, want: str | None
) -> None:
    got = sec_raw.filer_due(stored_rows, now=NOW, floor_days=30, filed_on=filed_on, force=force)
    assert got == want


# --- stage 1 -------------------------------------------------------------------------------------


def test_a_first_visit_asks_both_documents_and_every_older_page(tmp_path: Path, sec: Sec) -> None:
    sec.filer(AAPL, AAPL_SUB, (AAPL_PAGE, 200, A_PAGE), AAPL_FACTS)
    sec.filer(TSM, TSM_SUB, (TSM_PAGE, 404, b""), TSM_FACTS)
    instance = instance_with(K_AAPL, K_TSM)
    result = run(tmp_path, instance, [K_AAPL, K_TSM])
    assert result.success
    assert len(sec.calls) == 6
    m = meta(result, "raw_sec_submissions")
    assert (m["submissions_answered"], m["pages_fetched"], m["pages_absent"]) == (2, 1, 1)
    assert sums(m, "submissions") == m["requested"] - m["not_askable"] == 2
    f = meta(result, "raw_sec_companyfacts")
    assert f["companyfacts_answered"] == 2 and sums(f, "companyfacts") == 2
    assert f["due_never"] == 4
    held = stored(tmp_path, "raw_sec_submissions", K_AAPL)
    assert sorted((r["document"], r["page"]) for r in held) == [
        ("submissions", None),
        ("submissions_page", AAPL_PAGE),
    ]
    [facts] = stored(tmp_path, "raw_sec_companyfacts", K_AAPL)
    assert facts["http_status"] == 200 and bytes(facts["body"]) == AAPL_FACTS
    assert set(facts) - {"body"} <= {
        "url",
        "content_type",
        "fetched_at",
        "sha256",
        "run_id",
        "http_status",
    }


def test_a_visit_with_nothing_due_asks_nothing_and_rewrites_nothing(
    tmp_path: Path, sec: Sec
) -> None:
    sec.filer(AAPL, AAPL_SUB, (AAPL_PAGE, 200, A_PAGE), AAPL_FACTS)
    instance = instance_with(K_AAPL)
    assert run(tmp_path, instance, [K_AAPL]).success
    files = sorted(tmp_path.rglob("*.parquet"))
    before = {p: (p.stat().st_mtime_ns, p.read_bytes()) for p in files}
    sec.calls.clear()

    again = run(tmp_path, instance, [K_AAPL])
    assert again.success and sec.calls == []
    assert meta(again, "raw_sec_submissions")["submissions_not_due"] == 1
    assert meta(again, "raw_sec_companyfacts")["companyfacts_not_due"] == 1
    assert {p: (p.stat().st_mtime_ns, p.read_bytes()) for p in files} == before


def test_a_filing_makes_the_submissions_due_and_a_held_page_is_not_asked_again(
    tmp_path: Path, sec: Sec
) -> None:
    sec.filer(AAPL, AAPL_SUB, (AAPL_PAGE, 200, A_PAGE), AAPL_FACTS)
    instance = instance_with(K_AAPL)
    assert run(tmp_path, instance, [K_AAPL]).success
    sec.calls.clear()

    tomorrow = (datetime.now(UTC).date() + timedelta(days=1)).isoformat()
    result = run(tmp_path, instance, [K_AAPL], config={"filed_submissions": {K_AAPL: tomorrow}})
    assert result.success
    assert sec.calls == [sec_edgar.submissions_url(AAPL)]
    m = meta(result, "raw_sec_submissions")
    assert (m["due_filed"], m["pages_held"], m["submissions_answered"]) == (1, 1, 1)
    assert meta(result, "raw_sec_companyfacts")["companyfacts_not_due"] == 1
    # The new main document replaced the old one; the page stayed.
    assert len(stored(tmp_path, "raw_sec_submissions", K_AAPL)) == 2


def test_a_404_is_filed_only_once_the_control_shows_sec_answering(tmp_path: Path, sec: Sec) -> None:
    sec.answers[sec_edgar.submissions_url(ABSENT)] = (404, b"")
    sec.answers[sec_edgar.companyfacts_url(ABSENT)] = (404, b"")
    sec.answers[sec_edgar.submissions_url(sec_edgar.CONTROL_CIK)] = (200, AAPL_SUB)
    result = run(tmp_path, instance_with(K_ABSENT), [K_ABSENT])
    assert result.success
    assert sec.calls[-1] == sec_edgar.submissions_url(sec_edgar.CONTROL_CIK)
    m = meta(result, "raw_sec_submissions")
    assert (m["control_asked"], m["control_answered"], m["submissions_absent"]) == (1, 1, 1)
    assert [r["http_status"] for r in stored(tmp_path, "raw_sec_submissions", K_ABSENT)] == [404]
    assert [r["http_status"] for r in stored(tmp_path, "raw_sec_companyfacts", K_ABSENT)] == [404]


def test_a_404_that_nothing_proves_is_counted_and_not_filed(tmp_path: Path, sec: Sec) -> None:
    sec.answers[sec_edgar.submissions_url(ABSENT)] = (404, b"")
    sec.answers[sec_edgar.companyfacts_url(ABSENT)] = (404, b"")
    sec.answers[sec_edgar.submissions_url(sec_edgar.CONTROL_CIK)] = sec_edgar.SecTransport("reset")
    result = run(tmp_path, instance_with(K_ABSENT), [K_ABSENT])
    assert result.success
    m = meta(result, "raw_sec_submissions")
    assert (m["submissions_absent"], m["submissions_absent_unproven"]) == (0, 1)
    assert sums(m, "submissions") == 1
    assert stored(tmp_path, "raw_sec_submissions", K_ABSENT) == []


def test_a_refusal_stops_the_run_and_the_rest_are_unasked(tmp_path: Path, sec: Sec) -> None:
    sec.answers[sec_edgar.submissions_url(AAPL)] = sec_edgar.SecRefused(429, "x")
    result = run(tmp_path, instance_with(K_AAPL, K_TSM), [K_AAPL, K_TSM])
    assert result.success
    assert sec.calls == [sec_edgar.submissions_url(AAPL)]
    m = meta(result, "raw_sec_submissions")
    assert (m["submissions_refused"], m["submissions_unasked"], m["throttled"]) == (1, 1, 1)
    f = meta(result, "raw_sec_companyfacts")
    assert f["companyfacts_unasked"] == 2
    assert sums(m, "submissions") == sums(f, "companyfacts") == 2


def test_a_body_that_is_not_company_facts_never_replaces_the_stored_answer(
    tmp_path: Path, sec: Sec
) -> None:
    sec.filer(AAPL, AAPL_SUB, (AAPL_PAGE, 200, A_PAGE), AAPL_FACTS)
    instance = instance_with(K_AAPL)
    assert run(tmp_path, instance, [K_AAPL]).success
    sec.answers[sec_edgar.companyfacts_url(AAPL)] = (200, b"<html>maintenance</html>")
    result = run(tmp_path, instance, [K_AAPL], config={"force": True})
    assert result.success
    assert meta(result, "raw_sec_companyfacts")["companyfacts_unreadable"] == 1
    [facts] = stored(tmp_path, "raw_sec_companyfacts", K_AAPL)
    assert bytes(facts["body"]) == AAPL_FACTS


def test_a_filer_sec_does_not_know_is_parsed_once_not_every_visit(tmp_path: Path, sec: Sec) -> None:
    """It has no profile and never will, so "a share class with no profile" must not re-read its
    absences on every visit (step 5: the unknown CIK was parsed on each one)."""
    sec.answers[sec_edgar.submissions_url(ABSENT)] = (404, b"")
    sec.answers[sec_edgar.companyfacts_url(ABSENT)] = (404, b"")
    sec.answers[sec_edgar.submissions_url(sec_edgar.CONTROL_CIK)] = (200, AAPL_SUB)
    instance = instance_with(K_ABSENT)
    first = run(tmp_path, instance, [K_ABSENT], stage2=True)
    assert meta(first, "sec_statement")["filers_parsed"] == 1
    again = run(tmp_path, instance, [K_ABSENT], stage2=True)
    assert meta(again, "sec_statement")["filers_unchanged"] == 1


def test_a_cik_no_equity_holds_is_not_askable(tmp_path: Path, sec: Sec) -> None:
    key = cik_key(12345)
    result = run(tmp_path, instance_with(key), [key])
    assert result.success and sec.calls == []
    m = meta(result, "raw_sec_submissions")
    assert (m["requested"], m["not_askable"], sums(m, "submissions")) == (1, 1, 0)


# --- stage 2 -------------------------------------------------------------------------------------


def _inserts(table: str) -> list[tuple[str, list[Any]]]:
    return [(sql, p) for sql, p in CALLS if sql.startswith(f"insert into {table} ")]


def test_a_filer_fans_out_to_every_equity_holding_its_cik(tmp_path: Path, sec: Sec) -> None:
    """Both share classes get Apple's filings, profile, calendar and statements; the filings
    writer never names a segment marker; an unchanged statement keeps its `as_of`."""
    sec.filer(AAPL, AAPL_SUB, (AAPL_PAGE, 200, A_PAGE), AAPL_FACTS)
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL], stage2=True)
    assert result.success, result.get_step_failure_events()

    [(filings_sql, filings)] = _inserts("market.security_filing")
    assert "segments_" not in filings_sql
    assert {"s-a1", "s-a2"} <= set(filings)
    assert "0000320193-25-000079" in filings, "AAPL's own 10-K"
    assert filings_sql.endswith(
        "where (stored_row.filing_date, stored_row.report_date, stored_row.report_type, "
        "stored_row.report_url, stored_row.filing_detail_url, stored_row.is_xbrl, "
        "stored_row.source_code, stored_row.disclosure_source_code) is distinct from "
        "(excluded.filing_date, excluded.report_date, excluded.report_type, "
        "excluded.report_url, excluded.filing_detail_url, excluded.is_xbrl, "
        "excluded.source_code, excluded.disclosure_source_code)"
    ), filings_sql
    [(profile_sql, profile)] = _inserts("market.filer_profile")
    assert {"s-a1", "s-a2"} <= set(profile)
    assert profile_sql.startswith("insert into market.filer_profile as stored_row (")
    assert "stored_row.as_of" not in profile_sql.split(" where ", 1)[1], "as_of is not compared"
    [(_, names)] = _inserts("market.security_former_name")
    assert names.count("APPLE COMPUTER INC") == 2, "three former names under each share class"
    [sic] = [p for q, p in CALLS if q.startswith("update market.security as")]
    assert sic.count("3571") == 2, "SEC's SIC on both share classes"

    [(statements_sql, _)] = _inserts("market.security_statement")
    assert statements_sql.endswith(
        "where (stored_row.currency, stored_row.data) is distinct from "
        "(excluded.currency, excluded.data)"
    ), statements_sql
    retractions = [
        p for sql, p in CALLS if sql.startswith("delete from market.security_statement as")
    ]
    assert [p[:2] for p in retractions] == [["s-a1", "sec-xbrl"], ["s-a2", "sec-xbrl"]]
    calendars = [p for sql, p in CALLS if sql.startswith("delete from market.fiscal_calendar")]
    assert calendars == [["s-a1"], ["s-a2"]]
    [labels] = [p for sql, p in CALLS if sql.startswith("select market.label_fiscal_periods")]
    assert sorted(labels[0]) == ["s-a1", "s-a2"]

    m = meta(result, "sec_statement")
    assert (m["filers_parsed"], m["statement_rows"] > 0, m["labelled"]) == (1, True, 5)
    assert meta(result, "sec_fiscal_calendar")["calendar_filers"] == 1


def test_a_filer_whose_documents_did_not_change_is_not_parsed(tmp_path: Path, sec: Sec) -> None:
    sec.filer(AAPL, AAPL_SUB, (AAPL_PAGE, 200, A_PAGE), AAPL_FACTS)
    instance = instance_with(K_AAPL)
    assert run(tmp_path, instance, [K_AAPL], stage2=True).success
    PROFILED[:] = ["s-a1", "s-a2"]
    CALLS.clear()

    again = run(tmp_path, instance, [K_AAPL], stage2=True)
    assert again.success
    assert meta(again, "sec_statement")["filers_unchanged"] == 1
    assert not [sql for sql, _ in CALLS if sql.startswith(("insert", "delete", "update"))]

    # A share class newly mapped to the CIK has no profile: the filer is parsed again.
    PROFILED[:] = ["s-a1"]
    third = run(tmp_path, instance, [K_AAPL], stage2=True)
    assert meta(third, "sec_statement")["filers_parsed"] == 1


def test_no_calendar_means_no_statement_is_written_or_retracted(
    tmp_path: Path, sec: Sec, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every fact would be refused as off the calendar, and a retraction over nothing produced
    deletes every statement the filer has."""
    monkeypatch.setattr(fiscal, "calendar_from_sec", lambda *a, **k: fiscal.Calendar([], []))
    sec.filer(AAPL, AAPL_SUB, (AAPL_PAGE, 200, A_PAGE), AAPL_FACTS)
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL], stage2=True)
    assert result.success
    writes = [sql for sql, _ in CALLS if sql.startswith(("insert", "delete", "update"))]
    assert writes, "the filings and profile are still written"
    assert not [w for w in writes if "security_statement" in w or "market.fiscal_" in w]
    assert meta(result, "sec_fiscal_calendar")["calendar_none"] == 1


def test_a_stored_page_that_is_not_submissions_skips_the_filer_and_writes_nothing(
    tmp_path: Path, sec: Sec
) -> None:
    sec.filer(AAPL, AAPL_SUB, (AAPL_PAGE, 200, b"not json"), AAPL_FACTS)
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL], stage2=True)
    assert result.success
    assert meta(result, "sec_filing")["submissions_unreadable"] == 1
    assert not [sql for sql, _ in CALLS if sql.startswith(("insert", "delete", "update"))]


# --- the sensors ---------------------------------------------------------------------------------


def test_new_filers_are_added_sorted_and_nothing_is_requested() -> None:
    instance = instance_with(K_TSM)
    with dg.build_sensor_context(
        instance=instance, resources={"postgres": FakePostgres()}
    ) as context:
        result = sec_automation.new_sec_filers(context)
    assert isinstance(result, dg.SensorResult) and result.run_requests == []
    [request] = result.dynamic_partitions_requests or []
    assert list(request.partition_keys) == [K_ABSENT, K_AAPL]


AMEX, FLOWERS, NETWORK = 4962, 1084869, 1993097


def test_an_index_day_makes_its_filers_and_floor_and_never_collected_due() -> None:
    forms = sec_filings.forms(Conn())
    grid = [cik_key(c) for c in (AMEX, FLOWERS, NETWORK, AAPL, TSM)]
    day = date(2026, 10, 23)
    due, sub, facts = sec_automation.filers_due(
        day,
        [INDEX_1023],
        grid,
        [AMEX, FLOWERS, NETWORK, AAPL, TSM],
        forms,
        collected=set(grid) - {cik_key(TSM)},
    )
    # A 10-Q refreshes the facts; an 8-K and a 6-K refresh only the submissions.
    assert sub == {cik_key(c): "2026-10-23" for c in (AMEX, FLOWERS, NETWORK)}
    assert facts == {cik_key(AMEX): "2026-10-23"}
    floor = {k for k in grid if floor_day(k) == day.toordinal() % 30}
    assert due == set(sub) | {cik_key(TSM)} | floor


def test_the_due_sensor_requests_each_chunk_holding_a_due_filer_once(
    tmp_path: Path, sec: Sec
) -> None:
    """Chunks are aligned to grid positions, so a filer due twice in a chunk is one run, and the
    run's config names when each of its filers filed."""
    instance = dg.DagsterInstance.ephemeral()
    sec.answers[sec_edgar.quarter_listing_url(date(2026, 10, 23))] = (200, LISTING)
    sec.answers[sec_edgar.daily_index_url(date(2026, 10, 23))] = (200, INDEX_1023)
    resources = {
        "postgres": FakePostgres(),
        "parquet_io": ParquetIOManager(str(tmp_path)),
        "raw_store": RawStore(base_path=str(tmp_path)),
    }
    assert dg.materialize(
        [sec_raw.raw_sec_daily_index], instance=instance, partition_key="2026-10-23",
        resources=resources,
    ).success  # fmt: skip

    # 120 filler filers (all collected), then AmEx and 1-800-Flowers in the third chunk.
    fillers = [cik_key(10_000_000 + n) for n in range(120)]
    grid = [*fillers, cik_key(AMEX), cik_key(FLOWERS)]
    instance.add_dynamic_partitions(SEC_FILER_PARTITION, grid)
    FILERS[:] = [(AMEX, ["s-amex"]), (FLOWERS, ["s-flowers"])]
    collected = [*fillers, cik_key(AMEX), cik_key(FLOWERS)]
    for key in collected:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=sec_raw.COMPANYFACTS, partition=key)
        )

    with dg.build_sensor_context(instance=instance, resources=resources) as context:
        result = sec_automation.sec_filers_due(context)
    assert isinstance(result, dg.SensorResult)
    assert result.cursor == "2026-10-23"
    by_start = {r.tags[ASSET_PARTITION_RANGE_START_TAG]: r for r in result.run_requests or []}
    third = by_start[grid[2 * FILERS_PER_RUN]]
    assert third.tags[ASSET_PARTITION_RANGE_END_TAG] == grid[-1]
    assert third.run_key == f"2026-10-23:{grid[2 * FILERS_PER_RUN]}"
    assert third.tags[MAX_RUNTIME_SECONDS_TAG] == runtime.SEC_RUN[MAX_RUNTIME_SECONDS_TAG]
    config = third.run_config["ops"]["raw_sec_filer"]["config"]
    assert config["filed_submissions"] == {
        cik_key(AMEX): "2026-10-23",
        cik_key(FLOWERS): "2026-10-23",
    }
    assert config["filed_companyfacts"] == {cik_key(AMEX): "2026-10-23"}
    # The other chunks are requested only for a floor day; never twice.
    floor_chunks = {
        grid.index(k) // FILERS_PER_RUN
        for k in fillers
        if floor_day(k) == date(2026, 10, 23).toordinal() % 30
    }
    assert len(result.run_requests or []) == len(floor_chunks | {2})

    # The next tick has no new index day and requests nothing.
    with dg.build_sensor_context(
        instance=instance, resources=resources, cursor=result.cursor
    ) as context:
        quiet = sec_automation.sec_filers_due(context)
    assert isinstance(quiet, dg.SensorResult) and quiet.run_requests == []


def test_a_filer_run_has_room_for_its_budget() -> None:
    budget = sec_raw.SecFilerRun().budget_seconds
    assert float(runtime.SEC_RUN[MAX_RUNTIME_SECONDS_TAG]) >= 2 * budget


# --- the checks ----------------------------------------------------------------------------------


def _result(value: Any) -> dg.AssetCheckResult:
    assert isinstance(value, dg.AssetCheckResult)
    return value


def test_a_statement_currency_the_metrics_cannot_reference_fails_whoever_wrote_it() -> None:
    ANSWERS["not exists (select 1 from market.currency"] = [("yfinance", 3)]
    failed = _result(sec_checks.statements_carry_a_known_currency(FakePostgres()))
    assert failed.passed is False and failed.severity == dg.AssetCheckSeverity.ERROR
    assert failed.metadata["rows_by_source"].value == "yfinance 3"
    ANSWERS["not exists (select 1 from market.currency"] = []
    assert _result(sec_checks.statements_carry_a_known_currency(FakePostgres())).passed


def test_a_fiscal_period_twice_fails_for_sec_and_is_counted_for_yahoo() -> None:
    """SEC rows are placed on canonical periods, so a duplicate is the lane's defect; Yahoo's are
    observed and counted until the labelling drain is measured."""
    ANSWERS["having count(*) > 1"] = [("yfinance", 4)]
    yahoo = _result(sec_checks.one_fiscal_period_one_point(FakePostgres()))
    assert yahoo.passed and yahoo.metadata["by_source"].value == "yfinance 4"
    ANSWERS["having count(*) > 1"] = [("sec-xbrl", 1), ("yfinance", 4)]
    sec = _result(sec_checks.one_fiscal_period_one_point(FakePostgres()))
    assert sec.passed is False and sec.metadata["sec_xbrl_duplicates"].value == 1


def test_an_unlabelled_sec_statement_warns() -> None:
    ANSWERS["filter (where fiscal_period is null)"] = [(2, 120)]
    result = _result(sec_checks.sec_statements_are_labelled(FakePostgres()))
    assert result.passed is False and result.severity == dg.AssetCheckSeverity.WARN
    ANSWERS["filter (where fiscal_period is null)"] = [(0, 120)]
    assert _result(sec_checks.sec_statements_are_labelled(FakePostgres())).passed


def test_a_filing_the_index_named_and_the_lane_never_wrote_is_named(
    tmp_path: Path, sec: Sec
) -> None:
    instance = dg.DagsterInstance.ephemeral()
    sec.answers[sec_edgar.quarter_listing_url(date(2026, 10, 23))] = (200, LISTING)
    sec.answers[sec_edgar.daily_index_url(date(2026, 10, 23))] = (200, INDEX_1023)
    store = RawStore(base_path=str(tmp_path))
    assert dg.materialize(
        [sec_raw.raw_sec_daily_index], instance=instance, partition_key="2026-10-23",
        resources={"parquet_io": ParquetIOManager(str(tmp_path)), "raw_store": store},
    ).success  # fmt: skip
    FILERS[:] = [(AMEX, ["s-amex"]), (FLOWERS, ["s-flowers"])]
    lines = sec_index.parse(INDEX_1023)
    [amex] = [x.accession for x in lines if x.cik == AMEX]
    [flowers] = [x.accession for x in lines if x.cik == FLOWERS]
    ANSWERS["select distinct accession_number from market.security_filing"] = [(amex,)]

    result = _result(
        sec_checks.no_filer_is_behind_its_filings(
            dg.build_asset_check_context(instance=instance), FakePostgres(), store
        )
    )
    assert result.passed is False
    assert result.metadata["filings_named"].value == 2
    assert result.metadata["first_missing"].value == f"{flowers} (2026-10-23)"
