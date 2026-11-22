"""The segments family's China lane, end to end through Dagster.

CNINFO is faked at the provider (`cninfo.announcements`, `cninfo.report`), so the lane's own choice
of what to fetch runs and each test counts the requests a run made. The report reader is faked at
`cn_child.read`: the PDF reader has its own tests (`test_cn_pdf.py`) and its parity is measured on
real reports, so here a report is bytes that read as Yangtze Power's industry table. The database
is a fake that HOLDS `security_filing` and its marks: a report a run reads stops being due.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import cn_child, cn_pdf
from muffin_ingest.facets import cninfo as cn
from muffin_ingest.facets.cn_pdf import CnSegmentRow
from muffin_ingest.providers import cninfo
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.segments import automation, core, raw
from muffin_ingest_dagster.defs.segments.partitions import CNINFO_FILER_PARTITION, FLOOR_DAYS
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

VOCABULARY = json.loads((FIXTURES / "segments" / "vocabulary.json").read_bytes())

YANGTZE, LONGI = "gssh0600900", "gssh0601012"
STATIC = "https://static.cninfo.com.cn/finalpage"
FY25, FY24 = f"{STATIC}/2026-04-30/1225262036.PDF", f"{STATIC}/2025-04-29/1223196105.PDF"
SUMMARY = f"{STATIC}/2026-04-30/1225262035.PDF"
DATA_FILE = f"{STATIC}/2020-04-30/cninfo1207701730.js"
LONGI_FY25 = f"{STATIC}/2026-04-29/1225236001.PDF"
PDF = b"%PDF-1.7 a report" + b"." * 100


def announcement(url: str, title: str, day: str) -> dict[str, Any]:
    midnight_beijing = datetime.fromisoformat(f"{day}T00:00:00+08:00")
    return {
        "announcementTitle": title,
        "adjunctUrl": url.split("static.cninfo.com.cn/", 1)[1],
        "announcementTime": int(midnight_beijing.timestamp() * 1000),
        "adjunctSize": 1499,
    }


SEARCHES: dict[str, list[dict[str, Any]]] = {
    YANGTZE: [
        announcement(FY25, "长江电力2025年年度报告", "2026-04-30"),
        announcement(SUMMARY, "长江电力2025年年度报告摘要", "2026-04-30"),
        announcement(FY24, "长江电力2024年年度报告", "2025-04-29"),
        announcement(DATA_FILE, "2019年年度主要财务指标", "2020-04-30"),
    ],
    LONGI: [announcement(LONGI_FY25, "隆基绿能2025年年度报告", "2026-04-29")],
}


class Cninfo:
    """Answers by request. A request with no answer is a test asking for something it did not
    expect."""

    def __init__(self) -> None:
        self.searches: dict[str, tuple[int, bytes] | Exception] = {
            org: (200, json.dumps({"announcements": rows}).encode())
            for org, rows in SEARCHES.items()
        }
        self.reports: dict[str, tuple[int, bytes, bool] | Exception] = {
            url: (200, PDF, False) for url in (FY25, FY24, LONGI_FY25)
        }
        self.calls: list[str] = []

    def announcements(self, code: str, org_id: str, page: int = 1) -> cninfo.Answer:
        self.calls.append(f"search {org_id}")
        answer = self.searches.get(org_id)
        if answer is None:
            raise AssertionError(f"unexpected search: {org_id}")
        if isinstance(answer, Exception):
            raise answer
        status, body = answer
        return cninfo.Answer(_document(f"search:{org_id}", body), status, False)

    def report(self, public_url: str, *, max_bytes: int) -> cninfo.Answer:
        self.calls.append(public_url)
        answer = self.reports.get(public_url)
        if answer is None:
            raise AssertionError(f"unexpected report: {public_url}")
        if isinstance(answer, Exception):
            raise answer
        status, body, too_large = answer
        return cninfo.Answer(_document(public_url, body), status, too_large)


#: When a request's answer was fetched, by URL, where a test needs it older than now.
FETCHED_AT: dict[str, datetime] = {}


def _document(url: str, body: bytes) -> Document:
    return Document(
        url=url,
        body=body,
        content_type="application/pdf",
        fetched_at=FETCHED_AT.get(url, datetime.now(UTC)),
    )


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch) -> Iterator[Cninfo]:
    fake = Cninfo()
    monkeypatch.setattr(cninfo, "announcements", fake.announcements)
    monkeypatch.setattr(cninfo, "report", fake.report)
    yield fake


#: The reader's answer per report body; `READS` counts the reports read.
READS: list[bytes] = []
READER: dict[str, Any] = {"result": None}


def _yangtze() -> cn_pdf.CnParseResult:
    rows = [
        CnSegmentRow("cninfo:分行业", "境内水电行业", 75661563315.43, 25881578129.36),
        CnSegmentRow("cninfo:分行业", "其他行业", 10323376439.80, 7067072381.81),
    ]
    return cn_pdf.CnParseResult(cn_pdf.facts_from_rows(rows, "2025-12-31"), 13, "2025-12-31")


@pytest.fixture(autouse=True)
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    READS.clear()
    READER["result"] = _yangtze()

    def read(body: bytes, **_: Any) -> cn_pdf.CnParseResult | cn_child.Unreadable:
        READS.append(body)
        result = READER["result"]
        assert result is not None
        return result  # type: ignore[no-any-return]

    monkeypatch.setattr(cn_child, "read", read)
    yield


# --- the database --------------------------------------------------------------------------------

#: Module level: a resource's class attribute becomes a config field.
FILERS: dict[str, tuple[str, list[str]]] = {}
#: `security_filing` as the fake holds it: (security_id, accession) -> row, marks included.
FILING_ROWS: dict[tuple[str, str], dict[str, Any]] = {}
PARSER = {"version": 20}
CALLS: list[tuple[str, Any]] = []
#: Per accession, when it was marked read (for retention).
READ_AT: dict[str, datetime] = {}


def _unread(row: dict[str, Any]) -> bool:
    mark = row.get("segments_parser_version")
    return row.get("segments_parsed_at") is None or (mark or 0) < PARSER["version"]


def _is_report(row: dict[str, Any]) -> bool:
    return bool(row["report_type"] == cn.FULL and row["accession_number"].lower().endswith(".pdf"))


class Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, given: Sequence[Any] | Mapping[str, Any] = ()) -> None:
        text = " ".join(sql.split())
        params = [] if isinstance(given, Mapping) else list(given)
        CALLS.append((text, params))
        self.rows, self.rowcount = [], 0
        if "group by sf.filer_id" in text:
            self.rows = [(org, *FILERS[org]) for org in params[0] if org in FILERS]
        elif text.startswith("select distinct f.accession_number"):
            urls = set(params[0])
            self.rows = sorted(
                {(r["accession_number"],) for r in FILING_ROWS.values()
                 if r["accession_number"] in urls and not _unread(r)}
            )  # fmt: skip
        elif "having bool_or" in text:
            sids, named = set(params[0]), set(params[1])
            by: dict[str, list[dict[str, Any]]] = {}
            for r in FILING_ROWS.values():
                if r["security_id"] in sids and _is_report(r):
                    by.setdefault(r["accession_number"], []).append(r)
            for acc, rows in by.items():
                unread = sorted(r["security_id"] for r in rows if _unread(r))
                if unread or acc in named:
                    filed = min(date.fromisoformat(r["filing_date"]) for r in rows)
                    holding = sorted(r["security_id"] for r in rows)
                    self.rows.append((acc, filed, unread or None, holding))
        elif "having not bool_or" in text:
            urls = set(params[0])
            self.rows = [(u, at) for u, at in READ_AT.items() if u in urls]
        elif "from market.security_filer where source_code" in text:
            self.rows = [(org,) for org in FILERS]
        elif text.startswith("select s.security_id::text, min(l.symbol)"):
            self.rows = [("s-cn1", "600900"), ("s-cn2", "601012"), ("s-cn3", "688999")]
        elif "from market.segment_axis" in text:
            self.rows = [
                (a["axis"], a["kind"], a["priority"], a["required_member"])
                for a in VOCABULARY["axes"]
            ]
        elif "from market.xbrl_concept" in text:
            self.rows = [
                (c["metric_code"], c["concept"], c["priority"]) for c in VOCABULARY["concepts"]
            ]
        elif "from market.segment_member_class" in text:
            self.rows = [(r["member_code"], r["class"]) for r in VOCABULARY["member_roles"]]
        elif "from market.segment_parser" in text:
            self.rows = [(PARSER["version"],)]
        elif text.startswith("select code from market.currency"):
            self.rows = [("USD",), ("CNY",)]
        elif text.startswith("insert into market.security_filing"):
            columns = text[text.index("(") + 1 : text.index(")")].split(", ")
            for n in range(0, len(params), len(columns)):
                row = dict(zip(columns, params[n : n + len(columns)], strict=True))
                held = FILING_ROWS.setdefault(
                    (row["security_id"], row["accession_number"]),
                    {"segments_parsed_at": None, "segments_parser_version": None},
                )
                held.update(row)
                self.rowcount += 1
        elif text.startswith("insert into market.security_filer"):
            self.rowcount = len(params) // 3
        elif text.startswith("update market.security_filing"):
            at, version, accession, security_ids = params
            for sid in security_ids:
                row = FILING_ROWS[(sid, accession)]
                row["segments_parsed_at"], row["segments_parser_version"] = at, version

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        CALLS.append(("commit", []))

    def rollback(self) -> None:
        CALLS.append(("rollback", []))


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    FILERS.clear()
    FILERS.update({YANGTZE: ("600900", ["s-cn1"]), LONGI: ("601012", ["s-cn2"])})
    FILING_ROWS.clear()
    PARSER["version"] = 20
    CALLS.clear()
    READ_AT.clear()
    FETCHED_AT.clear()
    yield


def instance_with(*keys: str) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(CNINFO_FILER_PARTITION, list(keys))
    return instance


def resources(tmp_path: Path) -> dict[str, Any]:
    return {
        "postgres": FakePostgres(),
        "parquet_io": ParquetIOManager(str(tmp_path)),
        "raw_store": RawStore(base_path=str(tmp_path)),
    }


def run(
    tmp_path: Path,
    instance: dg.DagsterInstance,
    keys: Sequence[str],
    *,
    config: dict[str, Any] | None = None,
) -> dg.ExecuteInProcessResult:
    single = len(keys) == 1
    return dg.materialize(
        [raw.raw_cninfo_filings, core.cninfo_segment],
        instance=instance,
        partition_key=keys[0] if single else None,
        tags=None
        if single
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
        run_config={
            "ops": {"raw_cninfo_filings": {"config": {"pace_seconds": 0.0, **(config or {})}}}
        },
        resources=resources(tmp_path),
        raise_on_error=False,
    )


def meta(result: dg.ExecuteInProcessResult, key: str) -> dict[str, Any]:
    for event in result.get_asset_materialization_events():
        materialization = event.step_materialization_data.materialization
        if materialization.asset_key == dg.AssetKey(key):
            return {k: v.value for k, v in materialization.metadata.items()}
    raise AssertionError(f"{key} did not materialize")


def stored(tmp_path: Path, key: str) -> list[dict[str, Any]]:
    return [
        dict(r) for r in RawStore(base_path=str(tmp_path)).stored_rows_for(raw.CNINFO_FILINGS, key)
    ]


def segment_inserts() -> list[list[Any]]:
    return [p for sql, p in CALLS if sql.startswith("insert into market.security_segment")]


def marks(accession: str) -> dict[str, int | None]:
    return {
        sid: row["segments_parser_version"]
        for (sid, acc), row in FILING_ROWS.items()
        if acc == accession
    }


# --- the registry ---------------------------------------------------------------------------------


def test_the_stock_lists_map_each_a_share_to_its_org_id(tmp_path: Path) -> None:
    listed = [{"code": "600900", "orgId": YANGTZE}, {"code": "601012", "orgId": LONGI}]
    lists = [
        {"document": "szse_stock.json", "http_status": 200, "body": json.dumps(
            {"stockList": listed}
        ).encode()},
        {"document": "bj_stock.json", "http_status": 200, "body": b'{"stockList": []}'},
    ]  # fmt: skip

    @dg.asset(name="raw_cninfo_stock_list")
    def stock_lists() -> list[dict[str, Any]]:
        return lists

    result = dg.materialize(
        [stock_lists, core.security_cninfo_filer],
        resources={**resources(tmp_path), "io_manager": dg.mem_io_manager},
        raise_on_error=False,
    )
    assert result.success
    m = meta(result, "security_cninfo_filer")
    assert (m["listed"], m["securities"], m["mapped"], m["unmapped"]) == (2, 3, 2, 1)
    sql, params = next(
        (s, p) for s, p in CALLS if s.startswith("insert into market.security_filer")
    )
    columns = sql[sql.index("(") + 1 : sql.index(")")].split(", ")
    rows = [dict(zip(columns, params[n : n + 3], strict=True)) for n in range(0, len(params), 3)]
    assert sorted((r["security_id"], r["source_code"], r["filer_id"]) for r in rows) == [
        ("s-cn1", "cninfo", YANGTZE),
        ("s-cn2", "cninfo", LONGI),
    ]


# --- a filer read end to end ----------------------------------------------------------------------


def test_a_filer_is_searched_once_and_only_its_full_pdf_reports_are_fetched_and_read(
    tmp_path: Path, provider: Cninfo
) -> None:
    result = run(tmp_path, instance_with(YANGTZE), [YANGTZE])
    assert result.success

    # The summary and the `.js` data file are filed and never fetched; the newest report first.
    assert provider.calls == [f"search {YANGTZE}", FY25, FY24]
    m = meta(result, "raw_cninfo_filings")
    assert (m["requested"], m["search_answered"], m["reports_named"], m["report_answered"]) == (
        1, 1, 2, 2,
    )  # fmt: skip
    assert m["search_outcomes"] == m["requested"] and m["report_outcomes"] == m["reports_named"]

    # RAW IS WHAT CNINFO SERVED: the search and each report, byte for byte.
    held = {r["accession"]: r for r in stored(tmp_path, YANGTZE)}
    assert bytes(held[FY25]["body"]) == PDF and held[FY25]["outcome"] == "answered"
    assert set(held) == {"page-1", FY25, FY24}

    s = meta(result, "cninfo_segment")
    assert (s["announcements"], s["filings"], s["read"], s["stamped"]) == (4, 2, 2, 2)
    assert s["outcomes"] == s["filings"] and s["currency_unknown"] == 0
    # Every announcement is filed, as the edge filed it, but only the full PDF reports are read.
    assert {acc for (_, acc) in FILING_ROWS} == {FY25, SUMMARY, FY24, DATA_FILE}
    assert FILING_ROWS[("s-cn1", DATA_FILE)]["report_type"] == cn.FULL
    assert marks(FY25) == {"s-cn1": 20} and marks(FY24) == {"s-cn1": 20}
    assert marks(SUMMARY) == {"s-cn1": None} and marks(DATA_FILE) == {"s-cn1": None}
    assert len(READS) == 2 and len(segment_inserts()) == 2
    assert "cninfo" in segment_inserts()[0] and "CNY" in segment_inserts()[0]


def test_a_report_that_was_read_is_never_fetched_again(tmp_path: Path, provider: Cninfo) -> None:
    instance = instance_with(YANGTZE)
    assert run(tmp_path, instance, [YANGTZE]).success
    asked, READS[:] = len(provider.calls), []

    again = run(tmp_path, instance, [YANGTZE])
    assert again.success
    assert provider.calls[asked:] == [], "the search is fresh, and nothing is due"
    m = meta(again, "raw_cninfo_filings")
    assert (m["search_fresh"], m["reports_named"]) == (1, 0)
    assert m["search_outcomes"] == m["requested"]
    assert meta(again, "cninfo_segment")["filings"] == 0 and READS == []


# --- the floor asks each filer's search once a floor --------------------------------------------


@pytest.mark.parametrize(
    ("age_days", "searched_again"),
    # A list fetched on a filer's floor day is due on its next one, whatever the hour of the
    # tick: an hour short of a floor period is old enough.
    [(raw.LIST_MAX_AGE_DAYS - 1, False), (FLOOR_DAYS - 1 / 24, True)],
)
def test_a_search_younger_than_the_floor_is_not_asked_and_one_as_old_is(
    tmp_path: Path, provider: Cninfo, age_days: float, searched_again: bool
) -> None:
    instance = instance_with(YANGTZE)
    FETCHED_AT[f"search:{YANGTZE}"] = datetime.now(UTC) - timedelta(days=age_days)
    assert run(tmp_path, instance, [YANGTZE]).success
    asked = len(provider.calls)
    FETCHED_AT.clear()

    again = run(tmp_path, instance, [YANGTZE])
    assert again.success
    assert provider.calls[asked:] == ([f"search {YANGTZE}"] if searched_again else [])
    m = meta(again, "raw_cninfo_filings")
    assert (m["search_answered"], m["search_fresh"]) == ((1, 0) if searched_again else (0, 1))


def test_an_age_of_zero_searches_every_filer(tmp_path: Path, provider: Cninfo) -> None:
    instance = instance_with(YANGTZE)
    assert run(tmp_path, instance, [YANGTZE]).success
    asked = len(provider.calls)

    again = run(tmp_path, instance, [YANGTZE], config={"list_max_age_days": 0})
    assert again.success
    assert provider.calls[asked:] == [f"search {YANGTZE}"]


def test_a_search_cninfo_did_not_answer_is_asked_again_however_young(
    tmp_path: Path, provider: Cninfo
) -> None:
    instance = instance_with(YANGTZE)
    provider.searches[YANGTZE] = (404, b"")
    assert run(tmp_path, instance, [YANGTZE]).success
    asked = len(provider.calls)

    again = run(tmp_path, instance, [YANGTZE])
    assert again.success
    assert provider.calls[asked:] == [f"search {YANGTZE}"], "an absence is no list to plan from"


def test_a_fresh_search_still_names_its_reports_when_stage_2_never_filed_them(
    tmp_path: Path, provider: Cninfo
) -> None:
    """Stage 1 alone, its reports left unasked: `security_filing` holds nothing for the filer, so
    the next run can only know its reports from the stored search."""
    instance = instance_with(YANGTZE)
    first = dg.materialize(
        [raw.raw_cninfo_filings],
        instance=instance,
        partition_key=YANGTZE,
        run_config={
            "ops": {"raw_cninfo_filings": {"config": {"pace_seconds": 0.0, "budget_bytes": 0}}}
        },
        resources=resources(tmp_path),
    )
    assert first.success and provider.calls == [f"search {YANGTZE}"]
    assert FILING_ROWS == {}

    again = run(tmp_path, instance, [YANGTZE])
    assert again.success
    assert provider.calls[1:] == [FY25, FY24], "planned from the stored search, newest first"
    assert meta(again, "raw_cninfo_filings")["search_fresh"] == 1


def test_a_raised_parser_version_rereads_from_raw_without_asking_cninfo(
    tmp_path: Path, provider: Cninfo
) -> None:
    instance = instance_with(YANGTZE)
    assert run(tmp_path, instance, [YANGTZE]).success
    reports = [c for c in provider.calls if not c.startswith("search")]
    PARSER["version"] = 21

    again = run(tmp_path, instance, [YANGTZE])
    assert again.success
    assert [c for c in provider.calls if not c.startswith("search")] == reports
    m = meta(again, "raw_cninfo_filings")
    assert (m["reports_named"], m["report_held_already"]) == (2, 2)
    assert meta(again, "cninfo_segment")["read"] == 2 and marks(FY25) == {"s-cn1": 21}


# --- the ways a report ends -----------------------------------------------------------------------


def test_too_large_absent_and_unreadable_reports_are_marked_without_rows(
    tmp_path: Path, provider: Cninfo
) -> None:
    provider.reports[FY25] = (200, b"", True)
    provider.reports[FY24] = (404, b"", False)
    result = run(tmp_path, instance_with(YANGTZE, LONGI), [YANGTZE, LONGI])
    assert result.success
    m = meta(result, "raw_cninfo_filings")
    assert (m["report_too_large"], m["report_absent"], m["report_answered"]) == (1, 1, 1)
    s = meta(result, "cninfo_segment")
    assert (s["too_large"], s["no_report"], s["read"]) == (1, 1, 1)
    assert marks(FY25) == {"s-cn1": 20} and marks(FY24) == {"s-cn1": 20}
    assert len(READS) == 1, "only LONGi's report is read"

    # A report the child cannot read is marked unreadable, and the run goes on.
    PARSER["version"] = 21
    READER["result"] = cn_child.Unreadable("memory limit of 768 MB")
    again = run(tmp_path, instance_with(YANGTZE, LONGI), [YANGTZE, LONGI])
    assert again.success
    assert meta(again, "cninfo_segment")["unreadable"] == 1
    assert marks(LONGI_FY25) == {"s-cn2": 21}


def test_a_refusal_stops_the_run_and_files_nothing_it_did_not_finish(
    tmp_path: Path, provider: Cninfo
) -> None:
    provider.reports[FY25] = cninfo.CninfoRefused(403, FY25)
    result = run(tmp_path, instance_with(YANGTZE, LONGI), [YANGTZE, LONGI])
    assert result.success
    m = meta(result, "raw_cninfo_filings")
    assert (m["search_answered"], m["report_refused"], m["report_unasked"]) == (2, 1, 2)
    assert m["report_outcomes"] == m["reports_named"] == 3
    s = meta(result, "cninfo_segment")
    assert (s["read"], s["not_held"]) == (0, 3), "nothing fetched, nothing marked"
    assert all(v is None for v in marks(FY25).values())


def test_a_byte_budget_reads_every_filers_newest_report_before_anyones_second(
    tmp_path: Path, provider: Cninfo
) -> None:
    result = run(
        tmp_path,
        instance_with(YANGTZE, LONGI),
        [YANGTZE, LONGI],
        config={"budget_bytes": 2 * len(PDF)},
    )
    assert result.success
    assert provider.calls[2:] == [FY25, LONGI_FY25], "round robin, then the budget"
    assert meta(result, "raw_cninfo_filings")["report_unasked"] == 1
    assert marks(FY24) == {"s-cn1": None}, "the unasked report stays due"


# --- retention ------------------------------------------------------------------------------------


def test_retention_drops_a_read_report_past_thirty_days_and_stage_1_fetches_it_when_due(
    tmp_path: Path, provider: Cninfo
) -> None:
    instance = instance_with(YANGTZE)
    assert run(tmp_path, instance, [YANGTZE]).success
    READ_AT.update({FY25: datetime.now(UTC) - timedelta(days=31), FY24: datetime.now(UTC)})

    result = automation.cninfo_retention_job.execute_in_process(
        instance=instance, resources=resources(tmp_path)
    )
    assert result.success
    held = {r["accession"]: r for r in stored(tmp_path, YANGTZE)}
    assert held[FY25]["body"] is None and held[FY25]["pruned_at"] is not None
    assert bytes(held[FY24]["body"]) == PDF and held[FY24]["pruned_at"] is None

    # A pruned report is not held: a parser change fetches it again, the one still held is not.
    PARSER["version"] = 21
    asked = len(provider.calls)
    assert run(tmp_path, instance, [YANGTZE]).success
    assert provider.calls[asked:] == [FY25]
