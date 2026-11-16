"""DART's registry, filing lists and archives (Korea's segment lane).

Fixtures captured 2026-11-16 through http-cache from the node (`fixtures/dart/`): Samsung
Electronics' `list.json` page 1 (corp code 00126380, 47 periodic filings since 2015) and its FY2014
annual report's archive. DART's refusal bodies are the forms the edge recorded (`dart.ts`): they
arrive with HTTP 200.
"""

from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from muffin_ingest.facets import dart as fd
from muffin_ingest.providers import dart
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parent.parent / "fixtures" / "dart"
SAMSUNG_LIST = (FIXTURES / "samsung_list_p1.json").read_bytes()
SAMSUNG_2015 = (FIXTURES / "samsung_20150331002915.zip").read_bytes()
NO_FILE = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    "<result><status>014</status><message>파일이 존재하지 않습니다</message></result>"
).encode()
NO_DATA = '{"status":"013","message":"조회된 데이타가 없습니다."}'.encode()
LIMIT = '{"status":"020","message":"요청 제한을 초과하였습니다."}'.encode()


def _zip(entries: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for name, body in entries.items():
            z.writestr(name, body)
    return out.getvalue()


def _answer(body: bytes, status: int = 200, too_large: bool = False) -> dart.Answer:
    document = Document(
        url="https://opendart.fss.or.kr/api/x?crtfc_key=REDACTED",
        body=body,
        content_type="application/json",
        fetched_at=datetime(2026, 11, 16, tzinfo=UTC),
    )
    return dart.Answer(document=document, status=status, too_large=too_large)


def test_the_registry_maps_a_listed_company_s_stock_code_to_its_corp_code() -> None:
    """`corpCode.xml` is one ZIP of every filer; only a listed one carries a stock code, and the
    first corp code a stock code names is kept."""
    xml = (
        "<result>"
        "<list><corp_code>00126380</corp_code><corp_name>삼성전자</corp_name>"
        "<stock_code>005930</stock_code><modify_date>20250101</modify_date></list>"
        "<list><corp_code>00999999</corp_code><corp_name>비상장</corp_name>"
        "<stock_code> </stock_code></list>"
        "<list><corp_code>00144164</corp_code><stock_code>018670</stock_code></list>"
        "<list><corp_code>00000001</corp_code><stock_code>018670</stock_code></list>"
        "</result>"
    ).encode()
    assert fd.corp_codes(_zip({"CORPCODE.xml": xml})) == {
        "005930": "00126380",
        "018670": "00144164",
    }
    with pytest.raises(ValueError, match="no XML"):
        fd.corp_codes(_zip({"readme.txt": b"x"}))


def test_a_filing_list_names_each_filing_with_its_receipt_date() -> None:
    page = fd.parse_filings(SAMSUNG_LIST)
    assert (page.status, page.answered, page.total_pages, len(page.filed)) == ("000", True, 1, 47)
    annual = [f for f in page.filed if f.is_annual]
    assert annual[0] == fd.Filed("20260310002820", "사업보고서 (2025.12)", "2026-03-10")
    assert {f.rcept_no for f in annual} >= {"20150331002915", "20250311001085"}
    assert all("사업보고서" in f.report_name for f in annual)
    # 반기보고서 and 분기보고서 (half-year, quarter) are listed too, with no 11011 archive.
    assert len(annual) < len(page.filed)


def test_dart_saying_no_data_is_an_answer_and_any_other_status_a_refusal() -> None:
    """`013` is "no data", an answer about the company. `020` is the daily request limit: a filing
    list refused must not read as a company that never filed."""
    assert fd.parse_filings(NO_DATA) == fd.FilingsPage("013", (), 0)
    assert fd.parse_filings(NO_DATA).answered
    assert not fd.parse_filings(LIMIT).answered
    assert not fd.parse_filings(b"<html>maintenance</html>").answered
    assert fd.list_outcome(_answer(SAMSUNG_LIST)) == "answered"
    assert fd.list_outcome(_answer(NO_DATA)) == "answered"
    assert fd.list_outcome(_answer(LIMIT)) == "refused"
    assert fd.list_outcome(_answer(b"", status=404)) == "absent"


def test_an_archive_is_a_zip_dart_saying_no_or_neither() -> None:
    """Status 014 arrives as XML from an archive endpoint, 013 as JSON from a list: the edge held a
    backlog's head for 143 runs knowing only the JSON form."""
    assert fd.classify(SAMSUNG_2015) == "zip"
    assert fd.classify(NO_FILE) == "dart_says_no"
    assert fd.classify(NO_DATA) == "dart_says_no"
    assert fd.classify(b'{"status":"000"}') == "not_zip"
    assert fd.classify(b"<html>error</html>") == "not_zip"
    assert fd.archive_outcome(_answer(SAMSUNG_2015)) == "answered"
    assert fd.archive_outcome(_answer(NO_FILE)) == "absent"
    assert fd.archive_outcome(_answer(b"<html/>")) == "not_zip"
    assert fd.archive_outcome(_answer(b"", status=404)) == "absent"
    assert fd.archive_outcome(_answer(b"", too_large=True)) == "too_large"


def test_the_instance_is_the_archive_s_xbrl_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    instance = fd.instance_of(SAMSUNG_2015)
    assert instance is not None and len(instance) == 297_908
    assert b"<xbrli:xbrl" in instance[:4000] or b"<xbrl" in instance[:4000]
    assert fd.instance_of(_zip({"report.xml": b"<a/>"})) is None
    monkeypatch.setattr(fd, "MAX_INSTANCE_BYTES", 10)
    with pytest.raises(fd.TooLarge):
        fd.instance_of(_zip({"x.XBRL": b"0123456789ab"}))


def test_the_key_never_reaches_a_stored_url() -> None:
    url = "http://http-cache:8080/dart/api/list.json?crtfc_key=abc123SECRET&corp_code=00126380"
    assert dart.redacted(url) == (
        "http://http-cache:8080/dart/api/list.json?crtfc_key=REDACTED&corp_code=00126380"
    )
    assert "SECRET" not in dart.redacted("x?a=1&crtfc_key=abc123SECRET")


def test_only_annual_reports_are_filed_and_the_marks_are_never_written() -> None:
    page = fd.parse_filings(SAMSUNG_LIST)
    rows = fd.filing_rows(["s1", "s2"], page.filed)
    annual = [f for f in page.filed if f.is_annual]
    assert len(rows) == 2 * len(annual)
    assert {r["report_type"] for r in rows} == {"사업보고서"}
    assert rows[0] == {
        "security_id": "s1",
        "accession_number": "20260310002820",
        "report_type": "사업보고서",
        "filing_date": "2026-03-10",
        "source_code": "dart",
        "is_xbrl": True,
    }
    assert not {"segments_parsed_at", "segments_parser_version"} & set(rows[0])


def test_the_newest_finished_fetch_of_an_archive_decides() -> None:
    rows = [
        {"document": "instance", "accession": "A", "outcome": "absent",
         "fetched_at": "2026-11-01", "run_id": "r1", "body": NO_FILE},
        {"document": "instance", "accession": "A", "outcome": "answered",
         "fetched_at": "2026-11-16", "run_id": "r2", "body": SAMSUNG_2015},
        {"document": "instance", "accession": "B", "outcome": "answered",
         "fetched_at": "2026-11-16", "run_id": "r2", "body": b"PK"},
        {"document": "filings", "accession": "page-1", "outcome": "answered",
         "fetched_at": "2026-11-15", "run_id": "r1", "body": NO_DATA},
        {"document": "filings", "accession": "page-1", "outcome": "answered",
         "fetched_at": "2026-11-16", "run_id": "r2", "body": SAMSUNG_LIST},
        {"document": "filings", "accession": "page-2", "outcome": "refused",
         "fetched_at": "2026-11-16", "run_id": "r2", "body": LIMIT},
    ]  # fmt: skip
    held = fd.held_archive(rows, "A")
    assert held == fd.Held("answered", "2026-11-16", "r2")
    assert fd.archive_body([r for r in rows if r["accession"] == "A"], held) == SAMSUNG_2015
    assert fd.held_archive(rows, "C") is None
    assert fd.archive_body(rows, fd.Held("absent", "2026-11-01", "r1")) is None
    newest = fd.newest_lists(rows)
    assert [(r["accession"], r["run_id"]) for r in newest] == [("page-1", "r2")]
