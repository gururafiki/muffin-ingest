"""SEC submissions, over bodies captured 2026-10-26 and trimmed: AAPL, TSMC and a Greif page."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest.facets import sec_filings
from muffin_ingest.providers import sec_edgar
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parents[1] / "fixtures" / "sec"
AAPL = (FIXTURES / "submissions_AAPL.trimmed.json").read_bytes()
TSM = (FIXTURES / "submissions_TSM.trimmed.json").read_bytes()
GEF_PAGE = (FIXTURES / "CIK0000043920-submissions-001.trimmed.json").read_bytes()

#: `market.filing_form.kept` for SEC, as migration 20261025100300 seeds it (less the forms these
#: fixtures do not carry). Forms 4, 144 and 13G are not kept: they are someone else's filing.
KEPT = {"10-K", "10-Q", "20-F", "40-F", "6-K", "6-K/A", "8-K", "8-K/A", "DEF 14A", "10-K/A"}

#: `market.filer_profile`'s columns, from the baseline, less what the writer adds.
FILER_PROFILE_COLUMNS = {
    "entity_type", "owner_org", "ein", "lei", "category", "fiscal_year_end",
    "state_of_incorporation", "website", "investor_website", "phone", "us_ticker", "us_exchange",
    "hq_street", "hq_city", "hq_state_or_country", "hq_zip",
}  # fmt: skip

pytest.importorskip("edgar")


@pytest.fixture(autouse=True)
def _edgar_can_be_configured(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Reading the table configures edgar, which needs an agent; nothing is requested."""
    monkeypatch.setenv("MUFFIN_USER_AGENT", "muffin-test/1.0 (ops@example.com)")
    monkeypatch.setenv("EDGAR_SCRATCH_DIR", str(tmp_path / "edgar"))


def test_the_main_document_keeps_the_kept_forms_in_sec_order() -> None:
    kept = sec_filings.filings(AAPL, KEPT)
    assert [f.form for f in kept] == [
        "8-K/A", "10-Q", "8-K", "10-Q", "8-K", "8-K", "10-Q", "DEF 14A", "10-K", "10-K",
    ]  # fmt: skip
    k = kept[-2]
    assert (k.accession, k.filing_date, k.report_date) == (
        "0000320193-25-000079",
        date(2025, 10, 31),
        date(2025, 9, 27),
    )
    assert k.is_xbrl is True


def test_an_older_page_is_read_from_its_top_level_arrays() -> None:
    """A reader of only `filings.recent` would read nothing here, as if Greif had no history."""
    kept = sec_filings.filings(GEF_PAGE, KEPT)
    assert len(kept) == 10
    assert kept[0].accession == "0001193125-13-483163"
    old = kept[-1]
    assert (old.form, old.filing_date, old.is_xbrl) == ("10-K", date(2000, 1, 28), False)


def test_the_links_are_the_ones_the_app_opens_today() -> None:
    """Byte-identical to what openbb's `security-filings` stored for Apple (read 2026-10-26)."""
    by = {f.accession: f for f in sec_filings.filings(AAPL, KEPT)}
    amended = by["0001140361-26-035325"]
    assert amended.report_url(320193) == (
        "https://www.sec.gov/Archives/edgar/data/320193/000114036126035325/ef20081427_8ka.htm"
    )
    assert amended.filing_detail_url(320193) == (
        "https://www.sec.gov/Archives/edgar/data/320193/0001140361-26-035325-index.htm"
    )
    assert by["0000320193-26-000020"].report_url(320193) == (
        "https://www.sec.gov/Archives/edgar/data/320193/000032019326000020/aapl-20260627.htm"
    )


def test_a_link_is_built_on_sec_s_origin_even_through_the_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reader follows it, and http-cache is reachable only inside the node."""
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")
    [filing] = sec_filings.filings(AAPL, {"DEF 14A"})
    assert filing.report_url(320193).startswith("https://www.sec.gov/")  # type: ignore[union-attr]
    assert filing.filing_detail_url(320193).startswith("https://www.sec.gov/")


def test_a_filing_with_no_main_document_has_only_its_index_page() -> None:
    old = sec_filings.filings(GEF_PAGE, {"10-K"})[-1]
    assert old.primary_document is None
    assert old.report_url(43920) is None
    assert old.filing_detail_url(43920).endswith("/43920/0000043920-00-000002-index.htm")


def test_a_filing_with_no_report_date_reads_none() -> None:
    [schedule] = sec_filings.filings(TSM, {"SCHEDULE 13G/A"})
    assert schedule.report_date is None


def _main(**recent: list[Any]) -> bytes:
    """AAPL's main document with its filing table replaced."""
    payload = json.loads(AAPL)
    table = payload["filings"]["recent"]
    payload["filings"]["recent"] = {k: v[:1] for k, v in table.items()} | recent
    return json.dumps(payload).encode()


def test_an_xbrl_flag_is_a_number_and_a_string_zero_is_false() -> None:
    """`bool("0")` is True, which would send the segments parser after every pre-2009 filing."""
    body = _main(form=["10-K"], isXBRL=["0"])
    [filing] = sec_filings.filings(body, {"10-K"})
    assert filing.is_xbrl is False


def test_a_table_edgartools_cannot_read_raises_rather_than_dropping_rows() -> None:
    body = _main(form=["10-K"], filingDate=[""])
    with pytest.raises(sec_filings.SubmissionsUnreadable):
        sec_filings.filings(body, {"10-K"})


@pytest.mark.parametrize(
    "body",
    [
        b'<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>',
        b"[]",
        b'{"cik": "0000320193", "filings": {}}',
    ],
    ids=["s3-error", "not-an-object", "neither-shape"],
)
def test_a_body_that_is_not_submissions_raises(body: bytes) -> None:
    with pytest.raises(sec_filings.SubmissionsUnreadable):
        sec_filings.filings(body, KEPT)


def test_the_profile_is_the_registrant_s_identity() -> None:
    p = sec_filings.profile(AAPL)
    assert (p.name, p.sic, p.sic_description) == ("Apple Inc.", "3571", "Electronic Computers")
    assert (p.ein, p.lei, p.fiscal_year_end) == ("942404110", None, "0926")
    assert (p.us_ticker, p.us_exchange, p.hq_city, p.hq_zip) == (
        "AAPL",
        "Nasdaq",
        "CUPERTINO",
        "95014",
    )
    assert set(p.filer_profile_columns()) == FILER_PROFILE_COLUMNS


def test_an_ein_of_zeros_is_a_placeholder_not_an_identifier() -> None:
    """Every foreign issuer measured carries `000000000`; TSMC is one."""
    p = sec_filings.profile(TSM)
    assert p.ein is None
    assert (p.us_ticker, p.us_exchange) == ("TSM", "NYSE")


def test_an_older_page_carries_no_profile() -> None:
    with pytest.raises(sec_filings.SubmissionsUnreadable):
        sec_filings.profile(GEF_PAGE)


def test_the_main_document_names_the_older_pages() -> None:
    assert sec_filings.older_pages(AAPL) == ["CIK0000320193-submissions-001.json"]
    assert sec_filings.older_pages(GEF_PAGE) == []


def test_former_names_are_dated_and_one_per_name() -> None:
    names = {n.name: n for n in sec_filings.former_names(AAPL)}
    assert names["APPLE COMPUTER INC"].from_date == date(1994, 1, 26)
    assert names["APPLE COMPUTER INC"].to_date == date(2007, 1, 4)
    assert len(names) == 3
    payload = json.loads(AAPL)
    payload["formerNames"] = [
        {"name": "ACME", "from": "2001-01-01T00:00:00.000Z", "to": "2003-01-01T00:00:00.000Z"},
        {"name": "ACME", "from": "2008-01-01T00:00:00.000Z", "to": "2010-01-01T00:00:00.000Z"},
        {"name": "", "from": None, "to": None},
    ]
    [acme] = sec_filings.former_names(json.dumps(payload).encode())
    assert acme.to_date == date(2010, 1, 1)


def test_stage_1_adds_only_its_context_columns() -> None:
    document = Document(
        url="https://data.sec.gov/submissions/CIK0000320193.json",
        body=AAPL,
        content_type="application/json",
        fetched_at=datetime(2026, 10, 26, 12, tzinfo=UTC),
    )
    [row] = sec_filings.raw_rows(
        document, kind="submissions", page=None, http_status=200, run_id="r"
    )
    assert set(row) - set(document.as_row("r")) == sec_filings.CONTEXT_COLUMNS
    assert row["body"] == AAPL


def test_a_fault_of_the_run_is_not_counted_against_the_filer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unconfigurable edgar fails every filer alike, so it must stop the run, not mark one."""

    def broken(table: object) -> list[dict[str, Any]]:
        raise RuntimeError("MUFFIN_USER_AGENT must be set")

    monkeypatch.setattr(sec_edgar, "filings_table", broken)
    with pytest.raises(RuntimeError, match="MUFFIN_USER_AGENT") as raised:
        sec_filings.filings(AAPL, KEPT)
    # `SubmissionsUnreadable` is a RuntimeError too, so the TYPE is the assertion.
    assert type(raised.value) is RuntimeError


class _Conn:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.params: object = None

    def cursor(self) -> _Conn:
        return self

    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: object = ()) -> None:
        self.params = params

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


FORMS = sec_filings.forms(
    _Conn(
        [
            ("10-K", True, True, False, "annual"),
            ("10-K/A", True, True, True, None),
            ("10-Q", True, True, False, "interim"),
            ("10-KT", True, True, False, "transition"),
            ("8-K", True, False, False, None),
            ("6-K", True, False, False, None),
        ]
    )
)


def _filing(accession: str, form: str, report: str | None) -> sec_filings.Filing:
    return sec_filings.Filing(
        accession=accession,
        form=form,
        filing_date=date(2026, 1, 30),
        report_date=date.fromisoformat(report) if report else None,
        is_xbrl=True,
        primary_document="doc.htm",
    )


def test_a_report_is_a_form_with_a_fiscal_role_and_a_report_date() -> None:
    """An amendment restates a year the original gave, a 6-K is as often monthly revenue as
    results, and a filing SEC names no period for cannot place one."""
    found = [
        _filing("1", "10-K", "2025-12-31"),
        _filing("2", "10-K/A", "2025-12-31"),
        _filing("3", "10-Q", "2025-09-30"),
        _filing("4", "10-KT", "2025-06-30"),
        _filing("5", "8-K", "2026-01-29"),
        _filing("6", "6-K", "2025-11-30"),
        _filing("7", "10-K", None),
        _filing("8", "S-1", "2025-12-31"),
    ]
    got = sec_filings.reports(found, FORMS)
    assert [(r.accession, r.role) for r in got] == [
        ("1", "annual"),
        ("3", "interim"),
        ("4", "transition"),
    ]


def test_filing_rows_name_the_regulator_and_never_the_segment_markers() -> None:
    """The edge's segments handler owns `segments_parsed_at` and `segments_parser_version` until
    Phase 6: a writer naming either would re-queue or hide every filing it touched."""
    rows = sec_filings.filing_rows(
        [_filing("0000320193-26-000001", "10-K", "2025-09-27")],
        cik=320193,
        security_ids=["a", "b"],
        as_of="2026-10-29T00:00:00+00:00",
    )
    assert [r["security_id"] for r in rows] == ["a", "b"]
    assert all(not k.startswith("segments_") for r in rows for k in r)
    assert (
        rows[0]["disclosure_source_code"] == "sec" and rows[0]["source_code"] == "sec-submissions"
    )
    assert rows[0]["report_url"].startswith("https://www.sec.gov/Archives/edgar/data/320193/")
