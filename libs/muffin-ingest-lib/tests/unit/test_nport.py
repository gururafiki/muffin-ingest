"""The N-PORT parser and identity planning, driven over REAL captured filings.

`sec_nport_primary_ewj.xml` and `sec_nport_primary_xlk.xml` are the actual primary documents SEC
served on 2026-09-12 (182 and 79 holdings, both with placeholder CUSIPs); `sec_nport_fts_ewj.json`
is EDGAR's full-text-search answer for EWJ's series; `sec_company_tickers_mf.json` is the fund
directory. The one purely synthetic fixture is the PLACEHOLDER COLLAPSE, because the real filings
carry a usable ISIN beside every placeholder CUSIP — and the entire point of the guard is a holding
whose ONLY identifier is the placeholder.

Every rule here exists to survive a specific upstream shape; a fixture without that shape proves
nothing.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest.facets import nport

FIX = Path(__file__).parent.parent / "fixtures"


def _xlk() -> bytes:
    return (FIX / "sec_nport_primary_xlk.xml").read_bytes()


def _ewj() -> bytes:
    return (FIX / "sec_nport_primary_ewj.xml").read_bytes()


# --- the placeholder guard ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "value"),
    [
        ("cusip", "000000000"),  # SEC's placeholder for "no CUSIP" — 72% of holdings carry it
        ("cusip", "999999999"),
        ("isin", "AAAAAAAAAAAA"),
        ("cusip", "N/A"),
        ("cusip", "NONE"),
        ("cusip", "UNKNOWN"),
        ("isin", ""),
    ],
)
def test_is_usable_identifier_rejects_placeholders(kind: str, value: str) -> None:
    assert not nport.is_usable_identifier(kind, value)


@pytest.mark.parametrize(
    ("kind", "value"),
    [
        ("isin", "US0378331005"),
        ("cusip", "037833100"),
        ("figi", "BBG000B9XBU4"),
        ("lei", "5493008I6YRUC6I0S051"),
        ("other", "ANYTHING-GOES"),  # no format rule -> a non-empty non-placeholder is usable
    ],
)
def test_is_usable_identifier_accepts_real_identifiers(kind: str, value: str) -> None:
    assert nport.is_usable_identifier(kind, value)


def test_a_wrong_length_cusip_is_refused() -> None:
    assert not nport.is_usable_identifier("cusip", "03783310")  # 8 chars, not 9


def test_the_real_xlk_filing_carries_six_placeholder_cusips() -> None:
    """MEASURED, NOT INVENTED: the captured XLK filing has 6 of 79 holdings with `000000000`.
    They are present in the parse — the guard does not delete them from the RAW, it decides the
    identifiers a holding can resolve by."""
    holdings = nport.parse_holdings(_xlk())
    assert len(holdings) == 79
    placeholders = [h for h in holdings if h.get("cusip") == "000000000"]
    assert len(placeholders) == 6


def test_a_holding_whose_ONLY_identifier_is_the_placeholder_collapses_without_the_guard() -> None:
    """THE SINGLE MOST IMPORTANT TEST IN THIS FAMILY, WITH THE MUTATION BUILT IN.

    A filing can hold two companies whose only identifier is `<cusip>000000000</cusip>` — SEC's
    placeholder. With the guard removed, `identifiers_of` emits the placeholder, `plan_holdings`
    resolves both to the SAME security_id, and the fund reports one company. That is exactly how
    Accenture, Seagate, TE Connectivity and NXP collapsed into whichever was seen first, with no
    error anywhere, visible only as XLK's weights summing to 97.1%.

    THE FIXTURE MAKES THE RULES DISAGREE: a third holding with a REAL ISIN would collapse too
    under the broken rule (its ISIN and the placeholder both resolve), which is the only way a
    mutation of the guard can be told from a parser that simply fails.
    """
    xml = (
        b"<nport>"
        b"<invstOrSec><name>Accenture Plc</name><identifiers><cusip>000000000</cusip>"
        b"</identifiers></invstOrSec>"
        b"<invstOrSec><name>Seagate Technology</name><identifiers><cusip>000000000</cusip>"
        b"</identifiers></invstOrSec>"
        b'<invstOrSec><name>Apple Inc</name><identifiers><isin value="US0378331005"/>'
        b"</identifiers></invstOrSec>"
        b"</nport>"
    )
    holdings = nport.parse_holdings(xml)
    securities, identifiers, _, ids = nport.plan_holdings(
        holdings, known_identifiers={}, countries=set(), source="sec-nport"
    )
    # The two placeholder-only companies are SKIPPED, not invented; Apple resolves to its own id.
    assert ids[0] is None and ids[1] is None, ids
    assert ids[2] is not None
    assert len(securities) == 1, securities
    assert securities[0]["name"] == "Apple Inc"
    assert [i["value"] for i in identifiers] == ["US0378331005"]


def test_an_isin_is_an_attribute_not_element_text() -> None:
    """`<identifiers><isin value="JP…"/></…>` — reading the element TEXT yields an empty string and
    silently loses every ISIN. The EWJ capture is all-Japanese and carries an ISIN on every
    holding; one is the placeholder `N/A`, which the guard is exactly for."""
    holdings = nport.parse_holdings(_ewj())
    usable = sum(1 for h in holdings if nport.is_usable_identifier("isin", h.get("isin")))
    assert usable >= len(holdings) * 0.9, "ISINs were read as text"


# --- filing metadata ---------------------------------------------------------------------------


def test_filing_date_of_reads_the_documents_own_period_end() -> None:
    """`<repPdDate>` is the report period end (the FTS calls it `period_ending`); `<repPdEnd>` is a
    different as-of. The snapshot's `as_of` must be the DOCUMENT's own statement."""
    assert nport.filing_date_of(_xlk()) == date(2026, 6, 30)
    assert nport.filing_date_of(_ewj()) == date(2026, 5, 31)


def test_series_id_is_read_out_of_the_filing_header() -> None:
    assert nport.series_id(_xlk()) == "S000006415"
    assert nport.series_id(_ewj()) == "S000004249"


def test_the_fund_directory_is_an_object_with_parallel_columns() -> None:
    """THE SHAPE IS `{"fields": [...], "data": [[...]...]}`, not an array of objects. Read as an
    array of objects it yields nothing with no error."""
    body = (FIX / "sec_company_tickers_mf.json").read_bytes()
    directory = nport.fund_directory(body)
    assert directory["AGG"] == ("1100663", "S000004362"), directory.get("AGG")
    assert directory["EWJ"] == ("930667", "S000004249")
    assert len(directory) > 28_000


def test_fts_results_are_sorted_by_report_period_not_relevance() -> None:
    """EDGAR full-text search serves RELEVANCE order; "the newest filing" is only well-defined
    after the explicit sort. The captured EWJ response holds three filings for three periods."""
    refs = nport.filing_refs((FIX / "sec_nport_fts_ewj.json").read_bytes(), cik="930667")
    assert [r.report_date for r in refs] == sorted(r.report_date for r in refs)
    assert refs[-1].report_date == "2026-05-31"
    assert all(r.cik == "930667" for r in refs)
    assert all(len(r.accession) == 18 for r in refs)


def test_a_hitless_fts_search_is_an_absence_not_a_shape_change() -> None:
    """A fund whose last filing predates the sensor's window returns zero hits — that is a real
    answer, and returning [] is what lets the sensor skip it instead of raising for every fund."""
    assert nport.filing_refs(b'{"hits": {"hits": []}}', cik="930667") == []


def test_a_fts_response_with_no_hits_key_is_refused() -> None:

    with pytest.raises(nport.NportUnreadable, match="search-index"):
        nport.filing_refs(b"[1,2,3]", cik="930667")


def test_the_fts_key_carries_the_directory_cik_not_the_embedded_one() -> None:
    """MEASURED 2026-09-12: EWJ's primary document 404s under CIK 1004726 (the CIK the accession
    embeds) and is served under 930667 (the directory's). The partition key must carry the filer,
    which is why `FilingRef.key` includes it."""
    ref = nport.filing_refs((FIX / "sec_nport_fts_ewj.json").read_bytes(), cik="930667")[-1]
    assert ref.key.startswith("930667:")


# --- identity planning -------------------------------------------------------------------------


def test_a_known_identifier_reuses_its_security_and_adds_no_row() -> None:
    holdings = [
        {
            "name": "Apple Inc",
            "isin": "US0378331005",
            "country": "US",
            "weight": 5.0,
            "asset_category": "EC",
        }
    ]
    securities, identifiers, _, ids = nport.plan_holdings(
        holdings,
        known_identifiers={"isin:US0378331005": "11111111-1111-1111-1111-111111111111"},
        countries={"US"},
        source="sec-nport",
    )
    assert ids == ["11111111-1111-1111-1111-111111111111"]
    assert securities == [] and identifiers == []


def test_a_new_holding_creates_its_security_with_identifiers_from_the_filing() -> None:
    holdings = [
        {
            "name": "Apple Inc",
            "isin": "US0378331005",
            "cusip": "037833100",
            "currency": "USD",
            "country": "US",
            "asset_category": "EC",
            "units": "NS",
            "lei": "HWUPKR0MPOU8FGXBT394",
        }
    ]
    securities, identifiers, issuers, ids = nport.plan_holdings(
        holdings, known_identifiers={}, countries={"US"}, source="sec-nport"
    )
    sid = ids[0]
    assert sid is not None
    assert securities[0]["name"] == "Apple Inc"
    assert securities[0]["security_type_code"] == "equity"  # assetCat, not units
    assert securities[0]["is_tradeable"] is False
    assert securities[0]["issuer_id"] is not None
    assert {i["kind_code"] for i in identifiers} == {"isin", "cusip"}
    assert all(i["security_id"] == sid for i in identifiers)
    assert len(issuers) == 1 and issuers[0]["lei"] == "HWUPKR0MPOU8FGXBT394"


def test_an_unknown_asset_category_falls_back_to_units_and_then_other() -> None:
    holdings = [
        {"name": "A", "isin": "US0378331005", "asset_category": "CURRENCY", "units": "NS"},
        {"name": "B", "isin": "US0378331013", "asset_category": "CURRENCY", "units": "BT"},
    ]
    securities, _, _, _ = nport.plan_holdings(
        holdings, known_identifiers={}, countries=set(), source="sec-nport"
    )
    assert [s["security_type_code"] for s in securities] == ["equity", "other"]


def test_a_re_run_of_the_same_filing_is_a_no_op() -> None:
    """The resolution reads back what it wrote the first time — the whole reason a monthly schedule
    and a manual refresh are interchangeable. The second pass finds every identifier it created."""
    holdings = [
        {"name": "Apple Inc", "isin": "US0378331005", "country": "US", "asset_category": "EC"}
    ]
    _, first_identifiers, _, first_ids = nport.plan_holdings(
        holdings, known_identifiers={}, countries={"US"}, source="sec-nport"
    )
    # Pretend the whole file was ingested already: pass the identifiers it would have created.
    known = {f"{i['kind_code']}:{i['value']}": i["security_id"] for i in first_identifiers}
    sec2, id2, _, ids2 = nport.plan_holdings(
        holdings, known_identifiers=known, countries={"US"}, source="sec-nport"
    )
    assert sec2 == [] and id2 == []
    assert ids2 == [first_ids[0]]


def test_country_of_rejects_xx_and_the_unknown() -> None:
    known = {"US", "JP"}
    assert nport.country_of({"country": "XX"}, known) is None  # N-PORT's "country unknown"
    assert nport.country_of({"country": "us"}, known) == "US"
    assert (
        nport.country_of({"country": "FR"}, known) is None
    )  # not in the table: null, not invented
    assert nport.country_of({"country": None}, known) is None


def test_fund_holding_rows_combine_two_lots_of_one_security() -> None:
    """A fund can hold a security in two lots; the PK cannot. COMBINE rather than keep the first —
    XLK reported State Street twice and keeping only the first lost 0.07 of its 0.10%."""
    holdings: list[dict[str, Any]] = [
        {"name": "Apple Inc", "isin": "US0378331005", "weight": 1.2, "balance": 100},
        {"name": "Apple Inc", "isin": "US0378331005", "weight": 0.8, "balance": 50},
        {"name": "Other", "isin": "JP3102000001", "weight": None, "balance": None},
    ]
    _, _, _, ids = nport.plan_holdings(
        holdings, known_identifiers={}, countries=set(), source="sec-nport"
    )
    rows = nport.fund_holding_rows(
        holdings,
        ids,
        fund_id="ffffffff-ffff-ffff-ffff-ffffffffffff",
        report_date="2026-05-31",
        source="sec-nport",
    )
    assert len(rows) == 2
    assert rows[0]["weight"] == 2.0
    assert rows[0]["balance"] == 150
    assert rows[1]["weight"] is None  # two Nones stay None, not 0.0


def test_a_holding_resolved_to_nothing_is_omitted_from_the_snapshot() -> None:
    holdings = [{"name": "Unidentifiable", "cusip": "000000000"}]
    _, _, _, ids = nport.plan_holdings(
        holdings, known_identifiers={}, countries=set(), source="sec-nport"
    )
    assert (
        nport.fund_holding_rows(
            holdings, ids, fund_id="f", report_date="2026-05-31", source="sec-nport"
        )
        == []
    )


def test_the_parsed_ewj_holdings_are_real_names_not_placeholders() -> None:
    holdings = nport.parse_holdings(_ewj())
    names = {h["name"] for h in holdings}
    assert "TOYOTA MOTOR CORPORATION" in names  # verified in the capture; Nintendo is not in it
    assert all(h.get("name") for h in holdings)
