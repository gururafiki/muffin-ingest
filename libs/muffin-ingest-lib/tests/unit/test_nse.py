"""NSE's results lists, instances and the normaliser (India's segment lane).

Fixtures captured 2026-11-16 from the node (`fixtures/nse/`):
  * Reliance's results list from `www.nseindia.in`, NSE's API host since it began redirecting
    `www.nseindia.com/api` there (a 301, measured that day), and the answer for a symbol NSE does
    not list (`[]`, HTTP 200);
  * three instances through http-cache's `nse-archives` location: Reliance FY2024 consolidated and
    standalone (`INDAS_*`), Axis Bank FY2024 consolidated (`BANKING_*`, the bank taxonomy).

`expected_*.json` are the rows the EDGE produced for the two consolidated instances: its `in.ts`
normaliser and `segments.ts` parser, run under Deno with `fixtures/segments/vocabulary.json`. Like
the SEC golden rows, they cannot be regenerated once the edge's handlers are deleted (step 8).
"""

from __future__ import annotations

import json
from dataclasses import astuple
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest.facets import nse as fn
from muffin_ingest.facets import segments
from muffin_ingest.facets.segments import AxisSpec, ConceptSpec, MemberRole
from muffin_ingest.providers import nse
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parents[1] / "fixtures"
RELIANCE = (FIXTURES / "nse" / "INDAS_104634_1106038_22042024074704.xml").read_bytes()
RELIANCE_STANDALONE = (FIXTURES / "nse" / "INDAS_104633_1106029_22042024074028.xml").read_bytes()
AXIS = (FIXTURES / "nse" / "BANKING_104697_1107859_25042024023559.xml").read_bytes()
RELIANCE_LIST = (FIXTURES / "nse" / "nse_list_RELIANCE.json").read_bytes()
NOT_LISTED = (FIXTURES / "nse" / "nse_list_ZZQQNOTASYMBOL.json").read_bytes()

VOCABULARY: dict[str, Any] = json.loads((FIXTURES / "segments" / "vocabulary.json").read_bytes())
AXES = [AxisSpec(**a) for a in VOCABULARY["axes"]]
CONCEPTS = [ConceptSpec(**c) for c in VOCABULARY["concepts"]]
ROLES = [MemberRole(r["member_code"], r["class"]) for r in VOCABULARY["member_roles"]]


def _answer(body: bytes, status: int = 200) -> nse.Answer:
    document = Document(
        url="https://www.nseindia.in/api/corporates-financial-results?symbol=RELIANCE",
        body=body,
        content_type="application/json",
        fetched_at=datetime(2026, 11, 16, tzinfo=UTC),
    )
    return nse.Answer(document=document, status=status)


@pytest.mark.parametrize(
    ("label", "body"), [("reliance_fy2024", RELIANCE), ("axisbank_fy2024", AXIS)]
)
def test_a_consolidated_instance_parses_to_the_rows_the_edge_produced(
    label: str, body: bytes
) -> None:
    expected = json.loads((FIXTURES / "nse" / f"expected_{label}.json").read_bytes())
    assert expected["columns"] == list(segments.SegmentFact.__dataclass_fields__)

    normalised = fn.normalise(segments.text_of(body))
    facts = segments.segment_facts_from(normalised.xml, AXES, CONCEPTS, ROLES)

    assert (normalised.named, normalised.annualised) == (expected["named"], expected["annualised"])
    # The same rows in the same order: the writer keeps the LAST row per key, so order is an answer.
    assert [list(astuple(f)) for f in facts] == expected["rows"]
    assert len(facts) == 25 and all(f.partition_id > 0 for f in facts)


def test_the_year_is_told_from_the_quarter_by_the_context_id() -> None:
    """Reliance's FY2024 split: the `Four*` column becomes the year ending 2024-03-31 and reconciles
    to 11,094,900,000,000 (the edge measured 14,011,150,000,000 when quarter and year were
    unioned). The `One*` column stays a quarter."""
    normalised = fn.normalise(segments.text_of(RELIANCE))
    facts = segments.segment_facts_from(normalised.xml, AXES, CONCEPTS, ROLES)
    year = {
        f.member_code: f.value
        for f in facts
        if f.metric_code == "revenue" and f.period_type == "annual"
    }
    assert year["nse:RetailMember"] == 3_068_480_000_000
    assert {f.period_start for f in facts if f.period_type == "annual"} == {"2023-04-01"}
    assert {
        f.reconciled_to for f in facts if f.metric_code == "revenue" and f.period_type == "annual"
    } == {11_094_900_000_000}
    assert any(f.period_type == "quarter" for f in facts)
    assert "OneReportableSegmentRevenue01Member" not in normalised.xml


def test_a_standalone_filing_is_not_wanted_and_is_not_a_failure() -> None:
    with pytest.raises(fn.NotConsolidated):
        fn.normalise(segments.text_of(RELIANCE_STANDALONE))
    with pytest.raises(fn.NotConsolidated):
        fn.normalise("<xbrli:xbrl>no nature at all</xbrli:xbrl>")


def test_a_member_code_is_the_segment_s_own_name() -> None:
    assert fn.member_code_for("Oil to Chemicals (O2C)") == "nse:OilToChemicalsO2CMember"
    assert (
        fn.member_code_for("Energy, Utilities, Resources and Services")
        == "nse:EnergyUtilitiesResourcesAndServicesMember"
    )
    assert fn.member_code_for("  ") == "nse:UnnamedMember"


def test_the_year_runs_twelve_months_to_the_period_end_as_javascript_counts_them() -> None:
    assert fn.year_start_for("2024-03-31") == "2023-04-01"
    assert fn.year_start_for("2024-12-31") == "2024-01-01"
    # 29 February in a common year rolls over to 1 March, then the day after: the edge's Date.UTC.
    assert fn.year_start_for("2024-02-29") == "2023-03-02"


def test_a_results_list_names_each_xml_result_on_the_com_archives_host() -> None:
    """NSE now writes `nsearchives.nseindia.in`; every stored filing is keyed by the `.com` link,
    which serves the same bytes. A result with no XBRL is listed as `.../xbrl/-` and is not a
    filing (the edge filed that placeholder for 440 companies)."""
    filed = fn.parse_filings(RELIANCE_LIST)
    assert filed is not None and len(filed) == 11
    assert all(f.link.startswith("https://nsearchives.nseindia.com/corporate/xbrl/") for f in filed)
    assert all(f.link.endswith(".xml") for f in filed)
    assert filed[0] == fn.Filed(
        "https://nsearchives.nseindia.com/corporate/xbrl/INDAS_104634_1106038_22042024074704.xml",
        "2024-03-31",
        "2024-04-22",
    )
    assert fn.parse_filings(NOT_LISTED) == []
    assert fn.parse_filings(b"<html>Access Denied</html>") is None
    assert fn.parse_filings(b'{"data": []}') == []


def test_a_link_is_canonical_on_either_archives_host() -> None:
    link = "https://nsearchives.nseindia.in/corporate/xbrl/INDAS_1.xml"
    assert nse.canonical(link) == "https://nsearchives.nseindia.com/corporate/xbrl/INDAS_1.xml"
    assert nse.canonical(nse.canonical(link)) == nse.canonical(link)
    assert nse.canonical("https://example.com/x.xml") == "https://example.com/x.xml"


def test_a_list_or_an_instance_that_is_not_an_answer_is_asked_again() -> None:
    assert fn.list_outcome(_answer(RELIANCE_LIST)) == "answered"
    assert fn.list_outcome(_answer(NOT_LISTED)) == "answered"
    assert fn.list_outcome(_answer(b"<html>gateway</html>")) == "not_answer"
    assert fn.list_outcome(_answer(b"", status=404)) == "absent"
    assert fn.instance_outcome(_answer(RELIANCE)) == "answered"
    assert fn.instance_outcome(_answer(b"<html>gateway</html>")) == "not_answer"
    assert fn.instance_outcome(_answer(b"", status=404)) == "absent"


def test_filing_rows_carry_the_filing_date_and_never_the_marks() -> None:
    filed = fn.parse_filings(RELIANCE_LIST) or []
    rows = fn.filing_rows(["s1", "s2"], filed)
    assert len(rows) == 2 * len(filed)
    assert rows[0] == {
        "security_id": "s1",
        "accession_number": filed[0].link,
        "report_type": "Annual",
        "report_date": "2024-03-31",
        "filing_date": "2024-04-22",
        "report_url": filed[0].link,
        "source_code": "nse",
        "is_xbrl": True,
    }
    assert not {"segments_parsed_at", "segments_parser_version"} & set(rows[0])


def test_the_newest_finished_fetch_of_an_instance_decides() -> None:
    link = "https://nsearchives.nseindia.com/corporate/xbrl/A.xml"
    rows = [
        {"document": "instance", "accession": link, "outcome": "not_answer",
         "fetched_at": "2026-11-01", "run_id": "r1", "body": b"<html/>"},
        {"document": "instance", "accession": link, "outcome": "answered",
         "fetched_at": "2026-11-16", "run_id": "r2", "body": RELIANCE},
        {"document": "instance", "accession": "other", "outcome": "answered",
         "fetched_at": "2026-11-16", "run_id": "r2", "body": AXIS},
        {"document": "filings", "accession": "results", "outcome": "answered",
         "fetched_at": "2026-11-15", "run_id": "r1", "body": NOT_LISTED},
        {"document": "filings", "accession": "results", "outcome": "answered",
         "fetched_at": "2026-11-16", "run_id": "r2", "body": RELIANCE_LIST},
    ]  # fmt: skip
    held = fn.held_instance(rows, link)
    assert held == fn.Held("answered", "2026-11-16", "r2")
    assert fn.instance_body(rows, link, held) == RELIANCE
    assert fn.held_instance(rows, "missing") is None
    newest = fn.newest_list(rows)
    assert newest is not None and newest["body"] == RELIANCE_LIST


def test_a_date_reads_with_or_without_its_time() -> None:
    assert fn.iso_from_nse_date("31-Mar-2024") == "2024-03-31"
    assert fn.iso_from_nse_date("22-Apr-2024 19:47") == "2024-04-22"
    assert fn.iso_from_nse_date("2024-03-31") is None
    assert fn.iso_from_nse_date(None) is None
