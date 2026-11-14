"""The segment parser against the edge's own answers for five real SEC instances.

`fixtures/segments/` holds each filing's XBRL instance as SEC served it (gzipped; `manifest.json`
has the accession and the sha256 of the body), production's control tables as of 2026-11-09
(`vocabulary.json`), and the rows the edge's TypeScript parser produced for them
(`expected_<label>.json`: `market-refresh/segments.ts`, run under Deno).

THE EXPECTED ROWS CANNOT BE REGENERATED once the edge's handlers are deleted (Phase 6 step 8). They
are the frozen record of what the parser being replaced answered, so a change here that moves one
is a change of behaviour, to be decided and measured, never a fixture refresh. The instances can be
re-fetched: a filing is immutable, and the manifest's sha256 says whether SEC still serves the
same bytes.

Each filing is here for a rule: Apple's 10-Q (a subtotal beside its children), Alphabet's 10-Q
(cross-tab cells inside a segment), Chevron's 10-K (a grid summed both ways, a residual), Southern
Copper's FY2018 10-K (one elimination under two member names) and Equinor's 20-F (IFRS axes, a
segment column whose total is derived).
"""

from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import astuple
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest.facets import segments
from muffin_ingest.facets.segments import AxisSpec, ConceptSpec, MemberRole

FIXTURES = Path(__file__).parents[1] / "fixtures" / "segments"
MANIFEST: dict[str, Any] = json.loads((FIXTURES / "manifest.json").read_bytes())
VOCABULARY: dict[str, Any] = json.loads((FIXTURES / "vocabulary.json").read_bytes())

AXES = [AxisSpec(**a) for a in VOCABULARY["axes"]]
CONCEPTS = [ConceptSpec(**c) for c in VOCABULARY["concepts"]]
ROLES = [MemberRole(r["member_code"], r["class"]) for r in VOCABULARY["member_roles"]]


@pytest.mark.parametrize("filing", MANIFEST["filings"], ids=lambda f: str(f["label"]))
def test_a_real_instance_parses_to_the_rows_the_edge_produced(filing: dict[str, Any]) -> None:
    body = gzip.decompress((FIXTURES / f"sec_{filing['label']}.xml.gz").read_bytes())
    assert hashlib.sha256(body).hexdigest() == filing["sha256"]
    expected = json.loads((FIXTURES / f"expected_{filing['label']}.json").read_bytes())
    assert expected["columns"] == [f for f in segments.SegmentFact.__dataclass_fields__]

    facts = segments.segment_facts_from(segments.text_of(body), AXES, CONCEPTS, ROLES)

    # The same rows in the same order: the writer keeps the LAST row per key, so order is an answer.
    assert [list(astuple(f)) for f in facts] == expected["rows"]
    assert len(facts) == filing["rows"] > 0
    assert sum(1 for f in facts if f.partition_id > 0) == filing["placed"] > 0


def _expected(label: str) -> list[dict[str, Any]]:
    stored = json.loads((FIXTURES / f"expected_{label}.json").read_bytes())
    return [dict(zip(stored["columns"], row, strict=True)) for row in stored["rows"]]


def _flat(label: str, axis: str, ending: str, metric: str = "revenue") -> list[dict[str, Any]]:
    return [
        r
        for r in _expected(label)
        if r["axis"].endswith(axis)
        and r["period_ending"] == ending
        and r["metric_code"] == metric
        and r["period_type"] == "annual"
        and r["parent_member"] is None
        and r["partition_id"] > 0
    ]


def test_each_fixture_still_carries_the_rule_it_is_here_for() -> None:
    """A fixture that stopped exercising its rule would pass for ever. Each figure below is the one
    the rule was written for, read off the expected rows."""
    assert any(r["partition_id"] == 0 for r in _expected("aapl_10q_2026q3"))
    assert any(r["parent_member"] is not None for r in _expected("googl_10q_2026q3"))

    # Chevron states no flat business-line revenue: the grid is summed across its parent axis, and
    # by parent. Upstream is 45,518m + 42,861m; the US is 45,518m + 72,485m.
    lines = {
        r["member_code"]: r
        for r in _flat("cvx_10k_2025", ":StatementBusinessSegmentsAxis", "2025-12-31")
    }
    assert lines["cvx:UpstreamSegmentMember"]["value"] == 88_379_000_000
    assert lines["us-gaap:AllOtherSegmentsMember"]["value"] == 581_000_000
    assert {r["reconciled_to"] for r in lines.values()} == {231_370_000_000}
    regions = {
        r["member_code"]: r
        for r in _flat("cvx_10k_2025", ":StatementGeographicalAxis", "2025-12-31")
    }
    assert regions["country:US"]["value"] == 118_003_000_000

    # Southern Copper FY2018: segments include intersegment sales, and the one elimination is
    # stated under two member names, so the target is 7,096.7m + 79.3m and not + 158.6m.
    scco = _flat("scco_10k_2018", ":StatementBusinessSegmentsAxis", "2018-12-31")
    assert sum(r["value"] for r in scco) == 7_176_000_000
    assert {r["reconciled_to"] for r in scco} == {7_176_000_000}
    products = _flat("scco_10k_2018", ":ProductOrServiceAxis", "2018-12-31")
    assert {r["reconciled_to"] for r in products} == {7_096_700_000}

    # Equinor: IFRS axes, and a segment column whose total is the consolidated 106,462m less the
    # elimination of -42,421m.
    eqnr = _flat("eqnr_20f_2025", "ifrs-full:SegmentsAxis", "2025-12-31")
    assert len(eqnr) == 6 and {r["reconciled_to"] for r in eqnr} == {148_883_000_000}
    assert {r["currency"] for r in _expected("eqnr_20f_2025")} == {"USD"}
