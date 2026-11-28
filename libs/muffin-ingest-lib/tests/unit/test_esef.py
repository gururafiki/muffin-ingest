"""ESEF: the index's answers, a filer's list, and a report's segment note read.

The report fixtures are nine real annual reports from filings.xbrl.org, captured 2026-11-28 and
trimmed to what `read_report` reads: the segment-note blocks and every non-dimensional monetary
fact (the anchors). Each exercises a European table shape the gate found:
  - L'Oréal (FR): divisions as rows, zones with an `of which` row, a product table under
    `par métier`, and zones under a column headed only `2025`;
  - LVMH (FR): external sales beside gross sales, and a quarterly table restating the year;
  - Schneider (FR): labels wrapped over three header rows, a corporate-costs column;
  - Iberdrola (ES): `sin dato` in every empty cell, segments stated before discontinued ops;
  - Inditex (ES, January year end): a year row over every column, segments before intersegment;
  - Atlas Copco (SE): the year in the label cell above the segment names;
  - Enel (IT, English): labels continued on the next row, a heading broken mid-word;
  - ASML (NL): regions as rows under a year row;
  - Airbus (NL): a segment note with no table at all.
The `*_filings.json` are each filer's whole list as the index served it.
"""

from __future__ import annotations

import gzip
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from muffin_ingest.facets import esef
from muffin_ingest.facets import segment_note as sn
from muffin_ingest.providers import xbrl_org
from muffin_ingest.providers.documents import Document
from tests.unit import esef_rules

FIXTURES = Path(__file__).parent.parent / "fixtures" / "esef"
VOCABULARY = esef_rules.vocabulary()


def _report(name: str) -> esef.ReportRead:
    body = gzip.decompress((FIXTURES / name).read_bytes())
    return esef.read_report(body, VOCABULARY)


def _splits(
    read: esef.ReportRead, metric: str = "revenue"
) -> dict[tuple[str, str], dict[str, float]]:
    out: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for f in read.note.facts:
        if f.partition_id and f.metric_code == metric and f.parent_member is None:
            out[(f.axis, f.period_ending)][f.member_code.removeprefix("esef:")] = f.value
    return out


def _target(read: esef.ReportRead, axis: str, end: str) -> float | None:
    found = {
        f.reconciled_to
        for f in read.note.facts
        if f.partition_id
        and f.axis == axis
        and f.period_ending == end
        and f.metric_code == "revenue"
    }
    assert len(found) == 1, found
    return found.pop()


# ── Reports ────────────────────────────────────────────────────────────────────────────────────


def test_loreal_states_three_splits_each_adding_up_to_its_tagged_revenue() -> None:
    read = _report("loreal_2025-12-31.json.gz")
    splits = _splits(read)
    assert splits[("esef:segments", "2025-12-31")] == {
        "Produits Professionnels": 5_163_000_000.0,
        "Produits Grand Public": 16_089_700_000.0,
        "Luxe": 15_595_100_000.0,
        "Beauté Dermatologique": 7_204_100_000.0,
    }
    zones = splits[("esef:geography", "2025-12-31")]
    # `dont France` is an "of which" row: France is inside Europe, never beside it.
    assert set(zones) == {"Europe", "Amérique du Nord", "Asie du Nord", "SAPMENA \u2013 SSA",
                          "Amérique Latine"}  # fmt: skip
    assert set(splits[("esef:products", "2025-12-31")]) >= {"Maquillage", "Parfums", "Coloration"}
    for axis in ("esef:segments", "esef:geography", "esef:products"):
        assert _target(read, axis, "2025-12-31") == 44_052_000_000.0
    # Three years, each read from its own table.
    assert {end for _, end in splits} == {"2023-12-31", "2024-12-31", "2025-12-31"}


def test_a_wrapped_header_is_one_label_and_a_cost_centre_no_segment() -> None:
    read = _report("schneider_2025-12-31.json.gz")
    business = _splits(read)[("esef:segments", "2025-12-31")]
    assert business == {"Gestion de l'énergie": 33_130_000_000.0,
                        "Automatismes industriels": 7_022_000_000.0}  # fmt: skip
    # `dont France` under `Europe de l'Ouest` is a subset column, never a region of its own.
    assert set(_splits(read)[("esef:geography", "2025-12-31")]) == {
        "Europe de l'Ouest", "Asie Pacifique", "Amérique du Nord", "Reste du monde",
    }  # fmt: skip


def test_external_sales_are_the_split_and_a_restating_table_adds_nothing() -> None:
    read = _report("lvmh_2024-12-31.json.gz")
    business = _splits(read)[("esef:segments", "2024-12-31")]
    # `Ventes hors Groupe`, not `Total des ventes` (gross of intra-group sales): 40,990 m,
    # not 41,060.
    assert business["Mode et Maroquinerie"] == 40_990_000_000.0
    assert len(business) == 6
    per_member: defaultdict[str, int] = defaultdict(int)
    for f in read.note.facts:
        if (
            f.axis == "esef:segments"
            and f.metric_code == "revenue"
            and f.period_ending == "2024-12-31"
        ):
            per_member[f.member_code] += 1
    # The quarterly table's `Total 2024` row states the year again, gross: never a second split.
    assert set(per_member.values()) == {1}


def test_segments_stated_before_eliminations_reconcile_to_the_total_less_them() -> None:
    read = _report("inditex_2026-01-31.json.gz")
    business = _splits(read)[("esef:segments", "2026-01-31")]
    assert sum(business.values()) == 40_107_000_000.0
    # The tagged revenue is 39,864 m; the filing's own intersegment column is (243) m.
    assert _target(read, "esef:segments", "2026-01-31") == 40_107_000_000.0
    assert _target(read, "esef:geography", "2026-01-31") == 39_864_000_000.0
    assert set(_splits(read)[("esef:geography", "2026-01-31")]) == {
        "España", "Resto Europa", "América", "Asia y resto del Mundo",
    }  # fmt: skip


def test_a_business_table_after_a_geographic_sentence_stays_business() -> None:
    """Iberdrola introduces a table of assets "by geographic location" and follows it with its
    business segments under no sentence at all: a kind named in prose is local to its table."""
    read = _report("iberdrola_2025-12-31.json.gz")
    splits = _splits(read)
    assert set(splits[("esef:segments", "2025-12-31")]) == {
        "Redes", "Renovables y Generación Sostenible", "Clientes",
        "Otros negocios, Corporación y ajustes",
    }  # fmt: skip
    assert "México" in splits[("esef:geography", "2025-12-31")]  # `(Nota 18)` stripped
    # Before discontinued operations: 45,547 m against a tagged 44,076 m.
    assert _target(read, "esef:segments", "2025-12-31") == 45_547_000_000.0


def test_a_year_in_the_label_cell_is_a_header_not_a_figure() -> None:
    read = _report("atlas_copco_2025-12-31.json.gz")
    business = _splits(read)[("esef:segments", "2025-12-31")]
    assert business == {
        "Kompressorteknik": 76_513_000_000.0,
        "Vakuum teknik": 36_700_000_000.0,
        "Industriteknik": 26_313_000_000.0,
        "Energiteknik": 28_817_000_000.0,
    }
    assert {f.currency for f in read.note.facts} == {"SEK"}


def test_a_label_continued_on_the_next_row_is_one_label() -> None:
    read = _report("enel_2024-12-31.json.gz")
    business = _splits(read)[("esef:segments", "2024-12-31")]
    assert business["End-user Markets"] == 39_215_000_000.0
    assert business["Holding and Services"] == -12_000_000.0
    assert sum(business.values()) == 78_947_000_000.0
    assert "Italy" in _splits(read)[("esef:geography", "2024-12-31")]


def test_regions_as_rows_under_a_year_row() -> None:
    read = _report("asml_2025-12-31.json.gz")
    regions = _splits(read)[("esef:geography", "2025-12-31")]
    assert regions["Taiwan"] == 8_337_900_000.0
    assert len(regions) == 9
    assert _target(read, "esef:geography", "2025-12-31") == pytest.approx(32_667_300_000.0)


def test_a_note_with_no_table_reads_nothing() -> None:
    read = _report("airbus_2025-12-31.json.gz")
    assert read.block is not None
    assert read.note.tables == 0 and not read.note.facts


# ── The interpreter's European rules, each with the case its absence gets wrong ────────────────

SPACED = sn.NoteVocabulary(
    headings=(
        sn.Heading(r"(?i)clients", "skip"),
        sn.Heading(r"(?i)géograph", "geography"),
        sn.Heading(r"(?i)segment", "business"),
    ),
    labels=(
        sn.Label(r"(?i)^chiffre d.affaires$", "revenue"),
        sn.Label(r"(?i)^total$", "total"),
        sn.Label(r"(?i)^éliminations?\b", "adjustment"),
    ),
    members=(sn.Heading(r"(?i)^(france|europe|asie)\b", "geography"),),
    spaced=True,
)
AXES = {"business": "x:seg", "geography": "x:geo"}
ANCHORS = (sn.Anchor("ifrs-full:Revenue", 100e6, "2025-01-01", "2025-12-31", None, "EUR"),)


def _read(html: str) -> sn.NoteRead:
    return sn.read_note(html, ANCHORS, SPACED, axis_prefix="x:", axes=AXES, detect_numbers=True,
                        infer_scale=True)  # fmt: skip


def _table(rows: list[list[str]], before: str = "Information par segment") -> str:
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<p>{before}</p><table>{body}</table>"


def test_a_year_row_above_the_names_is_a_header() -> None:
    read = _read(
        _table([["2025", "Alpha", "Beta", "Total"], ["Chiffre d'affaires", "60", "40", "100"]])
    )
    assert {f.member_code: f.value for f in read.facts} == {"x:Alpha": 60e6, "x:Beta": 40e6}


def test_a_group_header_spanning_columns_is_no_part_of_a_name() -> None:
    html = (
        "<p>Information par segment</p><table>"
        "<tr><td></td><td colspan='2'>Activités</td><td></td></tr>"
        "<tr><td></td><td>Alpha</td><td>Beta</td><td>Total</td></tr>"
        "<tr><td>Chiffre d'affaires</td><td>60</td><td>40</td><td>100</td></tr></table>"
    )
    assert {f.member_code for f in _read(html).facts} == {"x:Alpha", "x:Beta"}


def test_a_wrapped_name_is_joined_in_its_own_column() -> None:
    read = _read(_table([["", "Gestion de", "Automatismes", ""],
                         ["", "l'énergie", "industriels", "Total"],
                         ["Chiffre d'affaires", "60", "40", "100"]]))  # fmt: skip
    assert {f.member_code for f in read.facts} == {"x:Gestion de l'énergie",
                                                   "x:Automatismes industriels"}  # fmt: skip


def test_a_lower_case_row_continues_the_label_above() -> None:
    read = _read(_table([["", "Alpha", "Beta", "Total"],
                         ["Chiffre", "60", "40", "100"],
                         ["d'affaires", "", "", ""]]))  # fmt: skip
    assert {f.metric_code for f in read.facts} == {"revenue"}


def test_a_derived_target_is_taken_only_when_the_members_add_up_to_it() -> None:
    gross = _read(_table([["", "Alpha", "Beta", "Éliminations", "Total"],
                          ["Chiffre d'affaires", "65", "40", "(5)", "100"]]))  # fmt: skip
    assert {f.reconciled_to for f in gross.facts} == {105e6}
    # The same table with members that already reconcile keeps the tagged figure.
    net = _read(_table([["", "Alpha", "Beta", "Éliminations", "Total"],
                        ["Chiffre d'affaires", "60", "40", "-", "100"]]))  # fmt: skip
    assert {f.reconciled_to for f in net.facts} == {100e6}


def test_a_kind_named_in_prose_is_local_to_its_table() -> None:
    first = _table([["", "France", "Europe", "Total"], ["Chiffre d'affaires", "60", "40", "100"]],
                   before="Ventilation géographique")  # fmt: skip
    second = (
        "<table><tr><td></td><td>Alpha</td><td>Beta</td><td>Total</td></tr>"
        "<tr><td>Chiffre d'affaires</td><td>70</td><td>30</td><td>100</td></tr></table>"
    )
    axes = {f.member_code: f.axis for f in _read(first + second).facts}
    assert axes == {"x:France": "x:geo", "x:Europe": "x:geo", "x:Alpha": "x:seg", "x:Beta": "x:seg"}


def test_the_kind_nearest_the_table_in_a_sentence_wins() -> None:
    """A skip pattern tried first would call this a customer table: Inditex's sentence names its
    customers' location and then the geographic table it introduces."""
    sentence = "Ventes selon la localisation des clients, par zone géographique"
    rows = [["", "Alpha", "Beta", "Total"], ["Chiffre d'affaires", "60", "40", "100"]]
    html = _table(rows, before=sentence)
    assert {f.axis for f in _read(html).facts} == {"x:geo"}


def test_the_first_table_to_state_a_split_keeps_it() -> None:
    once = _table([["", "Alpha", "Beta", "Total"], ["Chiffre d'affaires", "60", "40", "100"]])
    again = _table([["", "Alpha", "Beta", "Éliminations", "Total"],
                    ["Chiffre d'affaires", "62", "43", "(5)", "100"]])  # fmt: skip
    values = [f.value for f in _read(once + again).facts if f.member_code == "x:Alpha"]
    assert values == [60e6]


def test_a_reference_in_a_name_is_stripped() -> None:
    assert sn.member_name("México (Nota 18)", spaced=True) == "México"
    assert sn.member_name("Operación discontinuada (Notas 2.c y 18)", spaced=True) == (
        "Operación discontinuada"
    )
    assert sn.member_name("Activos no corrientes (*)", spaced=True) == "Activos no corrientes"
    # A Japanese name is untouched by the European rules.
    assert sn.member_name("日本 (注)2") == "日本"


# ── A filer's list ─────────────────────────────────────────────────────────────────────────────


def test_interim_reports_are_not_annual_ones() -> None:
    listed = esef.parse_filings((FIXTURES / "novo_nordisk_filings.json").read_bytes())
    assert listed is not None and len(listed) > 12
    annual = esef.annual_reports(listed, (12, 31))
    assert annual and {f.period_end[5:] for f in annual} == {"12-31"}
    assert annual[0].period_end > annual[-1].period_end  # newest first


def test_a_january_year_end_comes_from_the_calendar_and_a_refiling_wins() -> None:
    listed = esef.parse_filings((FIXTURES / "inditex_filings.json").read_bytes())
    assert listed is not None
    annual = esef.annual_reports(listed, (1, 31))
    by_end = {f.period_end: f.fxo_id for f in annual}
    assert len(by_end) == len({f.period_end for f in listed})
    # Two filings for FY2024 (`-0` and `-1`): the one the index added last is the one read.
    pair = sorted((f for f in listed if f.period_end == "2025-01-31"), key=lambda f: f.added or "")
    assert by_end["2025-01-31"] == pair[-1].fxo_id
    # Without its calendar nothing of Inditex's is annual: 31 January is no 31 December.
    assert not esef.annual_reports(listed, None)


def test_a_list_names_period_country_filing_date_and_report() -> None:
    listed = esef.parse_filings((FIXTURES / "loreal_filings.json").read_bytes())
    assert listed is not None
    newest = listed[0]
    assert newest.period_end == "2025-12-31" and newest.country == "FR"
    assert newest.filed == "2026-03-18"  # published, not the day the index added it
    assert newest.json_url is not None and newest.json_url.endswith(".json")
    assert esef.parse_filings(b"<html>Bad gateway</html>") is None
    rows = esef.filing_rows(["s1", "s2"], esef.annual_reports(listed, None))
    assert {r["report_type"] for r in rows} == {esef.ANNUAL}
    assert len(rows) == 2 * 5 and "segments_parsed_at" not in rows[0]


# ── Raw and the provider ───────────────────────────────────────────────────────────────────────


def _answer(body: bytes, status: int = 200, too_large: bool = False) -> xbrl_org.Answer:
    document = Document(
        url="https://filings.xbrl.org/x.json",
        body=body,
        content_type="application/json",
        fetched_at=datetime(2026, 11, 28, tzinfo=UTC),
    )
    return xbrl_org.Answer(document=document, status=status, too_large=too_large)


def test_a_report_is_answered_only_when_it_opens_as_json() -> None:
    assert esef.report_outcome(_answer(b'{"facts": {}}')) == "answered"
    assert esef.report_outcome(_answer(b"<html>gateway</html>")) == "not_json"
    assert esef.report_outcome(_answer(b"", status=404)) == "absent"
    assert esef.report_outcome(_answer(b"", too_large=True)) == "too_large"
    assert esef.list_outcome(_answer(b"<html></html>")) == "not_json"


def test_the_newest_finished_fetch_of_a_report_decides() -> None:
    rows = [
        {"document": "report", "accession": "A", "outcome": "not_json", "fetched_at": "2026-11-01",
         "run_id": "r1", "body": b"<html>"},
        {"document": "report", "accession": "A", "outcome": "answered", "fetched_at": "2026-11-02",
         "run_id": "r2", "body": b"{}"},
    ]  # fmt: skip
    held = esef.held_report(rows, "A")
    assert held is not None and held.outcome == "answered" and held.run_id == "r2"
    assert esef.report_body(rows, held) == b"{}"
    assert esef.held_report(rows, "B") is None


@respx.mock
def test_a_429_is_a_refusal_and_a_404_an_absence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MUFFIN_USER_AGENT", "muffin-test/1.0 (ops@example.com)")
    monkeypatch.delenv("HTTP_CACHE_URL", raising=False)
    monkeypatch.delenv("XBRL_ORG_BASE_URL", raising=False)
    route = respx.get(url__regex=r".*/api/filings.*").mock(
        return_value=httpx.Response(200, content=b'{"data": []}')
    )
    answer = xbrl_org.filings("529900JI1GG6F7RKVI53")
    assert answer.status == 200 and esef.parse_filings(answer.document.body) == []
    sent = route.calls.last.request
    assert "entity.identifier" in str(sent.url) and sent.headers["X-Muffin-Cache-Bypass"] == "1"
    respx.get(url__regex=r".*/gone\.json").mock(return_value=httpx.Response(404))
    assert xbrl_org.report("/gone.json").status == 404
    respx.get(url__regex=r".*/busy\.json").mock(return_value=httpx.Response(429))
    with pytest.raises(xbrl_org.XbrlOrgRefused) as refused:
        xbrl_org.report("/busy.json")
    assert refused.value.status == 429


def test_the_cache_location_proxies_to_the_index() -> None:
    assert xbrl_org._base().endswith(("filings.xbrl.org", "/xbrl-org"))
