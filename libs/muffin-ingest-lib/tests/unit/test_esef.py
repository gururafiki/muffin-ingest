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


# ── Rules the first drain found (2026-11-28: 207 reports since FY2022, 44 placing a split) ──────

#: The same vocabulary, able to earn a metric by arithmetic: a line labelled by its column header
#: (`1.1.` over `31.12.2025`) is revenue only because its members add up to a tagged Revenue.
EARNING = sn.NoteVocabulary(
    headings=SPACED.headings,
    labels=SPACED.labels,
    concepts=(("revenue", r"^Revenue$"),),
    members=SPACED.members,
    spaced=True,
)


def _earn(html: str) -> sn.NoteRead:
    return sn.read_note(html, ANCHORS, EARNING, axis_prefix="x:", axes=AXES, detect_numbers=True,
                        infer_scale=True)  # fmt: skip


def test_a_date_in_the_header_is_text_not_a_figure() -> None:
    """KONE heads its columns `1.1.` over `31.12.2025`. Read in the comma convention the date
    becomes the figure 31,122,025, the header ends a row early, and that row's label becomes a
    member worth thirty-one million that no total can match."""
    read = _earn(_table([["", "1.1.\u2013", "%"],
                         ["Liikevaihto", "31.12.2025", ""],
                         ["Alpha", "60,0", "60"],
                         ["Beta", "40,0", "40"],
                         ["Total", "100,0", ""]]))  # fmt: skip
    assert {f.member_code: f.value for f in read.facts} == {"x:Alpha": 60e6, "x:Beta": 40e6}
    # A date is never rewritten as a figure, and a figure beside it still is.
    table = sn.in_convention(sn.Table(rows=[["31.12.2025", "1.234,5", "1.1.2025"]]), "comma")
    assert table.rows == [["31.12.2025", "1234.5", "1.1.2025"]]


@pytest.mark.parametrize("total", ["Acme", "To t a l"])
def test_a_member_equal_to_the_sum_of_the_others_is_the_total(total: str) -> None:
    """Signify and MFE head their total column with the company's own name, and Johnson
    Matthey's converted page spaces `Total` into `To t a l`: no label names either, so the total
    was summed as a segment and every line came to twice the tagged figure."""
    read = _read(_table([["", "Alpha", "Beta", total],
                         ["Chiffre d'affaires", "60", "40", "100"]]))  # fmt: skip
    assert {f.member_code: f.value for f in read.facts} == {"x:Alpha": 60e6, "x:Beta": 40e6}


def test_a_total_found_by_value_allows_for_the_eliminations() -> None:
    read = _read(_table([["", "Alpha", "Beta", "Éliminations", "Acme"],
                         ["Chiffre d'affaires", "65", "40", "(5)", "100"]]))  # fmt: skip
    assert {f.member_code for f in read.facts} == {"x:Alpha", "x:Beta"}
    assert {f.reconciled_to for f in read.facts} == {105e6}


def test_a_line_that_already_reconciles_keeps_every_member() -> None:
    """Gamma happens to equal Alpha and Beta together. The members add up to the tagged figure as
    they stand, so none of them is the total: the rule applies only to a line that reconciles to
    nothing, and never between two members."""
    three = _read(_table([["", "Alpha", "Beta", "Gamma"],
                          ["Chiffre d'affaires", "30", "20", "50"]]))  # fmt: skip
    assert {f.member_code for f in three.facts} == {"x:Alpha", "x:Beta", "x:Gamma"}
    two = _read(_table([["", "Alpha", "Beta"], ["Chiffre d'affaires", "50", "50"]]))
    assert {f.member_code for f in two.facts} == {"x:Alpha", "x:Beta"}


def test_two_columns_of_one_name_under_two_groups_are_two_members() -> None:
    """Bakkafrost sets `Faroe Islands` and `Scotland` under both `Freshwater` and `Farming`.
    Named by their own cells alone, the second pair merged into the first and the split fell
    DKK 5.2 m short of its total."""
    html = (
        "<p>Information par segment</p><table>"
        "<tr><td></td><td colspan='2'>Freshwater</td><td colspan='2'>Farming</td><td></td></tr>"
        "<tr><td></td><td>Faroe</td><td>Scotland</td><td>Faroe</td><td>Scotland</td>"
        "<td>Total</td></tr>"
        "<tr><td>Chiffre d'affaires</td><td>10</td><td>20</td><td>30</td><td>40</td>"
        "<td>100</td></tr></table>"
    )
    assert {f.member_code: f.value for f in _read(html).facts} == {
        "x:Freshwater Faroe": 10e6, "x:Freshwater Scotland": 20e6,
        "x:Farming Faroe": 30e6, "x:Farming Scotland": 40e6,
    }  # fmt: skip


def test_a_table_of_places_after_a_sentence_about_customers_is_geography() -> None:
    """Sartorius Stedim states that no customer exceeds 10% of revenue, then gives its revenue by
    region. The sentence names customers, a skipped kind, and the table of places that follows it
    reconciled exactly and was read as nothing."""
    rows = [["", "France", "Europe", "Total"], ["Chiffre d'affaires", "60", "40", "100"]]
    read = _read(_table(rows, before="Aucun des clients ne dépasse 10 % du chiffre d'affaires"))
    assert {f.axis for f in read.facts} == {"x:geo"}
    # A customer table proper stays skipped: its members are no places.
    customers = [["", "Client A", "Client B", "Total"], ["Chiffre d'affaires", "60", "40", "100"]]
    assert not _read(_table(customers, before="Principaux clients")).facts


def test_a_word_split_by_an_empty_span_is_one_word() -> None:
    """A page converted from PDF (pdf2htmlEX) splits words with empty kerning spans:
    `Clean A<span></span>ir`. Joined with a space, every name and label was letter-spaced
    (`Cle an A ir`, `To t a l`), so no pattern matched it, and the text before a table arrived in
    fragments (`Mar`, `ch 2026`) no heading could match either. Only a block ends a word."""
    html = (
        "<div>Information par seg<span class='_'></span>ment</div>"
        "<table><tr><td></td><td>Fra<span class='_'></span>nce</td><td>Eu<span></span>rope</td>"
        "<td>To<span></span>tal</td></tr>"
        "<tr><td><p>Chiffre</p><p>d'affaires</p></td><td>60</td><td>40</td><td>100</td></tr>"
        "</table>"
    )
    [table] = sn.tables(html, inline=True)
    assert table.before == ["Information par segment"]
    assert table.rows[0] == ["", "France", "Europe", "Total"]
    assert table.rows[1][0] == "Chiffre d'affaires"
    assert {f.member_code: f.axis for f in _read(html).facts} == {
        "x:France": "x:geo", "x:Europe": "x:geo"
    }  # fmt: skip
    # Japanese notes are read as they always were: a chunk is a word.
    [jp] = sn.tables(html)
    assert jp.rows[0][1] == "Fra nce"


def test_a_header_cell_holding_a_year_is_no_part_of_a_name() -> None:
    """A period label over the columns (`2025/26`, `52 weeks to 27 February 2025`,
    `Januari-december 2024`) is no part of a segment's name, whether it spans the columns or sits
    above one: Matas read as `KICKS 2025/26`, and a split for two years named its members by the
    year and never merged them."""
    html = (
        "<p>Information par segment</p><table>"
        "<tr><td></td><td colspan='3'>2025/26</td><td colspan='3'>2024/25</td></tr>"
        "<tr><td></td><td>Alpha</td><td>Beta</td><td>Total</td>"
        "<td>Alpha</td><td>Beta</td><td>Total</td></tr>"
        "<tr><td>Chiffre d'affaires</td><td>60</td><td>40</td><td>100</td>"
        "<td>55</td><td>35</td><td>90</td></tr></table>"
    )
    assert {f.member_code for f in _read(html).facts} == {"x:Alpha", "x:Beta"}
    own = _read(_table([["", "52 weeks to 27 February 2025", "", ""],
                        ["", "Alpha", "Beta", "Total"],
                        ["Chiffre d'affaires", "60", "40", "100"]]))  # fmt: skip
    assert {f.member_code for f in own.facts} == {"x:Alpha", "x:Beta"}


def test_two_spans_meeting_letter_to_digit_are_two_words() -> None:
    """Matas's converter sets `KICKS` and `2025/26` as two spans on two lines of one cell with no
    space between them, where pdf2htmlEX splits inside a word. A letter meeting a digit, or a
    lower-case letter meeting a capital, is a boundary; letters meeting letters are one word."""
    html = (
        "<p>Information par segment</p><table>"
        "<tr><td></td><td><span>KICKS</span><span>2025/26</span></td>"
        "<td><span>Matas</span><span>2025/26</span></td><td>Total</td></tr>"
        "<tr><td><span>Chiffre d'</span><span>affaires</span></td><td>60</td><td>40</td>"
        "<td>100</td></tr></table>"
    )
    [table] = sn.tables(html, inline=True)
    assert table.rows[0][1:3] == ["KICKS 2025/26", "Matas 2025/26"]
    assert table.rows[1][0] == "Chiffre d'affaires"
    assert sn.tables("<table><tr><td><span>Asset</span><span>Management</span></td></tr></table>",
                     inline=True)[0].rows[0] == ["Asset Management"]  # fmt: skip


def test_a_skip_named_in_a_sentence_pointing_elsewhere_is_about_that_note() -> None:
    """Schroders closes the paragraph above its segment table with `A reconciliation … is
    included in note 1(b)`. That reconciliation is another table: the table below is the split."""
    rows = [["", "Alpha", "Beta", "Total"], ["Chiffre d'affaires", "60", "40", "100"]]
    prose = ("Le Groupe a deux segments, Alpha et Beta. La réconciliation au résultat "
             "figure dans la note 1(b).")  # fmt: skip
    rules = sn.NoteVocabulary(
        headings=(*SPACED.headings, sn.Heading(r"(?i)réconciliation", "skip")),
        labels=SPACED.labels, members=SPACED.members, spaced=True,
    )  # fmt: skip
    read = sn.read_note(_table(rows, before=prose), ANCHORS, rules, axis_prefix="x:", axes=AXES,
                        detect_numbers=True, infer_scale=True)  # fmt: skip
    assert {f.member_code for f in read.facts} == {"x:Alpha", "x:Beta"}
    # A reconciliation the sentence introduces is still skipped.
    here = _table(rows, before="La réconciliation au résultat est la suivante :")
    assert not sn.read_note(here, ANCHORS, rules, axis_prefix="x:", axes=AXES,
                            detect_numbers=True, infer_scale=True).facts  # fmt: skip


def test_a_date_casts_no_vote_on_the_number_convention() -> None:
    """A dotted date ends in four digits after a point, which reads as a decimal. Counted, one
    date outvotes three `6.000.000`s, which carry only the thousands evidence, and in the point
    convention those are no figures at all."""
    read = _read(_table([["", "31.12.2025", ""],
                         ["", "Alpha", "Beta"],
                         ["Chiffre d'affaires", "60.000.000", "40.000.000"]]))  # fmt: skip
    assert {f.member_code: f.value for f in read.facts} == {"x:Alpha": 60e6, "x:Beta": 40e6}


def test_two_members_are_never_a_member_and_its_total() -> None:
    """A two-member line that reconciles to nothing is not read as one segment and its total:
    a split of one member is no split."""
    assert not _read(_table([["", "Alpha", "Beta"], ["Chiffre d'affaires", "100", "100"]])).facts


def test_a_name_spanning_its_year_columns_is_the_name() -> None:
    """Bonheur sets each segment over its two years (`2025/26 | 2024/25`). With no cell of its
    own, the column is named by the span above it, never by the period below."""
    html = (
        "<p>Information par segment</p><table>"
        "<tr><td></td><td colspan='2'>Alpha</td><td colspan='2'>Beta</td>"
        "<td colspan='2'>Total</td></tr>"
        "<tr><td></td>" + "<td>2025/26</td><td>2024/25</td>" * 3 + "</tr>"
        "<tr><td>Chiffre d'affaires</td><td>60</td><td>55</td><td>40</td><td>35</td>"
        "<td>100</td><td>90</td></tr></table>"
    )
    assert {f.member_code: f.value for f in _read(html).facts} == {"x:Alpha": 60e6, "x:Beta": 40e6}


def test_a_skip_in_a_sentence_about_another_table_is_ignored() -> None:
    rows = [["", "Alpha", "Beta", "Total"], ["Chiffre d'affaires", "60", "40", "100"]]
    rules = sn.NoteVocabulary(
        headings=(*SPACED.headings, sn.Heading(r"(?i)réconciliation", "skip")),
        labels=SPACED.labels, members=SPACED.members, spaced=True,
    )  # fmt: skip
    prose = "Deux segments. La réconciliation au résultat figure au compte de résultat."
    read = sn.read_note(_table(rows, before=prose), ANCHORS, rules, axis_prefix="x:", axes=AXES,
                        detect_numbers=True, infer_scale=True)  # fmt: skip
    assert {f.member_code for f in read.facts} == {"x:Alpha", "x:Beta"}
    heading = _table(rows, before="(b) Réconciliation au résultat opérationnel")
    assert not sn.read_note(heading, ANCHORS, rules, axis_prefix="x:", axes=AXES,
                            detect_numbers=True, infer_scale=True).facts  # fmt: skip


def test_the_total_column_of_a_cross_tab_is_the_company_split_along_the_other_axis() -> None:
    """VBG sets its countries as rows and its segments as columns. The `Total` column holds the
    company's revenue by country; labelled a total, it once earned no metric."""
    read = _earn(_table([["", "Alpha", "Beta", "Total"],
                         ["France", "40", "20", "60"],
                         ["Europe", "25", "15", "40"],
                         ["Total", "65", "35", "100"]]))  # fmt: skip
    # Both ways reconcile here, two members each: the places are the geography split.
    assert {f.member_code: f.value for f in read.facts} == {"x:France": 60e6, "x:Europe": 40e6}
    assert {f.axis for f in read.facts} == {"x:geo"}


def test_a_caption_over_one_of_two_same_named_columns_is_no_group() -> None:
    """Pharming's `Amounts in US$ '000` spans its label column and its first `RUCONEST®`, not the
    second. A group tells two columns of one name apart only when both sit under one."""
    html = (
        "<p>Information par segment</p><table>"
        "<tr><td colspan='2'>Amounts in US$ '000</td><td></td><td>2025</td><td></td><td></td>"
        "<td>2024</td></tr>"
        "<tr><td></td><td>Alpha</td><td>Beta</td><td>Total</td><td>Alpha</td><td>Beta</td>"
        "<td>Total</td></tr>"
        "<tr><td>Chiffre d'affaires</td><td>60</td><td>40</td><td>100</td><td>55</td><td>35</td>"
        "<td>90</td></tr></table>"
    )
    assert {f.member_code: f.value for f in _read(html).facts} == {"x:Alpha": 60e6, "x:Beta": 40e6}


def test_two_neighbours_of_equal_text_are_two_cells_not_a_span() -> None:
    """Schroders sets `Management` under both `Asset` and `Wealth`. Taken for a span, it named
    neither: the parser knows which cells a colspan repeated, and these are two cells."""
    read = _read(_table([["", "Asset", "Wealth", ""],
                         ["", "Management", "Management", "Total"],
                         ["Chiffre d'affaires", "60", "40", "100"]]))  # fmt: skip
    assert {f.member_code for f in read.facts} == {"x:Asset Management", "x:Wealth Management"}
    # A unit set over every column, cell by cell, is a caption and names no column.
    units = _read(_table([["", "Alpha", "Beta", "Total"],
                          ["", "€ million", "€ million", "€ million"],
                          ["Chiffre d'affaires", "60", "40", "100"]]))  # fmt: skip
    assert {f.member_code for f in units.facts} == {"x:Alpha", "x:Beta"}


def test_a_value_total_closes_its_line_and_is_its_largest_member() -> None:
    """An income statement read down a column has a line equal to the sum of every line below it
    (EQT's revenue against its costs and results). Only the last member, and the largest, is
    taken for a total no label names."""
    first = _read(_table([["", "Acme", "Alpha", "Beta"],
                          ["Chiffre d'affaires", "100", "60", "40"]]))  # fmt: skip
    assert not first.facts
    small = _read(_table([["", "Alpha", "Beta", "Gamma", "Delta"],
                          ["Chiffre d'affaires", "150", "-80", "30", "100"]]))  # fmt: skip
    assert not small.facts


def test_a_year_at_either_end_of_a_name_is_no_part_of_it() -> None:
    assert sn._member_text("KICKS 2025/26", True) == "KICKS"
    assert sn._member_text("2024 Healthcare Services", True) == "Healthcare Services"
    assert sn._member_text("2024", True) == ""
    assert sn._member_text("Vision Europe", True) == "Vision Europe"


def test_a_name_opening_with_a_total_word_is_the_total() -> None:
    """Rolls-Royce's reconciliation sets `Total underlying` beside `Underlying adjustments`; read
    as members they split statutory revenue into its own bridge. And SCA's last column is
    `Summa koncernen`: it closes the line, it is not dropped from it."""
    read = _read(_table([["", "Total underlying", "Adjustments to statutory", "Statutory"],
                         ["Chiffre d'affaires", "95", "5", "100"]]))  # fmt: skip
    assert not read.facts
    sca = _read(_table([["", "Skog", "Massa", "Éliminations", "Summa koncernen"],
                        ["Chiffre d'affaires", "70", "50", "(20)", "100"]]))  # fmt: skip
    assert {f.member_code for f in sca.facts} == {"x:Skog", "x:Massa"}
    assert sn._member_text("TotalEnergies", True) == "TotalEnergies"


def test_a_unit_or_a_note_number_names_no_column() -> None:
    """Moonpig heads its columns `Note | £000 | £000 | £000 | £000`: the units joined every name
    and the `Group` column stopped reading as the total."""
    read = _read(_table([["", "", "Alpha", "Beta", "Total"],
                         ["", "Note", "£000", "£000", "£000"],
                         ["Chiffre d'affaires", "4", "60.000", "40.000", "100.000"]]))  # fmt: skip
    assert {f.member_code for f in read.facts} == {"x:Alpha", "x:Beta"}
    # A name spanning its value and share columns names both: the units under it name neither.
    html = (
        "<p>Information par segment</p><table>"
        "<tr><td></td><td colspan='2'>Alpha</td><td colspan='2'>Beta</td><td colspan='2'>Total</td>"
        "</tr><tr><td></td>" + "<td>€ million</td><td>%</td>" * 3 + "</tr>"
        "<tr><td>Chiffre d'affaires</td><td>60</td><td>60</td><td>40</td><td>40</td>"
        "<td>100</td><td>100</td></tr></table>"
    )
    assert {f.member_code: f.value for f in _read(html).facts} == {"x:Alpha": 60e6, "x:Beta": 40e6}
    # A caption repeated cell by cell over every column names none of them.
    caption = _read(_table([["", "Operating segments", "Operating segments", ""],
                            ["", "Alpha", "Beta", "Total"],
                            ["Chiffre d'affaires", "60", "40", "100"]]))  # fmt: skip
    assert {f.member_code for f in caption.facts} == {"x:Alpha", "x:Beta"}
    for unit in ("£m", "€ million", "US$000", "MSEK", "DKKm", "%", "(in € millions)"):
        assert sn._caption(unit), unit
    for name in ("USA", "Europe", "UK", "Mining"):
        assert not sn._caption(name), name


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
