"""A segment note tagged as a converted page: positioned spans rebuilt as tables.

The fixtures are three real note blocks from filings.xbrl.org, captured 2026-11-28 with the
report's non-dimensional monetary facts (the anchors):
  - Coloplast (DK, FY2023/24): five segments as rows, one figure column per year;
  - GN Store Nord (DK, FY2024): three divisions over repeating years, one group per division;
  - Nyfosa (SE, FY2025): segments as columns, figures with spaces as thousands separators.
The synthetic tests build lines whose stated widths match Helvetica's, so every run's extent is
exact and each rule is the only thing deciding its case.
"""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from pathlib import Path

import pytest

from muffin_ingest.facets import esef, positioned
from muffin_ingest.facets import segment_note as sn
from tests.unit import esef_rules

FIXTURES = Path(__file__).parent.parent / "fixtures" / "esef"
VOCABULARY = esef_rules.vocabulary()
SIZE = 10.0


def _w(text: str) -> float:
    return positioned._weight(text) * SIZE / 1000


def _span(left: float, top: float, *parts: str | float, open_end: bool = False) -> str:
    """A positioned line: text pieces and spacers (floats, in px). Its width is what Helvetica
    would give, so the reader's estimate of each run is exact."""
    width = sum(p if isinstance(p, float) else _w(p) for p in parts)
    inner = "".join(
        f'<span style="display: inline-block; width: {p}px;"><span> </span></span>'
        if isinstance(p, float)
        else f'<span style="display: inline-block;">{p}</span>'
        for p in parts
    )
    return (
        f'<span style="position: absolute; display: inline-block; left: {left}px; top: {top}px; '
        f'width: {width}px; margin-left: 0px; font-size: {SIZE}px;">{inner}</span>'
    )


def _node(html: str) -> positioned._Node:
    tree = positioned._Tree()
    tree.feed(html)
    node = tree.root.children[0]
    assert isinstance(node, positioned._Node)
    return node


def _page(*spans: str, heading: str = "Segment information") -> str:
    filler = "".join(_span(0.0, 900.0 + i, "X") for i in range(positioned.POSITIONED))
    lines = "".join(spans) + filler
    return (
        f'<span style="display:inherit;clear:left;">{heading}</span>'
        f'<span style="display:inherit;clear:left;position:relative;height:400px;">{lines}</span>'
    )


def _rows(html: str) -> list[list[str]]:
    tables = sn.tables(positioned.rebuild(html))
    assert tables, positioned.rebuild(html)
    return [[c.strip() for c in row] for row in tables[0].rows]


def _right(x1: float, text: str) -> float:
    """The left at which `text` ends at `x1`, as a right-aligned figure does."""
    return x1 - _w(text)


ANCHORS = (sn.Anchor("ifrs-full:Revenue", 100e6, "2025-01-01", "2025-12-31", None, "EUR"),)


def _read(html: str) -> dict[tuple[str, str], dict[str, float]]:
    note = sn.read_note(
        positioned.rebuild(html),
        ANCHORS,
        VOCABULARY.note,
        axis_prefix=esef.PREFIX,
        axes=dict(VOCABULARY.axes),
        detect_numbers=True,
        infer_scale=True,
    )
    out: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for f in note.facts:
        if f.partition_id and f.metric_code == "revenue" and f.parent_member is None:
            out[(f.axis, f.period_ending)][f.member_code.removeprefix("esef:")] = f.value
    return out


# ── Which blocks are pages ─────────────────────────────────────────────────────────────────────


def test_a_block_holding_a_table_is_no_page() -> None:
    assert positioned.is_positioned(_page(_span(0.0, 0.0, "Revenue")))
    assert not positioned.is_positioned(_page(_span(0.0, 0.0, "Revenue")) + "<table></table>")


def test_a_few_positioned_spans_make_no_page() -> None:
    few = "".join(_span(0.0, float(i), "x") for i in range(positioned.POSITIONED))
    assert not positioned.is_positioned(few)


# ── Lines ──────────────────────────────────────────────────────────────────────────────────────


def test_margin_left_is_not_the_left() -> None:
    line = positioned._line(_node(_span(300.0, 5.0, "Revenue")))
    assert line.runs[0].x0 == pytest.approx(300.0)


@pytest.mark.parametrize(
    ("gap", "text"),
    [(1.0, "Taxation"), (4.0, "Group revenue"), (12.0, "Group|revenue")],
)
def test_a_gap_is_a_kern_a_space_or_a_cell(gap: float, text: str) -> None:
    head, tail = ("T", "axation") if text == "Taxation" else ("Group", "revenue")
    line = positioned._line(_node(_span(0.0, 0.0, head, gap, tail)))
    assert "|".join(r.text for r in line.runs) == text


def test_no_space_comes_between_two_digits() -> None:
    line = positioned._line(_node(_span(0.0, 0.0, "20", 4.0, ".5%")))
    assert [r.text for r in line.runs] == ["20.5%"]


def test_a_figure_one_space_from_its_label_is_a_cell() -> None:
    line = positioned._line(_node(_span(0.0, 0.0, "Pacific Basin 42,726")))
    assert [r.text for r in line.runs] == ["Pacific Basin", "42,726"]
    assert line.runs[-1].x1 == pytest.approx(_w("Pacific Basin 42,726"))


def test_footnote_markers_name_nothing_and_a_figure_keeps_its_brackets() -> None:
    line = positioned._line(
        _node(
            _span(
                0.0,
                0.0,
                "Revenue goods 1) 2)",
                30.0,
                "(12)",
                30.0,
                "2)",
                30.0,
                "Biologics1)",
                30.0,
                "Kerecis\u00b9\u207e",
            )
        )
    )
    assert [r.text for r in line.runs] == ["Revenue goods", "(12)", "Biologics", "Kerecis"]


# ── Tables ─────────────────────────────────────────────────────────────────────────────────────


def _segments_by_column(second_line_offset: float = 0.0) -> str:
    """Two segments as columns, `Uniforms` over `Logistics and un-` / `allocated`, then figures."""
    a, b = 200.0, 300.0  # right edges
    return _page(
        _span(_right(a, "Uniforms"), 0.0, "Uniforms"),
        _span(_right(b, "Logistics and un-") + second_line_offset, 0.0, "Logistics and un-"),
        _span(_right(b, "allocated"), 12.0, "allocated"),
        _span(0.0, 30.0, "Revenue"),
        _span(_right(a, "60.0"), 30.0, "60.0"),
        _span(_right(b, "40.0"), 30.0, "40.0"),
        _span(0.0, 42.0, "Total"),
        _span(_right(a, "60.0"), 42.0, "60.0"),
        _span(_right(b, "40.0"), 42.0, "40.0"),
    )


def test_a_hyphenated_header_continues_in_its_column() -> None:
    rows = _rows(_segments_by_column())
    assert rows[0][1:] == ["Uniforms", "Logistics and unallocated"]


def test_a_header_keeps_a_hyphen_before_a_capital() -> None:
    a = 200.0
    rows = _rows(
        _page(
            _span(_right(a, "Hydra-"), 0.0, "Hydra-"),
            _span(_right(a, "Specma"), 12.0, "Specma"),
            _span(0.0, 30.0, "Revenue"),
            _span(_right(a, "60.0"), 30.0, "60.0"),
        )
    )
    assert rows[0][1] == "Hydra-Specma"


def test_both_halves_of_a_wrapped_name_land_in_one_column() -> None:
    """`Unallocated` is wider than its narrow column and spills over the next one; it still sits
    over the column it overlaps most."""
    a, b, c = 200.0, 240.0, 280.0
    rows = _rows(
        _page(
            _span(_right(a, "Ocean"), 0.0, "Ocean"),
            _span(_right(b, "Unallo-"), 0.0, "Unallo-"),
            _span(_right(c, "Total"), 0.0, "Total"),
            _span(_right(b, "cated") + 3.0, 12.0, "cated"),
            _span(0.0, 30.0, "Revenue"),
            *(_span(_right(x, "10.0"), 30.0, "10.0") for x in (a, b, c)),
        )
    )
    assert rows[0][1:] == ["Ocean", "Unallocated", "Total"]


def test_a_row_of_years_places_no_column() -> None:
    """The years sit left of the figures they head: counted as figures, they invent a column."""
    a, b = 200.0, 300.0
    rows = _rows(
        _page(
            _span(150.0, 0.0, "2025"),
            _span(250.0, 0.0, "2024"),
            _span(0.0, 20.0, "Revenue"),
            _span(_right(a, "60.0"), 20.0, "60.0"),
            _span(_right(b, "40.0"), 20.0, "40.0"),
        )
    )
    assert all(len(r) == 3 for r in rows)


def test_a_group_header_above_repeating_years_names_its_group() -> None:
    """GN Store Nord's shape: `Hearing` is narrower than its two year columns and sits over the
    first of them, yet names both."""
    edges = (200.0, 250.0, 330.0, 380.0)
    page = _page(
        _span(_right(edges[0], "Hearing") + 10.0, 0.0, "Hearing"),
        _span(_right(edges[2], "Enterprise") + 10.0, 0.0, "Enterprise"),
        *(_span(_right(x, y), 12.0, y) for x, y in zip(edges, ("2025", "2024") * 2, strict=True)),
        _span(0.0, 30.0, "Revenue"),
        *(_span(_right(x, "10.0"), 30.0, "10.0") for x in edges),
    )
    table = sn.tables(positioned.rebuild(page), inline=True)[0]
    assert table.rows[0][1:] == ["Hearing", "Hearing", "Enterprise", "Enterprise"]
    assert table.copies is not None and table.copies[0][2] and table.copies[0][4]


def test_a_data_rows_label_is_what_comes_before_its_first_figure() -> None:
    """A long label reaches past another row's first figure; it is still a label."""
    a, b = 120.0, 200.0
    rows = _rows(
        _page(
            _span(0.0, 0.0, "Revenue from external customers"),
            _span(_right(b, "60.0"), 0.0, "60.0"),
            _span(0.0, 12.0, "Tax"),
            _span(_right(a, "1.0"), 12.0, "1.0"),
            _span(_right(b, "2.0"), 12.0, "2.0"),
        )
    )
    assert rows[0][0] == "Revenue from external customers"


def test_a_label_set_just_below_its_figures_labels_them() -> None:
    """GN Store Nord's regions: each name sits half a line under its figures. Read apart, the
    figures belong to no region."""
    a, b = 200.0, 300.0
    rows = _rows(
        _page(
            _span(_right(a, "Hearing"), 0.0, "Hearing"),
            _span(_right(b, "Total"), 0.0, "Total"),
            _span(_right(a, "6.0"), 20.0, "6.0"),
            _span(_right(b, "10.0"), 20.0, "10.0"),
            _span(0.0, 26.0, "Europe"),
            _span(_right(a, "4.0"), 40.0, "4.0"),
            _span(_right(b, "5.0"), 40.0, "5.0"),
            _span(0.0, 46.0, "Rest of World"),
            _span(_right(a, "1.0"), 60.0, "1.0"),
            _span(_right(b, "1.0"), 60.0, "1.0"),
            _span(0.0, 75.0, "Notes"),
        )
    )
    assert [r[0] for r in rows[1:3]] == ["Europe", "Rest of World"]
    assert rows[1][1:] == ["6.0", "10.0"]
    # A line more than a line height away is not the figures' label.
    assert rows[3][0] == "" and rows[4][0] == "Notes"


def test_a_wrapped_label_joins_the_row_below() -> None:
    a = 200.0
    rows = _rows(
        _page(
            _span(0.0, 0.0, "Other segments and un-"),
            _span(0.0, 12.0, "allocated"),
            _span(_right(a, "5.0"), 12.0, "5.0"),
        )
    )
    assert rows[0][0] == "Other segments and unallocated"


def test_a_word_split_across_two_spans_is_one_word() -> None:
    a, b = 300.0, 400.0
    left = _right(a, "Sydamerika")
    page = _page(
        _span(left, 0.0, "Sydamer"),
        _span(left + _w("Sydamer"), 0.0, "ika"),
        _span(_right(b, "Summa"), 0.0, "Summa"),
        _span(0.0, 20.0, "Revenue"),
        _span(_right(a, "6.0"), 20.0, "6.0"),
        _span(_right(b, "6.0"), 20.0, "6.0"),
    )
    assert _rows(page)[0][1] == "Sydamerika"


def test_a_span_ending_in_a_space_is_a_word_of_its_own() -> None:
    a, b = 300.0, 400.0
    left = _right(a, "Reported Underlying")
    page = _page(
        _span(left, 0.0, "Reported "),
        _span(left + _w("Reported "), 0.0, "Underlying"),
        _span(0.0, 20.0, "Revenue"),
        _span(_right(a, "6.0"), 20.0, "6.0"),
        _span(_right(b, "6.0"), 20.0, "6.0"),
    )
    assert _rows(page)[0][1] != "ReportedUnderlying"


def test_a_rebuilt_page_reads_a_split_adding_up_to_the_tagged_revenue() -> None:
    """`EUR million` is a short line inside the table, and stays one of its rows: a paragraph would
    leave the table and say nothing about the table's unit where its rows are read."""
    a, b = 200.0, 300.0
    page = _page(
        _span(_right(a, "Uniforms"), 0.0, "Uniforms"),
        _span(_right(b, "Logistics"), 0.0, "Logistics"),
        _span(0.0, 12.0, "EUR million"),
        _span(0.0, 30.0, "Revenue"),
        _span(_right(a, "60.0"), 30.0, "60.0"),
        _span(_right(b, "40.0"), 30.0, "40.0"),
    )
    rows = _rows(page)
    assert rows[0][1:] == ["Uniforms", "Logistics"]
    assert ["EUR million", "", ""] in rows
    assert _read(page) == {("esef:segments", "2025-12-31"): {"Uniforms": 60e6, "Logistics": 40e6}}


# ── Real pages ─────────────────────────────────────────────────────────────────────────────────


def _block(name: str) -> tuple[str, tuple[sn.Anchor, ...]]:
    data = json.loads(gzip.decompress((FIXTURES / name).read_bytes()))
    return data["block"], tuple(sn.Anchor(**a) for a in data["anchors"])


def _real(name: str) -> dict[tuple[str, str], dict[str, float]]:
    html, anchors = _block(name)
    note = sn.read_note(
        positioned.rebuild(html),
        anchors,
        VOCABULARY.note,
        axis_prefix=esef.PREFIX,
        axes=dict(VOCABULARY.axes),
        detect_numbers=True,
        infer_scale=True,
    )
    out: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for f in note.facts:
        if f.partition_id and f.metric_code == "revenue" and f.parent_member is None:
            out[(f.axis, f.period_ending)][f.member_code.removeprefix("esef:")] = f.value / 1e6
    return out


def test_coloplast_reads_its_five_segments() -> None:
    """Its operating segments table, which a short line (`DKK million`) used to cut in two. FY2023's
    `Biologics1)` carries its footnote glued on."""
    split = _real("positioned_coloplast_2024-09-30.json.gz")
    newest = {k: round(v) for k, v in split[("esef:segments", "2024-09-30")].items()}
    assert newest == {
        "Chronic Care": 18085,
        "Voice and Respiratory Care": 2110,
        "Interventional Urology": 2775,
        "Advanced Wound Dressings": 3034,
        "Biologics": 1026,
    }
    assert "Biologics" in split[("esef:segments", "2023-09-30")]


def test_gn_store_nord_reads_its_divisions_over_repeating_years() -> None:
    split = _real("positioned_gn_store_nord_2024-12-31.json.gz")
    assert {k: round(v) for k, v in split[("esef:segments", "2024-12-31")].items()} == {
        "Hearing": 7104,
        "Enterprise": 7205,
        "Gaming & Consumer": 3676,
    }


def test_nyfosa_reads_spaced_thousands_without_inventing_columns() -> None:
    split = _real("positioned_nyfosa_2025-12-31.json.gz")
    newest = split[("esef:segments", "2025-12-31")]
    assert abs(sum(newest.values()) - 3590) <= 2  # each segment rounded to SEK millions
    assert {"Nyfosa Sverige", "Kielo", "Bratsberg"} <= set(newest)
