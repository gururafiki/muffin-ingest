"""`segment_note`: a segment note's HTML tables read against the filing's own tagged totals.

The fixtures are three real EDINET annual reports of FY2026 (`tests/fixtures/edinet/`), trimmed to
what the reader reads: the segment note's HTML and the non-dimensional numeric facts. Re-capture
with the script in the fixtures' `source` field; the archives are `documents/{docID}?type=1`.
  - Toyota (S100Y8NY, IFRS): segments as columns, regions as rows, revenue tagged only under the
    filer's own extension concepts;
  - Nintendo (S100Y9NX, JGAAP, single segment): a product split, and a regional split with
    「うち、米国」 nested inside 米大陸;
  - MUFG (S100YJQO, a bank): 粗利益, which is not revenue.
"""

from __future__ import annotations

import collections
import gzip
import json
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest.facets import segment_note as sn
from tests.unit import edinet_rules

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "edinet"

VOCABULARY = edinet_rules.vocabulary().note
AXES = dict(edinet_rules.vocabulary().axes)


def read(name: str) -> sn.NoteRead:
    with gzip.open(FIXTURES / f"edinet_{name}.json.gz", "rt", encoding="utf-8") as fh:
        doc = json.load(fh)
    contexts = {k: (v["start"], v["end"], v["instant"]) for k, v in doc["contexts"].items()}
    currencies = {u: ("JPY" if m.endswith("JPY") else None) for u, m in doc["units"].items()}
    anchors = sn.anchors_of(
        [(f["concept"], f["context"], f["unit"], f["value"]) for f in doc["facts"]],
        contexts,
        currencies,
    )
    html = next(
        text
        for concept, by_context in doc["blocks"].items()
        if "NotesSegmentInformation" in concept
        for text in by_context.values()
    )
    return sn.read_note(html, anchors, VOCABULARY, axis_prefix="edinet:", axes=AXES)


def split(r: sn.NoteRead, axis: str, metric: str, end: str) -> dict[str, tuple[float, int]]:
    return {
        f.member_code.removeprefix("edinet:"): (f.value, f.partition_id)
        for f in r.facts
        if f.axis == axis and f.metric_code == metric and f.period_ending == end
    }


def test_toyota_s_segments_reconcile_to_revenue_it_tags_only_in_its_own_extension() -> None:
    """The 外部顧客への営業収益 row sums to 50,684,952 百万円, a figure Toyota tags only as
    `jpcrp030000-asr_E02144-000:TotalNetRevenuesIFRS` and siblings. The 計 row, which includes
    intersegment sales, totals to the same 連結 cell and must not be taken in its place."""
    r = read("toyota-S100Y8NY")
    revenue = split(r, "edinet:報告セグメント", "revenue", "2026-03-31")
    assert revenue == {
        "自動車": (45_201_924e6, 1),
        "金融": (4_819_003e6, 1),
        "その他": (664_026e6, 1),
    }
    prior = split(r, "edinet:報告セグメント", "revenue", "2025-03-31")
    assert sum(v for v, _ in prior.values()) == 48_036_704e6
    assert {f.currency for f in r.facts} == {"JPY"}
    assert {f.period_type for f in r.facts if f.metric_code == "revenue"} == {"annual"}


def test_profit_and_assets_take_the_partition_revenue_learned() -> None:
    """Segment assets leave out corporate assets (13,828,157 百万円 in 消去又は全社), so they sum to
    nothing the filing tags and place no member on their own. They take revenue's split, without a
    `reconciled_to`, because they were not reconciled."""
    r = read("toyota-S100Y8NY")
    assets = [
        f for f in r.facts
        if f.metric_code == "total_assets" and f.period_ending == "2026-03-31"
    ]  # fmt: skip
    assert {f.member_code: f.partition_id for f in assets} == {
        "edinet:自動車": 1, "edinet:金融": 1, "edinet:その他": 1,
    }  # fmt: skip
    assert {f.period_type for f in assets} == {"instant"}
    assert all(f.reconciled_to is None for f in assets)
    revenue = [f for f in r.facts if f.metric_code == "revenue"]
    assert all(f.reconciled_to is not None for f in revenue)


def test_a_table_with_the_regions_as_rows_is_read_by_column() -> None:
    r = read("toyota-S100Y8NY")
    assert split(r, "edinet:地域", "revenue", "2026-03-31") == {
        "日本": (10_985_614e6, 1),
        "米国": (18_493_969e6, 1),
        "その他": (21_205_369e6, 1),
    }


def test_an_of_which_column_is_not_a_member() -> None:
    """Nintendo's 「うち、米国」 is a subset of 米大陸 under a joined header; counting it would put
    the US twice. The four regions alone sum to the total."""
    r = read("nintendo-S100Y9NX")
    regions = split(r, "edinet:地域", "revenue", "2026-03-31")
    assert set(regions) == {"日本", "米大陸", "欧州", "その他"}
    assert all(p == 1 for _, p in regions.values())
    assert not [f for f in r.facts if "うち" in f.member_code]


def test_a_single_segment_company_s_split_is_its_product_table() -> None:
    r = read("nintendo-S100Y9NX")
    assert split(r, "edinet:製品及びサービス", "revenue", "2026-03-31") == {
        "ゲーム専用機": (2_239_541e6, 1),
        "IP関連収入等": (73_510e6, 1),
    }
    assert not [f for f in r.facts if f.axis == "edinet:報告セグメント"]


def test_a_bank_s_gross_profit_is_not_revenue() -> None:
    """MUFG's segment table states 粗利益, a bank's gross operating profit. It is never stored as
    revenue; only its regional long-lived assets are read."""
    r = read("mufg-S100YJQO")
    assert not [f for f in r.facts if f.metric_code == "revenue"]
    assert {f.metric_code for f in r.facts} == {"long_lived_assets"}
    assert r.tables == 11 and r.read == 2


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        ("42,996,299", 42996299.0),
        ("△1,091,455", -1091455.0),
        ("▲12", -12.0),
        ("(1,234)", -1234.0),
        ("－", 0.0),
        ("-", 0.0),
        ("12.5", 12.5),
        ("自動車", None),
        ("2026年", None),
        ("", None),
    ],
)
def test_a_cell_is_a_number_only_in_the_forms_a_filer_writes(cell: str, expected: Any) -> None:
    assert sn.number(cell) == expected


def test_spans_are_expanded_so_a_value_sits_under_its_header() -> None:
    html = (
        "<p>(単位：千円)</p><table>"
        "<tr><td rowspan='2'></td><td colspan='2'>米大陸</td><td rowspan='2'>合計</td></tr>"
        "<tr><td></td><td>うち、米国</td></tr>"
        "<tr><td>売上高</td><td>10</td><td>4</td><td>10</td></tr></table>"
    )
    [table] = sn.tables(html)
    assert table.rows == [
        ["", "米大陸", "米大陸", "合計"],
        ["", "", "うち、米国", "合計"],
        ["売上高", "10", "4", "10"],
    ]
    assert table.before == ["(単位:千円)"]
    assert sn.scale_of(table.before) == 1e3


def test_a_table_whose_kind_cannot_be_established_is_skipped_and_counted() -> None:
    html = "<p>(単位：百万円)</p><table><tr><td></td><td>A</td><td>B</td></tr>" \
           "<tr><td>売上高</td><td>6</td><td>4</td></tr></table>"  # fmt: skip
    anchor = sn.Anchor("x:NetSales", 10e6, "2025-04-01", "2026-03-31", None, "JPY")
    r = sn.read_note(html, [anchor], VOCABULARY, axis_prefix="edinet:", axes=AXES)
    assert (r.tables, r.read, r.no_kind, r.facts) == (1, 0, 1, [])


def test_a_table_with_no_unit_is_skipped_rather_than_guessed() -> None:
    """A 千円 table read as 百万円 is a thousandfold error that looks ordinary."""
    html = "<p>報告セグメント</p><table><tr><td></td><td>A</td><td>B</td></tr>" \
           "<tr><td>売上高</td><td>6</td><td>4</td></tr></table>"  # fmt: skip
    anchor = sn.Anchor("x:NetSales", 10e6, "2025-04-01", "2026-03-31", None, "JPY")
    r = sn.read_note(html, [anchor], VOCABULARY, axis_prefix="edinet:", axes=AXES)
    assert (r.no_unit, r.facts) == (1, [])


def test_a_line_matching_no_tagged_fact_is_not_a_split() -> None:
    """A table that merely adds up (a reconciliation, a customer table) matches no consolidated
    fact, and nothing is read from it."""
    html = (
        "<p>報告セグメント</p><p>(単位：百万円)</p>"
        "<table><tr><td></td><td>A</td><td>B</td></tr>"
        "<tr><td>売上高</td><td>6</td><td>4</td></tr></table>"
    )
    anchor = sn.Anchor("x:NetSales", 11e6, "2025-04-01", "2026-03-31", None, "JPY")
    r = sn.read_note(html, [anchor], VOCABULARY, axis_prefix="edinet:", axes=AXES)
    assert (r.read, r.no_anchor, r.facts) == (0, 1, [])
    matched = sn.Anchor("x:NetSales", 10e6, "2025-04-01", "2026-03-31", None, "JPY")
    ok = sn.read_note(html, [matched], VOCABULARY, axis_prefix="edinet:", axes=AXES)
    assert collections.Counter(f.partition_id for f in ok.facts) == {1: 2}


def test_only_consolidated_money_facts_anchor() -> None:
    """A context outside the non-dimensional map (non-consolidated, dimensional) anchors nothing,
    and nor does a unit that is not money."""
    contexts = {"CurrentYearDuration": ("2025-04-01", "2026-03-31", None)}
    currencies = {"JPY": "JPY", "shares": None}
    anchors = sn.anchors_of(
        [
            ("a:NetSales", "CurrentYearDuration", "JPY", 10.0),
            ("a:NetSales", "CurrentYearDuration_NonConsolidatedMember", "JPY", 7.0),
            ("a:Shares", "CurrentYearDuration", "shares", 3.0),
        ],
        contexts,
        currencies,
    )
    assert [(a.local_name, a.value) for a in anchors] == [("NetSales", 10.0)]


# ── Rules the 40-report gate taught (2026-11-27) ─────────────────────────────────────────────


def _one(html: str, *anchors: sn.Anchor) -> sn.NoteRead:
    return sn.read_note(html, list(anchors), VOCABULARY, axis_prefix="edinet:", axes=AXES)


def _revenue(value: float) -> sn.Anchor:
    return sn.Anchor("jppfs_cor:NetSales", value, "2025-04-01", "2026-03-31", None, "JPY")


def _members(r: sn.NoteRead, metric: str = "revenue") -> dict[str, float]:
    return {
        f.member_code.removeprefix("edinet:"): f.value for f in r.facts if f.metric_code == metric
    }


#: EduLab's shape: a 報告セグメント group header over the segments and their 計, and the 連結
#: column's label set with spaces and a footnote. Read before its normalisation, the 連結 column was
#: a member and revenue counted twice.
GROUPED = (
    "<p>3.報告セグメントごとの売上高に関する情報</p><table>"
    "<tr><td></td><td></td><td></td><td></td><td>(単位:千円)</td><td>(単位:千円)</td></tr>"
    "<tr><td></td><td colspan='3'>報告セグメント</td><td rowspan='2'>調整額 (注)1</td>"
    "<td rowspan='2'>連結 財務諸表 計上額 (注)2</td></tr>"
    "<tr><td></td><td>テスト等 ライセンス 事業</td><td>AI事業</td><td>計</td></tr>"
    "<tr><td>売上高</td><td></td><td></td><td></td><td></td><td></td></tr>"
    "<tr><td>顧客との契約から生じる収益</td><td>6</td><td>4</td><td>10</td><td>-</td><td>10</td></tr>"
    "<tr><td>外部顧客への 売上高</td><td>6</td><td>4</td><td>10</td><td>-</td><td>10</td></tr>"
    "<tr><td>セグメント利益</td><td>2</td><td>1</td><td>3</td><td>△1</td><td>2</td></tr>"
    "</table>"
)


def test_a_total_label_is_judged_after_its_spaces_and_footnote_go() -> None:
    r = _one(GROUPED, _revenue(10e3))
    assert _members(r) == {"テスト等ライセンス事業": 6e3, "AI事業": 4e3}


def test_a_member_is_named_by_its_deepest_header_and_its_group_s_total_is_no_member() -> None:
    """「報告セグメント」 heads the columns and is no part of a segment's name; its 計 is a
    total."""
    r = _one(GROUPED, _revenue(10e3))
    assert not [
        f for f in r.facts if "報告セグメント" in f.member_code or f.member_code.endswith("計")
    ]


def test_a_revenue_by_contract_row_is_not_the_revenue_line() -> None:
    """「顧客との契約から生じる収益」 sums to the same total as external revenue here and is a
    disaggregation, never the line: only 外部顧客への売上高 is read, and it is read by its label."""
    profit = sn.Anchor("jppfs_cor:OperatingIncome", 2e3, "2025-04-01", "2026-03-31", None, "JPY")
    r = _one(GROUPED, _revenue(10e3), profit)
    assert {f.reconciled_to for f in r.facts if f.metric_code == "revenue"} == {10e3}
    assert {f.metric_code for f in r.facts} == {"revenue", "operating_income"}


def test_a_name_that_repeats_under_another_group_keeps_its_group() -> None:
    """Asahi Diamond sets 「その他」 under アジア・オセアニア and again on its own. Merged, the
    regions no longer summed to the total and nothing was read."""
    html = (
        "<p>2 地域ごとの情報</p><table>"
        "<tr><td></td><td></td><td></td><td>(単位:百万円)</td><td>(単位:百万円)</td></tr>"
        "<tr><td rowspan='2'>日本</td><td colspan='2'>アジア・オセアニア</td>"
        "<td rowspan='2'>その他</td><td rowspan='2'>合計</td></tr>"
        "<tr><td>台湾</td><td>その他</td></tr>"
        "<tr><td>50</td><td>20</td><td>10</td><td>20</td><td>100</td></tr></table>"
    )
    r = _one(html, _revenue(100e6))
    assert _members(r) == {
        "日本": 50e6,
        "台湾": 20e6,
        "アジア・オセアニアその他": 10e6,
        "その他": 20e6,
    }


def test_a_metric_row_is_never_a_member_read_the_other_way() -> None:
    """Yellow Hat's segment table read with its rows as members put 外部顧客への売上高 forward as a
    member whose value equals the total. A labelled row is a line, never a member."""
    html = (
        "<p>報告セグメントごとの売上高</p><p>(単位:百万円)</p><table>"
        "<tr><td></td><td>カー用品</td><td>不動産</td><td>合計</td></tr>"
        "<tr><td>外部顧客への売上高</td><td>90</td><td>10</td><td>100</td></tr>"
        "<tr><td>計</td><td>90</td><td>10</td><td>100</td></tr>"
        "<tr><td>セグメント利益</td><td>9</td><td>1</td><td>10</td></tr>"
        "<tr><td>セグメント資産</td><td>80</td><td>20</td><td>100</td></tr></table>"
    )
    r = _one(html, _revenue(100e6))
    assert _members(r) == {"カー用品": 90e6, "不動産": 10e6}
    assert not [f for f in r.facts if f.member_code.endswith(("売上高", "利益", "資産"))]


def test_a_skip_heading_wins_over_a_kind_on_the_same_line() -> None:
    """「報告セグメントごとののれんの償却額…」 names the segments and is a goodwill table."""
    html = (
        "<p>【報告セグメントごとののれんの償却額及び未償却残高に関する情報】</p>"
        "<p>(単位:百万円)</p><table><tr><td></td><td>A</td><td>B</td><td>合計</td></tr>"
        "<tr><td>当期償却額</td><td>6</td><td>4</td><td>10</td></tr></table>"
    )
    r = _one(html, _revenue(10e6))
    assert (r.skipped_kind, r.facts) == (1, [])


def test_a_footnote_or_a_sentence_is_no_heading() -> None:
    """Makita's footnote under its prior-year table says 「…の地域より構成されております。」:
    read as a heading, it filed the current year's segment table as geography."""
    html = (
        "<p>(2) 報告セグメントに関する情報</p><p>(単位:百万円)</p><table>"
        "<tr><td></td><td>日本</td><td>欧州</td><td>連結</td></tr>"
        "<tr><td>外部収益</td><td>6</td><td>4</td><td>10</td></tr></table>"
        "<p>(注) 1.セグメント間の内部売上収益は市場実勢に基づいております。</p>"
        "<p>2.「その他」は主にオーストラリアの地域より構成されております。</p>"
        "<p>(単位:百万円)</p><table>"
        "<tr><td></td><td>日本</td><td>欧州</td><td>連結</td></tr>"
        "<tr><td>外部収益</td><td>7</td><td>4</td><td>11</td></tr></table>"
    )
    current = sn.Anchor("jppfs_cor:NetSales", 11e6, "2025-04-01", "2026-03-31", None, "JPY")
    prior = sn.Anchor("jppfs_cor:NetSales", 10e6, "2024-04-01", "2025-03-31", None, "JPY")
    r = _one(html, current, prior)
    assert {f.axis for f in r.facts} == {"edinet:報告セグメント"}
    assert {f.period_ending for f in r.facts} == {"2025-03-31", "2026-03-31"}


def test_a_cell_with_its_own_of_which_is_its_first_figure() -> None:
    """Makita: 「欧州 (うち:ドイツ)」 over 「390,110 (81,935)」 is the member 欧州 at 390,110."""
    assert sn.number("390,110 (81,935)") == 390110.0
    assert sn.member_name("欧州 (うち:ドイツ)") == "欧州"
    html = (
        "<p>(4) 地域に関する情報</p><p>(単位:百万円)</p><table>"
        "<tr><td></td><td>当連結会計年度</td></tr>"
        "<tr><td>日本</td><td>60</td></tr><tr><td>欧州 (うち:ドイツ)</td><td>40 (15)</td></tr>"
        "<tr><td>合計</td><td>100</td></tr></table>"
    )
    r = _one(html, _revenue(100e6))
    assert _members(r) == {"日本": 60e6, "欧州": 40e6}


def test_a_line_with_no_label_needs_its_members_to_sum() -> None:
    """With no label of its own, a line's only evidence is arithmetic. A total cell equal to the
    figure is any 計 in the table, so a line whose members do not sum is not read."""
    html = (
        "<p>2.地域ごとの情報</p><p>(単位:百万円)</p><table>"
        "<tr><td>日本</td><td>米国</td><td>合計</td></tr>"
        "<tr><td>60</td><td>30</td><td>100</td></tr></table>"
    )
    assert _one(html, _revenue(100e6)).facts == []
    assert _members(_one(html, _revenue(90e6))) == {"日本": 60e6, "米国": 30e6}


def test_a_gain_on_sales_never_names_revenue() -> None:
    """`GainOnSalesOfNoncurrentAssetsEI` contains 「Sales」: a figure equal to it is not revenue."""
    html = (
        "<p>2.地域ごとの情報</p><p>(単位:百万円)</p><table>"
        "<tr><td>日本</td><td>米国</td><td>合計</td></tr>"
        "<tr><td>60</td><td>5</td><td>65</td></tr></table>"
    )
    gain = sn.Anchor("jppfs_cor:GainOnSalesOfNoncurrentAssetsEI", 65e6, "2025-04-01",
                     "2026-03-31", None, "JPY")  # fmt: skip
    assert _one(html, gain).facts == []


# ── Guards no measured report needed, each with the input that needs it ──────────────────────
#
# On the 40-report gate (2026-11-27) these rules changed no result, because the arithmetic rules
# cover the shapes measured. Each test below is a CONSTRUCTED table on which the rule is the only
# thing standing between the input and a wrong read, so deleting it fails a test.


def test_a_column_labelled_by_a_metric_is_no_member() -> None:
    """A product table whose total column is headed 「売上高」 rather than 合計: counted as a
    member, the line sums to twice the total and nothing is read."""
    html = (
        "<p>1.製品及びサービスごとの情報</p><p>(単位:百万円)</p><table>"
        "<tr><td></td><td>製品A</td><td>製品B</td><td>売上高</td></tr>"
        "<tr><td>外部顧客への売上高</td><td>60</td><td>40</td><td>100</td></tr></table>"
    )
    assert _members(_one(html, _revenue(100e6))) == {"製品A": 60e6, "製品B": 40e6}


def test_a_row_labelled_by_a_metric_is_no_member() -> None:
    """A regional table with the years as columns, closed by a row 「売上高」 restating the total:
    counted as a member, the year's line sums to twice the total and nothing is read."""
    html = (
        "<p>2.地域ごとの情報</p><p>(単位:百万円)</p><table>"
        "<tr><td></td><td>当連結会計年度</td></tr>"
        "<tr><td>日本</td><td>70</td></tr><tr><td>米国</td><td>30</td></tr>"
        "<tr><td>売上高</td><td>100</td></tr></table>"
    )
    assert _members(_one(html, _revenue(100e6))) == {"日本": 70e6, "米国": 30e6}


def test_a_goodwill_table_is_skipped_whatever_it_sums_to() -> None:
    """Under 「報告セグメントごとの…減損損失…」 the segments are named and the table
    is an impairment table; a total that happens to equal revenue must not make it a split. The
    skip wins in the code, not only by the rows' priorities: here the business heading is tried
    first."""
    html = (
        "<p>【報告セグメントごとの固定資産の減損損失に関する情報】</p><p>(単位:百万円)</p>"
        "<table><tr><td></td><td>A</td><td>B</td><td>合計</td></tr>"
        "<tr><td>減損損失</td><td>6</td><td>4</td><td>10</td></tr></table>"
    )
    business_first = sn.NoteVocabulary(
        headings=(sn.Heading("報告セグメント", "business"), sn.Heading("減損損失", "skip")),
        labels=VOCABULARY.labels,
        concepts=VOCABULARY.concepts,
    )
    r = sn.read_note(html, [_revenue(10e6)], business_first, axis_prefix="edinet:", axes=AXES)
    assert (r.skipped_kind, r.facts) == (1, [])


def test_a_hint_names_a_line_only_when_its_members_sum() -> None:
    """Under 「(2)有形固定資産」 an unlabelled line takes long-lived assets as its metric
    (no concept names it), but only when its members add up: a total cell equal to the figure
    is any 計."""
    html = (
        "<p>2.地域ごとの情報</p><p>(2)有形固定資産</p><p>(単位:百万円)</p><table>"
        "<tr><td>日本</td><td>米国</td><td>合計</td></tr>"
        "<tr><td>60</td><td>30</td><td>100</td></tr></table>"
    )
    ppe = sn.Anchor("jppfs_cor:PropertyPlantAndEquipment", 100e6, None, None, "2026-03-31", "JPY")
    assert _one(html, ppe).facts == []
    summed = sn.Anchor("jppfs_cor:PropertyPlantAndEquipment", 90e6, None, None, "2026-03-31", "JPY")
    assert _members(_one(html, summed), "long_lived_assets") == {"日本": 60e6, "米国": 30e6}


def test_an_of_which_group_over_countries_keeps_them_out_of_the_split() -> None:
    """「うち主要国」 heads 米国 and 中国 as a subset of 海外: the deepest header is a country, and
    only the group's role says it is not a member."""
    html = (
        "<p>2.地域ごとの情報</p><p>(単位:百万円)</p><table>"
        "<tr><td rowspan='2'>日本</td><td rowspan='2'>海外</td><td colspan='2'>うち主要国</td>"
        "<td rowspan='2'>合計</td></tr><tr><td>米国</td><td>中国</td></tr>"
        "<tr><td>60</td><td>40</td><td>25</td><td>10</td><td>100</td></tr></table>"
    )
    assert _members(_one(html, _revenue(100e6))) == {"日本": 60e6, "海外": 40e6}


#: Tokio Marine's shape (FY2026): the segments, their 計, an adjustment and a 連結合計 column. Read
#: with 連結合計 as a member, its profit row summed to within 0.02% of an unrelated operating
#: cash-flow subtotal and was stored as the split (2026-11-27).
CONSOLIDATED_TOTAL = (
    "<p>報告セグメントごとの損益等に関する情報</p><table>"
    "<tr><td></td><td></td><td></td><td></td><td>(単位:百万円)</td><td>(単位:百万円)</td></tr>"
    "<tr><td></td><td>国内損害 保険事業</td><td>海外 保険事業</td><td>計</td><td>調整額</td>"
    "<td>連結合計</td></tr>"
    "<tr><td>売上高</td><td>600</td><td>400</td><td>1,000</td><td>△10</td><td>990</td></tr>"
    "</table>"
)


def test_a_consolidated_total_column_is_no_member() -> None:
    """The 連結合計 cell is the line's total and anchors it; read as a member, the line sums to
    nothing tagged and the segments are lost."""
    r = _one(CONSOLIDATED_TOTAL, _revenue(990e6))
    assert _members(r) == {"国内損害保険事業": 600e6, "海外保険事業": 400e6}


def test_a_sum_matches_a_tagged_figure_to_the_table_s_own_rounding_and_no_further() -> None:
    """Two members truncated to 百万円 miss the figure by at most a unit each; 0.05% of a large
    figure is far more than that, and a coincidence within it is not a match."""
    html = (
        "<p>報告セグメント</p><p>(単位：百万円)</p>"
        "<table><tr><td></td><td>A</td><td>B</td></tr>"
        "<tr><td>売上高</td><td>600,000</td><td>400,000</td></tr></table>"
    )
    truncated = _one(html, _revenue(1_000_002e6))
    assert _members(truncated) == {"A": 600_000e6, "B": 400_000e6}
    near = _one(html, _revenue(1_000_400e6))
    assert (near.facts, near.no_anchor) == ([], 1)
