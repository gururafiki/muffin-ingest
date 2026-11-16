"""The CNINFO PDF parser, ported from the edge's `cn-pdf.ts` with its checks (`cn-pdf-check.ts`).

THE FIXTURE IS REAL GEOMETRY: the coordinates measured on China Yangtze Power's FY2025 report,
page 13, where one row occupies THREE visual lines with the money in the middle:

    境内水电@61        增加@505  3.28@529        name, first line, no money
    75,661,563,315.43@118  25,881,578,129.36@217   the money, on its own line, no name
    行业@61            百分点@531                 name, second line

Read as lines that yields the segment `行业` ("industry") beside the right number.

  Yangtze  境内水电行业 75,661,563,315.43 + 其他行业 10,323,376,439.80 = 85,984,939,755.23
"""

from __future__ import annotations

import sys
from dataclasses import replace

import pytest

from muffin_ingest.facets import cn_child, cn_pdf
from muffin_ingest.facets.cn_pdf import CnSegmentRow, TextItem


def segments(items: list[TextItem], rules: list[cn_pdf.Rule] | None = None) -> list[CnSegmentRow]:
    """The rows a page yields, less the grand totals it states (kept as `stated_total` rows)."""
    return [r for r in cn_pdf.parse_segment_page(items, rules or []) if not r.stated_total]


def it(s: str, x: float, y: float) -> TextItem:
    """An item as `page_items` gives one, ending where its text does: a CJK character is a full em
    of these tables' ~10.5 pt type, anything else half of one."""
    width = sum(10.5 if ord(ch) >= 0x2E80 else 5.25 for ch in s)
    return TextItem(s, x, y, x + width)


YANGTZE = [
    it("主营业务分行业情况", 264.8, 617.7),
    it("分行业", 67.7, 596.8),
    it("营业收入", 138.6, 596.8),
    it("营业成本", 238.9, 596.8),
    it("毛利率", 312.1, 596.8),
    it("境内水电", 61.2, 576.0),
    it("增加", 505.2, 576.0),
    it("3.28", 528.8, 576.0),
    it("75,661,563,315.43", 118.0, 570.0),
    it("25,881,578,129.36", 217.0, 570.0),
    it("65.79", 335.0, 570.0),
    it("行业", 61.2, 561.0),
    it("百分点", 531.0, 561.0),
    it("其他行业", 61.2, 546.0),
    it("10,323,376,439.80", 118.0, 546.0),
    it("7,067,072,381.81", 217.0, 546.0),
    # SET JUSTIFIED, as one item carrying the spaces (Canadian Solar's page 69).
    it("光 伏 组 件", 61.2, 532.0),
    it("5,000,000,000.00", 118.0, 532.0),
    it("4,000,000,000.00", 217.0, 532.0),
    # What follows the table must not be absorbed into the last member.
    it("（2）产销量情况", 61.2, 512.0),
    it("单位：万千瓦时", 61.2, 498.0),
]


def test_the_table_is_rebuilt_from_positions_not_lines() -> None:
    rows = cn_pdf.parse_segment_page(YANGTZE)
    assert [r.name for r in rows] == ["境内水电行业", "其他行业", "光伏组件"]
    assert rows[0].revenue == 75661563315.43 and rows[0].cost == 25881578129.36
    assert rows[1].revenue == 10323376439.80
    assert all(r.revenue > 1e9 for r in rows), "the trailing percentage column is not revenue"
    assert all(r.axis == "cninfo:分行业" for r in rows)


def test_a_name_line_after_the_table_is_not_a_suffix_of_its_last_row() -> None:
    """Without the 16 pt adjacency bound, `（2）产销量情况` and the unit line joined `光伏组件`."""
    rows = cn_pdf.parse_segment_page(YANGTZE)
    assert rows[-1].name == "光伏组件"


def test_a_cell_is_placed_by_where_it_ends_not_where_it_starts() -> None:
    """A right-aligned figure starts wherever its length puts it: here the revenue starts LEFT of
    where the revenue label starts, and is still the revenue, because it ends inside the column."""
    shifted = [i if i.s != "营业收入" else it("营业收入", 160.0, 596.8) for i in YANGTZE]
    rows = cn_pdf.parse_segment_page(shifted)
    assert rows[0].revenue == 75661563315.43
    assert rows[0].cost == 25881578129.36


#: 掌阅科技 FY2025, page 17, as pdfium itemises it (measured 2026-11-15). Two revenue cells are too
#: wide for their column and wrap: `1,255,970,60` above, `3.03` below, with the name and the cost
#: centred between. The edge stored 数字阅读平台 at 3.03 yuan with no cost, and 衍生业务 at 6.54.
ZHANGYUE = [
    it("主营业务分行业情况", 256.7, 524.9),
    it("毛利率", 323.4, 489.8),
    it("分行业", 97.4, 482.0),
    it("营业收入", 161.4, 482.0),
    it("营业成本", 246.6, 482.0),
    it("数字阅读", 89.9, 442.6),
    it("减少 9.05", 480.7, 442.6),
    it("1,255,970,60", 156.1, 441.7),
    it("365,188,802.53", 236.3, 434.8),
    it("70.92", 333.8, 434.8),
    it("3.03", 198.1, 428.0),
    it("平台", 89.9, 427.0),
    it("个百分点", 483.5, 427.0),
    it("增加 7.13", 480.7, 410.9),
    it("1,854,940,21", 156.1, 410.1),
    it("衍生业务", 89.9, 403.1),
    it("593,570,318.91", 236.3, 403.1),
    it("68.00", 333.8, 403.1),
    it("6.54", 198.1, 396.3),
    it("版权产品", 89.9, 371.6),
    it("128,518,101.03", 160.0, 371.6),
    it("40,237,699.25", 240.0, 371.6),
    it("68.69", 333.8, 371.6),
    it("其他", 89.9, 332.2),
    it("3,306,357.50", 170.0, 332.2),
    it("3,005,464.76", 250.0, 332.2),
    it("主营业务分行业、分产品、分地区、分销售模式情况的说明", 89.9, 300.5),
    it("衍生业务营业收入增长主要系持续扩大短剧等衍生业务产能规模所致；毛利率增", 113.9, 278.9),
    it("长主要系规模扩展的同时，产品品质和商业化能力稳步提升，规模效应进一步显现所", 89.9, 255.5),
    it("（2）产销量情况分析表", 89.9, 232.2),
    it("主要产品", 89.9, 220.0),
    it("1,000.00", 170.0, 208.0),
]


def test_a_money_cell_wrapped_onto_two_lines_is_read_whole_with_its_centred_cost() -> None:
    rows = cn_pdf.parse_segment_page(ZHANGYUE)
    assert [(r.name, r.revenue, r.cost) for r in rows] == [
        ("数字阅读平台", 1255970603.03, 365188802.53),
        ("衍生业务", 1854940216.54, 593570318.91),
        ("版权产品", 128518101.03, 40237699.25),
        ("其他", 3306357.50, 3005464.76),
    ]


def test_the_explanation_and_the_next_section_are_not_rows_of_the_last_table() -> None:
    """The edge read on until the next mandated heading, so a paragraph became a member name and the
    production table's `1,000.00` a revenue."""
    rows = cn_pdf.parse_segment_page(ZHANGYUE)
    assert rows[-1].name == "其他" and len(rows) == 4


def test_the_sales_model_table_ends_the_geography_table() -> None:
    """直销 and 经销 are not regions: read into `分地区`, they made its split exceed the company."""
    items = [
        it("主营业务分地区情况", 256.7, 600.0),
        it("分地区", 97.4, 580.0),
        it("营业收入", 161.4, 580.0),
        it("营业成本", 246.6, 580.0),
        it("境内", 89.9, 560.0),
        it("1,000,000.00", 160.0, 560.0),
        it("600,000.00", 240.0, 560.0),
        it("主营业务分销售模式情况", 256.7, 540.0),
        it("销售模式", 97.4, 520.0),
        it("营业收入", 161.4, 520.0),
        it("营业成本", 246.6, 520.0),
        it("直销", 89.9, 500.0),
        it("900,000.00", 160.0, 500.0),
        it("500,000.00", 240.0, 500.0),
    ]
    assert [r.name for r in cn_pdf.parse_segment_page(items)] == ["境内"]


def test_a_cost_whose_second_line_follows_its_row_completes_that_row() -> None:
    items = [
        it("主营业务分产品情况", 256.7, 600.0),
        it("分产品", 97.4, 580.0),
        it("营业收入", 161.4, 580.0),
        it("营业成本", 246.6, 580.0),
        it("2,345,678,", 224.0, 500.0),
        it("储能产品", 89.9, 494.0),
        it("9,876,543,210.98", 140.0, 494.0),
        it("901.23", 250.0, 488.0),
    ]
    assert [(r.name, r.revenue, r.cost) for r in cn_pdf.parse_segment_page(items)] == [
        ("储能产品", 9876543210.98, 2345678901.23)
    ]


def test_two_lines_that_do_not_make_one_figure_drop_the_row_rather_than_guess() -> None:
    items = [
        it("主营业务分产品情况", 256.7, 600.0),
        it("分产品", 97.4, 580.0),
        it("营业收入", 161.4, 580.0),
        it("营业成本", 246.6, 580.0),
        it("12,34", 156.0, 500.0),
        it("某产品", 89.9, 494.0),
        it("5,67.89", 190.0, 488.0),
    ]
    assert cn_pdf.parse_segment_page(items) == []


def test_the_margin_beside_the_cost_column_is_never_read_as_cost() -> None:
    """With no cost cell, the edge took the first figure right of the revenue: the margin %."""
    items = [
        it("主营业务分行业情况", 256.7, 600.0),
        it("分行业", 97.4, 580.0),
        it("营业收入", 161.4, 580.0),
        it("营业成本", 246.6, 580.0),
        it("金融业务", 89.9, 560.0),
        it("1,000,000.00", 160.0, 560.0),
        it("62.50", 333.8, 560.0),
    ]
    assert [(r.name, r.cost) for r in cn_pdf.parse_segment_page(items)] == [("金融业务", None)]


def test_the_fiscal_period_comes_from_the_document() -> None:
    assert cn_pdf.fiscal_year_end_from("中国长江电力股份有限公司2025年年度报告 1 / 259") == (
        "2025-12-31"
    )
    assert cn_pdf.fiscal_year_end_from("隆基绿能科技股份有限公司2023 年年度报告") == "2023-12-31"
    # 中信建投's header draws `2025` and then `025` over it; pdfium reports both.
    assert cn_pdf.fiscal_year_end_from("2025025年年度报告 中信建") == "2025-12-31"
    assert cn_pdf.fiscal_year_end_from("股票代码：6010662025年年度报告") == "2025-12-31"
    assert cn_pdf.fiscal_year_end_from("关于召开股东大会的通知") is None
    assert cn_pdf.fiscal_year_end_from("1890年年度报告") is None


def test_facts_partition_against_the_largest_table_and_state_their_currency() -> None:
    facts = cn_pdf.facts_from_rows(cn_pdf.parse_segment_page(YANGTZE), "2025-12-31")
    revenue = [f for f in facts if f.metric_code == "revenue"]
    assert len(revenue) == 3 and all(f.partition_id == 1 for f in revenue)
    assert all(f.reconciled_to == 90984939755.23 for f in revenue)
    assert all(f.currency == "CNY" for f in facts)
    assert all(f.period_type == "annual" and f.period_start == "2025-01-01" for f in facts)
    assert any(f.metric_code == "cost_of_revenue" for f in facts)
    assert all(f.member_code.startswith("cninfo:") for f in facts)


def test_a_partial_geography_is_partition_zero_beside_a_split_of_the_whole() -> None:
    """LONGi discloses geography covering 118.4bn of its 129.5bn: present, never summed as the
    whole."""
    facts = cn_pdf.facts_from_rows(
        [
            CnSegmentRow("cninfo:分行业", "光伏行业", 129497674192.20, None),
            CnSegmentRow("cninfo:分地区", "中国境内", 80960841085.85, None),
            CnSegmentRow("cninfo:分地区", "欧洲地区", 16816034208.79, None),
            CnSegmentRow("cninfo:分地区", "亚太地区", 20591381190.91, None),
        ],
        "2023-12-31",
    )
    geo = [f for f in facts if f.axis == "cninfo:分地区"]
    assert len(geo) == 3 and all(f.partition_id == 0 for f in geo)
    assert all(f.partition_id == 1 for f in facts if f.axis == "cninfo:分行业")
    assert not any(f.metric_code == "cost_of_revenue" for f in facts), "a missing cost is no row"


@pytest.mark.parametrize(
    ("raw", "tidy"),
    [
        ("光 伏 组 件 产品收入", "光伏组件产品收入"),
        ("境内水电行业", "境内水电行业"),
        ("A 股", "A 股"),
        ("Solar Systems", "Solar Systems"),
        ("  境内  ", "境内"),
    ],
)
def test_typesetting_between_cjk_characters_comes_out_and_nothing_else(raw: str, tidy: str) -> None:
    assert cn_pdf.tidy_member_name(raw) == tidy


def test_halves_round_up_as_javascript_rounds_them() -> None:
    """The line buckets are `Math.round(y / 3)`; Python's `round` sends 0.5 to the even integer, so
    a baseline at y = 1.5 would land in a different bucket from the edge's."""
    assert cn_pdf._js_round(0.5) == 1
    assert cn_pdf._js_round(2.5) == 3
    assert cn_pdf._js_round(-0.5) == 0


# ── The child process (Phase 6 decision 7) ────────────────────────────────────────────────────

#: A one-page PDF with one line of text and no table: the smallest real document.
TINY = (
    b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 300 200]/Contents 4 0 R"
    b"/Resources<</Font<</F1 5 0 R>>>>>>endobj\n"
    b"4 0 obj<</Length 44>>stream\nBT /F1 12 Tf 20 100 Td (Annual report) Tj ET\nendstream endobj\n"
    b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"trailer<</Root 1 0 R>>\n%%EOF\n"
)


def test_a_report_is_read_in_a_child_and_a_document_with_no_table_yields_no_facts() -> None:
    result = cn_child.read(TINY)
    assert isinstance(result, cn_pdf.CnParseResult)
    assert result.facts == [] and result.page is None


def test_bytes_that_are_not_a_pdf_are_unreadable_and_the_caller_survives() -> None:
    result = cn_child.read(b"%PDF-1.7 not really")
    assert isinstance(result, cn_child.Unreadable)


def test_a_child_out_of_time_is_killed_and_counted_unreadable() -> None:
    result = cn_child.read(TINY, timeout_s=0.0001)
    assert isinstance(result, cn_child.Unreadable)
    assert "longer than" in result.reason


@pytest.mark.parametrize(
    ("script", "reason"),
    [
        # killed by the kernel (the OOM killer, or a limit pdfium meets in C): no answer at all
        ("import os, signal; os.kill(os.getpid(), signal.SIGKILL)", "exited -9"),
        # died with something to say: the last line of stderr is the reason
        ("import sys; sys.stderr.write('pdfium: bad xref\\n'); sys.exit(4)", "pdfium: bad xref"),
        # answered, then died part-way through the answer
        ("import sys; sys.stdout.buffer.write(b'\\x80\\x05\\x95')", "answer was unreadable"),
    ],
)
def test_a_child_that_dies_is_unreadable_with_its_reason(
    monkeypatch: pytest.MonkeyPatch, script: str, reason: str
) -> None:
    monkeypatch.setattr(cn_child, "_COMMAND", (sys.executable, "-c", script))
    result = cn_child.read(TINY)
    assert isinstance(result, cn_child.Unreadable)
    assert reason in result.reason


def test_what_a_library_prints_cannot_corrupt_the_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    """pdfium and its bindings may write to file descriptor 1; the child moves it to stderr before
    parsing, so the answer on the real stdout stays one pickle."""
    real = cn_pdf.segment_facts_from_pdf
    script = (
        "import os, runpy, muffin_ingest.facets.cn_pdf as p; real = p.segment_facts_from_pdf\n"
        "def noisy(*a):\n"
        "    os.write(1, b'warning: font substituted\\n'); print('from python too')\n"
        "    return real(*a)\n"
        "p.segment_facts_from_pdf = noisy\n"
        "runpy.run_module('muffin_ingest.facets.cn_child', run_name='__main__')\n"
    )
    monkeypatch.setattr(cn_child, "_COMMAND", (sys.executable, "-c", script))
    result = cn_child.read(TINY)
    assert isinstance(result, cn_pdf.CnParseResult), result
    assert real is cn_pdf.segment_facts_from_pdf  # the parent's own module is untouched


@pytest.mark.skipif(sys.platform != "linux", reason="only Linux enforces RLIMIT_AS; runs are Linux")
def test_a_child_out_of_memory_is_unreadable_not_a_dead_run() -> None:
    """The limit is the address space, so a child that cannot even map the parser is unreadable."""
    result = cn_child.read(TINY, limit_mb=8)
    assert isinstance(result, cn_child.Unreadable)


def test_the_items_of_a_real_page_are_runs_not_characters() -> None:
    """pdfium draws a word as one object or as one per character, depending on the producer; the
    items are the runs pdf.js would build either way."""
    import pypdfium2 as pdfium

    document = pdfium.PdfDocument(TINY)
    try:
        items = cn_pdf.page_items(document, 0)
    finally:
        document.close()
    assert [i.s for i in items] == ["Annual report"]
    assert items[0].x == pytest.approx(20, abs=0.5) and items[0].y == pytest.approx(100, abs=0.5)


def test_two_money_cells_set_flush_against_each_other_are_two_cells() -> None:
    """中免 FY2025 page 16: revenue and cost abut and merge into one run."""
    items = [
        it("主营业务分行业情况", 263.5, 377.1),
        it("分行业", 90.7, 356.2),
        it("营业收入", 141.7, 356.2),
        it("营业成本", 220.9, 356.2),
        it("商品销", 90.1, 335.2),
        TextItem("52,552,697,083.9135,779,710,755.06", 123.4, 328.5, 281.8),
        it("31.92", 301.3, 328.5),
        it("售业", 90.1, 321.7),
    ]
    assert [(r.name, r.revenue, r.cost) for r in cn_pdf.parse_segment_page(items)] == [
        ("商品销售业", 52552697083.91, 35779710755.06)
    ]


#: 诺德新材料 FY2025, page 36: every money cell wraps, and in every position. The edge stored
#: 电子信息材料 at 86.37 yuan and 国内 at 16.16, and read the 合计 row as a segment.
NORD = [
    it("主营业务分行业情况", 256.7, 590.0),
    it("分行业", 100.0, 560.0),
    it("营业收入", 165.0, 560.0),
    it("营业成本", 235.0, 560.0),
    it("电子信息", 90.0, 530.0),
    it("6,745,344,4", 158.0, 530.0),
    it("6,229,790,21", 225.0, 530.0),
    it("7.64", 320.0, 524.0),
    it("材料", 90.0, 518.0),
    it("86.37", 180.0, 518.0),
    it("5.73", 250.0, 518.0),
    # Broken before the point, and after it.
    it("315,802,261", 158.0, 502.0),
    it("285,057,241.", 225.0, 502.0),
    it("电线电缆", 90.0, 496.0),
    it("9.74", 320.0, 496.0),
    it(".25", 190.0, 490.0),
    it("44", 258.0, 490.0),
    # Broken after the point, and between the decimals: `34,281,472.0` is not a cell yet.
    it("发电及储", 90.0, 474.0),
    it("59,507,780.", 158.0, 474.0),
    it("34,281,472.0", 225.0, 474.0),
    it("能", 90.0, 462.0),
    it("23", 190.0, 462.0),
    it("9", 262.0, 462.0),
    it("合计", 90.0, 446.0),
    it("7,241,774,2", 158.0, 446.0),
    it("6,549,517,78", 225.0, 446.0),
    it("80.32", 180.0, 440.0),
    it("3.37", 250.0, 440.0),
]


def test_a_cell_may_break_anywhere_and_is_read_whole_and_a_total_is_not_a_segment() -> None:
    assert [(r.name, r.revenue, r.cost) for r in segments(NORD)] == [
        ("电子信息材料", 6745344486.37, 6229790215.73),
        ("电线电缆", 315802261.25, 285057241.44),
        ("发电及储能", 59507780.23, 34281472.09),
    ]


def test_a_name_beside_a_cell_already_being_set_starts_the_next_row() -> None:
    """乙产品 is within 16 pt of 甲产品's figures, but its own revenue is already half set: it is
    the next row's name, not the end of the last one's."""
    items = [
        it("主营业务分产品情况", 256.7, 600.0),
        it("分产品", 100.0, 580.0),
        it("营业收入", 165.0, 580.0),
        it("营业成本", 235.0, 580.0),
        it("甲产品", 90.0, 500.0),
        it("1,000.00", 170.0, 500.0),
        it("500.00", 250.0, 500.0),
        it("2,345,678,", 158.0, 492.0),
        it("乙产品", 90.0, 488.0),
        it("901.23", 180.0, 484.0),
    ]
    assert [(r.name, r.revenue, r.cost) for r in cn_pdf.parse_segment_page(items)] == [
        ("甲产品", 1000.0, 500.0),
        ("乙产品", 2345678901.23, None),
    ]


def test_two_money_cells_a_space_apart_are_two_cells() -> None:
    """江苏农垦 FY2025 page 22, as pdfium itemises it."""
    items = [
        it("主营业务分行业情况", 371.5, 478.4),
        it("分行业", 105.6, 461.4),
        it("营业收入", 188.3, 461.4),
        it("营业成本", 265.4, 461.4),
        it("种植业", 73.4, 444.5),
        TextItem("3,581,881,091.36 2,832,712,714.37", 173.0, 444.5, 323.6),
        it("20.92", 365.0, 444.5),
        it("种子加工与销售业", 73.4, 427.6),
        TextItem("1,838,096,637.00 1,655,816,512.04", 173.0, 427.6, 323.6),
    ]
    assert [(r.name, r.revenue, r.cost) for r in cn_pdf.parse_segment_page(items)] == [
        ("种植业", 3581881091.36, 2832712714.37),
        ("种子加工与销售业", 1838096637.00, 1655816512.04),
    ]


def test_a_table_stated_in_ten_thousands_of_yuan_is_read_in_yuan() -> None:
    """华微电子 FY2025 page 30: `单位：万元`, so 75,846.29 is 758,462,900 yuan. The edge stored it
    as 75,846.29 yuan."""
    items = [
        it("单位：万元", 650.0, 560.0),
        it("币种：人民币", 700.0, 560.0),
        it("主营业务分行业情况", 300.0, 545.0),
        it("分行业", 110.0, 520.0),
        it("营业收入", 175.0, 520.0),
        it("营业成本", 245.0, 520.0),
        it("集成电路", 105.0, 495.0),
        it("75,846.29", 170.0, 495.0),
        it("22,817.58", 240.0, 495.0),
    ]
    assert [(r.name, r.revenue, r.cost) for r in cn_pdf.parse_segment_page(items)] == [
        ("集成电路", 758462900.0, 228175800.0)
    ]
    # The default is 元, as the edge read every page: YANGTZE states no unit.
    assert cn_pdf.parse_segment_page(YANGTZE)[0].revenue == 75661563315.43


def test_the_cost_column_ends_at_the_next_label_even_one_set_on_another_line() -> None:
    """嘉泽 FY2025 page 14: `毛利率` is centred in a taller header cell, 6.5 pt above the other
    labels. 新能源产业基金 has no cost (`-`); read against the label after it, the margin
    `100.00` became its cost."""
    items = [
        it("主营业务分行业情况", 271.4, 755.0),
        it("毛利率", 326.6, 735.1),
        it("分行业", 103.3, 728.6),
        it("营业收入", 172.6, 728.6),
        it("营业成本", 257.7, 728.6),
        it("比上年增", 374.6, 728.6),
        it("新能源产", 95.5, 600.0),
        TextItem("5,777.84", 192.5, 593.5, 233.2),
        TextItem("-", 306.3, 593.5, 311.3),
        TextItem("100.00", 331.1, 593.5, 361.1),
        it("业基金", 95.5, 587.0),
    ]
    assert [(r.name, r.revenue, r.cost) for r in cn_pdf.parse_segment_page(items)] == [
        ("新能源产业基金", 5777.84, None)
    ]


def test_a_subtotal_with_no_rows_of_its_own_is_its_groups_row() -> None:
    """中信建投 FY2025 page 47, the geography table: 境外 exists only as `境外小计`. The other
    totals restate rows above them; dropping every total would lose 1.49 bn of revenue."""
    rows = [
        ("北京市", 1989033455.58), ("上海市", 1151931187.35), ("广东省", 657646433.32),
        ("其他地区", 3571109438.88), ("小计", 7369720515.13), ("公司本部", 14460433593.98),
        ("境内小计", 21830154109.11), ("境外小计", 1491560351.62), ("合计", 23321714460.73),
    ]  # fmt: skip
    items = [
        it("主营业务分地区情况", 271.4, 755.0),
        it("分地区", 103.3, 728.6),
        it("营业收入", 172.6, 728.6),
        it("营业成本", 257.7, 728.6),
    ]
    for n, (name, revenue) in enumerate(rows):
        y = 700.0 - 16 * n
        items += [it(name, 95.5, y), TextItem(f"{revenue:,.2f}", 160.0, y, 245.0)]
    got = [(r.name, r.revenue) for r in segments(items)]
    assert got == [
        ("北京市", 1989033455.58), ("上海市", 1151931187.35), ("广东省", 657646433.32),
        ("其他地区", 3571109438.88), ("公司本部", 14460433593.98), ("境外", 1491560351.62),
    ]  # fmt: skip
    assert abs(sum(r for _, r in got) - 23321714460.73) < 0.01


def _table(rows: list[tuple[str, float]]) -> list[TextItem]:
    items = [
        it("主营业务分行业情况", 271.4, 755.0),
        it("分行业", 103.3, 728.6),
        it("营业收入", 172.6, 728.6),
        it("营业成本", 257.7, 728.6),
    ]
    for n, (name, revenue) in enumerate(rows):
        y = 700.0 - 16 * n
        items += [it(name, 95.5, y), TextItem(f"{revenue:,.2f}", 160.0, y, 245.0)]
    return items


def test_a_deduction_is_negative_and_the_lines_sum_to_the_stated_total() -> None:
    """南方出版传媒 FY2025 page 13: 小计 12,222,732,795.17, `减：内部抵销数` 3,803,338,513.45
    printed positive, 合计 8,419,394,281.72. Summed as printed the split was 16.03 bn; the
    deduction is stored negative, named without the `减：` its sign carries, and both totals still
    go. A filer that prints the deduction negative gets the same row."""
    printed = [
        ("出版", 3584416527.61), ("发行", 6976079950.12), ("物资", 818314197.64),
        ("印刷业务", 580295041.63), ("报媒", 174882851.54), ("其他", 88744226.63),
        ("小计", 12222732795.17), ("减：内部抵销数", 3803338513.45), ("合计", 8419394281.72),
    ]  # fmt: skip
    joined = cn_pdf._without_restated_totals(
        [
            (
                "合同履约成本减：合并抵消数",
                cn_pdf._Row("cninfo:分地区", 3107286261.47, None, 1.0, 1.0, []),
            )
        ]
    )
    assert [(r.name, r.revenue) for r in joined] == [("合并抵消数", -3107286261.47)]
    for deduction in (3803338513.45, -3803338513.45):
        parsed = cn_pdf.parse_segment_page(
            _table([*printed[:7], ("减：内部抵销数", deduction), printed[8]])
        )
        rows = [r for r in parsed if not r.stated_total]
        assert [r.name for r in rows][-1] == "内部抵销数"
        assert rows[-1].revenue == -3803338513.45
        assert abs(sum(r.revenue for r in rows) - 8419394281.72) < 0.01
        assert [(r.name, r.revenue) for r in parsed if r.stated_total] == [("合计", 8419394281.72)]
        facts = cn_pdf.facts_from_rows(parsed, "2025-12-31")
        assert {f.reconciled_to for f in facts} == {8419394281.72}
        assert all(f.partition_id == 1 for f in facts)


def test_an_of_which_row_is_a_level_below_the_row_above_it() -> None:
    """`其中：` rows are part of the row above them. Flat, the table summed them twice, the 合计
    that restates the real rows no longer matched (and was kept as a segment), and one table naming
    `其中：手续费及佣金收入` under two parents gave two rows one key."""
    parsed = cn_pdf.parse_segment_page(
        _table(
            [
                ("证券业务", 900.0),
                ("其中：佣金", 600.0),
                ("期货业务", 100.0),
                ("其中：佣金", 40.0),
                ("合计", 1000.0),
            ]
        )
    )
    rows = [r for r in parsed if not r.stated_total]
    assert [(r.name, r.parent) for r in rows] == [
        ("证券业务", None),
        ("佣金", "证券业务"),
        ("期货业务", None),
        ("佣金", "期货业务"),
    ]
    facts = {
        (f.parent_member, f.member_code): f
        for f in cn_pdf.facts_from_rows(parsed, "2025-12-31")
        if f.metric_code == "revenue"
    }
    top = facts[(None, "cninfo:证券业务")]
    assert top.reconciled_to == 1000.0 and top.partition_id == 1
    child = facts[("cninfo:证券业务", "cninfo:佣金")]
    assert child.parent_axis == child.axis == "cninfo:分行业"
    assert child.reconciled_to == 900.0 and child.partition_id == 0  # part of its parent, not all
    assert facts[("cninfo:期货业务", "cninfo:佣金")].reconciled_to == 100.0


def test_an_of_which_row_whose_whole_is_not_a_row_is_kept_in_no_split_and_no_sum() -> None:
    """After a subtotal that restates the rows above it, `其中：` is part of that group, which has
    no row; heading a table, it is part of nothing stated. Either way it stays as a fact, in
    partition 0, and the target is the table's real rows."""
    rows = cn_pdf.parse_segment_page(
        _table(
            [
                ("其中：电力", 5.0),
                ("华东", 60.0),
                ("华南", 40.0),
                ("小计", 100.0),
                ("其中：沪", 30.0),
            ]
        )
    )
    assert [(r.name, r.parent) for r in rows] == [
        ("电力", ""),
        ("华东", None),
        ("华南", None),
        ("沪", ""),
    ]
    facts = {f.member_code: f for f in cn_pdf.facts_from_rows(rows, "2025-12-31")}
    assert facts["cninfo:华东"].reconciled_to == 100.0 and facts["cninfo:华东"].partition_id == 1
    for orphan in ("cninfo:电力", "cninfo:沪"):
        assert facts[orphan].partition_id == 0 and facts[orphan].parent_member is None


def test_an_of_which_row_under_a_subtotal_kept_as_its_group_belongs_to_that_group() -> None:
    """中信建投's `境外小计` is the only row 境外 has (境内小计 before it ends 境内's rows), so
    it is kept as 境外, and the `其中：` under it is 境外's; a 合计 between ends the group."""
    parsed = cn_pdf.parse_segment_page(
        _table(
            [
                ("北京", 1989033455.58),
                ("上海", 1151931187.35),
                ("境内小计", 3140964642.93),
                ("境外小计", 1491560351.62),
                ("其中：香港", 820511443.08),
                ("合计", 4632524994.55),
                ("其中：澳门", 1.0),
            ]
        )
    )
    rows = [r for r in parsed if not r.stated_total]
    assert [(r.name, r.parent) for r in rows] == [
        ("北京", None),
        ("上海", None),
        ("境外", None),
        ("香港", "境外"),
        ("澳门", ""),
    ]


def _tree(rows: list[CnSegmentRow]) -> list[tuple[str, str | None]]:
    return [(r.name, r.parent) for r in rows]


def test_rows_summing_to_the_row_above_them_are_its_parts() -> None:
    """中远海控 FY2025 page 16, the geography table: each business, then its regions summing to
    it, labelled `其中：` on the first region only. Flat, the table counted every dollar twice and,
    as the largest sum, became the target, so the double count was served."""
    rows = cn_pdf.parse_segment_page(
        _table(
            [
                ("集装箱航运业务", 210731494147.06),
                ("其中：美洲地区", 55627048574.46),
                ("欧洲地区", 41552686927.42),
                ("亚太地区", 55507548255.69),
                ("中国大陆", 25096101715.46),
                ("其他国际地区", 32948108674.03),
                ("码头业务", 12041307540.80),
                ("其中：欧洲地区", 5616638953.08),
                ("亚太地区", 627744182.06),
                ("中国大陆", 5346687862.21),
                ("其他国际地区", 450236543.45),
                ("分部间抵销", -3268996465.16),
                ("收入合计", 219503805222.70),
            ]
        )
    )
    ship, port = "集装箱航运业务", "码头业务"
    assert _tree(rows) == [
        (ship, None),
        ("美洲地区", ship),
        ("欧洲地区", ship),
        ("亚太地区", ship),
        ("中国大陆", ship),
        ("其他国际地区", ship),
        (port, None),
        ("欧洲地区", port),
        ("亚太地区", port),
        ("中国大陆", port),
        ("其他国际地区", port),
        ("分部间抵销", None),
    ]
    facts = [f for f in cn_pdf.facts_from_rows(rows, "2025-12-31") if f.metric_code == "revenue"]
    assert {f.reconciled_to for f in facts if f.parent_member is None} == {219503805222.70}
    assert all(f.partition_id == 1 for f in facts)


def test_a_named_total_above_its_parts_is_their_parent_and_parts_nest_two_deep() -> None:
    """哈投 FY2025 page 21 prints each group's total FIRST (`热电业务合计`) and `总计` last; under
    证券业务 each business is itself the sum of its fees and interest. Dropped as a total, the group
    left its parts at the table's level."""
    parsed = cn_pdf.parse_segment_page(
        _table(
            [
                ("热电业务合计", 1616772729.24),
                ("其中：电力", 78346031.68),
                ("热力", 1504836976.13),
                ("建材", 33589721.43),
                ("证券业务合计", 1103338168.65),
                ("经纪业务", 656888922.24),
                ("其中：佣金", 528527819.70),
                ("利息收入", 128361102.54),
                ("信用业务", 277448891.47),
                ("利息收入", 277448891.47),
                ("其他", 169000354.94),
                ("总计", 2720110897.89),
            ]
        )
    )
    rows = [r for r in parsed if not r.stated_total]
    assert _tree(rows) == [
        ("热电业务", None),
        ("电力", "热电业务"),
        ("热力", "热电业务"),
        ("建材", "热电业务"),
        ("证券业务", None),
        ("经纪业务", "证券业务"),
        ("佣金", "经纪业务"),
        ("利息收入", "经纪业务"),
        ("信用业务", "证券业务"),
        ("利息收入", "信用业务"),
        ("其他", "证券业务"),
    ]


def test_a_heading_over_a_run_closed_by_its_subtotal_is_the_run_s_parent() -> None:
    """湘财 FY2025 page 17: `证券业：` and `实业：` head runs of products in bordered cells with no
    figures, each run closed by a `小计`; both runs carry an `其他`, which flat shared one key."""
    lines: list[tuple[str, float | None]] = [
        ("证券业：", None),
        ("经纪业务", 1056978534.13),
        ("其他", -223282865.64),
        ("小计", 833695668.49),
        ("实业：", None),
        ("成品油贸易", 114321697.59),
        ("其他", 12079972.38),
        ("小计", 126401669.97),
    ]
    items = [
        it("主营业务分产品情况", 271.4, 755.0),
        it("分产品", 103.3, 728.6),
        it("营业收入", 172.6, 728.6),
        it("营业成本", 257.7, 728.6),
    ]
    rules = [cn_pdf.Rule(708.0, 90.0, 520.0)]
    for n, (name, revenue) in enumerate(lines):
        y = 700.0 - 16 * n
        items.append(it(name, 95.5, y))
        if revenue is not None:
            items.append(TextItem(f"{revenue:,.2f}", 160.0, y, 245.0))
        rules.append(cn_pdf.Rule(y - 8.0, 90.0, 520.0))
    rows = cn_pdf.parse_segment_page(items, rules)
    assert [(r.name, r.parent, r.revenue) for r in rows] == [
        ("证券业", None, 833695668.49),
        ("经纪业务", "证券业", 1056978534.13),
        ("其他", "证券业", -223282865.64),
        ("实业", None, 126401669.97),
        ("成品油贸易", "实业", 114321697.59),
        ("其他", "实业", 12079972.38),
    ]


def test_a_header_label_may_start_a_run_but_not_sit_inside_one() -> None:
    """中科软 FY2025 page 30 (万元): the cost label arrives as `营业成本 毛利率`, and the growth
    columns' header line carries `）营业收入`. Matched only exactly, the table was never found."""
    items = [
        it("主营业务分行业情况", 250.4, 220.6),
        TextItem("单位：万元", 398.4, 234.8, 450.7),
        TextItem("分行业", 111.0, 192.9, 142.4),
        TextItem("营业收入", 187.6, 192.9, 229.4),
        TextItem("营业成本 毛利率", 238.9, 192.9, 317.3),
        TextItem("（", 312.4, 192.9, 322.8),
        TextItem("）营业收入", 331.6, 192.9, 383.6),
        TextItem("金融领域", 70.9, 165.2, 112.8),
        TextItem("367,126.15", 186.7, 165.9, 233.9),
        TextItem("269,176.25", 243.4, 165.9, 285.3),
        TextItem("26.68", 313.2, 165.9, 336.7),
        TextItem("政务领域", 70.9, 137.0, 112.8),
        TextItem("80,681.68", 192.0, 137.7, 233.9),
        TextItem("61,723.80", 243.4, 137.7, 285.3),
        TextItem("23.50", 313.2, 137.7, 336.7),
    ]
    rows = cn_pdf.parse_segment_page(items)
    assert [(r.name, r.revenue, r.cost) for r in rows] == [
        ("金融领域", 3671261500.0, 2691762500.0),
        ("政务领域", 806816800.0, 617238000.0),
    ]
    assert not cn_pdf._labelled(TextItem("）营业收入", 0, 0), "营业收入")


def test_the_table_title_does_not_bound_the_cost_column() -> None:
    """贵州茅台 FY2025 page 10: the title sits 15.0 pt above the header, centred at x 276.8, inside
    the cost column whose right-aligned cells end at 291.2. Taken as the next label, it closed the
    column early and every cost of the page was lost."""
    items = [
        TextItem("主营业务分行业情况", 276.8, 531.8, 344.3),
        TextItem("营业收入比上", 360.1, 521.6, 405.0),
        TextItem("分行业", 97.9, 516.8, 120.4),
        TextItem("营业收入", 159.8, 516.8, 189.7),
        TextItem("营业成本", 241.4, 516.8, 271.3),
        TextItem("毛利率（%）", 303.6, 516.8, 344.9),
        TextItem("酒类", 89.9, 496.7, 104.8),
        TextItem("168,774,585,187.65", 143.0, 496.7, 210.5),
        TextItem("14,805,900,139.59", 227.6, 496.7, 291.2),
        TextItem("91.23", 327.6, 496.7, 346.2),
    ]
    rows = cn_pdf.parse_segment_page(items)
    assert [(r.name, r.revenue, r.cost) for r in rows] == [
        ("酒类", 168774585187.65, 14805900139.59)
    ]


def test_a_name_run_into_its_revenue_is_split_by_character_width() -> None:
    """汾酒 FY2025 page 10: `食品制造业` fills its cell and arrives in one run with the revenue,
    `食品制造业 38,592,734,473.12` from x 49.4 to 194.7; the industry table was lost. Shared out
    by character count the name would end at x 87, by width at 101: both inside the name column
    here, so the test pins the width rule directly as well."""
    items = [
        TextItem("主营业务分行业情况", 255.2, 437.0, 349.7),
        TextItem("分行业", 59.9, 416.2, 91.3),
        TextItem("营业收入", 129.1, 416.2, 171.0),
        TextItem("营业成本", 220.3, 416.2, 262.2),
        TextItem("毛利率", 295.0, 422.9, 326.4),
        TextItem("食品制造业 38,592,734,473.12", 49.4, 395.2, 194.7),
        TextItem("9,666,616,593.02", 201.1, 395.2, 285.1),
    ]
    rows = cn_pdf.parse_segment_page(items)
    assert [(r.name, r.revenue, r.cost) for r in rows] == [
        ("食品制造业", 38592734473.12, 9666616593.02)
    ]
    name, cell = cn_pdf._split_cells(items[5])
    assert name.end is not None and 100 < name.end < 102 and cell.end == 194.7


def _blank_pages(n: int) -> bytes:
    kids = " ".join(f"{3 + k} 0 R" for k in range(n))
    pages = "".join(
        f"{3 + k} 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 300 200]>>endobj\n" for k in range(n)
    )
    return (
        "%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
        f"2 0 obj<</Type/Pages/Kids[{kids}]/Count {n}>>endobj\n{pages}"
        "trailer<</Root 1 0 R>>\n%%EOF\n"
    ).encode()


def test_a_unit_stated_at_the_foot_of_the_page_before_scales_the_table() -> None:
    """1225062013 FY2025: `单位：万元` is the last line of page 38 and the table opens page 39. Read
    from page 39 alone, 4.18 bn of revenue was stored as 418,000.20, by both readers."""
    before = [
        TextItem("2025年年度报告", 200.0, 790.0, 300.0),
        TextItem("单位：万元", 405.5, 89.6, 457.9),
        TextItem("币种：人民币", 468.5, 89.6, 531.5),
    ]
    table = [
        it("主营业务分行业情况", 263.4, 754.6),
        it("分行业", 111.1, 726.9),
        it("营业收入", 180.1, 726.9),
        it("营业成本", 257.7, 726.9),
        it("医药制造业", 89.9, 699.2),
        TextItem("418,000.20", 180.0, 699.2, 245.0),
    ]
    pages = {0: before, 1: table}
    result = cn_pdf.segment_facts_from_pdf(_blank_pages(2), items_of=lambda _d, i: pages[i])
    assert result.page == 2
    assert [(f.member_code, f.value) for f in result.facts] == [("cninfo:医药制造业", 4180002000.0)]
    # A unit the table's own page states wins over the one carried.
    assert cn_pdf.unit_stated_last([*before, TextItem("单位：元", 400, 50, 450)], 1) == 1
    assert cn_pdf.unit_stated_last([], 10000) == 10000


def test_one_row_is_a_part_only_when_it_carries_its_parent_s_figures() -> None:
    """江海's `信用业务` is all interest income: one row, the same revenue and cost. A row with the
    same revenue and another cost is a different line that happens to equal it."""
    parent = CnSegmentRow("cninfo:分产品", "信用业务", 277448891.47, 168851200.00)
    same = CnSegmentRow("cninfo:分产品", "利息收入", 277448891.47, 168851200.00)
    other = CnSegmentRow("cninfo:分产品", "利息收入", 277448891.47, 1.00)
    assert [r.parent for r in cn_pdf._nested([parent, same])] == [None, "信用业务"]
    assert [r.parent for r in cn_pdf._nested([parent, other])] == [None, None]


def test_a_row_named_as_a_heading_with_figures_loses_only_the_colon() -> None:
    """湘财's industry table states `证券业：` with its figures in the same row."""
    rows = cn_pdf.parse_segment_page(_table([("证券业：", 2011976952.75), ("贸易", 276748571.11)]))
    assert [r.name for r in rows] == ["证券业", "贸易"]


def test_two_rows_naming_one_member_under_one_parent_are_one_member_with_their_sum() -> None:
    """1225263103 FY2025: the product table lists `其他` twice at one level (drugs, then the
    company's other business). The key cannot hold both, and the six lines reconcile only with
    both, so they are one `其他` carrying the sum; a cost only when both rows state one."""
    rows = [
        CnSegmentRow("cninfo:分行业", "医药制造业", 4441368300.00, 1000.0),
        CnSegmentRow("cninfo:分行业", "医药流通业", 3755237000.00, 1000.0),
        CnSegmentRow("cninfo:分行业", "化妆品业", 566784300.00, 1000.0),
        CnSegmentRow("cninfo:分行业", "其他", 205752600.00, 1000.0),
        CnSegmentRow("cninfo:分产品", "肝病用药", 4267666600.00, 1000.0),
        CnSegmentRow("cninfo:分产品", "心脑血管用药", 115427300.00, 1000.0),
        CnSegmentRow("cninfo:分产品", "其他", 58274400.00, 300.0),
        CnSegmentRow("cninfo:分产品", "医药流通业", 3755237000.00, 1000.0),
        CnSegmentRow("cninfo:分产品", "化妆品业", 566784300.00, 1000.0),
        CnSegmentRow("cninfo:分产品", "其他", 205752600.00, 200.0),
    ]
    facts = cn_pdf.facts_from_rows(rows, "2025-12-31")
    other = [
        (f.metric_code, f.value, f.partition_id)
        for f in facts
        if f.axis == "cninfo:分产品" and f.member_code == "cninfo:其他"
    ]
    assert other == [("revenue", 264027000.0, 1), ("cost_of_revenue", 500.0, 1)]
    assert {f.partition_id for f in facts} == {1}
    no_cost = [*rows[:9], replace(rows[9], cost=None)]
    merged = [
        f for f in cn_pdf.facts_from_rows(no_cost, "2025-12-31") if f.member_code == "cninfo:其他"
    ]
    assert [(f.axis, f.metric_code) for f in merged] == [
        ("cninfo:分行业", "revenue"),
        ("cninfo:分行业", "cost_of_revenue"),
        ("cninfo:分产品", "revenue"),
    ]
    # Under different parents they are different members, and nothing is merged.
    nested = [replace(rows[6], parent="医药制造业"), replace(rows[9], parent=None)]
    assert len(cn_pdf._merged(nested)) == 2


def test_a_heading_read_again_is_another_year_s_table_and_ends_the_reading() -> None:
    """厦门钨业 FY2025 prints its 2025 tables on page 41 and, on page 42, 2024's restated under
    the same headings. Read with the next page, they were a second set of this year's rows."""
    restated = [
        it("主营业务分行业情况", 271.4, 400.0),
        it("分行业", 103.3, 380.0),
        it("营业收入", 172.6, 380.0),
        it("营业成本", 257.7, 380.0),
        it("钨钼", 95.5, 360.0),
        TextItem("16,754,700,008.91", 160.0, 360.0, 245.0),
        # and the rest of that year after it, whatever this year printed
        it("主营业务分地区情况", 271.4, 340.0),
        it("分地区", 103.3, 320.0),
        it("营业收入", 172.6, 320.0),
        it("营业成本", 257.7, 320.0),
        it("国内", 95.5, 300.0),
        TextItem("16,754,700,008.91", 160.0, 300.0, 245.0),
    ]
    rows = segments([*_table([("钨钼等有色金属", 19632101618.50)]), *restated])
    assert [(r.name, r.revenue) for r in rows] == [("钨钼等有色金属", 19632101618.50)]
    # A table counts once its header is read: 1225022492 titles its section `(1). 主营业务分行业
    # 情况` above the table's own heading. And an explanation naming a table is no heading: it
    # follows each table there, and the next table is still this year's.
    titled = [it("(1). 主营业务分行业情况", 170.1, 780.0), *_table([("投资类业务", 1554440867.45)])]
    explained = [
        it("主营业务分行业情况的说明", 165.4, 560.0),
        it("主营业务分产品情况", 271.4, 540.0),
        it("分产品", 103.3, 520.0),
        it("营业收入", 172.6, 520.0),
        it("营业成本", 257.7, 520.0),
        it("资管", 95.5, 500.0),
        TextItem("476,869,479.14", 160.0, 500.0, 245.0),
    ]
    assert [(r.axis, r.name) for r in segments([*titled, *explained])] == [
        ("cninfo:分行业", "投资类业务"),
        ("cninfo:分产品", "资管"),
    ]


def test_a_row_numbered_beside_the_foot_is_not_a_page_number() -> None:
    """1225121079 FY2025 page 13: `7、华南区` is two items, `7` and `、华南区`, on a line near the
    foot. `7` matches a folio and lost the row its number: only a folio alone on its line goes."""
    first = [
        it("主营业务分地区情况", 271.4, 300.0),
        it("分地区", 103.3, 280.0),
        it("营业收入", 172.6, 280.0),
        it("营业成本", 257.7, 280.0),
        TextItem("7", 95.5, 80.0, 100.8),
        it("、华南区", 100.8, 80.0),
        TextItem("749,334,430.96", 160.0, 80.0, 245.0),
        TextItem("13", 295.0, 40.0, 305.0),
    ]
    items, _ = cn_pdf.join_pages(first, [], [it("说明", 95.5, 760.0)], [])
    assert [i.s for i in items].count("7") == 1
    assert "13" not in [i.s for i in items]


def test_the_target_is_the_largest_total_the_page_states() -> None:
    """1225011770 FY2025: the product and geography tables detail only steel and agree on 37.24 bn;
    the industry table covers the company and states 合计 57.86 bn. 1225177612: the product split
    (18.65 bn, before eliminations) out-sums the industry table, whose 合计 states 18.39 bn. The
    filing's own total decides, as SEC's consolidated figure does."""
    steel = [
        CnSegmentRow("cninfo:分行业", "钢铁制造与销售", 37242455260.03, None),
        CnSegmentRow("cninfo:分行业", "焦炭", 20615878850.30, None),
        CnSegmentRow("cninfo:分行业", "合计", 57858334110.33, None, stated_total=True),
        CnSegmentRow("cninfo:分产品", "专用板材", 21901783230.50, None),
        CnSegmentRow("cninfo:分产品", "特钢长材", 15340672029.53, None),
        CnSegmentRow("cninfo:分产品", "合计", 37242455260.03, None, stated_total=True),
        CnSegmentRow("cninfo:分地区", "国内", 32180885137.10, None),
        CnSegmentRow("cninfo:分地区", "国外", 5061570122.93, None),
    ]
    facts = cn_pdf.facts_from_rows(steel, "2025-12-31")
    assert {f.reconciled_to for f in facts} == {57858334110.33}
    assert {f.axis: f.partition_id for f in facts if f.axis == "cninfo:分行业"} == {
        "cninfo:分行业": 1
    }
    assert {f.partition_id for f in facts if f.axis != "cninfo:分行业"} == {0}
    assert not any(f.member_code == "cninfo:合计" for f in facts)


def test_a_split_reconciles_to_the_fen_not_to_half_a_percent() -> None:
    """1225177612 FY2025: five of six products sum to 18,395.9 m against the stated 18,386.3 m, a
    coincidence 0.05% off that SEC's 0.5% tolerance placed as a split. A 万元 table's rounding,
    100 yuan a cell, still reconciles."""
    products = [
        ("纯碱", 3799632317.89),
        ("粘胶短纤维", 10209472004.91),
        ("聚氯乙烯树脂", 1887422654.18),
        ("电", 254781812.44),
        ("蒸汽", 1036340080.26),
        ("烧碱", 1463052917.56),
    ]
    rows = [CnSegmentRow("cninfo:分产品", n, v, None) for n, v in products]
    rows.append(CnSegmentRow("cninfo:分行业", "合计", 18386277345.79, None, stated_total=True))
    assert {f.partition_id for f in cn_pdf.facts_from_rows(rows, "2025-12-31")} == {0}
    # 20 万元 rows each rounded to 0.01 万元 (100 yuan), 1,900 yuan short of a 100 m total.
    rounded = [CnSegmentRow("cninfo:分地区", f"地区{n}", 4999900.0, None) for n in range(19)]
    rounded.append(CnSegmentRow("cninfo:分地区", "地区19", 5000000.0, None))
    rounded.append(CnSegmentRow("cninfo:分地区", "合计", 100000000.0, None, stated_total=True))
    assert {f.partition_id for f in cn_pdf.facts_from_rows(rounded, "2025-12-31")} == {1}


def test_parts_may_miss_their_parent_by_the_rounding_of_each_printed_figure() -> None:
    """中国电影 FY2025 page 26: regions 1-7 sum to 4,746,459,154.20 under `一、国内`
    4,746,459,154.21, every cell rounded to the fen. A 万元 table rounds to 0.01 万元 (100 yuan)."""
    regions = [216723199.73, 1073539126.98, 458385896.69, 1668144878.66, 242297794.84,
               338033826.34, 749334430.96]  # fmt: skip
    rows = [CnSegmentRow("cninfo:分地区", "一、国内", 4746459154.21, None)]
    rows += [CnSegmentRow("cninfo:分地区", f"{n + 1}、区", v, None) for n, v in enumerate(regions)]
    assert [r.parent for r in cn_pdf._nested(rows)][1:] == ["一、国内"] * 7
    # Off by more than the rounding allows: no parts.
    off = [*rows[:1], *(replace(r, revenue=r.revenue + 0.01) for r in rows[1:])]
    assert all(r.parent is None for r in cn_pdf._nested(off))

    # In 万元 a figure is printed to 0.01 万元, 100 yuan, so it is off by at most 50, and a parent
    # with two parts by 150: 100 short is rounding, 200 short is not.
    def wan(short: float) -> list[CnSegmentRow]:
        return [
            CnSegmentRow("cninfo:分行业", "合并", 30000.0, None),
            CnSegmentRow("cninfo:分行业", "甲", 10000.0, None),
            CnSegmentRow("cninfo:分行业", "乙", 20000.0 - short, None),
        ]

    assert [r.parent for r in cn_pdf._nested(wan(100.0), step=100.0)] == [None, "合并", "合并"]
    assert [r.parent for r in cn_pdf._nested(wan(200.0), step=100.0)] == [None, None, None]
    assert [r.parent for r in cn_pdf._nested(wan(100.0), step=0.01)] == [None, None, None]


def test_a_table_s_unit_sets_how_far_its_parts_may_round() -> None:
    """The step reaches the nesting from the page: in a 万元 table 1.00 + 1.99 under 3.00 is 100
    yuan short, within the rounding of three figures printed to 0.01 万元."""
    items = [
        TextItem("单位：万元", 400.0, 760.0, 450.0),
        *_table([("合并", 3.00), ("甲", 1.00), ("乙", 1.99)]),
    ]
    assert [(r.name, r.parent) for r in segments(items)] == [
        ("合并", None),
        ("甲", "合并"),
        ("乙", "合并"),
    ]


def _rows_of(
    cells: list[tuple[str, str, str]], extra: list[TextItem] | None = None
) -> list[TextItem]:
    """A one-table page with each row's name, revenue cell and cost cell as printed."""
    items = [
        it("主营业务分行业情况", 271.4, 755.0),
        it("分行业", 103.3, 728.6),
        it("营业收入", 172.6, 728.6),
        it("营业成本", 257.7, 728.6),
        *(extra or []),
    ]
    for n, (name, revenue, cost) in enumerate(cells):
        y = 700.0 - 16 * n
        items += [it(name, 95.5, y), TextItem(revenue, 160.0, y, 245.0)]
        items.append(TextItem(cost, 250.0, y, 320.0))
    return items


def test_a_figure_with_one_decimal_is_whole_unless_the_next_line_continues_it() -> None:
    """云天励飞 FY2025 page 41 (万元) drops trailing zeros: `62,389.8` beside `53,608.12`. Read as
    unfinished, 华南's cost `52,781.7` waited and took 华东's `35,812.07`. A wrapped cell's
    continuation is bare digits in the same column on the next line."""
    rows = segments(
        _rows_of(
            [
                ("华南", "83,824.73", "52,781.7"),
                ("华东", "40,152.03", "35,812.07"),
                ("消费级", "62,389.8", "53,830.34"),
                ("西南", "7,166.29", "6,490.64"),
            ]
        )
    )
    assert [(r.name, r.revenue, r.cost) for r in rows] == [
        ("华南", 83824.73, 52781.70),
        ("华东", 40152.03, 35812.07),
        ("消费级", 62389.80, 53830.34),
        ("西南", 7166.29, 6490.64),
    ]
    wrapped = _rows_of(
        [("甲", "1,234.5", "1,000.00"), ("", "6", ""), ("乙", "2,000.00", "1,500.00")]
    )
    assert [(r.name, r.revenue) for r in segments(wrapped)] == [("甲", 1234.56), ("乙", 2000.0)]


def test_a_one_decimal_figure_ending_the_page_is_whole_only_on_a_page_printed_so() -> None:
    """中海油服 FY2025 page 16 states every figure to one decimal (百万元), its last row at the foot
    of the page. 彤程 ends page 22 on `2,320,408,095.5` beside money printed to the fen elsewhere:
    its `5` opens page 23, so that cell is left unfinished rather than read as .50."""
    cnooc = _rows_of([("钻井服务", "14,898.0", "12,767.7"), ("船舶服务", "5,198.1", "4,899.1")])
    assert [(r.name, r.revenue, r.cost) for r in segments(cnooc)] == [
        ("钻井服务", 14898.0, 12767.7),
        ("船舶服务", 5198.1, 4899.1),
    ]
    elsewhere = [TextItem("232,511,791.93", 300.0, 790.0, 360.0)]
    tongcheng = _rows_of([("特种橡胶助剂", "2,320,408,095.5", "1,751,224,956.7")], elsewhere)
    assert segments(tongcheng) == []


#: 广州发展 FY2025 page 18 (元): rows the filer numbers, two of their names wrapped around figures
#: set 6.8 pt below the first line, each row between drawn borders.
GUANGZHOU = [
    it("主营业务分行业情况", 248.0, 187.2),
    it("毛利率", 322.2, 166.2),
    it("分行业", 52.8, 159.5),
    it("营业收入", 140.6, 159.5),
    it("营业成本", 243.5, 159.5),
    it("比上年增", 360.2, 159.5),
    it("(1)燃煤发电及", 31.4, 131.8),
    TextItem("4,349,947,127.70", 122.9, 125.0, 206.7),
    TextItem("3,731,967,878.88", 227.3, 125.0, 311.2),
    TextItem("14.21", 327.6, 125.0, 353.7),
    it("供热业务", 31.4, 118.1),
    it("(2)天然气发电", 31.4, 104.0),
    TextItem("3,204,831,500.79", 122.9, 97.2, 206.7),
    TextItem("2,773,644,846.63", 227.3, 97.2, 311.2),
    it("及供热业务", 31.4, 90.4),
    it("(3)能源物流业务", 31.4, 76.2),
    TextItem("29,485,020,343.45", 117.6, 76.2, 206.7),
    TextItem("29,159,979,509.93", 222.0, 76.2, 311.1),
]
GUANGZHOU_BORDERS = [cn_pdf.Rule(y, 25.3, 565.4) for y in (198.0, 183.9, 142.6, 114.8, 87.1, 73.1)]


def test_a_numbered_row_is_a_row_and_a_numbered_heading_ends_the_table() -> None:
    """A numbered line ended the table, so 广州发展's yielded nothing: `(1)燃煤发电及` is a row,
    told from the next section's heading by its cell, which holds its revenue. The heading below
    sits under the table's last border, 68 pt above the next one, and the figure set there belongs
    to whatever follows it."""
    after = [
        it("（4）主要销售客户", 89.9, 62.0),
        it("甲", 31.4, 40.0),
        TextItem("1,000,000.00", 122.9, 40.0, 206.7),
    ]
    rows = segments([*GUANGZHOU, *after], [*GUANGZHOU_BORDERS, cn_pdf.Rule(5.0, 25.3, 565.4)])
    assert [(r.name, r.revenue, r.cost) for r in rows] == [
        ("燃煤发电及供热业务", 4349947127.70, 3731967878.88),
        ("天然气发电及供热业务", 3204831500.79, 2773644846.63),
        ("能源物流业务", 29485020343.45, 29159979509.93),
    ]
    # A heading in a gap no taller than a row is still told by its figures: the next table's top
    # border is 43 pt below, and the figures under it are outside the heading's band.
    tight = [*GUANGZHOU_BORDERS, cn_pdf.Rule(30.0, 25.3, 565.4), cn_pdf.Rule(10.0, 25.3, 565.4)]
    after[1:] = [it("甲", 31.4, 20.0), TextItem("1,000,000.00", 122.9, 20.0, 206.7)]
    assert [r.name for r in segments([*GUANGZHOU, *after], tight)] == [
        "燃煤发电及供热业务",
        "天然气发电及供热业务",
        "能源物流业务",
    ]
    # With no borders drawn, a numbered line is a row only when its figures are on it.
    flat = [
        i
        for i in GUANGZHOU
        if i.s not in ("供热业务", "及供热业务", "(2)天然气发电") and not i.s.startswith("(1)")
    ]
    flat += [it("(1)燃煤发电", 31.4, 125.0), it("(2)天然气发电", 31.4, 97.2)]
    assert [r.name for r in segments(flat)] == ["燃煤发电", "天然气发电", "能源物流业务"]


HEADER = TextItem("2025年年度报告", 223.4, 790.0, 396.8)
UNDERLINE = cn_pdf.Rule(786.1, 89.8, 530.5)


def _border(*ys: float) -> list[cn_pdf.Rule]:
    return [cn_pdf.Rule(y, 83.7, 537.3) for y in ys]


#: 彤程 FY2025 page 22: the industry table's first row is cut by the page break, its name and the
#: first part of each figure (two cells in one run, a space apart) in the page's last cell.
TONGCHENG_FOOT = [
    HEADER,
    it("主营业务分行业情况", 269.0, 122.5),
    it("分行业", 105.2, 101.6),
    it("营业收入", 165.8, 101.6),
    it("营业成本", 237.6, 101.6),
    it("毛利率（%）", 295.8, 101.6),
    it("特种橡胶助剂", 90.4, 80.8),
    TextItem("2,320,408,095.5 1,751,224,956.7", 153.0, 81.4, 295.1),
    TextItem("22 / 259", 295.0, 63.1, 325.0),
]
#: Page 23: the rest of the cut row above the first border, then the next row.
TONGCHENG_HEAD = [
    HEADER,
    TextItem("5", 216.0, 760.0, 221.2),
    TextItem("2", 290.0, 760.0, 295.3),
    it("行业", 90.4, 759.8),
    it("电子材料行业", 90.4, 741.5),
    TextItem("985,615,772.46", 155.6, 742.0, 221.1),
    TextItem("704,568,590.39", 229.6, 742.0, 295.1),
    TextItem("23 / 259", 295.0, 63.1, 325.0),
]
TONGCHENG_ROWS = [
    ("特种橡胶助剂行业", 2320408095.55, 1751224956.72),
    ("电子材料行业", 985615772.46, 704568590.39),
]


def _joined(
    head: list[TextItem],
    head_rules: list[cn_pdf.Rule],
    tail: list[TextItem],
    tail_rules: list[cn_pdf.Rule],
) -> list[tuple[str, float, float | None]]:
    items, rules = cn_pdf.join_pages(head, head_rules, tail, tail_rules)
    return [(r.name, r.revenue, r.cost) for r in segments(items, rules)]


def test_a_table_the_page_break_cuts_is_read_whole() -> None:
    """上海隧道 FY2025: the geography table ends page 17 at 56.32 bn and page 18 opens with `澳门
    1,253,524,048.26`, completing it at the industry table's 57.58 bn. Read alone, page 17's
    partial table was the only total it had. Here the second page also states the 合计."""
    first = [
        HEADER,
        it("主营业务分行业情况", 271.4, 300.0),
        it("分行业", 103.3, 280.0),
        it("营业收入", 172.6, 280.0),
        it("营业成本", 257.7, 280.0),
        it("上海", 95.5, 120.0),
        TextItem("100.00", 160.0, 120.0, 245.0),
        it("浙江", 95.5, 104.0),
        TextItem("200.00", 160.0, 104.0, 245.0),
        TextItem("17 / 259", 295.0, 63.1, 325.0),
    ]
    second = [
        HEADER,
        it("澳门", 95.5, 754.4),
        TextItem("300.00", 160.0, 754.4, 245.0),
        it("合计", 95.5, 738.4),
        TextItem("600.00", 160.0, 738.4, 245.0),
        it("（2）产销量情况分析表", 89.9, 700.0),
        it("江苏", 95.5, 680.0),
        TextItem("1,000.00", 160.0, 680.0, 245.0),
        TextItem("18 / 259", 295.0, 63.1, 325.0),
    ]
    pages = {0: first, 1: second}
    result = cn_pdf.segment_facts_from_pdf(_blank_pages(2), items_of=lambda _d, i: pages[i])
    assert result.page == 1
    assert [(f.member_code, f.value, f.partition_id, f.reconciled_to) for f in result.facts] == [
        ("cninfo:上海", 100.0, 1, 600.0),
        ("cninfo:浙江", 200.0, 1, 600.0),
        ("cninfo:澳门", 300.0, 1, 600.0),
    ]
    # What repeats on every page is not read twice, and no folio is.
    items, rules = cn_pdf.join_pages(first, [UNDERLINE], second, [UNDERLINE])
    assert [i.s for i in items].count(HEADER.s) == 1
    assert not [i.s for i in items if "/ 259" in i.s]
    assert rules == [UNDERLINE]


def test_a_row_the_page_break_cuts_is_one_row_with_its_whole_name() -> None:
    """彤程 FY2025: `特种橡胶助剂` and `2,320,408,095.5 1,751,224,956.7` end page 22 in a drawn
    cell; `行业`, `5` and `2` open page 23 above its first border. The cell the break closed goes,
    so the name is whole; and two cells a space apart split even one decimal into being set."""
    assert _joined(
        TONGCHENG_FOOT, _border(119.2, 91.4, 77.4), TONGCHENG_HEAD, _border(756.0, 728.2)
    ) == (TONGCHENG_ROWS)
    # A second page that draws the cut row's cell its own top border: both borders go.
    assert (
        _joined(
            TONGCHENG_FOOT, _border(119.2, 91.4, 77.4), TONGCHENG_HEAD, _border(768.0, 756.0, 728.2)
        )
        == TONGCHENG_ROWS
    )


def test_a_page_opening_with_prose_keeps_the_border_the_table_ended_on() -> None:
    """A paragraph's last line set in the name column above the second page's first border is no
    part of a name: only figures there say a row was cut."""
    foot = [
        *TONGCHENG_FOOT[:6],
        it("橡胶助剂", 90.4, 80.8),
        TextItem("2,320,408,095.55 1,751,224,956.72", 153.0, 81.4, 295.1),
    ]
    head = [it("补偿。", 90.4, 770.0), *TONGCHENG_HEAD[4:]]
    assert _joined(foot, _border(119.2, 91.4, 77.4), head, _border(756.0, 728.2)) == [
        ("橡胶助剂", 2320408095.55, 1751224956.72),
        ("电子材料行业", 985615772.46, 704568590.39),
    ]


def test_a_stated_total_ends_a_run_of_parts() -> None:
    """Rows after a `合计` are never parts of a row before it: here 50 + 30 + 20 would make
    `合计`, 华东 and 华南 the parts of 全国 if the total did not close the run."""
    rows = cn_pdf._nested(
        [
            CnSegmentRow("cninfo:分地区", "全国", 100.0, None),
            CnSegmentRow("cninfo:分地区", "合计", 50.0, None, stated_total=True),
            CnSegmentRow("cninfo:分地区", "华东", 30.0, None),
            CnSegmentRow("cninfo:分地区", "华南", 20.0, None),
        ]
    )
    assert [r.parent for r in rows] == [None, None, None, None]


def test_the_target_is_the_total_most_tables_agree_on() -> None:
    """A table that runs onto the next page is read in part (哈投's geography): its sum, the
    largest, is no total the filing states, while the industry and product tables agree."""
    rows = [
        CnSegmentRow("cninfo:分行业", "热电", 60.0, None),
        CnSegmentRow("cninfo:分行业", "证券", 40.0, None),
        CnSegmentRow("cninfo:分产品", "供暖", 70.0, None),
        CnSegmentRow("cninfo:分产品", "经纪", 30.0, None),
        CnSegmentRow("cninfo:分地区", "哈尔滨", 60.0, None),
        CnSegmentRow("cninfo:分地区", "证券", 40.0, None),
        CnSegmentRow("cninfo:分地区", "上海", 7.0, None),
    ]
    facts = cn_pdf.facts_from_rows(rows, "2025-12-31")
    assert {f.reconciled_to for f in facts} == {100.0}
    by_axis = {f.axis: f.partition_id for f in facts if f.member_code != "cninfo:上海"}
    assert by_axis == {"cninfo:分行业": 1, "cninfo:分产品": 1, "cninfo:分地区": 1}
    assert next(f for f in facts if f.member_code == "cninfo:上海").partition_id == 0


def test_a_subtotal_restating_every_row_above_it_goes_even_straight_after_another() -> None:
    """`境内小计` directly under `小计` has no row of its own, yet it is not a group: it restates
    everything above it. Kept, it would be 境内 beside the regions it sums."""
    items = [
        it("主营业务分地区情况", 271.4, 755.0),
        it("分地区", 103.3, 728.6),
        it("营业收入", 172.6, 728.6),
        it("营业成本", 257.7, 728.6),
    ]
    for n, (name, revenue) in enumerate(
        [
            ("华东", 600.0),
            ("华南", 400.0),
            ("小计", 1000.0),
            ("境内小计", 1000.0),
            ("境外小计", 50.0),
        ]
    ):
        y = 700.0 - 16 * n
        items += [it(name, 95.5, y), TextItem(f"{revenue:,.2f}", 200.0, y, 245.0)]
    assert [(r.name, r.revenue) for r in cn_pdf.parse_segment_page(items)] == [
        ("华东", 600.0),
        ("华南", 400.0),
        ("境外", 50.0),
    ]


def test_a_note_well_below_the_last_row_is_not_part_of_its_name() -> None:
    items = [
        it("主营业务分产品情况", 271.4, 755.0),
        it("分产品", 103.3, 728.6),
        it("营业收入", 172.6, 728.6),
        it("营业成本", 257.7, 728.6),
        it("储能产品", 95.5, 700.0),
        TextItem("1,000.00", 200.0, 700.0, 245.0),
        it("注：含内部抵消", 95.5, 650.0),
    ]
    assert [r.name for r in cn_pdf.parse_segment_page(items)] == ["储能产品"]


def test_a_names_first_line_goes_to_the_row_whose_figures_start_beside_it() -> None:
    """The shape of 昭衍's first row: a three-line name beside a wrapped cell. Its first line is
    12 pt under the previous row's figures and 18 pt above its own row's LAST line, but only 6 pt
    above the line where its own figures start."""
    items = [
        it("主营业务分行业情况", 271.4, 560.0),
        it("分行业", 103.3, 520.0),
        it("营业收入", 172.6, 520.0),
        it("营业成本", 257.7, 520.0),
        it("甲业务", 95.5, 470.0),
        TextItem("2,000.00", 200.0, 470.0, 245.0),
        it("药物非临", 95.5, 458.0),
        TextItem("1,577,040,05", 180.0, 452.0, 245.0),
        it("床研究服", 95.5, 446.0),
        TextItem("7.63", 225.0, 440.0, 245.0),
        it("务", 95.5, 434.0),
    ]
    assert [(r.name, r.revenue) for r in cn_pdf.parse_segment_page(items)] == [
        ("甲业务", 2000.0),
        ("药物非临床研究服务", 1577040057.63),
    ]


def _two_rows(*, borders: bool) -> tuple[list[TextItem], list[cn_pdf.Rule]]:
    """A top-aligned table: each row's figures beside its name's FIRST line. 建筑施工业务's name
    runs to a third line, 24 pt below its figures and 12 pt above the next row's."""
    items = [
        it("主营业务分行业情况", 271.4, 560.0),
        it("分行业", 103.3, 530.0),
        it("营业收入", 172.6, 530.0),
        it("营业成本", 257.7, 530.0),
        it("建筑施", 95.5, 506.0),
        TextItem("1,000.00", 200.0, 506.0, 245.0),
        it("工业务", 95.5, 494.0),
        it("（含劳务）", 95.5, 482.0),
        it("石油化工", 95.5, 470.0),
        TextItem("2,000.00", 200.0, 470.0, 245.0),
    ]
    rules = [cn_pdf.Rule(y, 90.0, 520.0) for y in (518.0, 476.0, 452.0)] if borders else []
    return items, rules


def test_a_name_line_belongs_to_the_cell_the_table_draws_around_it() -> None:
    items, rules = _two_rows(borders=True)
    assert [r.name for r in cn_pdf.parse_segment_page(items, rules)] == [
        "建筑施工业务（含劳务）",
        "石油化工",
    ]


def test_without_borders_a_tie_goes_to_the_row_above() -> None:
    """Nothing drawn, two-line names set from the top: `工业务` is 12 pt from both rows' figures,
    and a top-aligned table means the row above."""
    items = [
        it("主营业务分行业情况", 271.4, 560.0),
        it("分行业", 103.3, 530.0),
        it("营业收入", 172.6, 530.0),
        it("营业成本", 257.7, 530.0),
        it("建筑施", 95.5, 506.0),
        TextItem("1,000.00", 200.0, 506.0, 245.0),
        it("工业务", 95.5, 494.0),
        it("石油化", 95.5, 482.0),
        TextItem("2,000.00", 200.0, 482.0, 245.0),
        it("工业务", 95.5, 470.0),
        it("- 10 -", 98.0, 460.0),
    ]
    assert [r.name for r in cn_pdf.parse_segment_page(items)] == ["建筑施工业务", "石油化工业务"]


TINY_WITH_A_LINE = (
    b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 300 200]/Contents 4 0 R>>endobj\n"
    b"4 0 obj<</Length 52>>stream\n0 0 0 RG 1 w 20 50 m 280 50 l S 20 60 260 130 re S\nendstream "
    b"endobj\ntrailer<</Root 1 0 R>>\n%%EOF\n"
)


def test_the_page_s_horizontal_lines_are_its_rules_and_a_box_is_not() -> None:
    import pypdfium2 as pdfium

    document = pdfium.PdfDocument(TINY_WITH_A_LINE)
    try:
        rules = cn_pdf.page_rules(document, 0)
    finally:
        document.close()
    assert len(rules) == 1
    assert rules[0].y == pytest.approx(50, abs=1) and rules[0].x0 < 21 and rules[0].x1 > 279


def _one_page(content: bytes) -> bytes:
    """A one-page PDF drawing `content`, with Helvetica as /F1."""
    return (
        b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
        b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
        b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 300 200]/Contents 4 0 R"
        b"/Resources<</Font<</F1 5 0 R>>>>>>endobj\n"
        + f"4 0 obj<</Length {len(content)}>>stream\n".encode()
        + content
        + b"\nendstream endobj\n"
        b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
        b"trailer<</Root 1 0 R>>\n%%EOF\n"
    )


#: 浙商证券's shape: a clip to a band at the top of the page, text and a rule drawn below it (so
#: never seen); text on every other side of a clip; then the visible page, with a second clip
#: that its text sits inside.
CLIPPED = _one_page(
    b"q 0 150 300 50 re W n BT /F1 12 Tf 20 100 Td (Hidden) Tj ET "
    b"0 0 0 RG 1 w 20 90 m 280 90 l S Q "
    b"q 200 0 100 200 re W n BT /F1 12 Tf 20 140 Td (Beside) Tj ET Q "
    b"q 0 0 100 200 re W n BT /F1 12 Tf 200 140 Td (Across) Tj ET Q "
    b"q 0 0 300 40 re W n BT /F1 12 Tf 120 170 Td (Above) Tj ET Q "
    b"BT /F1 12 Tf 20 60 Td (Seen) Tj ET "
    b"q 0 0 300 120 re W n BT /F1 12 Tf 20 30 Td (Inside) Tj ET Q "
    b"0 0 0 RG 1 w 20 20 m 280 20 l S"
)


def test_text_and_rules_drawn_outside_their_clip_are_not_on_the_page() -> None:
    """Measured 2026-11-15 on 浙商证券's FY2025 report: a running header drawn from another whole
    page, clipped to the header band, left that page's text under the table, and `（1）浙商资管…`
    ended the industry table after its first row."""
    import pypdfium2 as pdfium

    document = pdfium.PdfDocument(CLIPPED)
    try:
        items = cn_pdf.page_items(document, 0)
        rules = cn_pdf.page_rules(document, 0)
    finally:
        document.close()
    assert [i.s for i in items] == ["Seen", "Inside"]
    assert [round(r.y) for r in rules] == [20]


# --- Shenzhen's `营业收入构成` table -------------------------------------------------------------
#
# MEASURED 2026-11-16: the Dagster drain's first runs read rows from 0 of 562 Shenzhen reports,
# against 61 of 163 Shanghai ones. Shenzhen prints no mandated heading; its split is one table,
# `营业收入构成`, with the company's total first and a row per group (`分行业`, `分产品`, `分地区`,
# `分销售模式`) over its members, the amount and its share of revenue for each year, and no cost.
# Every fixture below is the geometry pdfium gives for a real report (x where an item starts, `end`
# where it ends, y its baseline), cut to the lines each rule is about.


def at(s: str, x: float, end: float, y: float) -> TextItem:
    return TextItem(s, x, y, end)


def _composition_header(y: float) -> list[TextItem]:
    """9900000141 FY2022 (page 16): the heading, the unit, the years and the column labels."""
    return [
        at("（", 56, 65, y),
        at("1", 66, 71, y),
        at("）", 71, 80, y),
        at("营业收入构成", 83, 139, y),
        at("单位：元", 494, 530, y - 27),
        at("2022", 235, 252, y - 45.7),
        at("年", 253, 262, y - 45.7),
        at("2021", 390, 408, y - 45.7),
        at("年", 409, 418, y - 45.7),
        at("同比增减", 494, 530, y - 55.3),
        at("金额", 202, 220, y - 64.9),
        at("占营业收入比重", 256, 319, y - 64.9),
        at("金额", 357, 375, y - 64.9),
        at("占营业收入比重", 413, 476, y - 64.9),
    ]


def _composition_row(name: str, amount: str, x: float, y: float) -> list[TextItem]:
    """A member's line: its name, this year's amount (ending at 246), and the other columns."""
    return [
        at(name, 62, 62 + 9 * len(name), y),
        at(amount, x, 246, y),
        at("12.34%", 294, 320, y),
        at("1,000,000.00", 341, 401, y),
        at("5.17%", 450, 477, y),
        at("10.63%", 509, 536, y),
    ]


ZHONGHUAN = [
    *_composition_header(678.5),
    *_composition_row("营业收入合计", "19,450,638,161.91", 181, 594.4),
    at("分行业", 62, 89, 575.2),
    *_composition_row("光伏产品", "12,592,573,895.18", 181, 557.2),
    *_composition_row("磁性材料", "4,203,950,982.71", 186, 542.2),
    *_composition_row("锂电池", "1,561,369,295.11", 186, 527.2),
    *_composition_row("器件", "720,697,317.94", 192, 511.5),
    *_composition_row("其他", "372,046,670.97", 192, 496.5),
    at("分地区", 62, 89, 383.7),
    *_composition_row("境内", "8,111,418,840.95", 186, 366.2),
    *_composition_row("境外", "11,193,276,049.60", 181, 351.2),
    *_composition_row("其他业务收入", "145,943,271.36", 192, 335.6),
]

#: What follows the groups: the sales-model group (直销 and 分销 are channels, not regions), then
#: the next section, whose table carries `分行业` rows of its own under `营业收入`/`营业成本`.
AFTER_THE_GROUPS = [
    at("分销售模式", 62, 107, 318.8),
    *_composition_row("直销", "13,015,039,304.28", 181, 301.4),
    *_composition_row("分销", "6,435,598,857.63", 186, 285.8),
    at("（2）占公司营业收入或营业利润10%以上的行业、产品、地区、销售模式的情况", 56, 385, 256.4),
    at("单位：元", 494, 530, 211.9),
    at("营业收入", 147, 183, 184.3),
    at("营业成本", 231, 267, 184.3),
    at("毛利率", 305, 332, 184.3),
    at("分行业", 62, 89, 156.1),
    at("光伏产品", 62, 98, 138.1),
    at("12,592,573,895.18", 136, 200, 138.1),
    at("10,721,147,446.83", 222, 286, 138.1),
]


def test_shenzhen_s_composition_table_is_read_group_by_group() -> None:
    rows = cn_pdf.parse_segment_page([*ZHONGHUAN, *AFTER_THE_GROUPS], composition=True)
    assert [(r.axis, r.name, r.revenue, r.cost, r.stated_total) for r in rows] == [
        ("cninfo:营业收入构成", "营业收入合计", 19450638161.91, None, True),
        ("cninfo:分行业", "光伏产品", 12592573895.18, None, False),
        ("cninfo:分行业", "磁性材料", 4203950982.71, None, False),
        ("cninfo:分行业", "锂电池", 1561369295.11, None, False),
        ("cninfo:分行业", "器件", 720697317.94, None, False),
        ("cninfo:分行业", "其他", 372046670.97, None, False),
        ("cninfo:分地区", "境内", 8111418840.95, None, False),
        ("cninfo:分地区", "境外", 11193276049.60, None, False),
        ("cninfo:分地区", "其他业务收入", 145943271.36, None, False),
    ]
    # The mandated reader reads none of it: without the composition reading, nothing changes.
    assert cn_pdf.parse_segment_page([*ZHONGHUAN, *AFTER_THE_GROUPS]) == []


def test_each_composition_group_is_a_split_of_the_company_s_total() -> None:
    rows = cn_pdf.parse_segment_page(ZHONGHUAN, composition=True)
    facts = cn_pdf.facts_from_rows(rows, "2022-12-31")
    assert {f.axis for f in facts} == {"cninfo:分行业", "cninfo:分地区"}
    assert all(f.metric_code == "revenue" and f.currency == "CNY" for f in facts)
    assert all(f.partition_id == 1 and f.reconciled_to == 19450638161.91 for f in facts)


def test_the_composition_table_ends_at_the_notice_before_the_quarterly_table() -> None:
    """With no sales-model group, the industry-disclosure notice and a quarterly table follow the
    last group (9900000382 FY2025, page 29): its `营业收入` row would be a region."""
    quarterly = [
        at("公司需遵守《深圳证券交易所上市公司自律监管指引第 3 号——行业信息披露》", 57, 516, 289.1),
        at("单位：元", 493, 529, 271.1),
        at("营业收入", 62, 98, 214.3),
        at("416,640,093.59", 160, 246, 214.3),
    ]
    rows = cn_pdf.parse_segment_page([*ZHONGHUAN, *quarterly], composition=True)
    assert rows[-1].name == "其他业务收入" and len(rows) == 9


def test_a_composition_header_may_set_its_share_label_over_two_lines() -> None:
    """9900003427 FY2024 (page 17): `占营业收入` 6 pt above `金额`, `比重` 6 pt below. The amount
    column ends where the share label starts, or the share (95.97%) would be read beside it."""
    items = [
        at("（1） 营业收入构成", 57, 151, 558.8),
        at("占营业收入", 249, 294, 499.0),
        at("占营业收入", 411, 456, 499.0),
        at("金额", 184, 202, 493.0),
        at("金额", 344, 362, 493.0),
        at("比重", 262, 280, 487.0),
        at("比重", 424, 442, 487.0),
        at("营业收入合计", 68, 122, 470.7),
        at("23,881,680,710.28", 158, 234, 470.7),
        at("100%", 280, 298, 470.7),
        at("分行业", 68, 95, 454.5),
        at("集成电路封装测试", 68, 140, 440.1),
        at("22,918,526,750.91", 158, 234, 440.1),
        at("95.97%", 271, 298, 440.1),
        at("21,134,845,804.57", 321, 397, 440.1),
    ]
    rows = cn_pdf.parse_segment_page(items, composition=True)
    assert [(r.name, r.revenue) for r in rows] == [
        ("营业收入合计", 23881680710.28),
        ("集成电路封装测试", 22918526750.91),
    ]


def test_a_group_label_centred_across_the_table_opens_its_group() -> None:
    """9900003433 FY2023 (page 20) centres `分行业` and `分产品` at x 283, in the share column.
    Read only in the name column, the groups never opened and no row had an axis to go to."""
    items = [
        at("（1） 营业收入构成", 90, 181, 611.9),
        at("金额", 163, 183, 546.8),
        at("占营业收入比重", 220, 289, 546.8),
        at("营业收入合计", 65, 125, 526.8),
        at("3,852,339,560.06", 137, 211, 526.8),
        at("分行业", 283, 313, 506.7),
        at("流体机械", 65, 105, 486.7),
        at("3,800,531,840.49", 137, 211, 486.7),
        at("98.66%", 259, 292, 486.7),
        at("加工制造业", 65, 115, 466.6),
        at("51,807,719.57", 150, 211, 466.6),
        at("分产品", 283, 313, 446.6),
        at("压缩机（组）", 65, 125, 426.5),
        at("1,946,583,539.68", 137, 211, 426.5),
    ]
    rows = cn_pdf.parse_segment_page(items, composition=True)
    assert [(r.axis, r.name) for r in rows if not r.stated_total] == [
        ("cninfo:分行业", "流体机械"),
        ("cninfo:分行业", "加工制造业"),
        ("cninfo:分产品", "压缩机（组）"),
    ]


def test_an_elimination_s_sign_is_read_wherever_it_is_set() -> None:
    """Measured 2026-11-16, two ways a negative amount is printed, both read as positive at first:
    9900009032 FY2024 sets the sign flush against the figure as an item of its own (it fell into
    the name, `合并抵消-`), and 9900004624 FY2023 sets it on a line of its own above a cell wrapped
    over three lines. An empty cell's dash beside its name is no sign: that row has no amount, and
    its name is no neighbour's."""
    flush = [
        at("（1） 营业收入构成", 57, 151, 600.0),
        at("金额", 165, 183, 566.5),
        at("占营业收入比重", 225, 288, 566.5),
        at("营业收入合计", 57, 111, 549.2),
        at("114,601,056,662.29", 138, 210, 549.2),
        at("分地区", 57, 84, 376.7),
        at("华南地区", 57, 93, 359.3),
        at("67,393,687,661.28", 143, 210, 359.3),
        at("华东地区", 57, 93, 342.2),
        at("16,343,599,813.21", 143, 210, 342.2),
        at("华北地区", 57, 93, 324.9),
        at("27,435,014,704.38", 143, 210, 324.9),
        at("华中地区", 57, 93, 307.6),
        at("30,865,136,138.53", 143, 210, 307.6),
        at("境外地区", 57, 93, 290.3),
        at("14,339,378,668.54", 143, 210, 290.3),
        at("合并抵消", 57, 93, 273.2),
        at("-", 140, 143, 273.2),
        at("41,775,760,323.65", 143, 210, 273.2),
        at("-", 261, 264, 273.2),
        at("36.45%", 264, 292, 273.2),
    ]
    rows = cn_pdf.parse_segment_page(flush, composition=True)
    assert rows[-1].name == "合并抵消" and rows[-1].revenue == -41775760323.65
    facts = cn_pdf.facts_from_rows(rows, "2024-12-31")
    # The regions reconcile to the company's total only less the elimination.
    assert all(f.partition_id == 1 for f in facts)

    wrapped = [
        at("（1） 营业收入构成", 57, 151, 650.0),
        at("金额", 163, 181, 623.9),
        at("占营业收入比重", 221, 284, 623.9),
        at("65,577,042,044.", 139, 207, 609.5),
        at("营业收入合计", 57, 111, 603.6),
        at("04", 198, 207, 597.5),
        at("分地区", 56.8, 83.8, 234.5),
        at("65,796,402,937.", 139.0, 206.5, 220.1),
        at("国内收入", 56.8, 92.8, 214.1),
        at("100.33%", 255.4, 287.0, 214.1),
        at("14", 197.6, 206.6, 208.1),
        at("6,368,311,293.9", 139.0, 206.5, 195.9),
        at("国外收入", 56.8, 92.8, 189.9),
        at("4", 202.0, 206.5, 183.9),
        at("-", 202.0, 206.5, 171.6),
        at("-", 362.6, 367.1, 171.6),
        at("合并抵消", 56.8, 92.8, 159.6),
        at("6,587,672,187.0", 139.0, 206.5, 159.6),
        at("-10.04%", 255.4, 287.0, 159.6),
        at("4", 202.0, 206.5, 147.6),
        # A business with no revenue this year: a dash in its amount cell, beside its name.
        at("停产业务", 56.8, 92.8, 135.0),
        at("-", 202.0, 206.5, 135.0),
        at("1,000.00", 330.0, 367.1, 135.0),
        at("其他", 56.8, 74.8, 122.0),
        at("5,000.00", 180.0, 206.5, 122.0),
    ]
    rows = cn_pdf.parse_segment_page(wrapped, composition=True)
    assert [(r.name, r.revenue) for r in rows if not r.stated_total] == [
        ("国内收入", 65796402937.14),
        ("国外收入", 6368311293.94),
        ("合并抵消", -6587672187.04),
        ("其他", 5000.00),
    ]


def test_the_composition_table_is_read_only_where_no_mandated_heading_is_printed() -> None:
    """A report printing a mandated table is read from it, exactly as before; one printing none is
    read from its composition table, found past a page that only mentions it in prose."""
    prose = [
        TextItem("2022年年度报告", 200.0, 790.0, 300.0),
        TextItem("报告期内公司营业收入构成未发生重大变化。", 60.0, 500.0, 420.0),
    ]
    pages = {0: prose, 1: ZHONGHUAN}
    result = cn_pdf.segment_facts_from_pdf(_blank_pages(2), items_of=lambda _d, i: pages[i])
    assert result.page == 2 and result.period_ending == "2022-12-31"
    assert len([f for f in result.facts if f.axis == "cninfo:分行业"]) == 5

    mandated = [TextItem("2022年年度报告", 200.0, 790.0, 300.0), *YANGTZE]
    pages = {0: ZHONGHUAN, 1: mandated}
    result = cn_pdf.segment_facts_from_pdf(_blank_pages(2), items_of=lambda _d, i: pages[i])
    assert result.page == 2
    assert {f.member_code for f in result.facts} == {
        "cninfo:境内水电行业",
        "cninfo:其他行业",
        "cninfo:光伏组件",
    }


def test_a_composition_heading_read_again_is_another_year_s_table() -> None:
    restated = [
        *_composition_header(250.0),
        *_composition_row("营业收入合计", "12,607,410,411.09", 181, 170.0),
        at("分行业", 62, 89, 150.0),
        *_composition_row("光伏产品", "6,499,472,478.03", 186, 130.0),
    ]
    rows = cn_pdf.parse_segment_page([*ZHONGHUAN, *restated], composition=True)
    assert [r.name for r in rows][-1] == "其他业务收入" and len(rows) == 9


def test_a_figure_set_one_digit_group_at_a_time_is_one_figure() -> None:
    """9900002261 FY2024 (Yunda, page 25) sets every digit group as an item of its own. A cell is
    placed by where it ends, and `48`, `,` and `543` end left of the amount label: the first groups
    were read as part of the name and the company's revenue as 32,234.43."""

    def groups(parts: list[tuple[str, float, float]], y: float) -> list[TextItem]:
        return [at(s, x, end, y) for s, x, end in parts]

    yunda = [
        at("（1） 营业收入构成", 57, 152, 654.5),
        at("金额", 168, 186, 600.6),
        at("占营业收入比重", 228, 291, 600.6),
        at("营业收入合计", 62, 116, 584.4),
        *groups(
            [("48", 139, 148), (",", 148, 153), ("543", 153, 166), (",", 166, 171),
             ("032", 171, 184), (",", 184, 189), ("234", 189, 202), (".", 202, 207),
             ("43", 207, 216), ("100", 274, 288), ("%", 288, 292)],
            584.4,
        ),
        at("分行业", 62, 89, 568.0),
        at("快递服务收入", 62, 116, 553.8),
        *groups(
            [("47", 139, 148), (",", 148, 153), ("887", 153, 166), (",", 166, 171),
             ("143", 171, 184), (",", 184, 189), ("764", 189, 202), (".", 202, 207),
             ("48", 207, 216), ("98", 265, 274), (".", 274, 279), ("65", 279, 288)],
            553.8,
        ),
    ]  # fmt: skip
    rows = cn_pdf.parse_segment_page(yunda, composition=True)
    assert [(r.name, r.revenue) for r in rows] == [
        ("营业收入合计", 48543032234.43),
        ("快递服务收入", 47887143764.48),
    ]
    # Two cells a column apart stay two: only pieces that still make one figure join.
    apart = cn_pdf._joined([at("1,000.00", 150, 186, 1.0), at("2,000.00", 186.5, 220, 1.0)])
    assert [i.s for i in apart] == ["1,000.00", "2,000.00"]


def test_composition_headings_and_labels_as_reports_actually_set_them() -> None:
    """The heading's numbering incomplete (9900003921 `（ ） 营业收入构成`), the share label split
    `占营业收`/`入比重` (9900004142, page 23), and the sales-model group with a note after it
    (9900002724: `分销售模式（统计数据仅包含机械制造业务）`), whose 直销/经销/OEM were read into
    the geography."""
    items = [
        at("（", 57, 66, 523.5),
        at("） 营业收入构成", 70, 136, 523.5),
        at("占营业收", 300, 336, 468.4),
        at("金额", 233, 251, 462.4),
        at("入比重", 305, 332, 456.4),
        at("营业收入合计", 62, 116, 438.2),
        at("2,820,493,237.55", 222, 285, 438.2),
        at("100%", 320, 341, 438.2),
        at("分地区", 62, 89, 418.1),
        at("境内", 62, 80, 398.0),
        at("2,805,952,645.98", 222, 285, 398.0),
        at("99.48%", 313, 341, 398.0),
        at("境外", 62, 80, 380.0),
        at("14,540,591.57", 235, 285, 380.0),
        at("分销售模式（统计数据仅包含机械制造业务）", 62, 242, 360.0),
        at("直销", 62, 80, 345.0),
        at("1,634,724,265.57", 222, 285, 345.0),
    ]
    rows = cn_pdf.parse_segment_page(items, composition=True)
    assert [(r.axis, r.name, r.revenue) for r in rows] == [
        ("cninfo:营业收入构成", "营业收入合计", 2820493237.55),
        ("cninfo:分地区", "境内", 2805952645.98),
        ("cninfo:分地区", "境外", 14540591.57),
    ]


def test_a_radical_drawn_for_an_ideograph_is_read_as_the_ideograph() -> None:
    """9900002701 FY2023's fonts draw `营业收⼊构成` (U+2F0A for 入) and `⾏业` (U+2F8F for 行):
    neither the heading nor a label could be found. Only the radical blocks are mapped; full-width
    brackets and colons, which member names carry, are left as printed."""
    assert cn_pdf._ideographs("营业收⼊构成") == "营业收入构成"
    assert cn_pdf._ideographs("⾏业（含其他）：") == "行业（含其他）："
