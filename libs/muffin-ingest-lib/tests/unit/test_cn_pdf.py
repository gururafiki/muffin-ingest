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

import pytest

from muffin_ingest.facets import cn_child, cn_pdf
from muffin_ingest.facets.cn_pdf import CnSegmentRow, TextItem


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
    assert [(r.name, r.revenue, r.cost) for r in cn_pdf.parse_segment_page(NORD)] == [
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
    got = [(r.name, r.revenue) for r in cn_pdf.parse_segment_page(items)]
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
    for deduction in (3803338513.45, -3803338513.45):
        rows = cn_pdf.parse_segment_page(
            _table([*printed[:7], ("减：内部抵销数", deduction), printed[8]])
        )
        assert [r.name for r in rows][-1] == "内部抵销数"
        assert rows[-1].revenue == -3803338513.45
        assert abs(sum(r.revenue for r in rows) - 8419394281.72) < 0.01
        facts = cn_pdf.facts_from_rows(rows, "2025-12-31")
        assert {f.reconciled_to for f in facts} == {8419394281.72}
        assert all(f.partition_id == 1 for f in facts)


def test_an_of_which_row_is_a_level_below_the_row_above_it() -> None:
    """`其中：` rows are part of the row above them. Flat, the table summed them twice, the 合计
    that restates the real rows no longer matched (and was kept as a segment), and one table naming
    `其中：手续费及佣金收入` under two parents gave two rows one key."""
    rows = cn_pdf.parse_segment_page(
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
    assert [(r.name, r.parent) for r in rows] == [
        ("证券业务", None),
        ("其中：佣金", "证券业务"),
        ("期货业务", None),
        ("其中：佣金", "期货业务"),
    ]
    facts = {
        (f.parent_member, f.member_code): f
        for f in cn_pdf.facts_from_rows(rows, "2025-12-31")
        if f.metric_code == "revenue"
    }
    top = facts[(None, "cninfo:证券业务")]
    assert top.reconciled_to == 1000.0 and top.partition_id == 1
    child = facts[("cninfo:证券业务", "cninfo:其中：佣金")]
    assert child.parent_axis == child.axis == "cninfo:分行业"
    assert child.reconciled_to == 900.0 and child.partition_id == 0  # part of its parent, not all
    assert facts[("cninfo:期货业务", "cninfo:其中：佣金")].reconciled_to == 100.0


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
        ("其中：电力", ""),
        ("华东", None),
        ("华南", None),
        ("其中：沪", ""),
    ]
    facts = {f.member_code: f for f in cn_pdf.facts_from_rows(rows, "2025-12-31")}
    assert facts["cninfo:华东"].reconciled_to == 100.0 and facts["cninfo:华东"].partition_id == 1
    for orphan in ("cninfo:其中：电力", "cninfo:其中：沪"):
        assert facts[orphan].partition_id == 0 and facts[orphan].parent_member is None


def test_an_of_which_row_under_a_subtotal_kept_as_its_group_belongs_to_that_group() -> None:
    """中信建投's `境外小计` is the only row 境外 has (境内小计 before it ends 境内's rows), so
    it is kept as 境外, and the `其中：` under it is 境外's; a 合计 between ends the group."""
    rows = cn_pdf.parse_segment_page(
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
    assert [(r.name, r.parent) for r in rows] == [
        ("北京", None),
        ("上海", None),
        ("境外", None),
        ("其中：香港", "境外"),
        ("其中：澳门", ""),
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
        ("其中：美洲地区", ship),
        ("欧洲地区", ship),
        ("亚太地区", ship),
        ("中国大陆", ship),
        ("其他国际地区", ship),
        (port, None),
        ("其中：欧洲地区", port),
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
    rows = cn_pdf.parse_segment_page(
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
    assert _tree(rows) == [
        ("热电业务", None),
        ("其中：电力", "热电业务"),
        ("热力", "热电业务"),
        ("建材", "热电业务"),
        ("证券业务", None),
        ("经纪业务", "证券业务"),
        ("其中：佣金", "经纪业务"),
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
