"""China's segment note, read from the positions of its text (Phase 6c step 6).

The fixtures are the text items of three 2026 reports (FY2025), captured whole on the node from
`cn_pdf.page_items` over exactly the pages `cn_note.candidate_pages` picks:
  - ICBC (a bank): segments 公司金融/个人金融/资金/其他, revenue labelled 营业收入;
  - PetroChina: wrapped headers (油气和 / 新能源), figures in pieces, a gross 分部收入 row beside
    the external 对外交易收入;
  - Ping An (an insurer): a member that carries the eliminations (其他业务及合并抵销), and the unit
    printed only on the page before the table.
Re-capture: tests/fixtures/capture.md (cninfo_note).
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from muffin_ingest.facets import cn_note
from muffin_ingest.facets import segment_note as sn

from . import cninfo_note_rules as rules

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "cninfo_note"
FY = "2025-12-31"


Lines = list[list[cn_note.Item]]


def pages_of(name: str, drop: frozenset[int] = frozenset()) -> list[tuple[int, Lines]]:
    data = json.loads((FIXTURES / f"{name}.json").read_text())
    out = []
    for key in sorted(data["items"], key=int):
        if int(key) in drop:
            continue
        raw = [SimpleNamespace(s=s, x=x, y=y, end=e) for s, x, y, e in data["items"][key]]
        out.append((int(key), cn_note._lines(cn_note._items(raw))))
    return out


def split(result: cn_note.NoteResult, metric: str, period: str) -> dict[str, float]:
    """The placed split of one metric and period: member -> value in yuan millions."""
    return {
        f.member_code.removeprefix("cninfo:"): round(f.value / 1e6)
        for f in result.facts
        if f.metric_code == metric and f.period_ending == period and f.partition_id == 1
    }


def read(name: str, **kw: object) -> cn_note.NoteResult:
    return cn_note.read_page_lines(pages_of(name, **kw), FY, rules.vocabulary())  # type: ignore[arg-type]


# ── The three reports ──────────────────────────────────────────────────────────────────────────


def test_a_bank_s_note_is_read_as_its_revenue_split_for_both_years() -> None:
    result = read("icbc")
    assert split(result, "revenue", "2025-12-31") == {
        "公司金融业务": 410676, "个人金融业务": 327739, "资金业务": 93990, "其他": 5865,
    }  # fmt: skip
    assert split(result, "revenue", "2024-12-31") == {
        "公司金融业务": 393953, "个人金融业务": 324882, "资金业务": 95967, "其他": 7001,
    }  # fmt: skip
    revenue = [f for f in result.facts if f.metric_code == "revenue" and f.period_ending == FY]
    assert {f.reconciled_to for f in revenue} == {838_270e6}
    assert {f.axis for f in result.facts} == {cn_note.NOTE_AXIS}
    assert {f.currency for f in result.facts} == {"CNY"}
    assert {f.period_type for f in result.facts} == {"annual"}
    assert {f.period_start for f in revenue} == {"2025-01-01"}


def test_the_external_row_is_the_revenue_where_a_gross_row_stands_beside_it() -> None:
    """分部收入 (4,882,272) carries intersegment sales; 对外交易收入 (2,864,469) is the company's
    revenue. Anchoring both would let the gross row, read first, win the period."""
    result = read("petrochina")
    revenue = split(result, "revenue", FY)
    assert revenue == {
        "油气和新能源": 135029, "炼油化工和新材料": 326429, "销售": 1822223,
        "天然气销售": 578857, "总部及其他": 1931,
    }  # fmt: skip
    assert sum(revenue.values()) == 2_864_469
    assert split(result, "revenue", "2024-12-31")["销售"] == 1878462


def test_a_member_carrying_the_eliminations_is_part_of_the_split() -> None:
    """Ping An's 其他业务及合并抵销 is negative and needed: without it the members exceed the
    total."""
    result = read("pingan")
    revenue = split(result, "revenue", FY)
    assert revenue["其他业务及合并抵销"] == -37083
    assert sum(revenue.values()) == 1_050_506
    assert len(revenue) == 6
    assert split(result, "operating_income", FY)["寿险及健康险"] == 124295


def test_the_unit_the_page_before_states_carries_to_the_table() -> None:
    """Ping An prints `金额单位为人民币百万元` on even pages only; its first table's page states
    none."""
    first_table = 248  # 0-based: the page holding 2025's revenue rows
    assert split(read("pingan"), "revenue", FY)
    without_prior = read("pingan", drop=frozenset({first_table - 1}))
    assert not split(without_prior, "revenue", FY)
    assert without_prior.no_unit >= 1


def test_a_balance_sheet_table_is_skipped() -> None:
    """A table stating only a date (`2025 年 12 月 31 日`) holds assets; no duration fact comes of
    it, and assets are no metric this reader writes."""
    result = read("icbc")
    assert result.no_year >= 1
    assert {f.metric_code for f in result.facts} == {"revenue", "operating_income"}


# ── The pieces ─────────────────────────────────────────────────────────────────────────────────


def item(s: str, x: float, end: float, y: float = 100.0) -> cn_note.Item:
    return cn_note.Item(s, x, y, end)


def test_a_figure_in_pieces_is_joined_while_flush() -> None:
    joined = cn_note._joined(
        [item("2,35", 312, 329), item("2", 330, 335), item(",", 335, 337), item("746", 337, 352)]
    )
    assert [i.s for i in joined] == ["2,352,746"]
    bracketed = cn_note._joined(
        [item("(", 252, 256), item("3", 256, 260), item(",", 260, 262), item("353", 262, 275),
         item(")", 275, 278)]
    )  # fmt: skip
    assert [i.s for i in bracketed] == ["(3,353)"]
    apart = cn_note._joined([item("135,029", 206, 239), item("326,429", 263, 295)])
    assert [i.s for i in apart] == ["135,029", "326,429"]


def line(y: float, *cells: tuple[str, float]) -> list[cn_note.Item]:
    return [cn_note.Item(s, x, y, x + 10 * len(s)) for s, x in cells]


def synthetic(heading: str, year_line: str, segments: tuple[str, str, str]) -> Lines:
    a, b, c = segments
    return [
        line(780, ("金额单位为人民币百万元", 90)),
        line(740, (heading, 90)),
        line(700, (year_line, 300)),
        line(680, (a, 220), (b, 300), (c, 380), ("合计", 460)),
        line(660, ("营业收入", 90), ("60", 230), ("30", 310), ("10", 390), ("100", 465)),
    ]


def test_the_next_note_ends_the_segment_note() -> None:
    """A table under `61 关联方关系及其交易` is no segment table, whatever its rows say."""
    note = synthetic("60 分部报告", "2025年", ("甲业务", "乙业务", "丙业务"))
    after = synthetic("61 关联方关系及其交易", "2025年", ("甲公司", "乙公司", "丙公司"))
    result = cn_note.read_page_lines([(0, note), (1, after)], FY, rules.vocabulary())
    assert set(split(result, "revenue", FY)) == {"甲业务", "乙业务", "丙业务"}


def test_one_table_per_period_and_metric_the_last_read() -> None:
    """The same year stated twice under different names (an MD&A summary, then the note) must not
    be unioned into one split worth twice the company."""
    summary = synthetic("经营分部", "2025年", ("公司业务", "个人业务", "资金"))
    note = synthetic("五、分部信息", "2025年", ("公司金融业务", "个人金融业务", "资金业务"))
    result = cn_note.read_page_lines([(0, summary), (1, note)], FY, rules.vocabulary())
    assert split(result, "revenue", FY) == {"公司金融业务": 60, "个人金融业务": 30, "资金业务": 10}
    assert result.pages == [2]


def test_a_geography_table_is_not_read_as_business() -> None:
    page = synthetic("(2) 地区信息", "2025年", ("中国大陆", "香港", "其他地区"))
    result = cn_note.read_page_lines([(0, page)], FY, rules.vocabulary())
    assert result.facts == []


@pytest.mark.parametrize(
    ("year_line", "expected"),
    [
        ("2025年", ("2025-01-01", "2025-12-31")),
        ("(a) 2025年度及2025年12月31日分部信息", ("2025-01-01", "2025-12-31")),
        ("于2024年12月31日及截至该日止年度的分部分析如下:", ("2024-01-01", "2024-12-31")),
        ("中国银行集团2025年12月31日及2025年", ("2025-01-01", "2025-12-31")),
        ("2025年12月31日", None),
        ("分部资产", None),
    ],
)
def test_the_year_a_table_covers(year_line: str, expected: tuple[str, str] | None) -> None:
    assert cn_note._period([year_line], FY) == expected


@pytest.mark.parametrize(
    ("line_text", "scale"),
    [
        ("(除特别注明外,金额单位为人民币百万元)", 1e6),
        ("单位:人民币千元", 1e3),
        ("(除特别注明外,货币单位均以人民币百万元列示)", 1e6),
        ("金额单位:万元", 1e4),
    ],
)
def test_the_unit_a_page_states(line_text: str, scale: float) -> None:
    found = cn_note._UNIT.search(line_text)
    assert found is not None and cn_note._SCALE[found.group(2)] == scale


def test_a_caption_naming_currency_and_unit_states_the_unit() -> None:
    """China Merchants Bank prints `（人民币百万元，百分比除外）` over its tables and states
    the unit no other way before its segment note."""
    unit_only = [line(780, ("(人民币百万元,百分比除外)", 90))]
    note = synthetic("54. 经营分部", "2025年", ("甲业务", "乙业务", "丙业务"))[1:]
    result = cn_note.read_page_lines([(0, unit_only), (30, note)], FY, rules.vocabulary())
    assert split(result, "revenue", FY) == {"甲业务": 60, "乙业务": 30, "丙业务": 10}
    assert cn_note._UNIT_BARE.search("人民币百万元") is None, "a phrase in prose is no caption"


def test_a_total_column_and_an_eliminations_column_are_never_members() -> None:
    vocabulary = rules.vocabulary()
    assert sn.role_of("合计", vocabulary) == "total"
    assert sn.role_of("分部间抵销", vocabulary) in sn.STRUCTURAL
    assert sn.role_of("抵销", vocabulary) == "adjustment"
    assert sn.role_of("其他业务及合并抵销", vocabulary) is None
    assert sn.role_of("其中:对外交易收入", vocabulary) == "revenue"
    assert sn.role_of("减:分部间交易收入", vocabulary) == "skip"


def test_the_pages_either_side_of_a_hit_are_itemised() -> None:
    texts = {4: "经营分部 … 合计", 5: "合计", 9: "分部信息 合计"}
    pages = cn_note.candidate_pages(lambda i: texts.get(i, ""), 10)
    assert pages == [3, 4, 5, 8, 9]


def test_the_pages_after_the_note_s_heading_are_itemised() -> None:
    """Industrial Bank and Bank of Communications open the note on one page and print its tables
    on the pages after, with no running header naming it."""
    texts = {
        3: "八、分部报告\r\n本集团管理层通过审阅内部报告进行业绩评价",
        4: "甲业务 乙业务 合计",
        20: "本集团的经营分部如下，合计三个",  # a sentence naming the note is no heading
        40: "八、\uffa0分部信息",  # a heading set with a filler for its space
    }
    pages = cn_note.candidate_pages(lambda i: texts.get(i, ""), 50)
    assert pages == [3, 4, 5, 6, 19, 20, 21, 40, 41, 42, 43]


def test_the_heading_carries_only_to_the_next_page_and_the_unit_carries_on() -> None:
    """Across a gap the heading would carry from the accounting policies (`23 分部报告`) to an
    unrelated table hundreds of pages on. The unit is the notes' own: SF Holding states it once,
    sixty pages before its segment note."""
    opener = [line(780, ("金额单位为人民币百万元", 90)), line(740, ("23 分部报告", 90))]
    table = synthetic("", "2025年", ("甲业务", "乙业务", "丙业务"))[2:]
    vocabulary = rules.vocabulary()
    next_page = cn_note.read_page_lines([(0, opener), (1, table)], FY, vocabulary)
    assert set(split(next_page, "revenue", FY)) == {"甲业务", "乙业务", "丙业务"}
    far = cn_note.read_page_lines([(0, opener), (9, table)], FY, vocabulary)
    assert far.facts == []
    unit_only = [line(780, ("金额单位为人民币百万元", 90))]
    note = synthetic("七、分部信息", "2025年", ("甲业务", "乙业务", "丙业务"))[1:]
    later = cn_note.read_page_lines([(0, unit_only), (60, note)], FY, vocabulary)
    assert split(later, "revenue", FY) == {"甲业务": 60, "乙业务": 30, "丙业务": 10}


def test_a_row_label_s_numbering_is_not_part_of_the_label() -> None:
    """Bank of China and China Life label their rows `一、营业收入`, `三、营业利润`."""
    page = synthetic("八、分部报告", "2025年", ("甲业务", "乙业务", "丙业务"))
    page[-1] = line(660, ("一 、营业收入", 90), ("60", 230), ("30", 310), ("10", 390), ("100", 465))
    page.append(line(640, ("三、营业利润", 90), ("6", 230), ("3", 310), ("1", 390), ("10", 465)))
    result = cn_note.read_page_lines([(0, page)], FY, rules.vocabulary())
    assert split(result, "revenue", FY) == {"甲业务": 60, "乙业务": 30, "丙业务": 10}
    assert set(split(result, "operating_income", FY)) == {"甲业务", "乙业务", "丙业务"}


def test_a_table_of_places_is_geography_whatever_its_heading_says() -> None:
    """Bank of China prints its geographic segments under `八 分部报告`, the heading its business
    segments share. Read as business, 境内 and 中国香港澳门台湾 would be business lines."""
    vocabulary = rules.vocabulary()
    places = synthetic("八、分部报告", "2025年", ("境内", "中国香港澳门台湾", "其他国家和地区"))
    assert cn_note.read_page_lines([(0, places)], FY, vocabulary).facts == []
    regions = synthetic("九、分部报告", "2025年", ("长三角地区", "粤港澳大湾区", "附属机构"))
    assert cn_note.read_page_lines([(0, regions)], FY, vocabulary).facts == []
    one_place = synthetic("八、分部报告", "2025年", ("公司金融业务", "个人金融业务", "海外业务"))
    read_ = cn_note.read_page_lines([(0, one_place)], FY, vocabulary)
    assert set(split(read_, "revenue", FY)) == {"公司金融业务", "个人金融业务", "海外业务"}


def test_unallocated_is_a_member_when_it_carries_revenue() -> None:
    """SF Holding's 未分配部分 carries ¥1,458 m of external revenue; without it the split does not
    add up to the total the note states."""
    page = [
        line(780, ("金额单位为人民币千元", 90)),
        line(740, ("七、分部信息", 90)),
        line(700, ("2025年度", 300)),
        line(680, ("甲分部", 220), ("乙分部", 300), ("未分配部分", 380), ("分部间抵销", 460),
             ("合计", 540)),
        line(660, ("对外交易收入", 90), ("600", 230), ("300", 310), ("100", 390), ("\u2013", 470),
             ("1,000", 545)),
    ]  # fmt: skip
    result = cn_note.read_page_lines([(0, page)], FY, rules.vocabulary())
    assert set(split(result, "revenue", FY)) == {"甲分部", "乙分部", "未分配部分"}


def test_a_header_wrapped_above_and_below_its_total_line_names_whole_segments() -> None:
    """Qingdao Rural Commercial Bank sets `公司银行业` above the total line and the `务` that ends
    it below; `项目` heads the label column on the line above, where a caption would start."""
    page = [
        line(780, ("金额单位为人民币千元", 90)),
        line(760, ("八、分部报告", 90)),
        line(740, ("2025年度", 347)),
        line(720, ("项目", 136), ("公司银行业", 228), ("零售银行业", 286)),
        line(708, ("资金业务", 349), ("其他业务", 408), ("合计", 473)),
        line(697, ("务", 246), ("务", 304)),
        line(680, ("营业收入", 136), ("60", 239), ("30", 297), ("6", 355), ("4", 415),
             ("100", 470)),
    ]  # fmt: skip
    result = cn_note.read_page_lines([(0, page)], FY, rules.vocabulary())
    assert set(split(result, "revenue", FY)) == {
        "公司银行业务",
        "零售银行业务",
        "资金业务",
        "其他业务",
    }


def gross_page(members: tuple[str, str, str], cells: tuple[str, ...]) -> Lines:
    """Bank of China's shape: segments, an elimination column, the total."""
    a, b, c = members
    return [
        line(780, ("金额单位为人民币百万元", 90)),
        line(740, ("八、分部报告", 90)),
        line(700, ("2025年度", 300)),
        line(680, (a, 220), (b, 300), (c, 380), ("抵销", 460), ("合计", 540)),
        line(660, ("一、营业收入", 90), *zip(cells, (230, 310, 390, 470, 545), strict=True)),
    ]


def test_a_gross_row_reconciles_to_its_total_less_the_eliminations() -> None:
    """Bank of China's segments add up to 663,127 and its 抵销 column removes 4,817 of
    intersegment revenue from the 658,310 total. The split is the whole of the segments."""
    page = gross_page(("甲业务", "乙业务", "丙业务"), ("6,000", "3,970", "40", "(10)", "10,000"))
    result = cn_note.read_page_lines([(0, page)], FY, rules.vocabulary())
    assert split(result, "revenue", FY) == {"甲业务": 6000, "乙业务": 3970, "丙业务": 40}
    assert {f.reconciled_to for f in result.facts} == {10_010e6}


def test_a_split_must_add_up_to_the_note_s_own_rounding() -> None:
    """Without the elimination column, 甲 and 乙 alone come within 0.3% of the total: XBRL's 0.5%
    would place them as the split and drop 丙. A note stated to the million must add up to it."""
    page = gross_page(("甲业务", "乙业务", "丙业务"), ("6,000", "3,970", "40", "-", "10,000"))
    result = cn_note.read_page_lines([(0, page)], FY, rules.vocabulary())
    assert split(result, "revenue", FY) == {}


def test_a_year_under_each_segment_splits_its_column_and_the_newest_is_read() -> None:
    """China Merchants Bank states 2025 and 2024 under every segment. Read as one column each, a
    figure would land in whichever year's place is nearer; the balance-sheet table below, whose
    years stand over `12 月 31 日`, is no duration table. The older year comes first here, so
    only choosing the year by its value reads 2025."""

    def years(y: float) -> list[cn_note.Item]:
        cells: list[tuple[str, float]] = []
        for x in (212, 297, 382, 468):
            cells += [("2024", x), ("年", x + 17), ("2025", x + 43), ("年", x + 60)]
        return [cn_note.Item(s, x, y, x + (16 if s.isdigit() else 9)) for s, x in cells]

    def figures(y: float, label: str, values: tuple[int, ...]) -> list[cn_note.Item]:
        xs = (214, 256, 299, 341, 384, 426, 469, 511)
        return [cn_note.Item(label, 85, y, 140)] + [
            cn_note.Item(f"{v:,}", x, y, x + 26) for v, x in zip(values, xs, strict=True)
        ]

    names = (("批发金融业务", 220), ("零售金融业务", 305), ("其他业务", 397), ("合计", 489))
    head = [cn_note.Item(n, x, 0, x + 50) for n, x in names]
    page = [
        line(800, ("(除特别注明外,金额单位均为人民币百万元)", 52)),
        line(780, ("54.", 57), ("经营分部", 85)),
        line(760, ("(a)", 57), ("分部业绩、资产及负债", 85)),
        [cn_note.Item(i.s, i.x, 740, i.end) for i in head],
        years(725),
        figures(705, "营业收入", (135, 138, 196, 191, 5, 8, 336, 337)),
        [cn_note.Item(i.s, i.x, 680, i.end) for i in head],
        years(665),
        [cn_note.Item(s, x, 650, x + 30) for s, x in (("12月31日", 205), ("12月31日", 247))],
        figures(630, "营业收入", (7, 7, 3, 3, 1, 1, 11, 11)),
    ]
    result = cn_note.read_page_lines([(0, page)], FY, rules.vocabulary())
    assert split(result, "revenue", FY) == {"批发金融业务": 138, "零售金融业务": 191, "其他业务": 8}
    assert {f.period_ending for f in result.facts} == {FY}


@pytest.mark.parametrize(
    ("text", "is_heading"),
    [
        ("60分部报告", True),
        ("五、分部信息(续)", True),
        ("(1)经营分部", True),
        ("九、分部报告-续", True),
        ("1.经营分部(续)", True),
        ("本集团管理层监控各经营分部的经营成果", False),
        ("经营分部信息概要", False),
    ],
)
def test_the_note_heading(text: str, is_heading: bool) -> None:
    assert bool(cn_note._NOTE_HEADING.match(text)) is is_heading


def test_the_vocabulary_reaches_the_child_whole() -> None:
    from muffin_ingest.facets import cn_child

    vocabulary = rules.vocabulary()
    assert cn_child.note_from_json(cn_child.note_to_json(vocabulary)) == vocabulary


def test_no_rows_seeded_means_no_note_stage() -> None:
    from muffin_ingest.facets import cninfo

    assert cninfo.build_note_vocabulary([]) is None
    assert cninfo.build_note_vocabulary([("heading", "分部报告", "business")]) is None
    rows = [(rule, p, role) for rule, p, role, _ in rules.RULES]
    assert cninfo.build_note_vocabulary(rows) is not None


def test_only_external_revenue_is_anchored_where_a_gross_revenue_row_stands_above_it() -> None:
    """营业收入 here includes intersegment sales; the split is its external part. Both rows add up,
    so only the anchor decides which is the company's revenue."""
    page = [
        line(780, ("金额单位为人民币百万元", 90)),
        line(740, ("五、分部报告", 90)),
        line(700, ("2025年", 300)),
        line(680, ("甲业务", 220), ("乙业务", 300), ("丙业务", 380), ("合计", 460)),
        line(660, ("营业收入", 90), ("70", 230), ("40", 310), ("20", 390), ("130", 465)),
        line(640, ("其中:对外交易收入", 90), ("60", 230), ("30", 310), ("10", 390), ("100", 465)),
    ]
    result = cn_note.read_page_lines([(0, page)], FY, rules.vocabulary())
    assert split(result, "revenue", FY) == {"甲业务": 60, "乙业务": 30, "丙业务": 10}


def test_a_label_drawn_off_its_figures_baseline_is_still_their_row() -> None:
    """PetroChina draws a label 1.92 pt off its figures, and Ping An sets `营业收入合计` BELOW its
    figures. Read as two lines, figures first, the revenue row would have no label and the label
    would name the row after it."""
    page = synthetic("五、分部报告", "2025年", ("甲业务", "乙业务", "丙业务"))
    items = [
        cn_note.Item(i.s, i.x, i.y - (1.92 if i.s == "营业收入" else 0.0), i.end)
        for row in page
        for i in row
    ]
    # Through the banding, as a real page goes: the lines are what is under test.
    result = cn_note.read_page_lines([(0, cn_note._lines(items))], FY, rules.vocabulary())
    assert split(result, "revenue", FY) == {"甲业务": 60, "乙业务": 30, "丙业务": 10}
