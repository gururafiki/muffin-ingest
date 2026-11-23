"""China's segment split, read out of the annual report PDF (a port of the edge's `cn-pdf.ts`).

WHY A PDF PARSER EXISTS HERE AT ALL. CNINFO publishes only PDFs, but they are TEXT PDFs, and the
CSRC MANDATES `主营业务分行业/分产品/分地区情况` in every A-share annual report, so this reads a
standard form rather than scraping per company. China Yangtze Power's industry split reconciles to
99.7% of its consolidated revenue (the gap is 其他业务, incidental revenue); LONGi's five products
reconcile exactly to its industry total.

WHY POSITIONS AND NOT TEXT. Yangtze writes its first segment as `境内水电` on one visual line, the
money on the next, and `行业` on a third, so a line-based reading names the segment `行业`
("industry") beside the right number. Rows are rebuilt from item COORDINATES.

Everything after `segment_facts_from_pdf` is the XBRL path's: it produces `SegmentFact`s, so the
partitions, the reconciliation, the retraction and the serving layer behave exactly as for SEC.

THE TEXT ITEMS. The edge read pdf.js's `getTextContent` items. `page_items` rebuilds them from
pdfium's characters with pdf.js's own rules (through pypdfium2, a pdfplumber dependency already
installed), close enough that 91% of the edge's rows came back identical before the fixes below.

NOT A LINE-FOR-LINE PORT, by decision (2026-11-15). Golden parity over 716 reports (2026-11-16)
found 355 of the 516 the edge read carrying at least one defect: a 万元 table stored as yuan
(73 reports), total rows stored as segments (118), the sales-model table read as rows (131), a
money cell set on two lines kept as its last line (fragments in 47, names glued or cut in 119),
a margin read as money (20). Each is fixed here, the table is read across the page break (256 of
537 tables run onto the next page), and every report the edge read is read again by this reader,
so all China segments come from one. Every difference from the edge is classified, never assumed:
docs/specs/2026-11-09-segments-and-regulators.md (step 4) in the umbrella.
"""

from __future__ import annotations

import ctypes
import re
import unicodedata
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any

from muffin_ingest.facets.segments import SegmentFact, assign_partitions

#: A report is refused above this, unread. Ported as-is: the edge set it where a 9.15 MB report
#: fitted its 384 MB worker. The child process here has its own limit (`cn_child`); widening the
#: gate is measured separately, not assumed.
MAX_PDF_BYTES = 12 * 1024 * 1024

#: A money cell: thousands-separated with one or two decimals, as every one of these tables uses.
NUM = re.compile(r"^-?[\d,]+\.\d{1,2}$")
#: A MONEY CELL TOO WIDE FOR ITS COLUMN IS SET ON SEVERAL LINES, AND MAY BREAK ANYWHERE: before
#: the point (`315,802,261` / `.25`), after it (`59,507,780.` / `23`), between the decimals
#: (`34,281,472.0` / `9`) or inside a group (`6,745,344,4` / `86.37`). Measured 2026-11-15, the edge
#: kept only the last line, so 储能行业 was stored at 194.20 yuan for 2,660,336,194.20; 138 of the
#: 516 reports it read carried a fragment or a junk name. A cell is therefore the concatenation of
#: its column's fragments until they form a figure with two decimals, every one of these tables
#: stating yuan and fen: a one-decimal figure is a cell still being set (`619,840,004.7` was `.75`).
_COMPLETE = re.compile(r"^-?\d{1,3}(?:,\d{3})*\.\d{2}$")
#: What a cell can be while it is still being set: a prefix of a `_COMPLETE` figure.
_PREFIX = re.compile(r"^-?(?:\d{1,3}(?:,\d{3})*(?:,\d{0,2}|\.\d?)?)?$")
#: An item that can be part of a money cell at all.
_NUMERIC = re.compile(r"^-?[\d,.]+$")
#: A figure printed to one decimal: whole (`62,389.8`, a trailing zero dropped), or the first part
#: of a cell wrapped after its first decimal.
_ONE_DECIMAL = re.compile(r"^-?\d{1,3}(?:,\d{3})*\.\d$")
#: Money printed to the fen anywhere on a page (a comma group, so no percentage).
_MONEY_TO_THE_FEN = re.compile(r"^-?\d{1,3}(?:,\d{3})+\.\d{2}$")
_DIGITS = re.compile(r"^\d+$")
#: THE UNIT THE TABLE STATES. The edge assumed 元 throughout; measured 2026-11-15, 华微电子 states
#: its table in 万元, so its 75,846.29 is 758,462,900 yuan and was stored ten thousand times too
#: small, in the right currency, looking ordinary. The last statement above or inside a table holds;
#: a page that states none is read as 元, as the edge read every page.
#: `人民币` may be bracketed (深物业 000591 FY2010: `单位：（人民币）万元`); missing it read a
#: 万元 table as yuan, every figure 10,000 times too small.
_UNIT = re.compile(
    r"单位\s*[:：]\s*[（(]?\s*(?:人民币)?\s*[)）]?\s*(亿元|千万元|百万元|万元|千元|元)"
)
_SCALE = {
    "元": 1,
    "千元": 1_000,
    "万元": 10_000,
    "百万元": 1_000_000,
    "千万元": 10_000_000,
    "亿元": 100_000_000,
}

#: A total row restates the rows above it; kept, it would be summed as a segment beside its parts.
_TOTAL = re.compile(r"(合计|小计|总计)$")
#: A row that is part of the row above it (`其中：自产金`), and one deducted (`减：内部抵销数`).
#: A deduction's `减：` is where its name starts wherever it falls: 江苏凤凰 sets a cost-only
#: line, `合同履约成本`, in the cell above `减：合并抵消数`, and the name read
#: `合同履约成本减：合并抵消数`.
_OF_WHICH = re.compile(r"^其中\s*[:：]")
_LESS = re.compile(r"减\s*[:：]\s*")

#: The three mandated tables and the axis each becomes. The axis is the Chinese heading, prefixed,
#: so it cannot drift with a translation; it lands in `security_segment.axis`.
CN_TABLES: tuple[tuple[str, str], ...] = (
    ("主营业务分行业情况", "cninfo:分行业"),
    ("主营业务分产品情况", "cninfo:分产品"),
    ("主营业务分地区情况", "cninfo:分地区"),
)

NAME_HDR = ("分行业", "分产品", "分地区")
REV_HDR = "营业收入"
COST_HDR = "营业成本"

#: SHENZHEN'S FORM: ONE TABLE, `营业收入构成` ("revenue composition"). Measured 2026-11-16 in the
#: Dagster drain's first runs: 0 of 562 Shenzhen reports gave rows, against 61 of 163 Shanghai
#: ones, and 11 of 12 sampled Shenzhen reports carry this table instead of the mandated headings.
#: 1,105 of the 2,173 CNINFO filers are Shenzhen's. Its layout, read off the PDFs:
#:   `（1） 营业收入构成`, a unit line, `2025 年 | 2024 年`, then
#:   `金额 | 占营业收入比重 | 金额 | 占营业收入比重 | 同比增减`,
#:   `营业收入合计` (the company's total), then `分行业`, `分产品`, `分地区` and often
#:   `分销售模式`, each a row of its own over its members.
#: Revenue only (no cost column), and the total is CONSOLIDATED revenue, which each group states in
#: full. Read only when a report carries no mandated heading, so every report the mandated tables
#: are read from is read exactly as before.
COMPOSITION = "营业收入构成"
#: The heading's wordings, measured over 624 Shenzhen reports: `营业收入构成`, `营业总收入构成`
#: (东方财富) and `营业收入与构成` (汉钟精机 FY2025).
_COMPOSITION_WORDS = re.compile(r"营业总?收入与?构成")
#: The table's heading as a line of its own (`（1） 营业收入构成`), never a phrase in prose. The
#: numbering may be incomplete: 9900000621 sets its `（` a few points off the line, 9900003921 its
#: `1` (`（ ） 营业收入构成`).
_COMPOSITION_HEADING = re.compile(
    r"^\s*(?:[（(]?\s*[0-9一二三四五六七八九十]*\s*[)）]|[0-9一二三四五六七八九十]+\s*[、.\uff0e])?"
    r"\s*营业总?收入与?构成\s*[:：]?\s*$"
)
#: The rows before the table's first group: its `营业收入合计`, the company's revenue. Never a
#: fact; it is what every group is reconciled against.
_COMPOSITION_AXIS = "cninfo:营业收入构成"
#: The groups, each a row of its own with no figures. `分销售模式` (直销/经销 are channels, not
#: regions) ends the reading, as `主营业务分销售模式` ends the mandated tables. Measured over 44
#: Shenzhen reports: 分行业 55, 分产品 50, 分地区 33, 分销售模式 25, and no other group label.
GROUPS: dict[str, str | None] = {
    "分行业": "cninfo:分行业",
    "分产品": "cninfo:分产品",
    "分地区": "cninfo:分地区",
    "分销售模式": None,
}
#: A note after a group's label: 9900002724 prints `分销售模式（统计数据仅包含机械制造业务）`.
_GROUP_NOTE = re.compile(r"\s*[（(][^（()）]*[)）]\s*$")
#: The amount column's label. The share column's label starts where the amount column ends: set
#: whole (`占营业收入比重`) or over two lines (`占营业收入`/`比重`, `占营业收`/`入比重`,
#: `占营业`/`收入比重`).
#: A group label the reader does not keep: `分` and two to six CJK characters alone on a line
#: (汉威科技 prints `分行业应用`, a second product split, after `分产品`). Its rows are skipped
#: rather than read into the group before it. Two characters are not enough: `分销` is a member.
_GROUP = re.compile(r"^分[㐀-鿿]{2,6}$")
#: The rows of a group the reader does not keep.
_SKIPPED_AXIS = "cninfo:skipped"
#: A figure in whole units, which a table stated in thousands or more prints (美的's `456,451,731`
#: 千元): never in a table stated in yuan, where every figure carries its fen.
_WHOLE = re.compile(r"^-?\d{1,3}(?:,\d{3})+$")
AMOUNT_HDR = "金额"
SHARE_HDR = "占营业"
#: What ends the table besides `_TABLE_ENDS`: the next section (`占公司营业收入或营业利润10%
#: 以上的…`), the industry-disclosure notice before the quarterly table (`公司需遵守《深圳证券
#: 交易所…》`), and a new unit line, which opens another table.
_COMPOSITION_ENDS = re.compile(r"占公司营业收入|公司需遵守|单位\s*[:：]")

#: WHAT ENDS A TABLE. The edge kept reading until the next mandated heading, so the explanation
#: under the tables (`…情况的说明`), the next numbered section (`（2）产销量情况分析表`) and
#: the unparsed sales-model table (`主营业务分销售模式情况`, whose 直销/经销 are not regions)
#: were read as rows of the last table: a paragraph became a member name, and a production volume
#: its revenue.
_TABLE_ENDS = re.compile(r"说明|产销量|成本分析|主营业务分销售模式")
#: A numbered line: the next section's heading, or a row the filer numbers (广州发展 FY2025 lists
#: `(1)燃煤发电及供热业务`, `(2)天然气发电及供热业务`, `(3)能源物流业务`). Only a heading ends the
#: table, and the difference is the cell: a row's holds its revenue (`_is_a_row`).
_NUMBERED = re.compile(r"^\s*[（(]\s*[0-9一二三四五六七八九十]+\s*[)）]\s*")
#: The tallest cell a numbered row can have: four wrapped name lines at 11 pt, with margins.
_CELL_HEIGHT = 60.0

#: THE OLDER FORMS, FY2007-2014, read only when the current forms give nothing (`older`). Measured
#: 2026-11-23 on 55 reports from CNINFO's second search page (2003-2019): the current reader read
#: none of them. The common layouts, read off the PDFs:
#:   - ONE TABLE FOR INDUSTRY AND PRODUCT, headed `主营业务分行业、产品情况` (also `、分产品`,
#:     `及分产品`): its name column is `分行业或分产品`, set over two or three lines
#:     (`分 行 业 或` / `分产品`, `分行业或分` / `产品`), and `分行业` and `分产品` are rows of
#:     their own above each group
#:     (大同煤业 601001 FY2010, 16 of 31 FY2010-2014 reports);
#:   - one table per axis, headed `主营业务分行业情况`, `主营业务收入分行业情况` or
#:     `分行业经营情况（单位：人民币万元）` (永新股份 002014 FY2010);
#:   - the revenue columns labelled `主营业务收入` and `主营业务成本` as often as `营业收入` and
#:     `营业成本` (太钢不锈 000825 FY2010);
#:   - the region table with a revenue column and no cost (`地区 | 营业收入 | 营业收入比上年增减`).
#: Many of these tables list only the lines at or above 10% of revenue (601001: `注：分别按照行业、
#: 产品列示占营业收入或营业利润10%以上的主要行业和产品`), so a table here is often a part.
#: FY2006 and earlier print the split in too many wordings to read (`占主营业务收入10%以上的主要
#: 产品`, `按产品或业务类别列示…`, `主营业务收入、利润构成情况表`): not read.
_OLDER_HEADING = re.compile(
    r"^\s*(?:[（(]?\s*[0-9一二三四五六七八九十]*\s*[)）]|[0-9一二三四五六七八九十]+\s*[、.\uff0e]"
    r"|[①②③④⑤⑥⑦⑧⑨⑩])?\s*(?:报告期内?)?\s*(?:公司)?\s*(?:主营业务(?:收入)?)?\s*"
    r"(?P<what>分行业[、及和与]\s*分?产品(?:[、及和与][^：:（(]{0,10})?|分(?:行业|产品|地区)"
    r"|按(?:主要)?(?:行业|产品|地区)(?:列示)?的?)"
    r"(?:经营)?情况\s*表?\s*[:：]?\s*"
    r"(?:[（(][^（）()]*单位[^（）()]*[)）]|单位\s*[:：]\s*\S{1,8})?\s*[:：]?\s*$"
)
#: A memo line under a row, never a member (永新股份 002014 FY2010: `其中：关联交易 0`).
_RELATED_MEMO = re.compile(r"^其中\s*[:：]?\s*关联交易")
#: The revenue and cost labels the older forms print beside `营业收入` and `营业成本`.
OLDER_REV_HDR = ("营业收入", "主营业务收入")
OLDER_COST_HDR = ("营业成本", "主营业务成本")
#: The name column's label, whole or in the pieces a header set over lines prints it in.
_OLDER_NAME_HDR = frozenset(
    (
        "分行业",
        "分产品",
        "分地区",
        "地区",
        "行业",
        "产品",
        "分行业或分产品",
        "分行业或",
        "分行业或分",
        "或分产品",
        "分产品或分行业",
    )
)
#: A table with no cost column: only the region table prints one.
_REVENUE_ONLY_NAME_HDR = frozenset(("地区", "分地区"))


def _older_axis(joined: str) -> str | None:
    """The axis an older form's heading line opens, or None. The combined table opens on industry;
    its `分产品` row moves it."""
    m = _OLDER_HEADING.match(joined)
    if m is None:
        return None
    what = m.group("what")
    if "行业" in what:
        return "cninfo:分行业"
    return "cninfo:分产品" if "产品" in what else "cninfo:分地区"


def _older_header(
    lines: Sequence[Sequence[TextItem]], k: int, heights: Sequence[float]
) -> tuple[float, float, float, float, float] | None:
    """An older form's header on line `k`, as (name_x, name_end, rev_end, cost_end, name_hi), or
    None.

    The line holds a revenue label (`OLDER_REV_HDR`), and a cost label right of it unless the name
    column is the region's; the name label is anywhere in the header block left of the revenue
    label, since 大同煤业 sets `分 行 业 或` 6 pt above the labels and `分产品` 6 pt below them. A
    revenue-only table's revenue column ends where the next label starts."""
    line = lines[k]
    rev_h = next((i for i in line if any(_labelled(i, h) for h in OLDER_REV_HDR)), None)
    if rev_h is None:
        return None
    cost_h = next(
        (i for i in line if i.x > rev_h.x and any(_labelled(i, h) for h in OLDER_COST_HDR)), None
    )
    near = [
        n
        for n in range(max(0, k - 3), min(len(lines), k + 4))
        if abs(heights[n] - heights[k]) <= 15
    ]
    block = [i for n in near for i in lines[n]]
    # Each header line's text left of the revenue label, joined: 永新股份 sets `地` and `区` as two
    # items 10 pt apart. And all of them joined top to bottom: 凤凰光学 (600071) sets
    # `分行业或分产` 6 pt above the labels and `品` 6 pt below them.
    labels: list[tuple[str, float]] = []
    pieces: list[TextItem] = []
    for n in near:
        left = [i for i in lines[n] if _end(i) <= rev_h.x + 1 and i.s.strip()]
        pieces += left
        text = re.sub(r"\s", "", "".join(i.s for i in left))
        if text in _OLDER_NAME_HDR:
            labels.append((text, (left[0].x + _end(left[-1])) / 2))
    whole = re.sub(r"\s", "", "".join(i.s for i in pieces))
    if not labels and pieces and whole in _OLDER_NAME_HDR:
        first = min(pieces, key=lambda i: i.x)
        labels.append((whole, (first.x + _end(first)) / 2))
    if not labels:
        return None
    name_x = min(labels, key=lambda t: t[1])[1]
    if cost_h is None:
        if not any(text in _REVENUE_ONLY_NAME_HDR for text, _ in labels):
            return None
        beyond = [
            i.x
            for i in block
            if i.x > rev_h.x + 1 and _CJK.search(i.s) and not _NUMERIC.match(i.s.strip())
        ]
        if not beyond:
            return None
        return name_x, rev_h.x, min(beyond), min(beyond), _end(rev_h) + _GUTTER
    beyond = [i.x for i in block if i.x > cost_h.x and not _NUMERIC.match(i.s.strip())]
    cost_end = min(beyond) if beyond else cost_h.x + (cost_h.x - rev_h.x)
    return name_x, rev_h.x, cost_h.x, cost_end, _end(cost_h) + _GUTTER


#: How far the page scan goes. The mandated table is in 管理层讨论与分析; measured on page 13, 23
#: and (Canadian Solar's 327-page report) 68. A bound is still right: its cost falls on every report
#: with no table.
SCAN_PAGES = 90

_CJK = re.compile(r"[㐀-鿿豈-﫿]")
#: The year before `年度报告`, which older covers letter-space (`2010 年 年 度 报 告`, `2010年 度 报
#: 告`) or write in Chinese numerals (`二○一○年度报告`); read as `2009`, a later page's mention of
#: last year's report, all three FY2010 reports would have filed their splits a year early.
_YEAR = re.compile(r"(\d{4,})\s*年?\s*年\s*度\s*报\s*告")
_CN_YEAR = re.compile(r"([〇○零一二三四五六七八九]{4})\s*年?\s*年\s*度\s*报\s*告")
_CN_DIGIT = {c: str(n) for n, c in enumerate("〇一二三四五六七八九")} | {"○": "0", "零": "0"}


@dataclass(frozen=True, slots=True)
class TextItem:
    """One run of text at its baseline origin, in PDF user space (y grows upward)."""

    s: str
    x: float
    y: float
    #: Where the run's last glyph ends, when known: how far along it a split falls.
    end: float | None = None


@dataclass(frozen=True, slots=True)
class CnSegmentRow:
    """One row of a mandated table, before it becomes facts."""

    axis: str
    name: str
    revenue: float
    cost: float | None
    #: The row an `其中：` ("of which") row is part of; None for a row of the table's own level,
    #: and "" for one whose whole is not a row of the table (it follows a dropped subtotal, or
    #: heads the table).
    parent: str | None = None
    #: A grand total the table states (`合计`, `总计`): never a fact, it is what a split is
    #: reconciled against.
    stated_total: bool = False


@dataclass(frozen=True, slots=True)
class CnParseResult:
    facts: list[SegmentFact]
    #: The 1-based page the table was found on, or None: a miss told apart from a bad parse.
    page: int | None
    period_ending: str | None


def tidy_member_name(raw: str) -> str:
    """A member name with the typesetting taken out: justified CJK is set with space between the
    characters (`光 伏 组 件` for 光伏组件), and the name is the upsert key. Only a gap BETWEEN TWO
    CJK characters goes; one beside a Latin letter or a digit may be real (`A 股`)."""
    out = []
    for i, ch in enumerate(raw):
        prev = raw[i - 1] if i > 0 else ""
        nxt = raw[i + 1] if i + 1 < len(raw) else ""
        if ch.isspace() and _CJK.match(prev) and _CJK.match(nxt):
            continue
        out.append(ch)
    return "".join(out).strip()


def _number(text: str) -> float:
    return float(text.replace(",", ""))


#: Two complete money cells in one item: the first ends at its two decimals, the next starts there.
_TWO_CELLS = re.compile(r"^(-?[\d,]+\.\d{2})(\s*)(-?\d[\d,]*\.\d{1,2})$")
#: Two cells a space apart, where the space marks the split and the first may be one decimal into
#: being set: 彤程's `2,320,408,095.5 1,751,224,956.7` ends page 22, both cells completed on 23.
_SPACED_CELLS = re.compile(r"^(-?[\d,]+\.\d{1,2})(\s+)(-?\d[\d,]*\.\d{1,2})$")
#: A name and the money cell after it in one item: 汾酒's `食品制造业 38,592,734,473.12`.
_NAME_THEN_CELL = re.compile(r"^(.*\D)(\s+)(-?\d{1,3}(?:,\d{3})+\.\d{1,2})$")


def _split_cells(item: TextItem) -> list[TextItem]:
    """An item holding two cells, as two items. Cells set close to each other merge into one run,
    flush (中免's `52,552,697,083.9135,779,710,755.06`, revenue then cost) or a space apart
    (江苏农垦's `3,581,881,091.36 2,832,712,714.37`, and a name filling its cell before the
    revenue, 汾酒's `食品制造业 38,592,734,473.12`), where pdf.js kept them apart for a reason
    pdfium does not expose. The split point is the run's span shared out by character width."""
    text = item.s.strip()
    if item.end is None or item.end <= item.x:
        return [item]
    match = _TWO_CELLS.match(text) or _SPACED_CELLS.match(text) or _NAME_THEN_CELL.match(text)
    if match is None:
        return [item]
    first, gap, second = match.groups()
    # A CJK character is set a full em, a digit or a separator half of one: shared out by those
    # weights, 汾酒's name ends at x 101 in a run from 49.4 to 194.7, inside its column.
    units = [_width(ch) for ch in text]
    per = (item.end - item.x) / sum(units)
    ends = item.x + per * sum(units[: len(first)])
    starts = item.x + per * sum(units[: len(first) + len(gap)])
    return [
        TextItem(first, item.x, item.y, ends),
        TextItem(second, starts, item.y, item.end),
    ]


def _width(ch: str) -> float:
    """A character's set width in half-ems: CJK and full-width forms take a whole em."""
    return 2.0 if _CJK.match(ch) or "\uff00" <= ch <= "\uffef" else 1.0


class _Column:
    """One money column's cell as it is set, line by line."""

    def __init__(self) -> None:
        self.text = ""

    def take(self, fragments: Sequence[str]) -> float | None:
        """The figure once the fragments complete one, else None while it is still being set."""
        for fragment in fragments:
            if fragment in ("-", "—", "－"):
                continue  # an empty cell
            joined = self.text + fragment
            if _COMPLETE.match(joined):
                self.text = ""
                return _number(joined)
            if _PREFIX.match(joined):
                self.text = joined
            elif _COMPLETE.match(fragment):
                self.text = ""
                return _number(fragment)
            else:
                # Not a continuation: whatever was being set was not a cell. Start again here.
                self.text = fragment if _PREFIX.match(fragment) else ""
        return None

    @property
    def open(self) -> bool:
        return bool(self.text)


@dataclass(frozen=True, slots=True)
class Rule:
    """A horizontal line drawn on the page: a table's row border, from `x0` to `x1` at `y`."""

    y: float
    x0: float
    x1: float


@dataclass
class _Row:
    axis: str
    revenue: float
    cost: float | None
    #: The lines its figures span, top and bottom (y grows upward).
    top: float
    bottom: float
    names: list[tuple[float, str]]


def _distance(row: _Row, y: float) -> float:
    if row.bottom <= y <= row.top:
        return 0.0
    return min(abs(y - row.top), abs(y - row.bottom))


#: A page number in the footer (`- 10 -`, `17 / 291`): never part of a name.
_PAGE_NUMBER = re.compile(r"^[-\s\d/]+$")

#: A name line further than this from every row's figures belongs to none of them.
_NAME_REACH = 24.0

#: Past the cost label, where a table's own columns go on and another page set two-up beside it
#: starts: 上海家化 FY2017's region table has its cost label end at 248.0 and the cost table beside
#: it starts at 657.6.
_GUTTER = 100.0
_NO_EDGE = float("inf")


def parse_segment_page(
    items: Sequence[TextItem],
    rules: Sequence[Rule] = (),
    scale: int = 1,
    composition: bool = False,
    older: bool = False,
) -> list[CnSegmentRow]:
    """Rebuild the mandated tables from one page's positioned text, or with `composition` the
    Shenzhen `营业收入构成` table (`COMPOSITION`), read with the same machinery: its amount column
    is the revenue column, it has no cost column, and its group rows (`GROUPS`) open each table.
    Without `composition` nothing of that reading runs, so the mandated tables read as before.

    With `older`, the FY2007-2014 forms (`_OLDER_HEADING`): a heading is a line of its own and
    keeps the columns of the header before it (太钢不锈's product table shares its industry
    table's header); a header is found across its block, with the older labels; the region table
    may have no cost column; `分行业`/`分产品` rows move the combined table's axis; and a row of
    labels that is no header (the share table after it) ends the table. Without `older` none of
    it runs.

    AN ITEM IS PLACED IN A COLUMN BY WHERE IT ENDS, between where two header labels START. A money
    cell is right-aligned, so where it starts depends on its length (中免's 17-digit revenue starts
    left of the name column's centre) while where it ends is inside its column, and so is every
    line of a wrapped cell (昭衍's cost tail `36` sits at the cell's right edge). A centred label
    starts right of its column's left border. The cost column ends where the next label starts, or a
    column further on, so the margin beside it is never read as cost.

    A ROW IS ITS REVENUE CELL. It completes when the column's fragments make a figure; a cost
    completed before it is the row's, and one completed after it, within 16 pt, completes the row
    just emitted.

    A NAME LINE BELONGS TO THE ROW WHOSE CELL HOLDS IT, decided once every row on the page is known.
    The cells are the bands between the row borders the table draws (`rules`): a wrapped name is
    centred on its figures in one table (昭衍's `实验模型` is 14 pt below 临床服务及其他 and 7 pt
    above its own figures) and set from the top in another (a name's second line exactly as far
    from its own figures as from the next row's), and no rule on the text alone reads both. Where
    the page draws no borders across the name column, a name line goes to the row whose figures it
    sits nearest, the upper on a tie, which is how a top-aligned table reads.

    A TABLE ENDS at its explanation, at the next numbered section and at the sales-model table
    (`_TABLE_ENDS`), and a total row (`合计`, `小计`) is not a segment.
    """
    buckets: dict[int, list[TextItem]] = {}
    for item in (part for whole in items for part in _split_cells(whole)):
        # ~3 pt buckets: items on one printed line rarely differ by more than a couple, and
        # consecutive lines are ~11 apart. JS `Math.round` rounds .5 up, Python's to even.
        buckets.setdefault(_js_round(item.y / 3), []).append(item)
    lines = [
        sorted(v, key=lambda i: i.x) for _, v in sorted(buckets.items(), key=lambda kv: -kv[0])
    ]
    if composition:
        lines = [_joined(line) for line in lines]

    tables: list[tuple[list[_Row], list[tuple[float, str]], float, int]] = []
    name_x = 0.0
    rows: list[_Row] = []
    names: list[tuple[float, str]] = []
    axis = ""
    name_end = rev_end = cost_end = 0.0
    rev, cost = _Column(), _Column()
    cost_held: float | None = None
    top: float | None = None
    #: The row just emitted is still owed its cost (its cell is being set).
    owed = False
    #: The unit in force when the open table's heading was read.
    opened_at = scale
    #: The axes whose table header has been read: each is read once.
    read: set[str] = set()
    #: Reading the composition table (from its heading until it ends), and whether its header has
    #: been read (a second heading after it is another year's table).
    composing = composed = False
    #: The next revenue completed is negative: its sign was set on a line of its own.
    negate = False
    #: The table's edges, once its header is read: nothing ending left of `name_lo` or starting
    #: right of `name_hi` is the table's (`_GUTTER`).
    name_lo, name_hi = 0.0, _NO_EDGE
    #: Where the older form's header line was: the header's other lines are no rows.
    header_y: float | None = None

    def close() -> None:
        nonlocal rows, names, rev, cost, cost_held, top, owed, negate
        if rows or names:
            tables.append((rows, names, name_x, opened_at))
        rows, names = [], []
        rev, cost = _Column(), _Column()
        cost_held, top, owed, negate = None, None, False, False

    heights = [sum(i.y for i in line) / len(line) for line in lines]
    to_the_fen = any(_MONEY_TO_THE_FEN.match(i.s.strip()) for i in items)

    def column(n: int, lo: float, hi: float) -> list[str]:
        return [i.s.strip() for i in lines[n] if lo < _end(i) <= hi and _NUMERIC.match(i.s.strip())]

    def settle(fragments: list[str], k: int, lo: float, hi: float, pending: str = "") -> list[str]:
        """A one-decimal figure, given its second decimal when it is whole: the next fragment in
        its column is not bare digits continuing it (云天励飞 prints `62,389.8` beside
        `53,608.12`), or nothing follows on a page printing no money to the fen (中海油服's
        百万元 table, `14,898.0`). At the foot of a page that does, it stays incomplete: 彤程's
        `2,320,408,095.5` ends page 22 and its `5` opens page 23.

        `pending` is what the column holds of a cell still being set: a whole figure can wrap, and
        中国中铁 (601390) FY2022 sets 境内's `1,095,921,752` as `1,095,921,7` over `52`."""
        out = []
        for n_fragment, fragment in enumerate(fragments):
            so_far = (pending if n_fragment == 0 else "") + fragment
            if _ONE_DECIMAL.match(fragment):
                following = next(
                    (c for n in range(k + 1, len(lines)) if (c := column(n, lo, hi))), []
                )
                if following and _DIGITS.match(following[0]):
                    out.append(fragment)
                elif following or not to_the_fen:
                    out.append(fragment + "0")
                else:
                    out.append(fragment)
            elif (scale > 1 or (older and not to_the_fen)) and (
                _WHOLE.match(so_far) or _WHOLE.match(fragment)
            ):
                # A table in 千元 or larger may print whole numbers, the composition table and the
                # mandated one alike (中国中铁 (601390) FY2022 and FY2023, `1,087,585,457` 千元).
                # Whole unless the next fragment in its column continues it.
                following = next(
                    (c for n in range(k + 1, len(lines)) if (c := column(n, lo, hi))), []
                )
                if following and (_DIGITS.match(following[0]) or following[0][:1] in ",."):
                    out.append(fragment)
                else:
                    out.append(fragment + ".00")
            else:
                out.append(fragment)
        return out

    def is_a_row(k: int) -> bool:
        """A numbered line is a row when its cell holds a revenue figure: on the line itself
        (`(3)能源物流业务`), or between the borders around it across the name column (广州发展's
        `(1)燃煤发电及` is 6.8 pt above its figures, in a cell 27.8 pt tall). A heading sits between
        two tables, where no figure is, or in no cell a row could fill."""
        if column(k, name_end, rev_end):
            return True
        borders = [r.y for r in rules if r.x0 <= name_x <= r.x1]
        above = [b for b in borders if b > heights[k]]
        below = [b for b in borders if b < heights[k]]
        if not above or not below:
            return False
        lo, hi = max(below), min(above)
        return hi - lo <= _CELL_HEIGHT and any(
            lo < heights[n] < hi and column(n, name_end, rev_end) for n in range(len(lines))
        )

    for k, line in enumerate(lines):
        joined = "".join(i.s for i in line)
        y = heights[k]
        unit = _UNIT.search(joined)
        if unit is not None:
            scale = _SCALE[unit.group(1)]
        heading = next((h for h, _ in CN_TABLES if h in joined), None)
        table = None if heading is None else dict(CN_TABLES)[heading]
        if older:
            # A heading line of its own, never a phrase in prose.
            table = _older_axis(joined)
            heading = joined.strip() if table is not None else None
        if table is not None and table in read and joined.strip().endswith(heading or ""):
            # The heading of a table already read is another year's: 厦门钨业 restates 2024 on
            # the page after its 2025 tables (`2024 年（调整后）`, `2024 年（调整前）`), and
            # nothing after it is this year's. A table counts once its header is read, so a
            # section title naming it first (`(1). 主营业务分行业情况`) does not; and only a line
            # ending with the heading is one (`主营业务分行业情况的说明` is not).
            close()
            break
        if table is not None:
            close()
            axis = table
            opened_at = scale
            if not older:
                name_end = rev_end = cost_end = name_lo = 0.0
                name_hi = _NO_EDGE
            continue
        if composition and _COMPOSITION_HEADING.match(joined):
            close()
            if composed:
                break  # another year's table, as for a mandated heading read twice
            axis, composing = _COMPOSITION_AXIS, True
            opened_at = scale
            name_end = rev_end = cost_end = name_x = name_lo = 0.0
            name_hi = _NO_EDGE
            continue
        if not axis:
            continue
        if name_lo > 0 or name_hi < _NO_EDGE:
            # A TWO-UP PAGE PRINTS ANOTHER PAGE BESIDE THE TABLE: 上海家化 (600315) FY2017 sets its
            # cash-flow table and the notes under it at x 56-537, left of a segment table whose
            # names start at 659, and on the next page its region table at x 58-532 beside a cost
            # table at 657-1133. Read with the table, the other page's text joined its member
            # names, and a `说明` there ended the product table before its first row and the
            # region table after its second.
            line = [i for i in line if _end(i) > name_lo and i.x < name_hi]
            if not line:
                continue
            joined = "".join(i.s for i in line)
        if older:
            found = _older_header(lines, k, heights)
            if found is not None:
                close()
                read.add(axis)
                name_x, name_end, rev_end, cost_end, name_hi = found
                name_lo = max(0.0, name_x - (name_end - name_x))
                header_y = y
                continue
            if (
                header_y is not None
                and abs(y - header_y) <= 15
                and not column(k, name_end, rev_end)
                and re.sub(r"\s", "", "".join(i.s for i in line if _end(i) <= name_end))
                in _OLDER_NAME_HDR | {""}
            ):
                # The header's other lines (`分产品`, `润率(%)`), which hold no figure and no name.
                # Not a row set close under a one-line header: 大同煤业's region table puts `国内`
                # 13.6 pt below it.
                continue
            if rows and any(_labelled(i, h) for i in line for h in OLDER_REV_HDR):
                # A row of labels that is no header, after this table's rows: the next table
                # (太钢不锈's shares by industry and product, `主营业务收入 | 主营业务利润`). Before
                # any row it is the header's upper line: 永新股份 labels its growth columns
                # `营业收入` / `比上年增`, 15.6 pt above the line holding `分产品`.
                close()
                axis = ""
                continue
            if _RELATED_MEMO.match(joined.strip()):
                continue  # `其中：关联交易 0`, a memo under a row (永新股份 002014): no member
            group_row = _GROUP_NOTE.sub("", _label(tidy_member_name(joined)))
            if rev_end and group_row in GROUPS and not column(k, name_end, cost_end):
                # The combined table's `分行业` and `分产品` rows (大同煤业 601001 FY2010).
                close()
                axis = GROUPS[group_row] or ""
                continue
        if rev_end and (
            _TABLE_ENDS.search(joined)
            or (composing and _COMPOSITION_ENDS.search(joined))
            or (_NUMBERED.match(joined) and not is_a_row(k))
            # PROSE ENDS A TABLE: text set from the name column across to the next label (the cost
            # label, or the composition table's share label) is no name, which stays in its cell.
            # 中国中铁 (601390) follows its regions with a note and its review of each line, whose
            # figures (`5,928.32` 亿元) fell in the money columns; 华大基因 (300676) FY2020 and
            # 翰宇药业 (300199) follow their composition tables with notes. What starts right of
            # the name column is not prose: the last column's own text (`增加 0.44 个百分点`), a
            # page footer (`第 15 页 / 共 202 页`), and a column header repeated where the table
            # runs onto the next page (粤高速, its `金额 占营业收入比重` between two regions).
            or any(_CJK.search(i.s) and i.x < name_end and _end(i) > rev_end for i in line)
        ):
            close()
            axis = ""
            composing = False
            continue

        if composing:
            if not rev_end:
                # The amount column's label, and the share column's beside it, which a narrow
                # table sets over two lines (`占营业收入` 6 pt above `金额`, `比重` 6 pt below).
                # This year's columns come first, so the FIRST amount label is this year's, set as
                # one item or letter-spaced over several (`_label_starts`).
                if not _label_starts(line, AMOUNT_HDR):
                    continue
                header_lines = [
                    n
                    for n in range(max(0, k - 3), min(len(lines), k + 4))
                    if abs(heights[n] - y) <= 15
                ]
                # THE LEFTMOST AMOUNT LABEL IN THE HEADER BLOCK, not on this line: 舒泰神 (300204)
                # FY2023 sets this year's `金额` 3.3 pt below the line holding last year's, so the
                # line met first named only last year's column (measured 2026-11-22).
                amount_x = min(x for n in header_lines for x in _label_starts(lines[n], AMOUNT_HDR))
                share = [
                    x
                    for n in header_lines
                    for x in _label_starts(lines[n], SHARE_HDR, prefix=True)
                    if x > amount_x
                ]
                if share:
                    close()
                    composed = True
                    name_end, rev_end = amount_x, min(share)
                    cost_end = rev_end  # no cost column
                continue
            # A group row is the label alone on its line, wherever it is set: in the name column,
            # or centred across the table (9900003433's `分行业` at x 283).
            label = _GROUP_NOTE.sub("", _label(tidy_member_name("".join(i.s for i in line))))
            if label in GROUPS:
                close()
                group = GROUPS[label]
                axis = group or ""
                composing = group is not None
                continue
            if _GROUP.match(label):
                close()
                axis = _SKIPPED_AXIS
                continue
            if not name_x:
                first = next((i for i in line if _end(i) <= name_end and i.s.strip()), None)
                if first is not None:
                    name_x = (first.x + _end(first)) / 2
            # A NEGATIVE CELL SET ON SEVERAL LINES CAN CARRY ITS SIGN ON A LINE OF ITS OWN:
            # 9900004624's `合并抵消` is `-` / `6,587,672,187.0` / `4`, the minus at the cell's
            # right edge on a line holding nothing else of the row (its share, -10.04%, is on the
            # figure's line). An empty cell's dash is on its row's line, beside the name: that row
            # has no revenue this year, and its name is no other row's.
            in_amount = [i for i in line if name_end < _end(i) <= rev_end]
            if len(in_amount) == 1 and in_amount[0].s.strip() in _MINUS and not rev.open:
                if any(_end(i) <= name_end for i in line):
                    continue
                negate = True

        name_h = None if composing else next((i for i in line if i.s.strip() in NAME_HDR), None)
        rev_h = next((i for i in line if _labelled(i, REV_HDR)), None)
        cost_h = next((i for i in line if _labelled(i, COST_HDR)), None)
        if name_h is not None and rev_h is not None and cost_h is not None:
            close()
            read.add(axis)
            name_end, rev_end = rev_h.x, cost_h.x
            name_x = (name_h.x + _end(name_h)) / 2
            # The name column is no wider to the left of its label's centre than to the right of
            # it, up to where the revenue label starts: a label set at the column's left edge
            # bounds nothing, and one centred in it bounds the column at its own left edge.
            name_lo = max(0.0, name_x - (name_end - name_x))
            # The header is often set on several lines, labels centred in their cells: 嘉泽's
            # `毛利率` sits 6.5 pt above the line holding the others, so the next label is looked
            # for across the header block, or the cost column would reach a column too far.
            block = [
                i
                for n in range(max(0, k - 3), min(len(lines), k + 4))
                if abs(heights[n] - y) <= 15
                for i in lines[n]
            ]
            # Not the table's own title: 茅台 centres `主营业务分行业情况` 15.0 pt above the header,
            # at x 276.8, inside the cost column, whose right-aligned cells end at 291.2.
            beyond = [
                i.x
                for i in block
                if i.x > cost_h.x
                and not _NUMERIC.match(i.s.strip())
                and not any(title in i.s for title, _ in CN_TABLES)
            ]
            cost_end = min(beyond) if beyond else cost_h.x + (cost_h.x - rev_h.x)
            # The table is read from its name column to its cost column, and another page set
            # beside it starts a gutter further on (`_GUTTER`). Its own later columns (margin,
            # changes) are never read.
            name_hi = _end(cost_h) + _GUTTER
            continue
        if not rev_end:
            continue

        here = [s for s in (i.s.strip() for i in line if _end(i) <= name_end) if s]
        # A line of figures alone is no name: a chart's labels below a table (上海家化's `0.24`,
        # `14.39`) or a page number.
        if here and not _PAGE_NUMBER.match("".join(here)) and not _NUMERIC.match("".join(here)):
            names.append((y, "".join(here)))
        in_rev = settle(column(k, name_end, rev_end), k, name_end, rev_end, rev.text)
        in_cost = settle(column(k, rev_end, cost_end), k, rev_end, cost_end, cost.text)
        if (in_rev or in_cost) and top is None:
            top = y
        cost_value = cost.take(in_cost)
        if owed:
            if cost_value is not None and rows and rows[-1].bottom - y < 16:
                rows[-1].cost = _scaled(cost_value, scale)
                cost_value = None
            if cost_value is not None or not cost.open:
                owed = False
        if cost_value is not None:
            cost_held = cost_value
        revenue = rev.take(in_rev)
        if revenue is None:
            continue
        if negate:
            revenue, negate = -abs(revenue), False
        rows.append(
            _Row(
                axis=axis,
                revenue=_scaled(revenue, scale),
                cost=None if cost_held is None else _scaled(cost_held, scale),
                top=top if top is not None else y,
                bottom=y,
                names=[],
            )
        )
        owed = cost_held is None and cost.open
        if not owed:
            cost = _Column()
        cost_held, top = None, None
    close()

    out: list[CnSegmentRow] = []
    for table_rows, name_lines, x, table_scale in tables:
        borders = sorted({round(r.y, 1) for r in rules if r.x0 <= x <= r.x1})
        headings: list[tuple[float, str]] = []
        for y, text in name_lines:
            if not table_rows:
                break
            best = _holding_row(table_rows, y, borders)
            if best is not None:
                table_rows[best].names.append((y, text))
            elif _HEADING.search(tidy_member_name(text)):
                headings.append((y, _label(tidy_member_name(text))))
        named = [
            (tidy_member_name("".join(t for _, t in sorted(r.names, key=lambda n: -n[0]))), r)
            for r in table_rows
        ]
        if table_rows and table_rows[0].axis == _COMPOSITION_AXIS:
            # The composition table's own total (`营业收入合计`), above its first group: what
            # each group is a split of. A row there that is no total belongs to no group.
            out.extend(
                CnSegmentRow(r.axis, n, r.revenue, None, stated_total=True)
                for n, r in named
                if _TOTAL.search(n)
            )
            continue
        if table_rows and table_rows[0].axis == _SKIPPED_AXIS:
            continue
        kept = _without_restated_totals([(n, r) for n, r in named if n], headings)
        if composition:
            # A group IS the company's total, so an `其中：` with no row above it in the group is
            # a part of the group, not an orphan: 安科生物 heads its regions `国内销售：` and
            # prints the first `其中：华东（不含安徽）`, which otherwise stood in no split.
            kept = [replace(r, parent=None) if r.parent == "" else r for r in kept]
        out.extend(_nested(kept, step=0.01 * table_scale))
    return out


#: A minus sign set as an item of its own.
_MINUS = ("-", "\uff0d", "\u2014", "\u2013")  # -, full-width, em and en dash
#: How close two items must be to be one figure: flush, either side (Yunda's gaps are ±0.06 pt).
_FLUSH = 1.0


def _joined(line: list[TextItem]) -> list[TextItem]:
    """A line with each figure set in pieces joined into one item.

    MEASURED 2026-11-16 ON THE COMPOSITION TABLES, two ways a figure arrives in pieces, each piece
    flush against the next:
    - AN ELIMINATION'S SIGN AS AN ITEM OF ITS OWN (9900009032's `合并抵消`: `-`@140-143 then
      `41,775,760,323.65`@143-210). Read apart, the sign was an empty cell and the figure positive,
      or the sign fell in the name column and the member was named `合并抵消-`; either way the
      elimination was added where the filing subtracts it.
    - EVERY DIGIT GROUP AS AN ITEM (9900002261, Yunda: `48` `,` `543` `,` `032` `,` `234` `.` `43`).
      A cell is placed by where it ends, and the first groups end left of the amount label, so
      they were read as part of the name and the amount as 32,234.43.

    Only pieces that still make a figure join, so a cell beside its neighbour stays two. An empty
    cell's dash stands alone, apart from any figure, and stays."""
    out: list[TextItem] = []
    for item in line:
        prev = out[-1] if out else None
        text = item.s.strip()
        if prev is not None and prev.end is not None and _NUMERIC.match(text):
            before = prev.s.strip()
            if abs(item.x - prev.end) <= _FLUSH and (
                before in _MINUS
                or (
                    _NUMERIC.match(before)
                    and (_PREFIX.match(before + text) or _COMPLETE.match(before + text))
                )
            ):
                joined = ("-" if before in _MINUS else before) + text
                out[-1] = TextItem(joined, prev.x, item.y, item.end)
                continue
        out.append(item)
    return out


#: How far apart two glyphs of one letter-spaced label may be set: an em.
_LABEL_GAP = 12.0


def _label_starts(line: Sequence[TextItem], label: str, *, prefix: bool = False) -> list[float]:
    """Where each occurrence of a header label starts on its line, in x order: set as one item,
    or LETTER-SPACED, one glyph per item.

    16 of the 22 Shenzhen reports that served last year's figures as this year's print this
    year's `金额` as `金` and `额` (x 167.3 and 176.3, flush) beside last year's `金额` as one
    item (x 326.7), measured 2026-11-22 (鱼跃 002223, 奥赛康 002755 among them). Looking for
    the label as one item found last year's column, so this year's amount and share fell into
    the name column (`其他业务16,327,089.580.36%`) and last year's amount was read as the
    revenue. With `prefix`, the joined glyphs need only start with `label` (`占营业收入比重` for
    `占营业`).
    """
    starts: list[float] = []
    for n, first in enumerate(line):
        text, end = "", first.x
        for item in line[n:]:
            piece = item.s.strip()
            if not piece:
                continue
            if text and item.x - end > _LABEL_GAP:
                break
            text += piece
            end = _end(item)
            if text.startswith(label) if prefix else text == label:
                if not starts or first.x > starts[-1]:
                    starts.append(first.x)
                break
            if not label.startswith(text):
                break
    return starts


def _labelled(item: TextItem, label: str) -> bool:
    """A header item that IS the label, or starts with it as a word: 中科软's FY2025 header sets
    `营业成本 毛利率` as one run (pdf.js kept the two apart), and a column is placed by where its
    label starts, which is where the run starts. Never a label inside a run (`）营业收入` belongs
    to the growth column)."""
    text = item.s.strip()
    return text == label or text.startswith(label + " ")


def _without_restated_totals(
    rows: Sequence[tuple[str, _Row]], headings: Sequence[tuple[float, str]] = ()
) -> list[CnSegmentRow]:
    """A table's rows less the totals that restate rows above them.

    A TOTAL IS DROPPED ONLY WHEN IT RESTATES ROWS: kept, it is summed as a segment beside its own
    parts. But a subtotal can be the only row its group has: 中信建投 lists 北京市…其他地区, 小计,
    公司本部, 境内小计, 境外小计, 合计, and 境外 exists only as `境外小计`. So a total equal to the
    rows since the previous one, or to every row above it, goes; a NAMED total that restates nothing
    is that group's own row, named without the `小计`/`合计` (哈投 prints `热电业务合计` above its
    parts, which `_nested` gives it); an unnamed total that restates nothing (`合计`, `总计`) goes.

    TWO LABELS STATE HOW A ROW ADDS UP, and both readers had ignored them (measured 2026-11-15 over
    700 reports):
    - `减：` ("less") is a deduction, printed positive by some filers and negative by others: 南方
      出版传媒 lists 小计 12,222,732,795.17, `减：内部抵销数` 3,803,338,513.45, and 合计
      8,419,394,281.72. Summed as printed, the split was 16.03 bn, a figure the filing never states.
      It is stored negative, named without the `减：` its sign now carries, as SEC's elimination
      members are.
    - `其中：` ("of which") is part of the row above it, not a row beside it: a child of that row,
      out of every sum at the table's own level. Flat, it double-counts, it stops a 合计 restating
      the real rows from matching (so the total was kept as a segment), and four of them named
      `其中：手续费及佣金收入` under four parents in one table share a key, which fails the upsert.
      After a subtotal that restates rows, it belongs to that subtotal's group, which has no row:
      it is kept, under no parent, and summed nowhere.

    A GROUP HEADING IS A PARENT ROW WITHOUT FIGURES. 湘财 sets `证券业：` and `实业：` in rows of
    their own, each over a run of products closed by a `小计` restating them, and both runs carry
    an `其他`: flat, the two shared a key and the writer kept one. A `小计` that restates the rows
    since the last one, under a heading set above the first of them, becomes that heading's row
    with the subtotal's figures, and the rows its children.
    """
    out: list[CnSegmentRow] = []
    since: list[CnSegmentRow] = []
    above: list[CnSegmentRow] = []
    parent: str | None = None
    run_start, run_heading = 0, ""
    floor = float("inf")  # the bottom of the row before this one: a heading sits between the two
    for name, row in rows:
        if not since:
            run_start = len(out)
            run_heading = next(
                (t for y, t in sorted(headings, key=lambda h: h[0]) if row.top < y < floor), ""
            )
        floor = row.bottom
        if _OF_WHICH.match(name):
            part = CnSegmentRow(row.axis, _label(name), row.revenue, row.cost, parent=parent or "")
            out.append(part)
            since.append(part)
            above.append(part)
            continue
        total = _TOTAL.search(name)
        if total is None:
            revenue, cost = row.revenue, row.cost
            less = _LESS.search(name)
            if less is not None:
                name = name[less.end() :].strip() or name
                revenue = -abs(revenue)
                cost = None if cost is None else -abs(cost)
            name = _label(name)
            kept = CnSegmentRow(row.axis, name, revenue, cost)
            out.append(kept)
            since.append(kept)
            above.append(kept)
            parent = name
            continue
        parent = None
        group = _label(name[: total.start()].strip()) if total.start() else ""
        restates_since, restates_above = (_restates(part, row.revenue) for part in (since, above))
        if restates_since and run_heading and name.endswith("小计"):
            run = out[run_start:]
            del out[run_start:]
            heading = CnSegmentRow(row.axis, run_heading, row.revenue, row.cost)
            out.append(heading)
            out.extend(
                r if r.parent not in (None, "") else replace(r, parent=run_heading) for r in run
            )
            above = [r for r in above if r not in run] + [heading]
        elif not group and name.endswith(("合计", "总计")):
            out.append(CnSegmentRow(row.axis, name, row.revenue, row.cost, stated_total=True))
        elif group and not (restates_since or restates_above):
            # A named total restating nothing is its group's own row: 中信建投's `境外小计`, and
            # 哈投's `热电业务合计` printed above its parts (which `_nested` then gives it).
            kept = CnSegmentRow(row.axis, group, row.revenue, row.cost)
            out.append(kept)
            above.append(kept)
            parent = group
            since = [kept]
            continue
        since = []
    return out


def _restates(rows: Sequence[CnSegmentRow], value: float) -> bool:
    """Whether a total equals these rows: summed as printed, or at the table's own level once the
    parts that sum to a row above them are set aside (a named subtotal after a nested run)."""
    if not rows:
        return False

    def near(total: float) -> bool:
        return abs(total - value) <= 0.005 * max(1.0, abs(value))

    printed = [r.revenue for r in rows if r.parent is None]
    level = [r.revenue for r in _nested(list(rows)) if r.parent is None]
    return near(_add_in_order(printed)) or near(_add_in_order(level))


#: A name line ending in a colon (`证券业：`) heads the rows below it.
_HEADING = re.compile(r"[:：]$")
#: How far below a parent its children may run.
_MAX_CHILDREN = 15


def _label(name: str) -> str:
    """A member's name without the colon a heading-style label ends with (`证券业：`), the number
    a filer gives its rows (`(1)燃煤发电及供热业务`) or the `其中：` a first part carries
    (中远海控's `其中：美洲地区`, its other regions unlabelled): the name is the upsert key, the
    parent says what `其中` said, and a row numbered or labelled one year and not the next is the
    same business."""
    return _NUMBERED.sub("", _OF_WHICH.sub("", name.rstrip(":："))).strip() or name


def _nested(rows: list[CnSegmentRow], step: float = 0.01) -> list[CnSegmentRow]:
    """Rows that sum to the row above them, as that row's children.

    MEASURED 2026-11-15: 13 of 349 reports with a table nest one level in it, the parent row first
    and its parts below it, with no label beyond, at most, a `其中：` on the first. 中远海控's
    geography lists 集装箱航运业务 210,731,494,147.06 and then 美洲, 欧洲, 亚太, 中国大陆 and 其他
    国际 summing to it, then 码头业务 and its four regions. Flat, that table is counted twice; and
    as the largest sum it became the TARGET, so the double count was served as a split while the
    true business split could not reconcile.

    The test is the table's own arithmetic, as for a subtotal written first: a row followed by two
    or more rows summing to it to the fen, or by one carrying the same figures: a business whose
    only line is its interest income (江海证券's `信用业务` and `利息收入`, both 277,448,891.47), or
    one listed again as its only product (广电网络's `其他业务`). Two sibling segments equal to the
    fen are not a thing real tables hold. A row already given a parent by its `其中：` label stays
    a candidate, so the label and the arithmetic agree.
    """
    out = list(rows)
    for i in reversed(range(len(out))):  # the inner level first: 哈投 nests twice
        head = out[i]
        if not head.revenue or head.stated_total:
            continue
        total = 0.0
        taken: list[int] = []
        inside: set[str] = set()
        for k in range(i + 1, len(out)):
            part = out[k]
            if part.stated_total:
                break
            if part.parent in inside:  # a part of a part: counted in its parent already
                continue
            if part.parent not in (None, "", head.name) or len(taken) == _MAX_CHILDREN:
                break
            total += part.revenue
            taken.append(k)
            inside.add(part.name)
            # Each printed figure is rounded to its last digit (`step`: a fen, or 0.01 万元), so
            # the parts may miss their parent by half a step each: 中国电影's regions 1-7 sum to
            # 4,746,459,154.20 under `一、国内` 4,746,459,154.21.
            if abs(total - head.revenue) <= step * (len(taken) + 1) / 2 + 1e-6 and (
                len(taken) >= 2 or (part.revenue == head.revenue and part.cost == head.cost)
            ):
                for t in taken:
                    out[t] = replace(out[t], parent=head.name)
                break
    return out


def _band(y: float, borders: Sequence[float]) -> tuple[float, float] | None:
    """The two borders around `y`, or None outside the drawn table."""
    above = [b for b in borders if b > y]
    below = [b for b in borders if b < y]
    if not above or not below:
        return None
    return (max(below), min(above))


def _holding_row(rows: Sequence[_Row], y: float, borders: Sequence[float]) -> int | None:
    """The row a name line at `y` belongs to: the one whose cell holds it where borders are drawn,
    else the nearest within reach, the upper on a tie."""
    band = _band(y, borders)
    if band is not None:
        sharing = [n for n, r in enumerate(rows) if _band(r.bottom, borders) == band]
        if sharing:
            return min(sharing, key=lambda n: (_distance(rows[n], y), n))
        if any(_band(r.bottom, borders) is not None for r in rows):
            return None  # a drawn cell with no figures in it: not a row's name
    best = min(range(len(rows)), key=lambda n: (_distance(rows[n], y), n))
    return best if _distance(rows[best], y) <= _NAME_REACH else None


def page_rules(document: object, index: int) -> list[Rule]:
    """The horizontal lines a page draws: paths no more than 2 pt high and at least 8 pt wide."""
    import pypdfium2 as pdfium
    import pypdfium2.raw as raw

    assert isinstance(document, pdfium.PdfDocument)
    page = document[index]
    try:
        out = []
        for obj in page.get_objects(max_depth=3):
            if obj.type != raw.FPDF_PAGEOBJ_PATH:
                continue
            left, bottom, right, top = obj.get_bounds()
            if top - bottom <= 2.0 and right - left >= 8.0 and not _clipped_away(raw, obj.raw):
                out.append(Rule((top + bottom) / 2, left, right))
        return out
    finally:
        page.close()


def _end(item: TextItem) -> float:
    """Where an item ends; a fixture that gives no end is read at its start."""
    return item.x if item.end is None else item.end


def unit_stated_last(items: Sequence[TextItem], default: int) -> int:
    """The scale of the last `单位：…` a page states, reading down it, else `default`.

    A UNIT LINE CAN END ONE PAGE AND ITS TABLE START THE NEXT: measured 2026-11-15 on 1225062013,
    `单位：万元` is the last line of page 38 and `主营业务分行业情况` opens page 39. Read from the
    table's page alone, every figure was taken as yuan, ten thousand times too small, by both the
    edge and this reader."""
    scale = default
    for item in sorted(items, key=lambda i: -i.y):
        unit = _UNIT.search(item.s)
        if unit is not None:
            scale = _SCALE[unit.group(1)]
    return scale


def _scaled(value: float, scale: int) -> float:
    """A stated figure in yuan, rounded to the fen it was printed at: `75,846.29` 万元 is
    758,462,900.00, not 758,462,899.99999 from the float product."""
    return value if scale == 1 else round(value * scale, 2)


def fiscal_year_end_from(text: str) -> str | None:
    """The period a report covers, from its own title: `2025年年度报告` is the year to 2025-12-31.
    Read from the document, never from the announcement date (the following April).

    THE YEAR IS THE FIRST PLAUSIBLE FOUR DIGITS OF THE RUN BEFORE `年年度报告`, not its last four:
    measured 2026-11-15, 中信建投's running header draws `2025` and then `025` over it, which pdfium
    reports as `2025025年年度报告` (pdf.js dropped the second), and a stock code can run into it."""
    found = [(m.start(), m.group(1)) for m in _YEAR.finditer(text)]
    found += [
        (m.start(), "".join(_CN_DIGIT[c] for c in m.group(1))) for m in _CN_YEAR.finditer(text)
    ]
    for _, digits in sorted(found):
        for start in range(len(digits) - 3):
            year = int(digits[start : start + 4])
            if 1990 <= year <= 2100:
                return f"{year}-12-31"
    return None


#: How close a split must come to its total. The tables are stated to the fen, and a 万元 table to
#: 0.01 万元 a cell (100 yuan), so rounding is at most a few thousand yuan; SEC's 0.5% let five of
#: 1225177612's six products, 18,395.9 m, pass for its stated 18,386.3 m. 0.01% refuses that and
#: leaves room for the rounding of a 20-row 万元 table of 100 m yuan (10,000 against ~2,000).
TOLERANCE_FRACTION = 0.0001

#: `营业收入` and `营业成本` are the two metrics these tables carry.
_METRICS = (("revenue", "revenue"), ("cost", "cost_of_revenue"))


def _merged(rows: Sequence[CnSegmentRow]) -> list[CnSegmentRow]:
    """Rows naming one member twice under one parent, as one row carrying their sum.

    1225263103's product table lists 肝病用药, 心脑血管用药 and `其他` (the drugs under its
    manufacturing), then 医药流通业, 化妆品业 and `其他` (the company's other business), all at one
    level: the six reconcile to the industry table's 8,969,064,200.00, and the segment key (axis,
    member, parent) cannot hold two `其他`. Their sum keeps the split whole. A cost is summed only
    when both rows state one. A stated total is never merged: it is no member."""
    out: list[CnSegmentRow] = []
    held: dict[tuple[str, str | None, str], int] = {}
    for r in rows:
        key = (r.axis, r.parent, r.name)
        if r.stated_total or key not in held:
            if not r.stated_total:
                held[key] = len(out)
            out.append(r)
            continue
        first = out[held[key]]
        out[held[key]] = replace(
            first,
            revenue=round(first.revenue + r.revenue, 2),
            cost=None if first.cost is None or r.cost is None else round(first.cost + r.cost, 2),
        )
    return out


def facts_from_rows(
    rows: Sequence[CnSegmentRow], period_ending: str, *, partial_lists: bool = False
) -> list[SegmentFact]:
    """Rows -> facts, each table partitioned against the LARGEST table's total.

    The tables cover 主营业务 (main business), while consolidated `营业收入` includes 其他业务, so
    the company's revenue is the wrong target. Every table is a view of the same main-business
    revenue, so one table's total is what all are reconciled against: the total most tables agree
    on, else the largest. Each table's own sum would make every split reconcile with itself by
    construction.

    An `其中：` row is a level below its parent, as a cross-tab cell is in SEC's facts: it carries
    the parent as `parent_member` (on the same axis), and its own split is reconciled against the
    parent's revenue, never against the table's. One whose whole is no row of the table is kept at
    the table's level in partition 0, outside the target.

    With `partial_lists` (the older forms), the target is the largest of the totals tables state
    and the sums of the tables that state none. Those forms list only the lines at or above 10% of
    revenue, so two partial tables can agree with each other: 大同煤业 601001 FY2010 lists one
    coal line of 10,290.0 m in both its industry and its product table, against a region table of
    10,322.2 m, the main-business revenue its text states. The total most tables agree on would
    have served the two partial lists as whole. And a table may state the total of a part:
    新城控股's FY2025 region table covers real estate alone and states `合计` 39.0 bn, while its
    industry table sums to 52.0 bn with property income; the stated total alone would have served
    the industry split as 100% real estate. A table that states a total is judged by it, never
    by its own sum (凤凰光学's positive `合并抵消` sums past its stated `合计`).
    """
    period_start = f"{int(period_ending[:4])}-01-01"
    rows = _merged(rows)
    by_axis: dict[str, list[CnSegmentRow]] = {}
    under: dict[tuple[str, str], list[CnSegmentRow]] = {}
    orphans: list[CnSegmentRow] = []
    stated = [round(r.revenue, 2) for r in rows if r.stated_total]
    for row in rows:
        if row.stated_total:
            continue
        if row.parent is None:
            by_axis.setdefault(row.axis, []).append(row)
        elif row.parent == "":
            orphans.append(row)
        else:
            under.setdefault((row.axis, row.parent), []).append(row)
    # Every figure is stated to the fen, so their sum is too: 8,419,394,281.72, not the float
    # 8419394281.719998 that `reconciled_to` would otherwise carry.
    totals = [round(_add_in_order(r.revenue for r in rs), 2) for rs in by_axis.values()]
    # THE TOTAL THE FILING STATES, the largest grand total (`合计`, `总计`) on the page: a
    # partial table's sum is no total (哈投's geography runs onto the next page, 2.79 bn against the
    # 2.72 bn `总计`), nor is one table detailing a part (1225011770's product and geography tables
    # both cover only steel, 37.24 bn, against the industry `合计` of 57.86 bn). With no total
    # stated, the total most tables agree on, the largest of those, else the largest.
    if partial_lists:
        stating = {r.axis for r in rows if r.stated_total}
        candidates = stated + [
            t for axis, t in zip(by_axis, totals, strict=True) if axis not in stating
        ]
        target = max(candidates) if candidates else 0.0
    elif stated:
        target = max(stated)
    else:
        target = max(totals, key=lambda t: (totals.count(t), t)) if totals else 0.0

    def facts(
        r: CnSegmentRow, parent: str | None, against: float, partition: int
    ) -> list[SegmentFact]:
        return [
            SegmentFact(
                axis=r.axis,
                member_code=f"cninfo:{r.name}",
                metric_code=code,
                period_type="annual",
                period_start=period_start,
                period_ending=period_ending,
                value=value,
                # A-share figures are in 元, stated rather than inferred: a 万元 scale error
                # would be four orders of magnitude and look ordinary.
                currency="CNY",
                parent_axis=None if parent is None else r.axis,
                parent_member=None if parent is None else f"cninfo:{parent}",
                reconciled_to=against if against > 0 else None,
                partition_id=partition,
            )
            for key, code in _METRICS
            if (value := r.revenue if key == "revenue" else r.cost) is not None
        ]

    out: list[SegmentFact] = []
    for rs in by_axis.values():
        # A REVENUE TOTAL IS POSITIVE, the rule SEC's path has applied since muffin-ingest#185.
        # Partitioned against a total of zero or less, `assign_partitions` finds a subset adding up
        # to it and places that: China Eastern's FY2020 report read its change column (-75.14%
        # international, -44.58% domestic) as revenue, every table summed below zero, and five
        # members were served as a split totalling -51.48 m (measured 2026-11-22, the only such
        # split in production). Unplaced, the rows stay stored and serve as nothing.
        parts = (
            assign_partitions(
                [(f"cninfo:{r.name}", r.revenue) for r in rs], target, TOLERANCE_FRACTION
            )
            if target > 0
            else {}
        )
        for r in rs:
            out.extend(facts(r, None, target, parts.get(f"cninfo:{r.name}", 0)))
    for (axis, parent), rs in under.items():
        whole = next((r.revenue for r in by_axis.get(axis, []) if r.name == parent), 0.0)
        parts = (
            assign_partitions(
                [(f"cninfo:{r.name}", r.revenue) for r in rs], whole, TOLERANCE_FRACTION
            )
            if whole > 0
            else {}
        )
        for r in rs:
            out.extend(facts(r, parent, whole, parts.get(f"cninfo:{r.name}", 0)))
    for r in orphans:  # part of a whole the table states as no row: in no split, in no sum
        out.extend(facts(r, None, target, 0))
    return out


#: A page number alone on its line at the foot of a page: `18 / 259`, `- 10 -`, `17`.
_FOLIO = re.compile(r"^[-\u2013\u2014\s]*\d{1,4}(?:\s*/\s*\d{1,4})?[-\u2013\u2014\s]*$")
#: How far up from the bottom of the page a folio is looked for.
_FOOT = 100.0
#: The space left between the table page's lowest line and the next page's highest, read as one.
_PAGE_GAP = 14.0


def join_pages(
    first: Sequence[TextItem],
    first_rules: Sequence[Rule],
    second: Sequence[TextItem],
    second_rules: Sequence[Rule],
) -> tuple[list[TextItem], list[Rule]]:
    """The table page and the page after it, read as one page: the second set below the first.

    A TABLE RUNS ONTO THE NEXT PAGE IN HALF THE REPORTS. Measured 2026-11-16 over the 537 golden
    reports with a table page: in 256 the next page opens with money figures before any heading or
    prose. 上海隧道's geography table ends page 17 at 56,324,226,895.94 and page 18 opens with
    `澳门 1,253,524,048.26`, which brings it to the industry table's 57,577,750,944.20; read one
    page at a time, a partial table is either refused or, with no other table to disagree, served
    as a whole split (the edge served 广州发展's first three industries so). One stream also joins a
    cell the page break splits (彤程's `2,320,408,095.5` and `5`), places a total on the second
    page against rows on the first, and reads a table that only starts on the second page.

    What repeats on every page goes: the second page's running header, which sits where the
    first's does, its underline among the rules, and each page's folio. A table that has ended by
    then ends the stream's reading of it as before (`_TABLE_ENDS`).

    A ROW THE BREAK CUTS IS ONE CELL AGAIN. 彤程's `特种橡胶助剂` and the first part of its figures
    are in page 22's last cell, `行业` and the rest in page 23's first, and a name goes to the row
    whose cell holds it: the cell's half on page 22 holds no row's last line, so the name was
    `行业`. When the table runs to the foot of the first page, the border closing it bounds the
    break, not a row, and goes: with the border opening the second page when that one has nothing
    above it, or alone when the second page opens with figures above its first border (彤程 draws
    none over the cut row's remainder). Only figures: a page opening with prose keeps the border,
    or a paragraph's last line would join the last row's name."""
    if not second:
        return list(first), list(first_rules)

    def folio(item: TextItem, page: Sequence[TextItem]) -> bool:
        """A page number: matching, near the foot, and alone on its line. A table's numbered row
        set near the foot is not (1225121079's `7` and `、华南区` are one line, page 13)."""
        return (
            item.y < _FOOT
            and _FOLIO.match(item.s) is not None
            and not any(o is not item and abs(o.y - item.y) < 3.0 for o in page)
        )

    on_first = {(i.s.strip(), round(i.x), round(i.y)) for i in first}
    ruled = {(round(r.y), round(r.x0), round(r.x1)) for r in first_rules}
    head = [i for i in first if not folio(i, first)]
    tail = [
        i
        for i in second
        if not folio(i, second) and (i.s.strip(), round(i.x), round(i.y)) not in on_first
    ]
    tail_rules = [r for r in second_rules if (round(r.y), round(r.x0), round(r.x1)) not in ruled]
    if not tail:
        return head, list(first_rules)
    head_rules = list(first_rules)
    closing = min(head_rules, key=lambda r: r.y, default=None)
    if closing is not None and all(i.y > closing.y for i in head):
        opening = max(tail_rules, key=lambda r: r.y, default=None)
        above = [i for i in tail if opening is None or i.y > opening.y]
        if not above and opening is not None:
            head_rules.remove(closing)
            tail_rules.remove(opening)
        elif any(_NUMERIC.match(i.s.strip()) for i in above) and not any(
            _TABLE_ENDS.search(i.s) or any(h in i.s for h, _ in CN_TABLES) for i in above
        ):
            head_rules.remove(closing)
    low = min([i.y for i in head] + [r.y for r in head_rules], default=_FOOT)
    high = max([i.y for i in tail] + [r.y for r in tail_rules])
    shift = low - _PAGE_GAP - high
    return (
        [*head, *(TextItem(i.s, i.x, i.y + shift, i.end) for i in tail)],
        [*head_rules, *(Rule(r.y + shift, r.x0, r.x1) for r in tail_rules)],
    )


def _older_heading_on(items: Sequence[TextItem]) -> bool:
    """Whether a page sets an older form's heading (`_OLDER_HEADING`) as a line of its own."""
    buckets: dict[int, list[TextItem]] = {}
    for item in items:
        buckets.setdefault(_js_round(item.y / 3), []).append(item)
    return any(
        _older_axis("".join(i.s for i in sorted(line, key=lambda i: i.x))) is not None
        for line in buckets.values()
    )


def _composition_heading_on(items: Sequence[TextItem]) -> bool:
    """Whether a page sets `营业收入构成` as a heading line of its own. A page mentioning it in
    prose is not where the table is, and reading the table from the page before its own would
    miss a table that runs onto the page after."""
    buckets: dict[int, list[TextItem]] = {}
    for item in items:
        buckets.setdefault(_js_round(item.y / 3), []).append(item)
    return any(
        _COMPOSITION_HEADING.match("".join(i.s for i in sorted(line, key=lambda i: i.x)))
        for line in buckets.values()
    )


def segment_facts_from_pdf(
    body: bytes,
    scan_pages: int = SCAN_PAGES,
    items_of: Callable[[object, int], list[TextItem]] | None = None,
) -> CnParseResult:
    """Read one annual report: the period from the first page stating it, the tables from the first
    page carrying the industry heading, read with the page after it (`join_pages`).

    A report carrying no mandated heading is read from its `营业收入构成` table (`COMPOSITION`):
    Shenzhen's form, and the only split most Shenzhen reports state. Never both: the mandated
    tables win wherever they are printed, so a report read from them is read exactly as before.
    The phrase also appears in prose, so each page carrying it is tried in order until one holds
    the table.

    A report neither yields anything from is read in the older forms (`_OLDER_HEADING`, FY2007-2014)
    from the first page holding one of their headings that gives rows. Every report the current
    forms read is read exactly as before."""
    import pypdfium2 as pdfium

    read_items = items_of or page_items
    document = pdfium.PdfDocument(body)
    try:

        def joined(index: int, items: list[TextItem]) -> tuple[list[TextItem], list[Rule]]:
            rules = page_rules(document, index)
            if index + 1 < len(document):
                return join_pages(
                    items, rules, read_items(document, index + 1), page_rules(document, index + 1)
                )
            return items, rules

        def older(period: str) -> CnParseResult | None:
            carried = 1
            for index in range(min(scan_pages, len(document))):
                items = read_items(document, index)
                if _older_heading_on(items):
                    rows = parse_segment_page(*joined(index, items), carried, older=True)
                    if rows:
                        facts = facts_from_rows(rows, period, partial_lists=True)
                        return CnParseResult(facts, index + 1, period)
                carried = unit_stated_last(items, carried)
            return None

        period: str | None = None
        carried = 1  # the unit the previous page stated last
        #: Pages carrying the composition heading's words, with the unit in force above each.
        candidates: list[tuple[int, int]] = []
        for index in range(min(scan_pages, len(document))):
            items = read_items(document, index)
            text = "".join(i.s for i in items)
            if period is None:
                period = fiscal_year_end_from(text)
            if CN_TABLES[0][0] not in text:
                if _COMPOSITION_WORDS.search(text) and _composition_heading_on(items):
                    candidates.append((index, carried))
                carried = unit_stated_last(items, carried)
                continue
            rows = parse_segment_page(*joined(index, items), carried)
            if period is None:
                return CnParseResult([], index + 1, period)
            if not rows:
                return older(period) or CnParseResult([], index + 1, period)
            return CnParseResult(facts_from_rows(rows, period), index + 1, period)
        for index, unit in candidates:
            rows = parse_segment_page(
                *joined(index, read_items(document, index)), unit, composition=True
            )
            if rows:
                if period is None:
                    return CnParseResult([], index + 1, period)
                return CnParseResult(facts_from_rows(rows, period), index + 1, period)
        if period is not None and (found := older(period)) is not None:
            return found
        return CnParseResult([], None, period)
    finally:
        document.close()


#: pdf.js 4.6.82's thresholds for building text items (`evaluator.js`, `getTextContent`), the
#: version the edge ran (unpdf 0.12.1), as fractions of the item's font size.
NOT_A_SPACE = 0.03
TRACKING = 0.102
NEGATIVE_SPACE = -0.2
SPACE_MAX = 0.6
VERTICAL_SHIFT = 0.25


def page_items(document: object, index: int) -> list[TextItem]:
    """One page's text, itemised as pdf.js 4.6.82 itemised it, from pdfium's characters.

    THE STORED ROWS CAME FROM pdf.js's ITEMS, and the table reader treats an item as a cell, so the
    items have to be the same ones. pdfium's text OBJECTS are not: many reports draw every character
    as an object of its own (measured 2026-11-15 on 掌阅科技's FY2025 report), so `分行业` arrived
    as three items and no header was ever found. pdf.js's rules, each rebuilt here per character:

    - A NEW ITEM ON A FONT CHANGE (`setFont`) AND AT EVERY MARKED-CONTENT BOUNDARY (`BDC`/`EMC`).
      A tagged report wraps each span in one, which is why `增` and `加` are separate items and why
      `CMOS` and `图像`, set in two fonts, are never joined by a space. Read here as a change in the
      character's text object's font, `Tf` size or marks.
    - Between glyphs of one item, by the gap after the last glyph's advance: up to `TRACKING` joins,
      up to `SPACE_MAX` joins with a space, wider starts a new item, and an overlap past
      `NEGATIVE_SPACE` starts one too (two money cells set flush against each other).
    - A vertical move past `VERTICAL_SHIFT` starts a new item.
    - A space GLYPH is not text: it is skipped, its advance counts in the next gap, and a single
      space followed by a tight glyph ends the item (`shouldAddWhitepsace`).

    All in the size a character is drawn at (`_effective_size`): pdf.js measures the gap in text
    space against `Tf`, which is the same ratio.
    """
    import pypdfium2 as pdfium
    import pypdfium2.raw as raw

    assert isinstance(document, pdfium.PdfDocument)
    page = document[index]
    textpage = page.get_textpage()
    try:
        handle = textpage.raw
        items: list[TextItem] = []
        run: list[str] = []
        run_x = run_y = end_x = last_y = size = 0.0
        run_key: tuple[object, ...] | None = None
        last_two = [" ", " "]
        slot = 0
        keys: dict[int | None, tuple[object, ...]] = {}
        x = ctypes.c_double()
        y = ctypes.c_double()
        box = raw.FS_RECTF()
        matrix = raw.FS_MATRIX()

        def flush() -> None:
            nonlocal run_key
            text = "".join(run)
            if text.strip():
                items.append(TextItem(s=_ideographs(text), x=run_x, y=run_y, end=end_x))
            run.clear()
            run_key = None

        def reset_last() -> None:
            nonlocal slot
            last_two[0] = last_two[1] = " "
            slot = 0

        def save_last(ch: str) -> bool:
            nonlocal slot
            nxt = (slot + 1) % 2
            space_before = last_two[slot] != " " and last_two[nxt] == " "
            last_two[slot] = ch
            slot = nxt
            return space_before

        for i in range(raw.FPDFText_CountChars(handle)):
            if raw.FPDFText_IsGenerated(handle, i) == 1:
                continue
            code = raw.FPDFText_GetUnicode(handle, i)
            # A C0 control is a glyph with no Unicode mapping (`\\x02` where pdf.js read `-`), never
            # text: it goes, as the line breaks do.
            if code in (0, 0xFFFE, 0xFFFF) or (code < 0x20 and code != 0x09):
                continue
            ch = chr(code)
            key = _object_key(raw, raw.FPDFText_GetTextObject(handle, i), keys)
            if key[-1] is _CLIPPED:
                continue
            if run and key != run_key:
                flush()
            if ch.isspace():
                save_last(" ")
                continue
            raw.FPDFText_GetCharOrigin(handle, i, x, y)
            raw.FPDFText_GetLooseCharBox(handle, i, box)
            char_size = _effective_size(
                float(raw.FPDFText_GetFontSize(handle, i)),
                matrix if raw.FPDFText_GetMatrix(handle, i, matrix) else None,
            )
            if run:
                gap = x.value - end_x
                dy = abs(y.value - last_y)
                if gap < NEGATIVE_SPACE * size or dy > size:
                    # pdf.js names the two cases (a new line, or an overlap) and does the same.
                    reset_last()
                    flush()
                else:
                    if gap <= NOT_A_SPACE * size:
                        reset_last()
                    if gap <= TRACKING * size:
                        if last_two[slot] != " " and last_two[(slot + 1) % 2] == " ":
                            reset_last()
                            flush()
                    elif gap <= SPACE_MAX * size:
                        reset_last()
                        run.append(" ")
                    else:
                        flush()
                        reset_last()
                    if dy > VERTICAL_SHIFT * size:
                        flush()
            if not run:
                run_x, run_y, size, run_key = x.value, y.value, char_size, key
            if save_last(ch):
                run.append(" ")
            run.append(ch)
            end_x = max(box.right, x.value)
            last_y = y.value
        flush()
        return items
    finally:
        textpage.close()
        page.close()


#: The Kangxi radicals and the CJK radicals supplement.
_RADICAL = re.compile(r"[\u2e80-\u2fdf]")


def _ideographs(text: str) -> str:
    """Text with each radical drawn for an ideograph read as that ideograph.

    MEASURED 2026-11-16: some reports' fonts map common characters to the Kangxi radicals block, so
    9900002701 draws `营业收⼊构成` (U+2F0A for 入) and `⾏业` (U+2F8F for 行), and neither the
    table's heading nor its labels were found. NFKC maps each radical to its ideograph; it is
    applied to those blocks alone, because it would also turn full-width brackets and colons into
    ASCII, changing member names read before. No row the golden reports stored held a radical."""
    return _RADICAL.sub(lambda m: unicodedata.normalize("NFKC", m.group()), text)


def _object_key(
    raw: Any, obj: Any, cache: dict[int | None, tuple[object, ...]]
) -> tuple[object, ...]:
    """What pdf.js would start a new item for: the font, its `Tf` size, and the marked-content
    spans the text object sits in. A span is identified by its mark OBJECT, not its name: pdfium
    shares one mark item among the objects drawn inside one `BDC`/`BMC`, and two consecutive spans
    with the same tag (a run of `/Span`s) are still two items to pdf.js. Cached per object.

    The last element is `_CLIPPED` for an object drawn wholly outside its clip (`_clipped_away`),
    whose characters are skipped."""
    address = ctypes.cast(obj, ctypes.c_void_p).value
    held = cache.get(address)
    if held is not None:
        return held
    if not address:
        key: tuple[object, ...] = (None,)
    else:
        font = ctypes.cast(raw.FPDFTextObj_GetFont(obj), ctypes.c_void_p).value
        tf = ctypes.c_float()
        raw.FPDFTextObj_GetFontSize(obj, tf)
        spans = tuple(
            ctypes.cast(raw.FPDFPageObj_GetMark(obj, k), ctypes.c_void_p).value
            for k in range(max(0, raw.FPDFPageObj_CountMarks(obj)))
        )
        key = (font, round(tf.value, 4), spans, _CLIPPED if _clipped_away(raw, obj) else None)
    cache[address] = key
    return key


#: Marks a text object no reader of the page can see.
_CLIPPED = object()


def _clipped_away(raw: Any, obj: Any) -> bool:
    """True when a text object lies wholly outside its clip path, so nothing of it is drawn.

    MEASURED 2026-11-15 ON 浙商证券's FY2025 REPORT: every page draws its running header from a form
    XObject that is a whole other page (its subsidiary list, `PAGE 08`), clipped to the header band
    at y 719-808. The rest of that page is still in the content stream, under the table, and pdf.js
    and pdfium both report it: `（1）浙商资管持有…` between two table rows ended the table after its
    first row, and `3、浙商资管` became part of a segment's name. The edge stored the same junk.

    The clip's extent is the box around its path points, Bézier control points included, which can
    only be larger than the clip: a test against it can keep a hidden object, never drop a seen one.
    Each path of a clip narrows it further, so the object is hidden if it misses any one of them.
    Invisible TEXT (render mode 3) is kept: in a scanned report it is the only text there is."""
    clip = raw.FPDFPageObj_GetClipPath(obj)
    if not clip:
        return False
    left, bottom, right, top = (ctypes.c_float() for _ in range(4))
    if not raw.FPDFPageObj_GetBounds(obj, left, bottom, right, top):
        return False
    x = ctypes.c_float()
    y = ctypes.c_float()
    for path in range(raw.FPDFClipPath_CountPaths(clip)):
        xs: list[float] = []
        ys: list[float] = []
        for segment in range(raw.FPDFClipPath_CountPathSegments(clip, path)):
            if raw.FPDFPathSegment_GetPoint(
                raw.FPDFClipPath_GetPathSegment(clip, path, segment), x, y
            ):
                xs.append(x.value)
                ys.append(y.value)
        if not xs:
            continue
        slack = 0.5
        if (
            right.value < min(xs) - slack
            or left.value > max(xs) + slack
            or top.value < min(ys) - slack
            or bottom.value > max(ys) + slack
        ):
            return True
    return False


def _effective_size(font_size: float, matrix: object | None) -> float:
    """The size a character is DRAWN at, in points. pdfium reports the `Tf` operand, before the text
    matrix: measured 2026-11-15, 掌阅科技 sets `Tf 180` under a 0.05 matrix, a 9 pt character, so
    every threshold read 20 times too wide and each table row merged into one item."""
    scale = 1.0
    if matrix is not None:
        c = float(getattr(matrix, "c", 0.0))
        d = float(getattr(matrix, "d", 0.0))
        scale = (c * c + d * d) ** 0.5 or 1.0
    return (font_size * scale) or 1.0


def _add_in_order(values: object) -> float:
    total = 0.0
    for v in values:  # type: ignore[attr-defined]
        total += v
    return total


def _js_round(value: float) -> int:
    """JavaScript's `Math.round`: halves round toward +infinity."""
    import math

    return math.floor(value + 0.5)
