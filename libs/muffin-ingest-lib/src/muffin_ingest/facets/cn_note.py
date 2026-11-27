"""China's segment note: the operating-segment table of a report's financial-statement notes.

WHY. Banks, insurers and many state firms print none of the mandated main-business tables
(`主营业务分行业…`), and Shenzhen's `营业收入构成` does not apply to them. Measured
2026-11-27: of the large Chinese companies (over $2 bn) with no split, 47 were banks (~$4.5 T),
9 insurers and 2 integrated oil companies. Every one of them states its operating segments in the
note `分部报告` / `分部信息` (Phase 6c step 6, validated on ICBC, Ping An and PetroChina, and
surveyed over the 38 older reports raw held).

THE TABLE IS EDINET'S SHAPE, DRAWN IN PDF POSITIONS: the segments are COLUMNS with `合计` last,
the metrics are rows, negatives are bracketed and a dash is an empty cell. So this module builds
the grid from `cn_pdf`'s text items and hands it to `segment_note`, which reads EDINET's notes.
Three details only positions answer:
  - a header can wrap (`油气和` over `新能源`, `寿险及` over `健康险`), so a column is every header
    fragment stacked over the same place;
  - a figure can arrive in pieces (`2,35` `2` `,` `746`; `(` `3` `,` `353` `)`), joined while flush;
  - a row's figures can sit off its label's baseline (0.84 pt in ICBC's note, 1.92 pt in
    PetroChina's other tables), so a row is a band of items, not one baseline.

NO TAGGED FACT ANCHORS IT, so each metric row is anchored on its OWN `合计` cell: a split is
accepted when its members add up to the total the note states, which is what the mandated tables
are reconciled against too. Evidence beyond the arithmetic comes from the vocabulary
(`market.segment_note_rule`, regulator `cninfo`): the table's kind from its heading, the metric
from the row label, and totals, eliminations and "of which" rows never members. Where a table
states both a gross row and an external one, only external revenue is anchored: PetroChina's
`分部收入` (4,882,272, with intersegment sales) beside `对外交易收入` (2,864,469, the company's
revenue).

ONLY DURATION METRICS. A table's year comes from the line above its header (`2025 年`,
`2025 年度及…`, `截至该日止年度`); a table stating only a date (`2025 年 12 月 31 日`) is a balance
sheet and is skipped, and so is everything below a date line inside a table.
"""

from __future__ import annotations

import datetime as dt
import re
import unicodedata
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from muffin_ingest.facets import segment_note as sn
from muffin_ingest.facets.segments import SegmentFact

#: The axis a note's operating segments are written to (a `market.segment_axis` row).
NOTE_AXIS = "cninfo:经营分部"
PREFIX = "cninfo:"

#: A page worth itemising: it names the note and carries a total column. A quick text scan finds
#: these in a 400-page report in under a second; itemising every page would take tens.
_PAGE_HINT = re.compile(r"分部报告|分部信息|经营分部|业务分部")
_TOTAL_HINT = re.compile(r"合\s*计|总\s*计")

#: The note's heading as a line of its own: `五、 分部信息（续）`, `60 分部报告`,
#: `(1) 经营分部`, `九、 分部报告 - 续`, matched with the spaces removed (`60分部报告`). A
#: sentence that merely names a segment is no heading.
_NOTE_HEADING = re.compile(
    r"^\s*(?:[（(]?\s*(?:\d{1,3}|[一二三四五六七八九十]{1,3}|[a-z]{1,2}|[ivx]{1,4})\s*[)）、.\uff0e]\s*)*"
    r"(?:\d{1,3}\s*)?(分部报告|分部信息|经营分部|业务分部|报告分部的财务信息)"
    r"\s*(?:[（(]\s*续\s*[)）]|[-—]\s*续)?\s*[:：]?\s*$"
)
#: The next note of the statements (`61 关联方关系及其交易`, `六、合并范围`): the segment
#: note has ended.
_TOP_HEADING = re.compile(
    r"^\s*(?:[一二三四五六七八九十]{1,3}\s*、|\d{1,3}\s*[、.\uff0e]?)\s*[^\d\s,，。.:：]{2,20}$"
)
#: A numbered sub-heading (`(2) 地区信息`, `2. 地理信息`): it ends the table above it.
_SUBHEADING = re.compile(
    r"^\s*[（(]?\s*(?:\d{1,2}|[a-z]|[一二三四五六七八九十]{1,3})\s*[)）、.\uff0e]\s*\S"
)
#: The unit, as the notes state it on every page's running header: `金额单位均为人民币百万元`,
#: `(除特别注明外，金额单位为人民币百万元)`, `单位：人民币千元`, and China Merchants Bank's
#: `(除特别注明外，货币单位均以人民币百万元列示)`.
_UNIT = re.compile(
    r"(?:金额|货币)?单位\s*(?:均|全部)?\s*(?:为|是|以)?\s*[:：]?\s*[（(]?\s*(人民币|港币|美元)?\s*[)）]?\s*"
    r"(亿元|千万元|百万元|万元|千元|元)"
)
#: A table caption naming the currency and unit with no `单位` (`（人民币百万元）`,
#: `（人民币百万元，百分比除外）`): China Merchants Bank states its unit no other way near the note.
_UNIT_BARE = re.compile(r"[（(]\s*(人民币|港币|美元)\s*(亿元|千万元|百万元|万元|千元|元)\s*[)）,，]")
_SCALE = {"元": 1, "千元": 1e3, "万元": 1e4, "百万元": 1e6, "千万元": 1e7, "亿元": 1e8}
#: The year a table states. A month after it makes it a date (a balance sheet), unless the line
#: also says it covers the year (`年度`, `截至该日止年度`, Bank of China's `2025 年 12 月 31 日及
#: 2025 年`).
_YEAR = re.compile(r"((?:19|20)\d{2})\s*年")
_DATE = re.compile(r"(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日")
_COVERS_YEAR = re.compile(r"年\s*度|止\s*年\s*度|日\s*及\s*(?:19|20)\d{2}\s*年")
#: A figure, or a piece of one: digits and their commas and point, a bracket, a leading minus.
_PIECE = re.compile(r"^[(（]?-?[\d,.]*[)）]?$")
_FIGURE = re.compile(r"^[(（]?-?\d[\d,]*(?:\.\d+)?[)）]?$")
#: A row label's numbering (`一、营业收入`, `一 、营业收入`, `(一)`, `1、折旧`, `- 其他`): the
#: vocabulary matches the label, not its place in a list.
_ENUMERATOR = re.compile(
    r"^\s*(?:[一二三四五六七八九十]{1,3}\s*[、.\uff0e]|[（(]\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})\s*[)）]"
    r"|\d{1,2}\s*[、\uff0e]|[-\u2014\u2013]\s)\s*"
)
#: An empty cell.
_NIL = frozenset({"-", "\u2013", "\u2014", "\uff0d", "\u2015"})
#: A footnote mark alone on a line (`（1）`), drawn between rows.
_MARK = re.compile(r"^[（(]?\s*\d{1,2}\s*[)）]?$")

#: How far apart two baselines may be and still be one row: a third of a 10 pt line, above the
#: 1.92 pt PetroChina draws between a label and its figures, well below a row's ~12 pt.
_ROW_BAND = 3.5
#: How close two pieces must be to be one figure.
_FLUSH = 1.2
#: How far a figure may sit from its column's centre.
_REACH = 45.0
#: How close two header fragments' centres must be to be one column.
_STACK = 22.0


@dataclass(frozen=True, slots=True)
class Item:
    s: str
    x: float
    y: float
    end: float

    @property
    def centre(self) -> float:
        return (self.x + self.end) / 2


@dataclass(slots=True)
class NoteResult:
    facts: list[SegmentFact] = field(default_factory=list)
    #: The 1-based pages a table was read from.
    pages: list[int] = field(default_factory=list)
    tables: int = 0
    no_year: int = 0
    no_unit: int = 0


#: Hangul fillers some fonts draw for a space (`八、\uffa0分部信息`): neither NFKC nor `\s` treats
#: them as one, so a heading set with them would not read as a heading.
_FILLER = re.compile("[\u115f\u1160\u3164\uffa0]")


def _items(raw: Sequence[object]) -> list[Item]:
    out: list[Item] = []
    for it in raw:
        s = _FILLER.sub("", unicodedata.normalize("NFKC", str(getattr(it, "s", "")))).strip()
        if not s:
            continue
        x = float(getattr(it, "x", 0.0))
        end = getattr(it, "end", None)
        y = float(getattr(it, "y", 0.0))
        out.append(Item(s, x, y, float(end) if end else x + 5.0 * len(s)))
    return out


def _lines(items: Sequence[Item]) -> list[list[Item]]:
    """Items in bands of nearby baselines, top of the page first, each band left to right."""
    bands: list[list[Item]] = []
    for item in sorted(items, key=lambda i: (-i.y, i.x)):
        if bands and bands[-1][0].y - item.y <= _ROW_BAND:
            bands[-1].append(item)
        else:
            bands.append([item])
    return [_joined(sorted(b, key=lambda i: i.x)) for b in bands]


def _joined(line: list[Item]) -> list[Item]:
    """A figure set in pieces made one item, while each piece is flush against the last and the
    whole still reads as a figure."""
    out: list[Item] = []
    for item in line:
        prev = out[-1] if out else None
        if (
            prev is not None
            and abs(item.x - prev.end) <= _FLUSH
            and _PIECE.match(prev.s)
            and _PIECE.match(item.s)
            and _PIECE.match(prev.s + item.s)
        ):
            out[-1] = Item(prev.s + item.s, prev.x, prev.y, item.end)
            continue
        out.append(item)
    return out


def text_of(line: Sequence[Item]) -> str:
    return re.sub(r"\s+", "", "".join(i.s for i in line))


def _is_figure(item: Item) -> bool:
    return item.s in _NIL or bool(_FIGURE.match(item.s))


@dataclass(slots=True)
class _Column:
    name: str
    centre: float
    left: float
    #: The year a column states under its segment (`2025 年`), where the header gives one.
    year: int | None = None


def _nearest(columns: Sequence[_Column], centre: float) -> _Column:
    return min(columns, key=lambda c: abs(c.centre - centre))


def _labels_at(lines: Sequence[list[Item]], at: int) -> float | None:
    """Where the row labels start under a header: the leftmost text in the few lines after it."""
    xs = [
        i.x
        for ln in lines[at + 1 : at + 5]
        for i in ln
        if not _is_figure(i) and not _YEAR_CELL.fullmatch(i.s) and i.s not in ("年", "年度")
    ]
    return min(xs) if xs else None


def _header(lines: Sequence[list[Item]], at: int) -> list[_Column] | None:
    """The columns of a header ending on line `at`: the line holding the total label, with the
    fragments stacked above it (up to two lines) merged by place. None unless it names at least
    two columns besides the total."""
    line = lines[at]
    if not any(_TOTAL_HINT.fullmatch(i.s) for i in line) or any(_is_figure(i) for i in line):
        return None
    total = next(i for i in line if _TOTAL_HINT.fullmatch(i.s))
    fragments = [i for i in line]
    # A wrapped header sits over the value columns; a line reaching back to the row labels is a
    # caption (`(a) 分部业绩、资产及负债`, China Merchants Bank), not a header fragment.
    labels_at = _labels_at(lines, at)
    for back in (1, 2):
        if at - back < 0:
            break
        above = lines[at - back]
        if any(_is_figure(i) for i in above) or line[0].y + 30 < above[0].y:
            break
        # A year line or a sentence spanning the page is no header fragment.
        if _YEAR.search(text_of(above)) or text_of(above).endswith(("：", ":", "。")):
            break
        above = [i for i in above if i.s not in ("项目", "项 目")]
        if labels_at is not None and any(i.x <= labels_at + 5 for i in above):
            break
        fragments += above
    columns: list[list[Item]] = []
    for frag in sorted(fragments, key=lambda i: (-i.y, i.x)):
        for col in columns:
            if abs(sum(i.centre for i in col) / len(col) - frag.centre) <= _STACK:
                col.append(frag)
                break
        else:
            columns.append([frag])
    out = [
        _Column(
            name="".join(i.s for i in sorted(col, key=lambda i: -i.y)),
            centre=sum(i.centre for i in col) / len(col),
            left=min(i.x for i in col),
        )
        for col in columns
    ]
    out.sort(key=lambda c: c.centre)
    # A header can wrap BELOW the total line too: Qingdao Rural Commercial Bank sets `公司银行业`
    # above it and the `务` that ends it below.
    below = lines[at + 1] if at + 1 < len(lines) else []
    if (
        below
        and below[0].y >= line[0].y - 14
        and not any(_is_figure(i) for i in below)
        and not _YEAR.search(text_of(below))
    ):
        under = [_nearest(out, i.centre) for i in below]
        if all(abs(c.centre - i.centre) <= _STACK for c, i in zip(under, below, strict=True)):
            for c, i in zip(under, below, strict=True):
                c.name += i.s
    # The leftmost fragment may be a label header (`项目`) over the label column, never a value.
    if out and out[0].name in ("项目", "项 目"):
        out = out[1:]
    names = [c.name for c in out]
    if len(out) < 3 or not any(_TOTAL_HINT.fullmatch(n) for n in names):
        return None
    if out[-1].centre < total.centre - 1:
        return None
    return out


_YEAR_CELL = re.compile(r"((?:19|20)\d{2})(?:年度?)?")
_MONTH_DAY = re.compile(r"\d{1,2}月\d{1,2}日")


def _years_below(
    lines: Sequence[list[Item]], at: int, columns: list[_Column]
) -> tuple[list[_Column], int, int | None, bool]:
    """A header can state a year under each segment (`2025 年` `2024 年`, China Merchants Bank):
    each column then splits into one per year, at the year's place. Returns the columns, the lines
    the years used, the newest year, and whether a date line under the years makes the table a
    balance sheet (`12 月 31 日`)."""
    if at + 1 >= len(lines):
        return columns, 0, None, False
    cells: list[Item] = []
    for item in lines[at + 1]:
        if item.s in ("年", "年度", "度") and cells and item.x - cells[-1].end <= 3:
            last = cells[-1]
            cells[-1] = Item(last.s + item.s, last.x, last.y, item.end)
        elif _YEAR_CELL.fullmatch(item.s):
            cells.append(item)
        else:
            return columns, 0, None, False
    if not columns or len(cells) < 2 * len(columns) or len(cells) % len(columns):
        return columns, 0, None, False
    per = len(cells) // len(columns)
    owner = [
        min(range(len(columns)), key=lambda c: abs(columns[c].centre - y.centre)) for y in cells
    ]
    if any(owner.count(c) != per for c in range(len(columns))):
        return columns, 0, None, False
    split = [
        _Column(columns[c].name, y.centre, y.x, int(_YEAR_CELL.fullmatch(y.s).group(1)))  # type: ignore[union-attr]
        for c, y in zip(owner, cells, strict=True)
    ]
    instant = at + 2 < len(lines) and len(_MONTH_DAY.findall(text_of(lines[at + 2]))) >= 2
    return split, 1 + instant, max(c.year for c in split if c.year), instant


def _period(texts: Sequence[str], fy_end: str) -> tuple[str, str] | None:
    """(start, end) of the year the table covers, from the lines above its header, nearest first;
    None for a date alone (a balance sheet) or no year at all."""
    month_day = fy_end[4:]
    for text in reversed(texts):
        years = _YEAR.findall(text)
        if not years:
            continue
        if _DATE.search(text) and not _COVERS_YEAR.search(text):
            return None
        year = int(years[0])
        end = dt.date.fromisoformat(f"{year}{month_day}")
        start = dt.date.fromisoformat(f"{year - 1}{month_day}") + dt.timedelta(days=1)
        return start.isoformat(), end.isoformat()
    return None


def _grid(
    lines: Sequence[list[Item]], start: int, columns: Sequence[_Column]
) -> tuple[list[list[str]], int]:
    """The data rows under a header, as `[label, cell per column]`, and the index of the line after
    them. A label set alone on the line above its figures joins them."""
    label_edge = min(c.left for c in columns) - 4
    rows: list[list[str]] = []
    pending: list[str] = []
    i = start
    while i < len(lines):
        line = lines[i]
        text = text_of(line)
        figures = [it for it in line if _is_figure(it) and it.end > label_edge]
        label = "".join(it.s for it in line if it not in figures and it.x < label_edge)
        if not figures:
            if _MARK.match(text):
                i += 1
                continue
            if _DATE.search(text) or _NOTE_HEADING.match(text) or _SUBHEADING.match(text):
                break
            if _header(lines, i) is not None:
                break
            if label:
                pending.append(label)
                if len(pending) > 3:  # prose, not a wrapped label
                    break
            i += 1
            continue
        cells = [""] * len(columns)
        for fig in figures:
            nearest = min(range(len(columns)), key=lambda c: abs(columns[c].centre - fig.centre))
            if abs(columns[nearest].centre - fig.centre) <= _REACH and not cells[nearest]:
                cells[nearest] = fig.s
        rows.append([_ENUMERATOR.sub("", "".join(pending) + label), *cells])
        pending = []
        i += 1
    return rows, i


def read_page_lines(
    pages: Sequence[tuple[int, list[list[Item]]]],
    fy_end: str,
    vocabulary: sn.NoteVocabulary,
    *,
    default_scale: float | None = None,
) -> NoteResult:
    """The note's duration tables across `pages` (index, lines), read in order. The heading and the
    unit carry from page to page, as the note runs over several; the next numbered note ends it.

    ONE TABLE PER (period, metric), THE LAST READ: a report can state the same split twice (an
    MD&A summary before the audited note), under member names that differ, and keeping both would
    union two complete splits of one period into a double count."""
    out = NoteResult()
    heading: str | None = None
    scale = default_scale
    read: list[tuple[int, list[SegmentFact]]] = []
    previous: int | None = None
    for index, lines in pages:
        # The heading carries onto the NEXT page only: across a gap it would carry from the
        # accounting policies (`23 分部报告`) to an unrelated table hundreds of pages on. The unit
        # does carry: SF Holding and Qilu Bank state it once for all the notes, far before the
        # segment note.
        if previous is not None and index != previous + 1:
            heading = None
        previous = index
        before: list[str] = []
        i = 0
        while i < len(lines):
            text = text_of(lines[i])
            for m in [*_UNIT.finditer(text), *_UNIT_BARE.finditer(text)]:
                scale = None if m.group(1) in ("港币", "美元") else _SCALE[m.group(2)]
            if _NOTE_HEADING.match(text):
                heading = text
                before = []
            elif _TOP_HEADING.match(text):
                heading = None
            columns = _header(lines, i)
            if columns is None:
                if text:
                    before.append(text)
                i += 1
                continue
            out.tables += 1
            columns, used, year, instant = _years_below(lines, i, columns)
            rows, i = _grid(lines, i + 1 + used, columns)
            texts = ([heading] if heading else []) + before[-6:]
            before = []
            if instant:
                continue
            period = _period(texts if year is None else [f"{year}年度"], fy_end)
            if period is None:
                out.no_year += 1
                continue
            if scale is None:
                out.no_unit += 1
                continue
            if year is not None:  # only the newest year's columns
                keep = [c for c, col in enumerate(columns) if col.year == year]
                columns = [columns[c] for c in keep]
                rows = [[row[0], *(row[1 + c] for c in keep)] for row in rows]
            table = sn.Table(rows=[["", *(c.name for c in columns)], *rows], before=texts)
            anchors = _anchors(table, vocabulary, scale, period)
            facts = sn.read_grid(
                table, anchors, vocabulary, scale=scale, axes={"business": NOTE_AXIS},
                prefix=PREFIX, to_rounding=True,
            )  # fmt: skip
            if facts:
                read.append((index, facts))
    last: dict[tuple[str, str], int] = {}
    for n, (_, facts) in enumerate(read):
        for f in facts:
            last[(f.period_ending, f.metric_code)] = n
    for n, (index, facts) in enumerate(read):
        kept = [f for f in facts if last[(f.period_ending, f.metric_code)] == n]
        if kept:
            out.facts += kept
            out.pages.append(index + 1)
    return out


#: Labels naming a company's revenue from OUTSIDE it: where a table states one, a gross row
#: beside it (with intersegment sales) is not anchored.
_EXTERNAL = re.compile(r"对外交易收入|来自外部客户的收入|外部客户收入|对外营业收入|外部营业收入")


def _anchors(
    table: sn.Table, vocabulary: sn.NoteVocabulary, scale: float, period: tuple[str, str]
) -> list[sn.Anchor]:
    """Each metric row's own total, as the figure its members must add up to."""
    header = table.rows[0]
    total_col = next(
        (c for c, name in enumerate(header) if c and sn.role_of(name, vocabulary) == "total"), None
    )
    if total_col is None:
        return []
    # A GROSS ROW BESIDE AN ELIMINATION COLUMN: the members add up to the total LESS the
    # elimination, so both are offered and the members' own sum chooses (Bank of China's 营业收入:
    # 663,127 of segments, (4,817) eliminated, 658,310 in all). Offering cannot pick a wrong target:
    # an anchor is taken only where the members already add up to it.
    eliminations = [
        c for c, name in enumerate(header)
        if c and c != total_col and sn.role_of(name, vocabulary) in ("adjustment", "skip")
    ]  # fmt: skip
    found: list[tuple[str, str, float]] = []
    for row in table.rows[1:]:
        metric = sn.role_of(row[0], vocabulary)
        if metric is None or metric in sn.STRUCTURAL:
            continue
        value = sn.number(row[total_col]) if total_col < len(row) else None
        if value is None or value == 0:
            continue
        found.append((metric, row[0], value * scale))
        eliminated = sum(
            v for c in eliminations if c < len(row) and (v := sn.number(row[c])) is not None
        )
        if eliminated:
            found.append((metric, f"{row[0]}:分部合计", (value - eliminated) * scale))
    external = any(m == "revenue" and _EXTERNAL.search(label) for m, label, _ in found)
    return [
        sn.Anchor(f"cninfo:{label}", value, period[0], period[1], None, "CNY")
        for metric, label, value in found
        if not (external and metric == "revenue" and not _EXTERNAL.search(label))
    ]


#: How many pages after the note's heading may hold its tables when they do not name the note:
#: Industrial Bank opens `八、分部报告` and prints its tables on the pages after, with no running
#: header naming it.
_AFTER_HEADING = 3


def candidate_pages(page_texts: Callable[[int], str], count: int) -> list[int]:
    """The pages to itemise: each that names the note and carries a total, with the pages either
    side, and each that opens the note as a heading, with the pages after it. The note runs on to
    the next page, and the page before states what the hit page may not: Ping An prints its unit
    only in the running header of even pages, and opens the note (`七、分部报告`) on the page
    before its first table."""
    found: set[int] = set()
    for index in range(count):
        text = page_texts(index)
        if not _PAGE_HINT.search(text):
            continue
        if _TOTAL_HINT.search(text):
            found.update((index - 1, index, index + 1))
        lines = (_FILLER.sub("", unicodedata.normalize("NFKC", t)) for t in text.splitlines())
        if any(_NOTE_HEADING.match(re.sub(r"\s+", "", t)) for t in lines):
            found.update(range(index, index + 1 + _AFTER_HEADING))
    return sorted(i for i in found if 0 <= i < count)


def read_document(
    document: object,
    fy_end: str,
    vocabulary: sn.NoteVocabulary,
    items_of: Callable[[object, int], Sequence[object]],
) -> NoteResult:
    """A pdfium document's segment note."""
    pdf = document

    def page_text(index: int) -> str:
        page = pdf[index]  # type: ignore[index]
        try:
            return str(page.get_textpage().get_text_range())
        finally:
            page.close()

    count = len(pdf)  # type: ignore[arg-type]
    pages = [(i, _lines(_items(items_of(document, i)))) for i in candidate_pages(page_text, count)]
    return read_page_lines(pages, fy_end, vocabulary)
