"""A segment note stated as an HTML table, read against the filing's own tagged totals.

Japan's EDINET and Europe's ESEF tag the primary statements in XBRL and file the segment note as a
TEXT BLOCK: the note's own HTML, with every number in a table cell and none of it tagged. Measured
2026-11-26/27: Toyota's IFRS block sums three segments to 48,036,704 百万円 per year, L'Oréal's to
44,052.0 €m. So the numbers are there; what is missing is which row is revenue, which column is a
segment, and which year a table states.

THE FILING ANSWERS ALL THREE, AND NOT THROUGH ITS LABELS. A line (a row with the members as
columns, or a column with the members as rows) is a metric when its total, or the sum of its
members, equals a consolidated fact the filing tags: a non-dimensional fact of that exact value.
The fact's context gives the period, and its concept's local name, with the line's label, gives the
metric. Matching on VALUES is what makes this independent of language and taxonomy: Toyota tags its
revenue only under its own extension concepts (`jpcrp030000-asr_E02144-000:TotalNetRevenuesIFRS`),
which no list of standard concepts would find.

A value match alone is not enough evidence, because tables that merely add up to a total exist: a
reconciliation, an intersegment `計` row, a major-customer table. So:
  - the table's KIND comes from its heading (`NoteVocabulary.headings`: 報告セグメント is business,
    製品及びサービス product, 地域 geography; 主要な顧客 and 差異調整 are skipped), and a table
    whose kind cannot be established is skipped and counted, never guessed: a geography axis read
    as business would put countries into industry classification;
  - totals, adjustments, subtotals and "of which" columns are never members
    (`NoteVocabulary.labels`, roles `total`, `adjustment`, `subtotal`, `subset`, `skip`);
  - a table is read only if it has at least two members and a line matched a tagged fact.

PROFIT AND ASSETS DO NOT RECONCILE (ASC 280 and IFRS 8 require a reconciliation, not an identity),
so a table's other metric lines take the period of its matched revenue line and the members'
partitions from it, as `segments.segment_facts_from` does for an XBRL instance.
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from html.parser import HTMLParser

from muffin_ingest.facets.segments import SegmentFact, assign_partitions, period_type_for

# ── Vocabulary (control rows, passed in) ──────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Heading:
    """A heading pattern and the kind of table under it: `business`, `product`, `geography`, or
    `skip` (a major-customer table, a reconciliation)."""

    pattern: str
    kind: str


@dataclass(frozen=True, slots=True)
class Label:
    """A row or column label pattern and its role: a metric code (`revenue`, `operating_income`,
    `total_assets`, …) or a structural role (`total`, `adjustment`, `subtotal`, `subset`, `skip`).
    Patterns are regular expressions matched against the NFKC-normalised label."""

    pattern: str
    role: str


@dataclass(frozen=True, slots=True)
class NoteVocabulary:
    headings: tuple[Heading, ...]
    labels: tuple[Label, ...]
    #: Metric -> local-name patterns of a concept that can anchor it (`Revenue`, `NetSales`).
    concepts: tuple[tuple[str, str], ...] = ()
    #: Member-name patterns and the kind they imply (`境内`, `长三角地区` -> geography). A table
    #: whose members are mostly places is a geography table whatever its heading says: Chinese
    #: banks print geographic operating segments under the same `分部报告` heading as business ones.
    members: tuple[Heading, ...] = ()
    #: Names keep their single spaces (`Chiffre d'affaires`, `Asia y resto del Mundo`): Japanese and
    #: Chinese labels carry no meaningful spaces and drop them, European ones would run together.
    spaced: bool = False


STRUCTURAL = frozenset({"total", "adjustment", "subtotal", "subset", "skip"})


# ── Anchors: the filing's own consolidated facts ────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Anchor:
    """A non-dimensional numeric fact the filing tags: the figure a line may total to."""

    concept: str
    value: float
    start: str | None
    end: str | None
    instant: str | None
    currency: str | None

    @property
    def local_name(self) -> str:
        return self.concept.rsplit(":", 1)[-1]


# ── Tables ──────────────────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class Table:
    """One `<table>`, its spans expanded into a full grid, with the text that came before it."""

    rows: list[list[str]]
    before: list[str] = field(default_factory=list)


class _Grids(HTMLParser):
    """Tables in document order, each with the text between it and the previous one. Spans are
    expanded: a `colspan` repeats the cell to the right, a `rowspan` carries it down, so every row
    has one entry per column and a value sits under its own header."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[Table] = []
        self._text: list[str] = []
        self._depth = 0
        self._raw: list[list[tuple[str, int, int]]] = []
        self._row: list[tuple[str, int, int]] | None = None
        self._cell: list[str] | None = None
        self._span = (1, 1)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._depth += 1
            if self._depth == 1:
                self._raw = []
        elif tag == "tr" and self._depth == 1:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            a = dict(attrs)
            self._cell = []
            self._span = (_int(a.get("colspan")), _int(a.get("rowspan")))
        elif tag == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._cell is not None and self._row is not None:
            self._row.append((clean(" ".join(self._cell)), *self._span))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self._raw.append(self._row)
            self._row = None
        elif tag == "table":
            self._depth -= 1
            if self._depth == 0:
                self.tables.append(Table(rows=_expand(self._raw), before=self._text))
                self._text = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)
        elif self._depth == 0:
            text = clean(data)
            if text:
                self._text.append(text)


def _int(value: str | None) -> int:
    try:
        return max(1, int(value or 1))
    except ValueError:
        return 1


def _expand(raw: list[list[tuple[str, int, int]]]) -> list[list[str]]:
    grid: list[list[str]] = []
    carried: dict[tuple[int, int], str] = {}
    for r, cells in enumerate(raw):
        row: list[str] = []
        c = 0
        queue = list(cells)
        while queue or (r, c) in carried:
            if (r, c) in carried:
                row.append(carried.pop((r, c)))
                c += 1
                continue
            text, colspan, rowspan = queue.pop(0)
            for dc in range(colspan):
                row.append(text)
                for dr in range(1, rowspan):
                    carried[(r + dr, c + dc)] = text
            c += colspan
        grid.append(row)
    width = max((len(r) for r in grid), default=0)
    return [r + [""] * (width - len(r)) for r in grid]


def tables(html: str) -> list[Table]:
    """Every table of the note, in document order."""
    parser = _Grids()
    parser.feed(html)
    parser.close()
    return parser.tables


# ── Cells ───────────────────────────────────────────────────────────────────────────────────

#: A footnote marker in a label: `(注)1`, `(注3)`, `※2`, `(*1)`, a trailing `注1` (after NFKC).
_FOOTNOTE = re.compile(r"(\(注\)\s*\d*|\(注\s*\d+\)|※\s*\d*|\(\*\d+\)|\*\d+|注\s*\d+$|\(\d\)$)")
#: A dash alone: hyphen-minus, full-width, em, horizontal bar, hyphen, minus sign, en dash.
_NIL = frozenset({"-", "\uff0d", "\u2014", "\u2015", "\u2010", "\u2212", "\u2013"})
_NUMBER = re.compile(
    "^(?P<neg>[△▲\\-\u2212])?" r"\s*(?P<num>\d{1,3}(?:,\d{3})*|\d+)(?P<dec>\.\d+)?$"
)
_PAREN = re.compile(r"^\((?P<num>[\d,]+(?:\.\d+)?)\)$")
#: A figure followed by its own "of which" in brackets: Makita's 「371,798 (86,069)」 under
#: 「欧州 (うち:ドイツ)」. The first figure is the cell's value.
_WITH_SUBSET = re.compile(r"^(?P<main>[^()\s]+)\s*\([\d,.]+\)$")
#: An "of which" named inside a label: 「欧州 (うち:ドイツ)」 is the member 欧州.
_OF_WHICH = re.compile(r"\(うち[^)]*\)")
_SCALE = (("百万円", 1e6), ("千円", 1e3), ("円", 1.0), ("百万", 1e6), ("千", 1e3))
#: A unit line inside a cell: 「(単位：千円)」, which a filer often sets in a header cell.
_UNIT_TEXT = re.compile(r"\(?\s*単位\s*[:：]\s*[^)）]*[)）]?")


#: A cross-reference in a European label: `México (Nota 18)`, `(Notas 2.c y 18)`, `(note 5)`,
#: and an asterisk footnote: `Activos no corrientes (*)`.
_REFERENCE = re.compile(r"\s*\((?:notas?|notes?|not|anm\.?|nota\s)\b[^)]*\)|\s*\(\*+\)", re.I)


def clean(text: str) -> str:
    """NFKC, whitespace collapsed. Full-width digits and brackets become ASCII, so a label read
    from one filer compares with another's."""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", text)).strip()


def member_name(label: str, spaced: bool = False) -> str:
    """A label as a member: footnote markers and a stray unit line stripped, whitespace removed
    (Japanese labels carry no meaningful spaces, and a filer may space one label two ways across
    years: 「連結 財務諸表 計上額 (注)2」 is 「連結財務諸表計上額」). `spaced` keeps single spaces,
    for languages that need them."""
    bare = _FOOTNOTE.sub("", _OF_WHICH.sub("", _UNIT_TEXT.sub("", label)))
    if spaced:
        bare = _REFERENCE.sub("", bare)
        return re.sub(r"\s+", " ", bare).strip(" ・:：")
    return re.sub(r"\s+", "", bare).strip("・:：")


def number(cell: str) -> float | None:
    """A cell as a number, or None when it is text. A dash alone is nil (0); △ and ▲ are Japan's
    minus signs, and parentheses a negative."""
    s = cell.replace(" ", "")
    if s in _NIL:
        return 0.0
    m = _NUMBER.match(s)
    if m:
        value = float(m.group("num").replace(",", "") + (m.group("dec") or ""))
        return -value if m.group("neg") else value
    p = _PAREN.match(s)
    if p:
        return -float(p.group("num").replace(",", ""))
    w = _WITH_SUBSET.match(cell.strip())
    if w:
        return number(w.group("main"))
    return None


# ── Number conventions: ESEF tables are written in each country's own ────────────────────────

#: A cell that is a figure in SOME convention: digits with spaces, points and commas between them,
#: an optional leading minus (hyphen, U+2212, or the en dash Atlas Copco prints for `-88`) and
#: optional brackets.
_FIGURE_ANY = re.compile("^[(]?[-\u2212\u2013]?\\d[\\d .,]*[)]?$")
_DECIMAL_TAIL = re.compile(r"[.,](\d+)\)?$")


def convention_of(table: Table) -> str:
    """`point` (`1,234.5`: EDINET, English) or `comma` (`1.234,5`, `1 234,5`: French, Spanish,
    Swedish) for one table, decided by its own cells.

    - where a cell holds both marks, the later one is the decimal mark;
    - a mark followed by one, two or four-plus digits is a decimal mark;
    - a mark followed by exactly three digits is a thousands separator, which only breaks the tie:
      a lone `16.845` is Spanish thousands, a lone `10,355` English.

    Spaces are thousands separators in every convention, so they decide nothing."""
    strong = {"point": 0, "comma": 0}
    weak = {"point": 0, "comma": 0}
    for row in table.rows:
        for cell in row:
            core = cell.replace(" ", "")
            if not core or not _FIGURE_ANY.match(core):
                continue
            if "," in core and "." in core:
                strong["comma" if core.rfind(",") > core.rfind(".") else "point"] += 1
                continue
            tail = _DECIMAL_TAIL.search(core)
            if tail is None:
                continue
            mark = core[tail.start()]
            if len(tail.group(1)) != 3:
                strong["comma" if mark == "," else "point"] += 1
            else:
                weak["comma" if mark == "." else "point"] += 1
    if strong["comma"] != strong["point"]:
        return "comma" if strong["comma"] > strong["point"] else "point"
    return "comma" if weak["comma"] > weak["point"] else "point"


def in_convention(table: Table, convention: str) -> Table:
    """The table with every figure rewritten to `point`, which `number` reads: thousands marks
    and spaces dropped, a comma decimal made a point, an en-dash minus made a hyphen. Text cells
    and an en dash alone (an empty cell) are left as they are."""

    def one(cell: str) -> str:
        core = cell.replace(" ", "")
        if not core or core in _NIL or not _FIGURE_ANY.match(core):
            return cell
        if core[0] == "\u2013" or core[:2] == "(\u2013":
            core = core.replace("\u2013", "-", 1)
        if convention == "comma":
            core = core.replace(".", "").replace(",", ".")
        return core

    return Table(rows=[[one(c) for c in row] for row in table.rows], before=table.before)


#: The scales an inferred unit may take, the likeliest first.
_INFERRED_SCALES = (1e6, 1e3, 1.0, 1e9)


def scale_of(texts: Iterable[str]) -> float | None:
    """The table's unit from its `(単位：百万円)` line, or None when nothing states one."""
    found: float | None = None
    for text in texts:
        if "単位" not in text and "金額" not in text:
            continue
        for word, factor in _SCALE:
            if word in text:
                found = factor
                break
    return found


# ── Reading ─────────────────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class Line:
    """One metric line of a table: its label, and each member's value in table units."""

    label: str
    members: list[tuple[str, float]]
    total: float | None
    #: The line's eliminations (an `adjustment` column or row), when it states any: segments are
    #: often disclosed before them, so they add up to the total LESS the eliminations.
    adjustment: float = 0.0


@dataclass(slots=True)
class NoteRead:
    """What a note came to: the facts, and how many tables were skipped and why."""

    facts: list[SegmentFact] = field(default_factory=list)
    tables: int = 0
    read: int = 0
    no_kind: int = 0
    no_anchor: int = 0
    no_unit: int = 0
    skipped_kind: int = 0


def _role(label: str, vocabulary: NoteVocabulary) -> str | None:
    """The label's role, judged on its normalised form: a pattern anchored at both ends must not
    miss 「連結 財務諸表 計上額 (注)2」 for its spaces and footnote."""
    normal = member_name(label, vocabulary.spaced)
    for entry in vocabulary.labels:
        if re.search(entry.pattern, normal):
            return entry.role
    return None


def _structural(parts: Sequence[str], vocabulary: NoteVocabulary) -> str | None:
    """A structural role on ANY part of a label marks it, the deepest first: Nintendo's
    「うち、米国」 sits under 米大陸 and Yellow Hat's 計 under 報告セグメント."""
    roles = [_role(part, vocabulary) for part in parts]
    return next((r for r in reversed(roles) if r in STRUCTURAL), None)


def _kind(texts: Sequence[str], vocabulary: NoteVocabulary) -> str | None:
    """The kind named by the LAST heading among `texts` that the vocabulary knows. Within one
    heading a `skip` wins: 「報告セグメントごとののれんの償却額…に関する情報」 names the segments
    and is a goodwill table."""
    found: str | None = None
    ordered = sorted(vocabulary.headings, key=lambda h: h.kind != "skip")
    for text in texts:
        if not _is_heading(text):
            continue
        if vocabulary.spaced:
            # EUROPEAN PROSE NAMES SEVERAL KINDS IN ONE SENTENCE, and the noun nearest the table is
            # its kind: Inditex's "las ventas están basadas en la localización geográfica de los
            # clientes y … de los activos" introduces a geographic table, not a customer one.
            last = max(
                ((m.start(), e.kind) for e in vocabulary.headings
                 for m in re.finditer(e.pattern, text)),
                default=None,
            )  # fmt: skip
            if last is not None:
                found = last[1]
            continue
        for entry in ordered:
            if re.search(entry.pattern, text):
                found = entry.kind
                break
    return found


#: A note mark opening a line: 「(注)」「注1」「※」「*1」. Such a line is a footnote.
_NOTE_MARK = re.compile(r"^(\(注|注\s*\d|※|\*)")


def _is_heading(text: str) -> bool:
    """A heading is a line, not a sentence and not a footnote. Makita's note under its prior-year
    table names 「のれん」 in a footnote, and read as a heading it skipped the current year's."""
    return not text.endswith("。") and not _NOTE_MARK.match(text)


def _is_number_row(row: Sequence[str]) -> bool:
    return any(number(c) is not None and c not in _NIL for c in row)


#: A calendar year as a column heading: `2025`.
_YEAR = re.compile(r"^(19|20)\d\d$")


def _is_year_row(row: Sequence[str]) -> bool:
    """A header row naming years: Atlas Copco sets `2025` in its label cell above segment names,
    Inditex repeats `2025` over every column, ASML heads `2023 | 2024 | 2025`. Every figure in it
    is a year, and it holds two of them or a name beside one: a data row never does."""
    figures = [c for c in row if number(c) is not None and c not in _NIL]
    if not figures or not all(_YEAR.match(c.strip()) for c in figures):
        return False
    return len(figures) >= 2 or any(c.strip() and number(c) is None for c in row[1:])


def _is_header_row(row: Sequence[str], spaced: bool) -> bool:
    return not _is_number_row(row) or (spaced and _is_year_row(row))


def _column_names(rows: Sequence[Sequence[str]], head: int, spaced: bool) -> list[list[str]]:
    """Each column's header parts, top to bottom.

    `spaced` (ESEF): a European header wraps ONE label over several rows (Schneider's `Gestion de`
    over `l'énergie`), and a group header spans its columns (Inditex's `2025` over all six). So a
    column's parts are the cells that are its own, joined into one, and a year is never part of a
    name: the group header stays a part only where the column has nothing else."""
    width = len(rows[0]) if rows else 0
    out: list[list[str]] = []
    for c in range(width):
        parts: list[str] = []
        own: list[str] = []
        for r in range(head):
            text = rows[r][c]
            if not text or not member_name(text, spaced) or (parts and parts[-1] == text):
                continue
            parts.append(text)
            if spaced and not _YEAR.match(text.strip()):
                spans = (c > 0 and rows[r][c - 1] == text) or (
                    c + 1 < width and rows[r][c + 1] == text
                )
                if not spans:
                    own.append(text)
        if spaced:
            words = [p for p in parts if not _YEAR.match(p.strip())]
            out.append([" ".join(own)] if own else words[-1:] or parts[-1:])
        else:
            out.append(parts)
    return out


#: A label continued on the next row: it opens with a lower-case letter (Enel's `Revenue and other
#: income` over `from third parties`).
_CONTINUED = re.compile(r"^[a-zà-ÿ]")


def _lines(table: Table, vocabulary: NoteVocabulary) -> tuple[list[Line], list[Line]]:
    """The table read both ways: members as COLUMNS (one line per data row) and members as ROWS
    (one line per numeric column). Header rows are the leading rows holding no number.

    A label's NAME is its deepest part: a column under the group header 報告セグメント is the
    segment named beneath it, and a unit line set in a header cell is no part at all. A label with
    ANY role is never a member: a metric row (外部顧客への売上高, セグメント利益) read the other way
    would otherwise be a member whose value happens to equal the total."""
    rows = table.rows
    spaced = vocabulary.spaced
    head = 0
    while head < len(rows) and _is_header_row(rows[head], spaced):
        head += 1
    if head == len(rows):
        return [], []
    header_parts = _column_names(rows, head, spaced)
    headers = _names(header_parts, spaced)
    header_roles = [
        _structural(parts, vocabulary) or _role(name, vocabulary)
        for parts, name in zip(header_parts, headers, strict=True)
    ]
    data = rows[head:]

    # A data row's label is its text cells before the first number; the rest are values.
    def split(row: Sequence[str]) -> tuple[list[str], list[tuple[int, float]]]:
        label_parts: list[str] = []
        values: list[tuple[int, float]] = []
        for c, cell in enumerate(row):
            n = number(cell)
            if n is None or (not values and cell in _NIL and not label_parts and c == 0):
                if not values and cell and (not label_parts or label_parts[-1] != cell):
                    label_parts.append(cell)
                continue
            values.append((c, n))
        return label_parts, values

    split_rows = [split(row) for row in data]
    if spaced:
        split_rows = _continued(split_rows)
    row_names = _names([parts for parts, _ in split_rows], spaced)

    def row_label(i: int) -> tuple[str, str | None]:
        """A row's name and role (structural on any part, else the name's own)."""
        parts = split_rows[i][0]
        return row_names[i], _structural(parts, vocabulary) or _role(row_names[i], vocabulary)

    by_column: list[Line] = []
    for i, (_, values) in enumerate(split_rows):
        label, _ = row_label(i)
        members: list[tuple[str, float]] = []
        total: float | None = None
        adjustment = 0.0
        for c, value in values:
            name = headers[c] if c < len(headers) else ""
            role = header_roles[c] if c < len(header_roles) else None
            if role == "total":
                total = value  # the last one: 連結 after 合計 after 計
            elif role == "adjustment":
                adjustment += value
            elif role is not None or not member_name(name, spaced):
                continue
            else:
                members.append((member_name(name, spaced), value))
        by_column.append(
            Line(label=label, members=_merged(members), total=total, adjustment=adjustment)
        )

    by_row: list[Line] = []
    numeric_columns = sorted({c for _, values in split_rows for c, _ in values})
    for c in numeric_columns:
        members = []
        total = None
        adjustment = 0.0
        for i, (_, values) in enumerate(split_rows):
            cell = dict(values).get(c)
            if cell is None:
                continue
            name, role = row_label(i)
            if role == "total":
                total = cell
            elif role == "adjustment":
                adjustment += cell
            elif role is not None or not member_name(name, spaced):
                continue
            else:
                members.append((member_name(name, spaced), cell))
        by_row.append(Line(label=headers[c] if c < len(headers) else "", members=_merged(members),
                           total=total, adjustment=adjustment))  # fmt: skip
    return by_column, by_row


def _continued(
    rows: list[tuple[list[str], list[tuple[int, float]]]],
) -> list[tuple[list[str], list[tuple[int, float]]]]:
    """Rows whose label carries on below them, joined: a row holding only text that opens in
    lower case continues the label above it, and is no row of its own."""
    out: list[tuple[list[str], list[tuple[int, float]]]] = []
    for parts, values in rows:
        if (out and out[-1][0] and not values and len(parts) == 1
                and _CONTINUED.match(parts[0].strip())):  # fmt: skip
            prior, prior_values = out[-1]
            out[-1] = ([*prior[:-1], f"{prior[-1]} {parts[0]}"], prior_values)
            continue
        out.append((parts, values))
    return out


def _names(labels: Sequence[Sequence[str]], spaced: bool = False) -> list[str]:
    """Each label's name: its deepest part, or its whole path where the deepest part repeats under
    another parent. Asahi Diamond sets 「その他」 under アジア・オセアニア and again on its own, and
    the two are different regions."""
    deepest = [parts[-1] if parts else "" for parts in labels]
    paths: dict[str, set[tuple[str, ...]]] = {}
    for parts, name in zip(labels, deepest, strict=True):
        paths.setdefault(member_name(name, spaced), set()).add(tuple(parts))
    return [
        ("".join(parts) if not spaced else " ".join(parts))
        if len(paths[member_name(name, spaced)]) > 1 and len(parts) > 1
        else name
        for parts, name in zip(labels, deepest, strict=True)
    ]


def _merged(members: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """A member repeated by a span (the same name under two expanded columns) is one member."""
    out: dict[str, float] = {}
    for name, value in members:
        if name not in out:
            out[name] = value
    return list(out.items())


def _close(a: float, b: float, slack: float) -> bool:
    """Equal to the table's own precision, and never further than 0.05%.

    A FIGURE IS TRUNCATED BELOW THE TABLE'S UNIT (百万円未満切捨て), so a sum of n cells may miss
    the tagged figure by up to n units: `slack` is that many units, in yen. A percentage alone
    grows with the company: 0.05% of Tokio Marine's figures is ¥540 m, and its members (a 連結合計
    column among them) landed ¥246 m from an unrelated operating cash-flow subtotal and were read
    as its split (measured 2026-11-27). Units alone are no bound for a sum of a few units."""
    return abs(a - b) <= max(1.0, min(slack, abs(b) * 0.0005))


def _metric(label: str, anchors: Sequence[Anchor], vocabulary: NoteVocabulary) -> str | None:
    """The line's metric: its label first, then the matched anchors' concept names."""
    role = _role(label, vocabulary)
    if role is not None:
        return None if role in STRUCTURAL else role
    for metric, pattern in vocabulary.concepts:
        if any(re.search(pattern, a.local_name) for a in anchors):
            return metric
    return None


def read_note(
    html: str,
    anchors: Sequence[Anchor],
    vocabulary: NoteVocabulary,
    *,
    axis_prefix: str,
    axes: dict[str, str],
    default_scale: float | None = None,
    detect_numbers: bool = False,
    infer_scale: bool = False,
) -> NoteRead:
    """Every split a note's tables state, as `SegmentFact`s.

    `axes` maps a kind (`business`, `product`, `geography`) to its axis code; `axis_prefix` is the
    member namespace (`edinet:`). `default_scale` applies where a table states no unit, and None
    means such a table is skipped: a 千円 table read as 百万円 is a thousandfold error that looks
    ordinary.

    ESEF (`detect_numbers`, `infer_scale`): each table's number convention is read from its own
    cells (`convention_of`), and where no unit line is found the scale is the one of
    `_INFERRED_SCALES` under which the table places a split: a split is placed only when it adds up
    to a figure the filing tags to the unit, which no wrong scale can do. A table of places reads as
    geography whatever its heading (`members`).
    """
    out = NoteRead()
    kind: str | None = None
    scale: float | None = default_scale
    placed: set[tuple[str, str, str, str]] = set()
    for table in tables(html):
        out.tables += 1
        if detect_numbers:
            table = in_convention(table, convention_of(table))
        own = _kind(table.before, vocabulary)
        # A KIND NAMED IN EUROPEAN PROSE IS LOCAL TO ITS TABLE. Iberdrola introduces a table of
        # non-current assets "by geographic location" and follows it with its business segments
        # under no sentence at all; carried over, the business table read as geography. The
        # block is the segment note, so a table it says nothing about is business, unless its
        # members are places (`_member_kind`).
        kind = (own or "business") if vocabulary.spaced else (own or kind)
        scale = scale_of([*table.before, *(" ".join(r) for r in table.rows[:3])]) or scale
        table_kind = kind
        if kind is not None and kind != "skip":
            table_kind = _member_kind(table, vocabulary) or kind
        if table_kind is None:
            out.no_kind += 1
            continue
        if table_kind == "skip" or table_kind not in axes:
            out.skipped_kind += 1
            continue
        if scale is None and not infer_scale:
            out.no_unit += 1
            continue
        hint = next(
            (_role(t, vocabulary) for t in reversed(table.before)
             if _role(t, vocabulary) not in (None, *STRUCTURAL)),
            None,
        )  # fmt: skip
        if scale is not None:
            facts = _read_table(table, anchors, vocabulary, scale, table_kind, hint,
                                axis=axes[table_kind], prefix=axis_prefix)  # fmt: skip
        else:
            facts = []
            for candidate in _INFERRED_SCALES:
                facts = _read_table(table, anchors, vocabulary, candidate, table_kind, hint,
                                    axis=axes[table_kind], prefix=axis_prefix)  # fmt: skip
                if any(f.partition_id for f in facts):
                    break
            else:
                facts = []
        if vocabulary.spaced:
            # THE FIRST TABLE TO STATE A SPLIT KEEPS IT. LVMH restates its business lines in a
            # quarterly table whose `Total 2024` row is gross of intra-group sales; read as a second
            # split of the same year, every member would arrive twice with two values.
            facts = [f for f in facts if (f.axis, f.metric_code, f.period_type, f.period_ending)
                     not in placed]  # fmt: skip
        if facts:
            out.read += 1
            out.facts += facts
            placed |= {(f.axis, f.metric_code, f.period_type, f.period_ending)
                       for f in facts if f.partition_id}  # fmt: skip
        else:
            out.no_anchor += 1
    return out


def role_of(label: str, vocabulary: NoteVocabulary) -> str | None:
    """A label's role (a metric code or a structural role), as the reader judges it."""
    return _role(label, vocabulary)


def _member_kind(table: Table, vocabulary: NoteVocabulary) -> str | None:
    """The kind most of a table's member names imply, or None. Members are the header cells, or
    the row labels, that the vocabulary gives no role (a metric, a total, an elimination)."""
    if not vocabulary.members or not table.rows:
        return None
    candidates = [table.rows[0][1:], [row[0] for row in table.rows[1:] if row]]
    if vocabulary.spaced:
        # The header's own names (a wrapped label joined, years dropped) and the row labels.
        rows = table.rows
        head = 0
        while head < len(rows) and _is_header_row(rows[head], True):
            head += 1
        if head < len(rows):
            columns = _names(_column_names(rows, head, True), True)
            candidates = [columns[1:], [row[0] for row in rows[head:] if row]]
    for cells in candidates:
        # A dash or a figure is no name: Inditex sets `-` in its label column above the regions.
        names = [
            c for c in cells
            if c.strip() and _role(c, vocabulary) is None
            and not (vocabulary.spaced and (c in _NIL or number(c) is not None))
        ]  # fmt: skip
        if len(names) < 2:
            continue
        kinds = [
            next(
                (
                    m.kind
                    for m in vocabulary.members
                    if re.search(m.pattern, member_name(c, vocabulary.spaced))
                ),
                None,
            )
            for c in names
        ]
        for kind in {k for k in kinds if k}:
            if 2 * kinds.count(kind) > len(names):
                return kind
    return None


def read_grid(
    table: Table,
    anchors: Sequence[Anchor],
    vocabulary: NoteVocabulary,
    *,
    scale: float,
    axes: dict[str, str],
    prefix: str,
    to_rounding: bool = False,
) -> list[SegmentFact]:
    """A grid built elsewhere (China's notes are PDF positions, not HTML) read as `read_note` reads
    one of its tables: its kind from `table.before`, and nothing for a kind `axes` does not name.

    `to_rounding`: a split must add up to its anchor within the table's own rounding (one unit per
    member and one more), not the 0.5% XBRL's millions need. A note whose figures are stated to the
    unit then cannot place a subset that happens to come close: Bank of China's 2024 members less
    投资银行业务 come within 0.42% of the gross total its elimination column explains."""
    kind = _kind(table.before, vocabulary)
    if kind is not None and kind != "skip":
        kind = _member_kind(table, vocabulary) or kind
    if kind is None or kind == "skip" or kind not in axes:
        return []
    hint = next(
        (_role(t, vocabulary) for t in reversed(table.before)
         if _role(t, vocabulary) not in (None, *STRUCTURAL)),
        None,
    )  # fmt: skip
    return _read_table(
        table, anchors, vocabulary, scale, kind, hint, axis=axes[kind], prefix=prefix,
        to_rounding=to_rounding,
    )  # fmt: skip


def _read_table(
    table: Table,
    anchors: Sequence[Anchor],
    vocabulary: NoteVocabulary,
    scale: float,
    kind: str,
    hint: str | None,
    *,
    axis: str,
    prefix: str,
    to_rounding: bool = False,
) -> list[SegmentFact]:
    by_column, by_row = _lines(table, vocabulary)
    best: list[SegmentFact] = []
    for lines in (by_column, by_row):
        facts = _read_lines(
            lines, anchors, vocabulary, scale, hint, axis=axis, prefix=prefix,
            to_rounding=to_rounding,
        )  # fmt: skip
        if len({f.member_code for f in facts}) > len({f.member_code for f in best}):
            best = facts
    return best


@dataclass(slots=True)
class _Match:
    line: Line
    anchor: Anchor
    metric: str
    reconciles: bool


def _read_lines(
    lines: Sequence[Line],
    anchors: Sequence[Anchor],
    vocabulary: NoteVocabulary,
    scale: float,
    hint: str | None,
    *,
    axis: str,
    prefix: str,
    to_rounding: bool = False,
) -> list[SegmentFact]:
    matched: list[_Match] = []
    for line in lines:
        if len(line.members) < 2:
            continue
        member_sum = sum(v for _, v in line.members) * scale
        total = None if line.total is None else line.total * scale
        # One unit for each member's truncation, and one more for the tagged figure's own rounding.
        sum_slack, total_slack = scale * (len(line.members) + 1), scale * 2
        hits = [a for a in anchors if (total is not None and _close(total, a.value, total_slack))
                or _close(member_sum, a.value, sum_slack)]  # fmt: skip
        if not hits:
            continue
        if vocabulary.spaced and line.adjustment:
            # SEGMENTS STATED BEFORE THEIR ELIMINATIONS add up to the total less them (Inditex's
            # three segments sum €40,107 m beside intersegment (243) and a total of €39,864 m).
            # The derived figure is offered beside the tagged one, never instead of it: only the
            # members' own sum can choose it.
            hits += [
                replace(a, value=a.value - line.adjustment * scale)
                for a in hits
                if _close(member_sum, a.value - line.adjustment * scale, sum_slack)
            ]
        labelled = _metric(line.label, (), vocabulary)
        # A LINE NAMED BY NO LABEL OF ITS OWN must earn its metric by arithmetic: its members sum
        # to a fact whose concept names that metric. A total cell alone is any 計 in the table.
        summed = [a for a in hits if _close(member_sum, a.value, sum_slack)]
        metric = labelled or _metric(line.label, summed, vocabulary)
        metric = metric or (hint if not line.label and summed else None)
        if metric is None:
            continue
        anchor = min(hits, key=lambda a: abs(member_sum - a.value))
        matched.append(_Match(line, anchor, metric, _close(member_sum, anchor.value, sum_slack)))
    if not matched:
        return []

    # ONE LINE PER (metric, period): the one whose members reconcile, else the first. Toyota's 計
    # row (with intersegment sales) totals to the same 連結 figure as its external-revenue row.
    chosen: dict[tuple[str, str | None, str | None], _Match] = {}
    for m in matched:
        key = (m.metric, m.anchor.end or m.anchor.instant, m.anchor.start)
        prior = chosen.get(key)
        if prior is None or (m.reconciles and not prior.reconciles):
            chosen[key] = m

    def partitions_of(m: _Match) -> dict[str, int]:
        members = [(f"{prefix}{name}", value * scale) for name, value in m.line.members]
        if to_rounding and m.anchor.value:
            slack = scale * (len(members) + 1)
            return assign_partitions(members, m.anchor.value, slack / abs(m.anchor.value))
        return assign_partitions(members, m.anchor.value)

    # THE SPLIT IS LEARNED FROM REVENUE AND APPLIED TO THE REST of the same table and period end:
    # segment profit and assets reconcile to nothing by design (unallocated costs, corporate
    # assets), so on their own they would place no member at all.
    learned = {
        m.anchor.end or m.anchor.instant: partitions_of(m)
        for m in chosen.values()
        if m.metric == "revenue"
    }

    facts: list[SegmentFact] = []
    for m in chosen.values():
        period_end = m.anchor.end or m.anchor.instant
        if period_end is None:
            continue
        if m.anchor.start is not None:
            period_type = period_type_for(m.anchor.start, period_end)
            if period_type is None:
                continue
        else:
            period_type = "instant"
        partitions = partitions_of(m)
        inherited = False
        if not any(partitions.values()) and period_end in learned:
            partitions = {code: learned[period_end].get(code, 0) for code in partitions}
            inherited = True
        for name, value in m.line.members:
            code = f"{prefix}{name}"
            facts.append(
                SegmentFact(
                    axis=axis,
                    member_code=code,
                    metric_code=m.metric,
                    period_type=period_type,
                    period_start=m.anchor.start or period_end,
                    period_ending=period_end,
                    value=value * scale,
                    currency=m.anchor.currency,
                    parent_axis=None,
                    parent_member=None,
                    reconciled_to=m.anchor.value if partitions[code] and not inherited else None,
                    partition_id=partitions[code],
                )
            )
    return facts


def anchors_of(
    facts: Iterable[tuple[str, str, str | None, float]],
    contexts: Mapping[str, tuple[str | None, str | None, str | None]],
    currencies: Mapping[str, str | None],
) -> list[Anchor]:
    """`(concept, context, unit, value)` of NON-DIMENSIONAL contexts -> anchors. A context the map
    does not hold (a dimensional one, a non-consolidated one) is not consolidated and anchors
    nothing; neither does a unit that is not money."""
    out: list[Anchor] = []
    for concept, context, unit, value in facts:
        period = contexts.get(context)
        currency = currencies.get(unit) if unit else None
        if period is None or currency is None or not math.isfinite(value):
            continue
        start, end, instant = period
        out.append(Anchor(concept, value, start, end, instant, currency))
    return out
