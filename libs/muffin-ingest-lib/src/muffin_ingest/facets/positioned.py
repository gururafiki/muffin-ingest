"""A page converted to absolutely positioned spans, rebuilt as paragraphs and tables.

ABOUT ONE EUROPEAN NOTE IN ELEVEN HAS NO `<table>` AT ALL. Measured 2026-11-28 over every ESEF
report production held, 221 of 2,366 since FY2022 (most of Denmark's and Norway's, Novo Nordisk's
among them) tag the segment note as a page converted from PDF: `position:relative` containers
holding absolutely positioned line spans. The segment reader reads tables, so it read none of them.

The layout is enough to rebuild the grid:
  - a line span states `left`, `top` and `width`, and holds text runs separated by spacer spans of
    stated `width`;
  - rows are the spans sharing a `top`;
  - a run's extent is its span's width shared out by Helvetica's glyph widths (a span's first and
    last runs have exact outer edges);
  - columns are the right edges of the figures, where right-aligned amounts end;
  - header pieces are placed over the columns (see `_header_cells`).

What comes out is ordinary `<table>` HTML for `segment_note.read_note`, which then applies every
rule it applies to a tagged table: the anchors, the kinds, the partitions. Nothing here decides what
a table means. Prose lines become paragraphs, so a heading or a sentence naming the kind stays where
the reader looks for it.
"""

from __future__ import annotations

import html as htmllib
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser

#: A length in a style attribute. The lookbehind stops `margin-left: 0px` reading as `left`.
_PX = re.compile(r"(?<![\w-])(left|top|width|font-size)\s*:\s*(-?[\d.]+)px")
_POSITION = re.compile(r"position\s*:\s*(absolute|relative)")
#: A gap under this share of the font size is a kern inside a word.
KERN = 0.25
#: A gap under this share of the font size is a space between words; wider, a break between cells.
BREAK = 1.0
#: Two spans meeting within this share of the font size are one word (`Sydamer` | `ika`).
TOUCH = 0.1
#: Right edges this close (px) are one column.
COLUMN = 6.0
#: A single line longer than this, or wider than this share of its container, is prose.
PROSE_CHARS = 60
PROSE_WIDTH = 0.6
#: A short line before a table indented past this share of the container is a header over columns.
PROSE_INDENT = 0.25
#: How many absolutely positioned spans make a block a converted page.
POSITIONED = 20

#: Helvetica's advance widths in thousandths of an em: how a span's width is shared out between its
#: runs. A flat width per character drifted far enough over a long line of Swedish figures
#: (`715 944`) to invent columns.
_WIDTHS: dict[str, int] = {
    **dict.fromkeys("0123456789", 556),
    " ": 278,
    ",": 278,
    ".": 278,
    "-": 333,
    "(": 333,
    ")": 333,
    "%": 889,
    "/": 278,
    "&": 667,
    "*": 389,
    "'": 191,
    **dict(
        zip(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            (
                667,
                667,
                722,
                722,
                667,
                611,
                778,
                722,
                278,
                500,
                667,
                556,
                833,
                722,
                778,
                667,
                778,
                722,
                667,
                611,
                722,
                667,
                944,
                667,
                667,
                611,
            ),
            strict=True,
        )
    ),
    **dict(
        zip(
            "abcdefghijklmnopqrstuvwxyz",
            (
                556,
                556,
                500,
                556,
                556,
                278,
                556,
                556,
                222,
                222,
                500,
                222,
                833,
                556,
                556,
                556,
                556,
                333,
                500,
                278,
                556,
                500,
                722,
                500,
                500,
                500,
            ),
            strict=True,
        )
    ),
}

_DIGITISH = re.compile(r"[\d.,%)(]")
#: An amount that may follow a label in one run: thousands groups, decimals, or three digits.
_AMOUNT = re.compile(
    r"^\(?[-\u2013\u2212]?(\d{1,3}([,. ]\d{3})+([.,]\d+)?|\d+[.,]\d+|\d{3,})\)?%?$"
)
#: A figure cell, a dash standing for nothing included.
_FIGURE = re.compile(r"^\(?[-\u2013\u2212]?\s*\d[\d.,\s\u00a0]*\)?%?$|^[-\u2013\u2014]$")
_YEAR = re.compile(r"^(19|20)\d\d$")
#: One footnote marker ending a run: `1)` after a space or a letter or alone (`Revenue goods 1)`,
#: `Biologics1)`), so the `12)` of `(12)` is never one; or a superscript (`Biologics¹⁾`). Stripped
#: one at a time by `_strip_footnotes`, which keeps the pattern free of nested repetition.
_FOOTNOTE = re.compile(
    r"(?:(?:^|(?<=\s)|(?<=[^\W\d_]))\d{1,2}\)|[\u00b9\u00b2\u00b3\u2070-\u2079\u207d\u207e])$"
)
_BLOCK_TAGS = frozenset({"p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6"})


def is_positioned(html: str) -> bool:
    """A block is a converted page when it holds no table and many absolutely positioned spans."""
    if "<table" in html:
        return False
    return len(re.findall(r"position\s*:\s*absolute", html)) > POSITIONED


def rebuild(html: str) -> str:
    """The block with every positioned container rebuilt as paragraphs and tables."""
    tree = _Tree()
    tree.feed(html)
    out: list[str] = []
    _walk(tree.root, out)
    return "".join(out)


# ── The tree ───────────────────────────────────────────────────────────────────────────────────


@dataclass
class _Node:
    tag: str
    style: dict[str, float]
    position: str | None
    block: bool
    children: list[_Node | str] = field(default_factory=list)


class _Tree(HTMLParser):
    VOID = frozenset({"br", "img", "hr", "meta", "input", "col", "wbr"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _Node("root", {}, None, False)
        self.stack = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        style = dict(attrs).get("style") or ""
        position = _POSITION.search(style)
        node = _Node(
            tag,
            {k: float(v) for k, v in _PX.findall(style)},
            position.group(1) if position else None,
            tag in _BLOCK_TAGS or "clear:left" in style.replace(" ", ""),
        )
        self.stack[-1].children.append(node)
        if tag == "br":
            node.children.append("\n")
        if tag not in self.VOID:
            self.stack.append(node)

    def handle_endtag(self, tag: str) -> None:
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                return

    def handle_data(self, data: str) -> None:
        self.stack[-1].children.append(data)


def _text(node: _Node | str) -> str:
    return node if isinstance(node, str) else "".join(_text(c) for c in node.children)


def _walk(node: _Node | str, out: list[str]) -> None:
    if isinstance(node, str):
        if node.strip():
            out.append(htmllib.escape(node))
        return
    if node.position == "relative":
        out.append(_container(node))
        return
    if node.position == "absolute":
        out.append(_container(_Node("div", {}, "relative", True, [node])))
        return
    if node.block:
        out.append("<p>")
    for child in node.children:
        _walk(child, out)
    if node.block:
        out.append("</p>")


# ── Lines and runs ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _Run:
    text: str
    x0: float
    x1: float
    first: bool = False  # its span's first run: x0 is exact
    last: bool = False  # its span's last run: x1 is exact
    open_end: bool = False  # its span's text ends inside a word, with no trailing space


@dataclass
class _Line:
    top: float
    size: float
    runs: list[_Run]


def _weight(text: str) -> float:
    return float(sum(_WIDTHS.get(c, 556) for c in text)) or 556.0


def _line(node: _Node) -> _Line:
    """One positioned span: its runs, with their extents.

    Its children are text pieces and spacer spans of stated width, and consecutive spacers are one
    gap. A gap under `KERN` of the font size is a kern inside a word (`T` | `axation`); under
    `BREAK`, a space between words, though never between two digits; wider, a break between cells.
    """
    items: list[tuple[str, float | None]] = []

    def gather(n: _Node | str) -> None:
        if isinstance(n, str):
            items.append((n, None))
        elif "width" in n.style and not _text(n).strip():
            items.append(("", n.style["width"]))
        else:
            for child in n.children:
                gather(child)

    for child in node.children:
        gather(child)
    size = node.style.get("font-size", 10.0)
    runs: list[list[object]] = []  # [text, gap before, gaps inside]
    gap = 0.0
    for text, width in items:
        if width is not None:
            gap += width
            continue
        if not text:
            continue
        if runs and str(runs[-1][0]).strip() and not text[:1].isspace() and gap < BREAK * size:
            prev = str(runs[-1][0])
            digits = bool(_DIGITISH.match(prev[-1:]) and _DIGITISH.match(text[:1]))
            tight = gap < KERN * size or digits
            runs[-1][0] = prev + ("" if tight or gap == 0.0 else " ") + text
            runs[-1][2] = float(runs[-1][2]) + gap  # type: ignore[arg-type]
        elif runs and (not text.strip() or gap == 0.0):
            runs[-1][0] = str(runs[-1][0]) + text
        else:
            runs.append([text, gap, 0.0])
        gap = 0.0
    kept = [(str(t), float(b), float(i)) for t, b, i in runs if str(t).strip()]  # type: ignore[arg-type]
    texts = [re.sub(r"\s+", " ", _strip_footnotes(t)).strip() for t, _, _ in kept]
    spacers = sum(b + i for _, b, i in kept)
    per = max(node.style.get("width", 0.0) - spacers, 0.0) / (sum(map(_weight, texts)) or 1.0)
    x = node.style.get("left", 0.0)
    out: list[_Run] = []
    for (_, before, inside), clean in zip(kept, texts, strict=True):
        x += before
        width = per * _weight(clean) + inside
        if clean:
            out.extend(_split_amounts(_Run(clean, x, x + width), per))
        x += width
    if out:
        out[0].first = True
        out[-1].last = True
        out[-1].open_end = not kept[-1][0][-1:].isspace()
    return _Line(node.style.get("top", 0.0), size, out)


def _strip_footnotes(text: str) -> str:
    """The run without the footnote markers ending it (`Revenue goods 1) 2)` is `Revenue goods`)."""
    text = text.rstrip()
    while (marker := _FOOTNOTE.search(text)) is not None:
        text = text[: marker.start()].rstrip()
    return text


def _split_amounts(run: _Run, per: float) -> list[_Run]:
    """`Pacific Basin 42,726` as a label and a figure: a narrow column can put a figure one space
    from its label, in one run. The figure ends where the run does."""
    words = run.text.split(" ")
    k = len(words)
    while k > 0 and _AMOUNT.match(words[k - 1]):
        k -= 1
    label = " ".join(words[:k])
    if k == len(words) or k == 0 or not re.search(r"[^\W\d_]", label):
        return [run]
    out = [_Run(label, run.x0, run.x0 + per * _weight(label))]
    x = run.x0 + per * _weight(label + " ")
    for word in words[k:]:
        out.append(_Run(word, x, x + per * _weight(word)))
        x += per * _weight(word + " ")
    out[-1].x1 = run.x1
    return out


def _is_figure(text: str) -> bool:
    return bool(_FIGURE.match(text.strip()))


def _data_row(row: list[_Run]) -> bool:
    """A row holding a figure that is not a bare year. A row of years places no column."""
    return any(_is_figure(r.text) and not _YEAR.match(r.text) for r in row)


# ── Containers ─────────────────────────────────────────────────────────────────────────────────


def _container(node: _Node) -> str:
    """A container's lines in reading order: rows holding figures or several cells become a table,
    single-run rows between them paragraphs."""
    lines: list[_Line] = []

    def collect(n: _Node) -> None:
        for child in n.children:
            if isinstance(child, _Node):
                if child.position == "absolute":
                    lines.append(_line(child))
                else:
                    collect(child)

    collect(node)
    lines = sorted((ln for ln in lines if ln.runs), key=lambda ln: ln.top)
    rows: list[tuple[float, float, list[_Run]]] = []  # (top, font size, runs)
    for ln in lines:
        if rows and abs(ln.top - rows[-1][0]) <= 0.4 * ln.size:
            rows[-1][2].extend(ln.runs)
        else:
            rows.append((ln.top, ln.size, list(ln.runs)))
    out: list[str] = []
    block: list[list[_Run]] = []

    def flush() -> None:
        if not block:
            return
        if any(_is_figure(r.text) for row in block for r in row):
            out.append(_table(block))
        else:
            out.extend(f"<p>{htmllib.escape(' '.join(r.text for r in row))}</p>" for row in block)
        block.clear()

    joined = _pair_labels(
        [
            (top, size, _join_split_words(sorted(row, key=lambda r: r.x0), size))
            for top, size, row in rows
        ]
    )
    margin = min((r.x0 for row in joined for r in row), default=0.0)
    extent = max((r.x1 for row in joined for r in row), default=0.0) - margin
    for i, row in enumerate(joined):
        if len(row) > 1 or any(_is_figure(r.text) for r in row):
            block.append(row)
            continue
        text = row[0].text
        if len(text) > PROSE_CHARS or row[0].x1 - row[0].x0 > PROSE_WIDTH * extent:
            flush()
            out.append(f"<p>{htmllib.escape(text)}</p>")
        elif block or _opens_table(row[0], joined[i + 1 :], margin, extent):
            # A SHORT LINE INSIDE A TABLE is one of its rows (`Hydra-`, `allocated`, `EUR million`):
            # as a paragraph it would end the table and turn the headers above it into prose.
            block.append(row)
        else:
            out.append(f"<p>{htmllib.escape(text)}</p>")
    flush()
    return "".join(out)


def _pair_labels(rows: list[tuple[float, float, list[_Run]]]) -> list[list[_Run]]:
    """A LABEL SET A LITTLE ABOVE OR BELOW ITS FIGURES is their row's label. GN Store Nord centres
    each region's name in a taller row than its figures, so `Europe` is a line of its own just
    under a line of figures with no label; read apart, the figures belong to no region and the
    divisions across the top were filed as geography. A label-only line goes to the nearer
    adjacent row of figures with no label, within one line height."""

    def unlabelled(row: list[_Run]) -> bool:
        return bool(row) and _is_figure(row[0].text) and _data_row(row)

    def label_only(row: list[_Run]) -> bool:
        return len(row) == 1 and not _is_figure(row[0].text)

    taken: dict[int, int] = {}  # figure row -> label row
    for j, (top, size, row) in enumerate(rows):
        if not label_only(row):
            continue
        near = [
            (abs(rows[i][0] - top), i)
            for i in (j - 1, j + 1)
            if 0 <= i < len(rows)
            and unlabelled(rows[i][2])
            and abs(rows[i][0] - top) < size
            and row[0].x1 < rows[i][2][0].x0
        ]
        if not near:
            continue
        distance, i = min(near)
        if i not in taken or distance < abs(rows[taken[i]][0] - rows[i][0]):
            taken[i] = j
    used = set(taken.values())
    return [
        [rows[taken[i]][2][0], *row] if i in taken else row
        for i, (_, _, row) in enumerate(rows)
        if i not in used
    ]


def _opens_table(run: _Run, after: list[list[_Run]], margin: float, extent: float) -> bool:
    """A short line before a table is a heading, unless it is the first line of a wrapped label
    (it ends in a hyphen, or the next row's label goes on in lower case) or a header over the figure
    columns (it sits indented past the label column)."""
    if not after:
        return False
    following = after[0][0].text if after[0] else ""
    return (
        run.text.endswith("-") or following[:1].islower() or run.x0 > margin + PROSE_INDENT * extent
    )


def _join_split_words(row: list[_Run], size: float) -> list[_Run]:
    """ONE WORD SPLIT ACROSS TWO SPANS (`Sydamer` | `ika`): the first span's text ends inside a
    word and the second starts where it stops. A span's stated width can run on to the next one,
    so meeting edges alone prove nothing: `Reported ` ends in a space and stays a word of its
    own."""
    out: list[_Run] = []
    for r in row:
        prev = out[-1] if out else None
        if (
            prev is not None
            and prev.last
            and prev.open_end
            and r.first
            and abs(r.x0 - prev.x1) < TOUCH * size
            and prev.text[-1:].isalpha()
            and r.text[:1].isalpha()
        ):
            out[-1] = _Run(prev.text + r.text, prev.x0, r.x1, prev.first, r.last, r.open_end)
        else:
            out.append(r)
    return out


# ── Tables ─────────────────────────────────────────────────────────────────────────────────────


@dataclass
class _Built:
    label: str
    cells: list[list[str]]
    spans: dict[int, int]
    data: bool


def _table(rows: list[list[_Run]]) -> str:
    """Rows of runs as one HTML table. Columns are the right edges of the data rows' figures."""
    edges = sorted(r.x1 for row in rows if _data_row(row) for r in row if _is_figure(r.text))
    clusters: list[list[float]] = []
    for e in edges:
        if clusters and e - clusters[-1][-1] <= COLUMN:
            clusters[-1].append(e)
        else:
            clusters.append([e])
    cols = [sum(c) / len(c) for c in clusters]
    if not cols:
        return ""
    first = min(r.x0 for row in rows if _data_row(row) for r in row if _is_figure(r.text))
    # A column's centre is where its figures sit; it owns the stretch between the midpoints to its
    # neighbours' centres.
    placed: list[list[float]] = [[] for _ in cols]
    for row in rows:
        if _data_row(row):
            for r in row:
                if _is_figure(r.text):
                    placed[_nearest(cols, r.x1)].append((r.x0 + r.x1) / 2)
    centres = [sum(v) / len(v) if v else c - 10.0 for v, c in zip(placed, cols, strict=True)]
    stretches = [
        (
            (centres[k - 1] + centres[k]) / 2 if k else first - 2.0,
            (centres[k] + centres[k + 1]) / 2 if k + 1 < len(cols) else cols[k] + COLUMN,
        )
        for k in range(len(cols))
    ]
    split: list[tuple[str, list[_Run], bool]] = []
    for row in rows:
        row = sorted(row, key=lambda r: r.x0)
        if _data_row(row):
            # A DATA ROW'S LABEL IS WHAT COMES BEFORE ITS OWN FIRST FIGURE: a long label can reach
            # past another row's first figure.
            k = next(i for i, r in enumerate(row) if _is_figure(r.text))
            split.append((" ".join(r.text for r in row[:k]), row[k:], True))
        else:
            label = " ".join(r.text for r in row if r.x1 < first - 2.0)
            split.append((label, [r for r in row if r.x1 >= first - 2.0], False))
    groups = _period_groups(split, len(cols))
    built: list[_Built] = []
    for n, (label, rest, data) in enumerate(split):
        cells: list[list[str]] = [[] for _ in cols]
        spans: dict[int, int] = {}
        for r in rest:
            if data or _is_figure(r.text):
                cells[_nearest(cols, r.x1)].append(r.text)
            else:
                _place_header(r, cells, spans, centres, stretches, groups.get(n, 0))
        built.append(_Built(label, cells, spans, data))
    _join_hyphenated_headers(built)
    return _html(_join_wrapped_labels(built), len(cols))


def _nearest(points: list[float], x: float) -> int:
    return min(range(len(points)), key=lambda k: abs(points[k] - x))


def _period_groups(split: list[tuple[str, list[_Run], bool]], n: int) -> dict[int, int]:
    """For each header row, the size of the column groups its run of header rows sits over.

    A SEGMENTS-BY-YEARS TABLE (GN Store Nord's `Hearing` over 2024 and 2023) has a header row of one
    year per column, the years repeating. The number of distinct years is the group size; 0 when
    the run has no such row."""
    out: dict[int, int] = {}
    i = 0
    while i < len(split):
        if split[i][2]:
            i += 1
            continue
        j = i
        while j < len(split) and not split[j][2]:
            j += 1
        size = 0
        for k in range(i, j):
            years = [r.text for r in split[k][1]]
            distinct = len(set(years))
            if (
                len(years) == n
                and all(_YEAR.match(y) for y in years)
                and 1 < distinct < n
                and n % distinct == 0
                and years == years[:distinct] * (n // distinct)
            ):
                size = distinct
        out.update(dict.fromkeys(range(i, j), size))
        i = j
    return out


def _place_header(
    r: _Run,
    cells: list[list[str]],
    spans: dict[int, int],
    centres: list[float],
    stretches: list[tuple[float, float]],
    group: int,
) -> None:
    """A header piece over its columns.

    Above a row of repeating years it names the group of columns under its centre. Otherwise it
    spans the columns whose stretches it covers at least half of each, or else sits over the column
    nearest its centre, so both halves of a wrapped name land in one column."""
    mid = (r.x0 + r.x1) / 2
    if group and not _YEAR.match(r.text):
        k = _nearest(centres, mid) // group * group
        cells[k].append(r.text)
        spans[k] = group
        return
    overlap = [max(0.0, min(r.x1, hi) - max(r.x0, lo)) for lo, hi in stretches]
    wide = [k for k, (lo, hi) in enumerate(stretches) if overlap[k] / max(hi - lo, 1.0) >= 0.5]
    if len(wide) >= 2:
        cells[wide[0]].append(r.text)
        spans[wide[0]] = max(spans.get(wide[0], 1), wide[-1] - wide[0] + 1)
        return
    cells[_nearest(centres, mid)].append(r.text)


def _join_hyphenated_headers(built: list[_Built]) -> None:
    """A HYPHENATED HEADER continues in the next header row of its column: `Elimine-` over `ringar`
    is `Elimineringar`, and `Hydra-` over `Specma` is `Hydra-Specma`."""
    for n, row in enumerate(built):
        if row.data:
            continue
        for k, texts in enumerate(row.cells):
            if not texts or k in row.spans or not texts[-1].endswith("-"):
                continue
            for below in built[n + 1 :]:
                if below.data:
                    break
                cell = below.cells[k]
                if cell and k not in below.spans:
                    head = texts[-1][:-1] if cell[0][:1].islower() else texts[-1]
                    texts[-1] = head + cell.pop(0)
                    break


def _join_wrapped_labels(built: list[_Built]) -> list[_Built]:
    """A WRAPPED LABEL: a table puts the figures on a label's last line, so a label-only row joins
    the row below when that row's label goes on in lower case or the line above ends in a hyphen
    (`Other segments and un-` / `allocated 12 34`)."""
    out: list[_Built] = []
    for row in built:
        above = out[-1] if out else None
        if (
            above is not None
            and above.label
            and not any(above.cells)
            and row.label
            and (row.label[:1].islower() or above.label.endswith("-"))
        ):
            hyphen = above.label.endswith("-")
            label = above.label[:-1] + row.label if hyphen else f"{above.label} {row.label}"
            out[-1] = _Built(label, row.cells, row.spans, row.data)
        else:
            out.append(row)
    return out


def _html(built: list[_Built], n: int) -> str:
    out = ["<table>"]
    for row in built:
        tds = [f"<td>{htmllib.escape(row.label)}</td>"]
        k = 0
        while k < n:
            span = row.spans.get(k, 1)
            attr = f' colspan="{span}"' if span > 1 else ""
            tds.append(f"<td{attr}>{htmllib.escape(' '.join(row.cells[k]))}</td>")
            k += span
        out.append("<tr>" + "".join(tds) + "</tr>")
    out.append("</table>")
    return "".join(out)
