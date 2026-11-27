"""Japan's segment lane: EDINET's filers, the annual reports a day's list names, and the segment
note each report files as a text block.

WHY A NOTE AND NOT AXES. An EDINET annual report tags its primary statements in XBRL and files
the segment note (セグメント情報等) as one text block of escaped HTML: Nintendo's FY2026
instance carries eight dimensions and no segment axis. So the split is read from the note's
tables by `facets.segment_note`, against the report's own tagged consolidated figures, and
written through the same `SegmentFact` rows, partitions and writer as every other regulator's.

WHICH FIGURES ANCHOR. A company with subsidiaries files consolidated statements, and its note is
the consolidated one (`…ConsolidatedFinancialStatements…TextBlock`); its figures sit in the
non-dimensional contexts (`CurrentYearDuration`). A company with none files only its own
statements: its note is `NotesSegmentInformationEtcFinancialStatementsTextBlock` and its figures
carry `ConsolidatedOrNonConsolidatedAxis = NonConsolidatedMember`. Measured 2026-11-27:
神戸天然物化学 and カネソウ read nothing until their non-consolidated figures could
anchor. The block's role (`market.segment_note_rule`) says which; a consolidated note never
anchors on a non-consolidated figure.

THE VOCABULARY IS DATA: block concepts, headings, labels and metric concepts are rows of
`market.segment_note_rule`, and the axes are `market.segment_axis` rows of taxonomy `edinet`.
"""

from __future__ import annotations

import csv
import html
import io
import json
import re
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from muffin_ingest.facets import segment_note as sn
from muffin_ingest.facets import segments
from muffin_ingest.providers import edinet

REGULATOR = "edinet"
#: `market.data_source` code of every row this lane writes.
SOURCE = "edinet"
#: `market.filing_form` code: the annual report (有価証券報告書).
ANNUAL = "有価証券報告書"
#: The member namespace (`edinet:自動車`).
PREFIX = "edinet:"

#: An instance larger than this is refused unread. The largest measured is 13.2 MB (MUFG).
MAX_INSTANCE_BYTES = 64 * 1024 * 1024

_NON_CONSOLIDATED = ("ConsolidatedOrNonConsolidatedAxis", "NonConsolidatedMember")


# ── The code list ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Filer:
    edinet_code: str
    local_code: str
    name: str


def code_list(archive: bytes) -> dict[str, Filer]:
    """The code list ZIP -> four-character local code -> filer. A filer with no securities code is
    unlisted and skipped. Measured 2026-11-27: 11,469 filers, 3,812 with a code, which is the local
    code plus a check digit (`72030`, `409A0`); the local code is what `.T` symbols carry."""
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise ValueError("EDINET's code list archive holds no CSV")
        text = z.read(names[0]).decode("cp932")
    lines = text.splitlines()
    rows = list(csv.reader(lines[1:]))
    if not rows or not rows[0] or "コード" not in rows[0][0]:
        raise ValueError(f"EDINET's code list has an unknown header: {rows[:1]!r}")
    header = [sn.clean(h) for h in rows[0]]
    edinet_at, code_at, name_at = (
        header.index("EDINETコード"),
        header.index("証券コード"),
        header.index("提出者名"),
    )
    out: dict[str, Filer] = {}
    for row in rows[1:]:
        if len(row) <= code_at:
            continue
        code = row[code_at].strip()
        if len(code) != 5:
            continue
        local = code[:4]
        out.setdefault(local, Filer(row[edinet_at].strip(), local, row[name_at].strip()))
    return out


# ── A day's list ───────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Listed:
    """One document a day's list names, as EDINET states it."""

    doc_id: str
    edinet_code: str
    doc_type: str
    ordinance: str
    form: str
    period_start: str | None
    period_end: str | None
    submitted: str | None
    withdrawn: bool
    xbrl: bool

    @property
    def is_annual(self) -> bool:
        a = edinet.ANNUAL_REPORT
        return (self.doc_type, self.ordinance, self.form) == (
            a["docTypeCode"],
            a["ordinanceCode"],
            a["formCode"],
        )

    @property
    def filed(self) -> date | None:
        """The day submitted, in Tokyo, as EDINET states it (`2026-06-25 15:00`)."""
        if not self.submitted or len(self.submitted) < 10:
            return None
        try:
            return date.fromisoformat(self.submitted[:10])
        except ValueError:
            return None


@dataclass(frozen=True)
class DayList:
    status: int
    count: int
    listed: tuple[Listed, ...]


def parse_day(body: bytes) -> DayList:
    """A `documents.json?type=2` body. `status` is the list's own (404 for a day outside the
    rolling ten years), and `count` is `metadata.resultset.count`, which `listed` must match."""
    parsed: Any = json.loads(body.decode("utf-8-sig"))
    meta = parsed.get("metadata") or {}
    status = int(str(meta.get("status") or 0))
    count = int(str((meta.get("resultset") or {}).get("count") or 0))
    listed = tuple(
        Listed(
            doc_id=str(r.get("docID") or ""),
            edinet_code=str(r.get("edinetCode") or ""),
            doc_type=str(r.get("docTypeCode") or ""),
            ordinance=str(r.get("ordinanceCode") or ""),
            form=str(r.get("formCode") or ""),
            period_start=r.get("periodStart") or None,
            period_end=r.get("periodEnd") or None,
            submitted=r.get("submitDateTime") or None,
            withdrawn=str(r.get("withdrawalStatus") or "0") != "0",
            xbrl=str(r.get("xbrlFlag") or "0") == "1",
        )
        for r in (parsed.get("results") or [])
        if r.get("docID")
    )
    return DayList(status, count, listed)


# ── An archive ─────────────────────────────────────────────────────────────────────────────────


class TooLarge(Exception):
    """The archive's instance is over the gate."""


def instance_of(archive: bytes) -> bytes | None:
    """The report's instance (`XBRL/PublicDoc/jpcrp…xbrl`), or None when the archive holds none.
    An auditor's report (`jpaud…`) is a second instance in the same folder and is never it."""
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        found = [
            i
            for i in z.infolist()
            if i.filename.startswith("XBRL/PublicDoc/") and i.filename.endswith(".xbrl")
        ]
        main = [i for i in found if i.filename.split("/")[-1].startswith("jpcrp")] or found
        if not main:
            return None
        info = main[0]
        if info.file_size > MAX_INSTANCE_BYTES:
            raise TooLarge(info.filename)
        return z.read(info)


# ── The vocabulary ─────────────────────────────────────────────────────────────────────────────

#: `market.segment_note_rule` rows, in the order the reader tries them (priority, highest first).
RULES = """
select rule, pattern, role
  from market.segment_note_rule
 where regulator = %s
 order by rule, priority desc, pattern
"""
#: The axis each kind of table is written to.
AXES = "select kind, axis from market.segment_axis where taxonomy = %s order by kind, axis"
PARSER_VERSION = "select version from market.segment_parser where regulator = %s"


@dataclass(frozen=True)
class Vocabulary:
    note: sn.NoteVocabulary
    #: (local-name pattern, `consolidated` | `nonconsolidated`), in priority order.
    blocks: tuple[tuple[str, str], ...]
    axes: Mapping[str, str]
    parser_version: int


def build_vocabulary(
    rules: Sequence[tuple[str, str, str]], axes: Sequence[tuple[str, str]], parser_version: int
) -> Vocabulary:
    """The reader's vocabulary from its control rows. A kind with two axes, or a vocabulary with no
    block, heading or label, is refused: an empty vocabulary reads every report as empty."""
    by_rule: dict[str, list[tuple[str, str]]] = {}
    for rule, pattern, role in rules:
        by_rule.setdefault(rule, []).append((pattern, role))
    for needed in ("block", "heading", "label"):
        if not by_rule.get(needed):
            raise segments.VocabularyEmpty(f"no `{needed}` rule for {REGULATOR}")
    axis_by_kind: dict[str, str] = {}
    for kind, axis in axes:
        if kind in axis_by_kind:
            raise ValueError(f"two {REGULATOR} axes of kind {kind}: {axis_by_kind[kind]}, {axis}")
        axis_by_kind[kind] = axis
    return Vocabulary(
        note=sn.NoteVocabulary(
            headings=tuple(sn.Heading(p, r) for p, r in by_rule["heading"]),
            labels=tuple(sn.Label(p, r) for p, r in by_rule["label"]),
            concepts=tuple((r, p) for p, r in by_rule.get("concept", [])),
        ),
        blocks=tuple(by_rule["block"]),
        axes=axis_by_kind,
        parser_version=parser_version,
    )


def vocabulary(conn: Any) -> Vocabulary:
    with conn.cursor() as cur:
        cur.execute(RULES, (REGULATOR,))
        rules = [(str(a), str(b), str(c)) for a, b, c in cur.fetchall()]
        cur.execute(AXES, (REGULATOR,))
        axes = [(str(a), str(b)) for a, b in cur.fetchall()]
        cur.execute(PARSER_VERSION, (REGULATOR,))
        row = cur.fetchone()
    if row is None:
        raise segments.VocabularyEmpty(f"no parser version for {REGULATOR}")
    return build_vocabulary(rules, axes, int(row[0]))


# ── Reading a report ───────────────────────────────────────────────────────────────────────────

_ELEMENT = re.compile(r"<([\w-]+:([\w.-]+))\b([^>]*)>([^<]*)</\1>", re.A)
_CONTEXT_REF = re.compile(r'\bcontextRef="([^"]+)"', re.A)
_UNIT_REF = re.compile(r'\bunitRef="([^"]+)"', re.A)
_NUMBER = re.compile(r"-?\d+(\.\d+)?", re.A)
_TYPED = re.compile(
    r'<xbrli:context\s+id="([^"]+)"[^>]*>(?:(?!</xbrli:context>).)*typedMember', re.S
)


@dataclass(frozen=True)
class ReportRead:
    """What a report came to: the note's block (None when it files none), whether it anchors on
    non-consolidated figures, and the note's read."""

    block: str | None
    nonconsolidated: bool
    note: sn.NoteRead


def _local(name: str) -> str:
    return name.split(":", 1)[-1]


def _block(xml: str, blocks: Sequence[tuple[str, str]]) -> tuple[str, str, str] | None:
    """The first segment-note block the vocabulary knows: (concept, role, HTML)."""
    for m in _ELEMENT.finditer(xml):
        local = m.group(2)
        if not local.endswith("TextBlock"):
            continue
        for pattern, role in blocks:
            if re.search(pattern, local):
                return m.group(1), role, html.unescape(m.group(4))
    return None


def _numeric_facts(xml: str) -> list[tuple[str, str, str | None, float]]:
    out: list[tuple[str, str, str | None, float]] = []
    for m in _ELEMENT.finditer(xml):
        unit = _UNIT_REF.search(m.group(3))
        ctx = _CONTEXT_REF.search(m.group(3))
        raw = m.group(4).strip()
        if unit is None or ctx is None or not _NUMBER.fullmatch(raw):
            continue
        out.append((m.group(1), ctx.group(1), unit.group(1), float(raw)))
    return out


def anchor_contexts(
    xml: str, *, nonconsolidated: bool
) -> dict[str, tuple[str | None, str | None, str | None]]:
    """The contexts a figure may anchor in: non-dimensional ones for a consolidated note, those
    whose only dimension is `NonConsolidatedMember` for a company without subsidiaries."""
    typed = {m.group(1) for m in _TYPED.finditer(xml)}
    out: dict[str, tuple[str | None, str | None, str | None]] = {}
    for ctx_id, ctx in segments.parse_contexts(xml).items():
        if ctx_id in typed:
            continue
        dims = {_local(a): _local(m) for a, m in ctx.dims.items()}
        wanted = dims in ({}, dict([_NON_CONSOLIDATED])) if nonconsolidated else dims == {}
        if wanted:
            out[ctx_id] = (ctx.start, ctx.end, ctx.instant)
    return out


def read_report(xml: str, vocabulary: Vocabulary) -> ReportRead:
    """A report's segment note read against its own figures."""
    found = _block(xml, vocabulary.blocks)
    if found is None:
        return ReportRead(None, False, sn.NoteRead())
    concept, role, text = found
    nonconsolidated = role == "nonconsolidated"
    anchors = sn.anchors_of(
        _numeric_facts(xml),
        anchor_contexts(xml, nonconsolidated=nonconsolidated),
        segments.parse_units(xml),
    )
    note = sn.read_note(
        text, anchors, vocabulary.note, axis_prefix=PREFIX, axes=dict(vocabulary.axes)
    )
    return ReportRead(concept, nonconsolidated, note)
