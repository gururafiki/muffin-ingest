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
from typing import Any, Literal

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
    #: The five-character securities code (`72030`), for a listed filer.
    sec_code: str | None = None
    #: The document an amendment or an attachment belongs to.
    parent_doc_id: str | None = None

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
            sec_code=r.get("secCode") or None,
            parent_doc_id=r.get("parentDocID") or None,
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


# ── Queries ────────────────────────────────────────────────────────────────────────────────────

#: Japanese equities, the four-character local code Tokyo lists them under (a Japanese venue's
#: listing symbol, or a `.T` price symbol), and whether the SEC already gives the company a split.
#:
#: ONE REGULATOR PER SECURITY (Phase 6c decision 2): a company holding a placed SEC revenue split
#: for one of its last two fiscal years is the SEC's, so EDINET skips it. Measured 2026-11-27:
#: 2,483 Japanese equities, 2,358 with a code the code list holds, eight of them SEC's (Toyota,
#: Sony, Honda, MUFG, Mizuho, Nomura, ORIX, Takeda). A local code can be alphanumeric (`130A`).
#: Executed with no parameters, so `%` is a literal.
SECURITIES = r"""
select s.security_id::text, min(c.code),
       exists (select 1 from market.security_segment g
                where g.security_id = s.security_id and g.source_code like 'sec%'
                  and g.metric_code = 'revenue' and g.partition_id > 0 and g.parent_member is null
                  and g.period_type = 'annual'
                  and g.period_ending >= current_date - interval '30 months')
  from market.security s
  join lateral (
        select l.symbol code
          from market.listing l join market.exchange e on e.exch_code = l.exch_code
         where l.security_id = s.security_id and e.country_iso2 = 'JP'
           and l.symbol ~ '^[0-9][0-9A-Z]{3}$'
        union all
        select substring(p.symbol from '^([0-9][0-9A-Z]{3})\.T$')
          from market.security_provider_symbol p where p.security_id = s.security_id
       ) c on c.code is not null
 where s.security_type_code = 'equity' and s.country_iso2 = 'JP'
 group by s.security_id
"""

#: The run's filers: EDINET code -> every security holding it.
FILERS = """
select sf.filer_id, array_agg(distinct sf.security_id::text)
  from market.security_filer sf
 where sf.source_code = 'edinet' and sf.filer_id = any(%s)
 group by sf.filer_id
"""

_UNREAD = segments.unread_sql(REGULATOR)

#: A filer's annual reports that are due, or named: each with the securities whose row is unread.
FILINGS = f"""
select f.accession_number, min(f.filing_date),
       array_agg(f.security_id::text order by f.security_id) filter (where {_UNREAD}),
       array_agg(f.security_id::text order by f.security_id)
  from market.security_filing f
 where f.source_code = 'edinet' and f.report_type = '{ANNUAL}'
   and f.security_id = any(%s::uuid[])
 group by f.accession_number
having bool_or({_UNREAD}) or f.accession_number = any(%s)
"""

#: How recent a report must be to be asked for while the queue is a history (`due_by_filer`).
RECENT_DAYS = 14

#: Every filer holding an annual report not read at the current parser version: how many, and
#: whether one was filed in the last `RECENT_DAYS` days.
DUE_BY_FILER = f"""
select sf.filer_id, count(distinct f.accession_number),
       bool_or(f.filing_date >= current_date - {RECENT_DAYS})
  from market.security_filing f
  join market.security_filer sf on sf.security_id = f.security_id and sf.source_code = 'edinet'
 where f.source_code = 'edinet' and f.report_type = '{ANNUAL}' and {_UNREAD}
 group by sf.filer_id
"""

#: Annual reports never read that we have held for N days, and those behind the parser.
UNREAD = f"""
select count(distinct f.accession_number) filter (where {{never}}),
       count(distinct f.security_id) filter (where {{never}}),
       count(distinct f.accession_number) filter (where f.segments_parsed_at is not null)
  from market.security_filing f
 where f.source_code = 'edinet' and f.report_type = '{ANNUAL}' and {_UNREAD}
""".format(
    never="f.segments_parsed_at is null and f.as_of <= now() - make_interval(days => %(days)s)"
)

#: Files the annual reports of the filers named (or of every filer, given NULL) into
#: `security_filing`, and removes a withdrawn report's (muffin-deployment, 20261127100000).
FILE_FILINGS = (
    "select filed, withdrawn_filings, withdrawn_segments from market.file_edinet_filings(%s)"
)


def filers(conn: Any, codes: Sequence[str]) -> dict[str, tuple[str, ...]]:
    if not codes:
        return {}
    with conn.cursor() as cur:
        cur.execute(FILERS, (list(codes),))
        return {str(c): tuple(sorted(str(s) for s in ids)) for c, ids in cur.fetchall()}


@dataclass(frozen=True)
class Filing:
    accession: str
    filing_date: date | None
    security_ids: tuple[str, ...]
    due: bool


def filings(conn: Any, security_ids: Sequence[str], named: Sequence[str] = ()) -> list[Filing]:
    """A filer's due annual reports and any named ones, OLDEST FIRST: an amendment restates the year
    its original stated, and the newer one must be written last."""
    if not security_ids:
        return []
    with conn.cursor() as cur:
        cur.execute(FILINGS, (list(security_ids), list(named)))
        found = [
            Filing(str(acc), filed, tuple(unread or holding), bool(unread))
            for acc, filed, unread, holding in cur.fetchall()
        ]
    return sorted(found, key=lambda f: (f.filing_date or date.min, f.accession))


@dataclass(frozen=True)
class Due:
    filings: int
    recent: bool


def due_by_filer(conn: Any) -> dict[str, Due]:
    with conn.cursor() as cur:
        cur.execute(DUE_BY_FILER)
        return {str(c): Due(int(n), bool(r)) for c, n, r in cur.fetchall()}


@dataclass(frozen=True)
class Unread:
    never_read: int
    securities: int
    behind_the_parser: int


def unread(conn: Any, days: int) -> Unread:
    with conn.cursor() as cur:
        cur.execute(UNREAD, {"days": days})
        row = cur.fetchone()
    if row is None:
        return Unread(0, 0, 0)
    return Unread(int(row[0]), int(row[1]), int(row[2]))


@dataclass(frozen=True)
class Filed:
    filed: int
    withdrawn_filings: int
    withdrawn_segments: int


def file_filings(conn: Any, codes: Sequence[str] | None) -> Filed:
    """`file_edinet_filings` for the filers named, or for every filer given None. An empty list
    files nothing and asks nothing: None and [] are different requests."""
    if codes is not None and not codes:
        return Filed(0, 0, 0)
    with conn.cursor() as cur:
        cur.execute(FILE_FILINGS, (None if codes is None else list(codes),))
        row = cur.fetchone()
    if row is None:
        return Filed(0, 0, 0)
    return Filed(int(row[0]), int(row[1]), int(row[2]))


# ── The catalogue ──────────────────────────────────────────────────────────────────────────────

#: The document types `market.edinet_document` keeps: the annual report and its amendment.
CATALOGUED = ("120", "130")

#: Marks the documents a list names as withdrawn (`withdrawalStatus` 2 carries no other metadata,
#: not even the filer), and says whose they were.
MARK_WITHDRAWN = """
update market.edinet_document set withdrawn = true, as_of = now()
 where doc_id = any(%s) and not withdrawn
returning edinet_code
"""


@dataclass(frozen=True)
class Catalogued:
    rows: list[dict[str, Any]]
    #: Documents withdrawn, named by id alone.
    withdrawn: list[str]
    #: Entries of a catalogued type that carry no filer or no submission day: not kept.
    incomplete: int


def document_rows(listed: Sequence[Listed], *, listed_on: str) -> Catalogued:
    """A day list's annual reports and amendments as `market.edinet_document` rows, the documents
    it names only as withdrawn, and how many it named without the metadata a row needs."""
    rows: list[dict[str, Any]] = []
    withdrawn: list[str] = []
    incomplete = 0
    for doc in listed:
        if doc.withdrawn and not doc.edinet_code:
            withdrawn.append(doc.doc_id)
            continue
        if doc.doc_type not in CATALOGUED:
            continue
        filed = doc.filed
        if not doc.edinet_code or filed is None:
            incomplete += 1
            continue
        rows.append(
            {
                "doc_id": doc.doc_id,
                "edinet_code": doc.edinet_code,
                "sec_code": doc.sec_code,
                "doc_type_code": doc.doc_type,
                "ordinance_code": doc.ordinance or None,
                "form_code": doc.form or None,
                "period_start": doc.period_start,
                "period_end": doc.period_end,
                "filed_on": filed.isoformat(),
                "withdrawn": doc.withdrawn,
                "xbrl": doc.xbrl,
                "parent_doc_id": doc.parent_doc_id,
                "listed_on": listed_on,
                "source_code": SOURCE,
            }
        )
    return Catalogued(rows, withdrawn, incomplete)


# ── Raw ────────────────────────────────────────────────────────────────────────────────────────

#: What stage 1 adds to a document's row, and why each may be added (raw-layer rule):
#:   `document`     `code_list`, `day` (a `documents.json` list) or `archive` (a document's ZIP);
#:   `accession`    the day asked (`2026-06-25`) or the document's id: the key to our model;
#:   `http_status`  the status EDINET's body states (it answers every request HTTP 200);
#:   `outcome`      how the fetch ENDED (`answered`, `absent`, `too_large`, `not_zip`).
CONTEXT_COLUMNS = ("document", "accession", "http_status", "outcome")
MERGE_ON = ("document", "accession")

ArchiveOutcome = Literal["answered", "absent", "too_large", "not_zip"]
ARCHIVE_OUTCOMES: frozenset[str] = frozenset(("answered", "absent", "too_large", "not_zip"))
DayOutcome = Literal["answered", "absent"]


def day_outcome(answer: edinet.Answer) -> DayOutcome:
    """`absent` is EDINET saying the day is outside its rolling ten years (or not yet begun)."""
    return "absent" if answer.status == 404 else "answered"


def day_row(answer: edinet.Answer, *, day: str, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "day",
        "accession": day,
        "http_status": answer.status,
        "outcome": day_outcome(answer),
    }


def archive_outcome(answer: edinet.Answer) -> ArchiveOutcome:
    if answer.too_large:
        return "too_large"
    if answer.status == 404:
        return "absent"
    return "answered" if answer.document.body[:2] == b"PK" else "not_zip"


def archive_row(answer: edinet.Answer, *, doc_id: str, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "archive",
        "accession": doc_id,
        "http_status": answer.status,
        "outcome": archive_outcome(answer),
    }


@dataclass(frozen=True)
class Held:
    """An archive's newest finished fetch."""

    outcome: ArchiveOutcome
    fetched_at: str
    run_id: str


def held_archive(rows: Sequence[Mapping[str, Any]], doc_id: str) -> Held | None:
    done = [
        r
        for r in rows
        if r.get("document") == "archive"
        and r.get("accession") == doc_id
        and r.get("outcome") in ARCHIVE_OUTCOMES
        and r.get("fetched_at") is not None
    ]
    if not done:
        return None
    newest = max(done, key=lambda r: str(r["fetched_at"]))
    return Held(newest["outcome"], str(newest["fetched_at"]), str(newest.get("run_id") or ""))


def archive_body(rows: Sequence[Mapping[str, Any]], held: Held) -> bytes | None:
    """The archive the given fetch wrote, or None."""
    if held.outcome != "answered":
        return None
    for row in rows:
        if (
            row.get("document") == "archive"
            and str(row.get("run_id") or "") == held.run_id
            and row.get("body") is not None
        ):
            return bytes(row["body"])
    return None


def held_day(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """The day's newest answered list a partition holds, or None."""
    answered = [r for r in rows if r.get("document") == "day" and r.get("outcome") == "answered"]
    return max(answered, key=lambda r: str(r.get("fetched_at")), default=None)
