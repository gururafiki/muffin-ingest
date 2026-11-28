"""ESEF annual reports, as filings.xbrl.org serves them in xBRL-JSON: their segment note read
against their own tagged figures (Phase 6c step 7).

WHY THE TEXT BLOCK. ESEF mandates detailed tagging of the primary statements only; the IFRS 8 note
is block-tagged (`ifrs-full:DisclosureOfEntitysReportableSegmentsExplanatory`), so its numbers are
in the block's own HTML tables, not in facts. Measured 2026-11-28: L'Oréal's four divisions sum
in that table to the 44 052,0 €m the report tags as `ifrs-full:Revenue`.

THE TABLES ARE IN EACH COUNTRY'S OWN CONVENTIONS. `5 163,0` (French), `16.845` (Spanish),
`76 513`, with an en dash for a minus (Swedish), `21,622.4` (English).
`segment_note.read_note(detect_numbers=True)`
reads each table's convention from its own cells, and `infer_scale=True` takes the table's unit
from the one scale under which a line adds up to a tagged figure: Iberdrola, Inditex and Atlas
Copco state their unit in prose or not at all.

xBRL-JSON's periods END EXCLUSIVE: `2025-01-01T00:00:00/2026-01-01T00:00:00` is calendar 2025,
and an instant at `2026-01-01T00:00:00` is the close of 2025-12-31.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from muffin_ingest.facets import segment_note as sn
from muffin_ingest.facets import segments
from muffin_ingest.providers import xbrl_org

#: The regulator: `disclosure_source`, `security_filer`, `filing_form` and `segment_parser` rows.
REGULATOR = "esef"
#: The provider: `market.data_source` of every filing and segment row this lane writes.
SOURCE = xbrl_org.PROVIDER
PREFIX = "esef:"
#: `filing_form` code: the annual financial report, the only report ESEF mandates.
ANNUAL = "AFR"

#: The dimensions every fact carries; a fact with any other is dimensional, and never an anchor.
_PLAIN = frozenset({"concept", "entity", "period", "unit", "language"})


# ── The vocabulary ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Vocabulary:
    note: sn.NoteVocabulary
    #: Local-name patterns of the blocks holding the note, in priority order.
    blocks: tuple[str, ...]
    axes: Mapping[str, str]
    parser_version: int


def build_vocabulary(
    rules: Sequence[tuple[str, str, str]], axes: Sequence[tuple[str, str]], parser_version: int
) -> Vocabulary:
    """The reader's vocabulary from its control rows, in the order given (priority, highest
    first). A vocabulary with no block, heading or label is refused: it would read every report as
    empty."""
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
            members=tuple(sn.Heading(p, r) for p, r in by_rule.get("member", [])),
            concepts=tuple((r, p) for p, r in by_rule.get("concept", [])),
            spaced=True,
        ),
        blocks=tuple(p for p, _ in by_rule["block"]),
        axes=axis_by_kind,
        parser_version=parser_version,
    )


# ── Reading a report ───────────────────────────────────────────────────────────────────────────


def _local(concept: str) -> str:
    return concept.split(":", 1)[-1]


def _day_before(stamp: str) -> str:
    """`2026-01-01T00:00:00` -> `2025-12-31`: the last day a period ending exclusive covers. A
    stamp with a time of day (rare) is that day."""
    day = dt.date.fromisoformat(stamp[:10])
    if stamp[11:19] in ("", "00:00:00"):
        day -= dt.timedelta(days=1)
    return day.isoformat()


def period_of(period: str) -> tuple[str | None, str | None, str | None]:
    """(start, end, instant) of an xBRL-JSON period, ends made inclusive."""
    if "/" in period:
        start, end = period.split("/", 1)
        return start[:10], _day_before(end), None
    return None, None, _day_before(period)


def anchors_of(facts: Mapping[str, Mapping[str, Any]]) -> list[sn.Anchor]:
    """Every non-dimensional monetary fact, as the figure a line may total to."""
    out: list[sn.Anchor] = []
    for fact in facts.values():
        dims = fact.get("dimensions") or {}
        unit = str(dims.get("unit") or "")
        if not unit.startswith("iso4217:") or set(dims) - _PLAIN:
            continue
        try:
            value = float(fact["value"])
        except (TypeError, ValueError, KeyError):
            continue
        start, end, instant = period_of(str(dims.get("period") or ""))
        out.append(
            sn.Anchor(str(dims["concept"]), value, start, end, instant, unit.split(":", 1)[1])
        )
    return out


def blocks_of(
    facts: Mapping[str, Mapping[str, Any]], patterns: Sequence[str]
) -> list[tuple[str, str]]:
    """The note's blocks as (concept, HTML), in the vocabulary's order; a concept the report
    states twice (two languages) is read once, the longest text."""
    found: dict[str, str] = {}
    for fact in facts.values():
        dims = fact.get("dimensions") or {}
        concept = str(dims.get("concept") or "")
        value = fact.get("value")
        if not isinstance(value, str) or set(dims) - _PLAIN:
            continue
        matches = any(re.search(p, _local(concept)) for p in patterns)
        if matches and len(value) > len(found.get(concept, "")):
            found[concept] = value
    ordered: list[tuple[str, str]] = []
    for pattern in patterns:
        for concept in sorted(found):
            if re.search(pattern, _local(concept)) and all(c != concept for c, _ in ordered):
                ordered.append((concept, found[concept]))
    return ordered


@dataclass(frozen=True)
class ReportRead:
    """What a report came to: the block read (None when it files none of the vocabulary's), the
    period it covers, and the note's read."""

    block: str | None
    note: sn.NoteRead


def load(body: bytes) -> Mapping[str, Mapping[str, Any]]:
    """The report's facts."""
    data = json.loads(body)
    facts = data.get("facts") if isinstance(data, dict) else None
    return facts if isinstance(facts, dict) else {}


def read_report(body: bytes, vocabulary: Vocabulary) -> ReportRead:
    """A report's segment note read against its own figures: the first of its blocks, in the
    vocabulary's order, that places a split. Schneider files two blocks holding the same table,
    and reading both would union one split twice."""
    facts = load(body)
    anchors = anchors_of(facts)
    first: ReportRead | None = None
    for concept, text in blocks_of(facts, vocabulary.blocks):
        note = sn.read_note(
            text,
            anchors,
            vocabulary.note,
            axis_prefix=PREFIX,
            axes=dict(vocabulary.axes),
            detect_numbers=True,
            infer_scale=True,
        )
        read = ReportRead(concept, note)
        if any(f.partition_id for f in note.facts):
            return read
        first = first or read
    return first or ReportRead(None, sn.NoteRead())


def placed(facts: Iterable[segments.SegmentFact]) -> list[segments.SegmentFact]:
    return [f for f in facts if f.partition_id]


# ── The vocabulary, from its control rows ──────────────────────────────────────────────────────

#: `market.segment_note_rule` rows, in the order the reader tries them (priority, highest first).
RULES = """
select rule, pattern, role
  from market.segment_note_rule
 where regulator = %s
 order by rule, priority desc, pattern
"""
AXES = "select kind, axis from market.segment_axis where taxonomy = %s order by kind, axis"
PARSER_VERSION = "select version from market.segment_parser where regulator = %s"


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


# ── The registry ───────────────────────────────────────────────────────────────────────────────

#: Every equity in a country the index covers whose issuer holds an LEI, and whether the SEC already
#: gives it a split (Phase 6c decision 2: the home lane skips the SEC's companies). The LEI comes
#: from N-PORT's issuer, which is the only LEI this schema holds for a non-US company.
SECURITIES = r"""
select s.security_id::text, i.lei,
       exists (select 1 from market.security_segment g
                where g.security_id = s.security_id and g.source_code like 'sec%'
                  and g.metric_code = 'revenue' and g.partition_id > 0 and g.parent_member is null
                  and g.period_type = 'annual'
                  and g.period_ending >= current_date - interval '30 months')
  from market.security s
  join market.issuer i on i.issuer_id = s.issuer_id
 where s.security_type_code = 'equity'
   and i.lei ~ '^[A-Z0-9]{18}[0-9]{2}$'
   and coalesce(s.provider_country_iso2, s.country_iso2) in
       (select c.country_iso2 from market.disclosure_coverage c where c.source_code = 'esef')
"""

#: The run's filers: LEI -> every security holding it.
FILERS = """
select sf.filer_id, array_agg(distinct sf.security_id::text)
  from market.security_filer sf
 where sf.source_code = 'esef' and sf.filer_id = any(%s)
 group by sf.filer_id
"""

#: A filer's fiscal year end, from the newest regime any of its securities holds.
YEAR_END = """
select c.year_end_month, c.year_end_day
  from market.fiscal_calendar c
 where c.security_id = any(%s::uuid[]) and c.year_end_month is not null
 order by c.last_year_end desc nulls first, c.regime desc, c.as_of desc
 limit 1
"""

_UNREAD = segments.unread_sql(REGULATOR)

#: Of some filings, those read at the current parser version: the rest are due.
READ_ALREADY = f"""
select distinct f.accession_number
  from market.security_filing f
 where f.source_code = '{SOURCE}' and f.accession_number = any(%s) and not {_UNREAD}
"""

#: A filer's annual reports that are due, or named: each with the securities whose row is unread.
FILINGS = f"""
select f.accession_number, min(f.filing_date),
       array_agg(f.security_id::text order by f.security_id) filter (where {_UNREAD}),
       array_agg(f.security_id::text order by f.security_id)
  from market.security_filing f
 where f.source_code = '{SOURCE}' and f.report_type = '{ANNUAL}'
   and f.security_id = any(%s::uuid[])
 group by f.accession_number
having bool_or({_UNREAD}) or f.accession_number = any(%s)
"""

#: Annual reports never read that we have held for N days, and those behind the parser.
UNREAD = f"""
select count(distinct f.accession_number) filter (where {{never}}),
       count(distinct f.security_id) filter (where {{never}}),
       count(distinct f.accession_number) filter (where f.segments_parsed_at is not null)
  from market.security_filing f
 where f.source_code = '{SOURCE}' and f.report_type = '{ANNUAL}' and {_UNREAD}
""".format(
    never="f.segments_parsed_at is null and f.as_of <= now() - make_interval(days => %(days)s)"
)


def filers(conn: Any, leis: Sequence[str]) -> dict[str, tuple[str, ...]]:
    if not leis:
        return {}
    with conn.cursor() as cur:
        cur.execute(FILERS, (list(leis),))
        return {str(c): tuple(sorted(str(s) for s in ids)) for c, ids in cur.fetchall()}


def year_end(conn: Any, security_ids: Sequence[str]) -> tuple[int, int] | None:
    if not security_ids:
        return None
    with conn.cursor() as cur:
        cur.execute(YEAR_END, (list(security_ids),))
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0]), int(row[1] or 0)


def read_already(conn: Any, accessions: Sequence[str]) -> set[str]:
    if not accessions:
        return set()
    with conn.cursor() as cur:
        cur.execute(READ_ALREADY, (list(accessions),))
        return {str(r[0]) for r in cur.fetchall()}


@dataclass(frozen=True)
class Filing:
    accession: str
    filing_date: dt.date | None
    security_ids: tuple[str, ...]
    due: bool


def filings(conn: Any, security_ids: Sequence[str], named: Sequence[str] = ()) -> list[Filing]:
    """A filer's due annual reports and any named ones, OLDEST FIRST: a re-filing restates the year
    its original stated, and the newer one must be written last."""
    if not security_ids:
        return []
    with conn.cursor() as cur:
        cur.execute(FILINGS, (list(security_ids), list(named)))
        found = [
            Filing(str(acc), filed, tuple(unread or holding), bool(unread))
            for acc, filed, unread, holding in cur.fetchall()
        ]
    return sorted(found, key=lambda f: (f.filing_date or dt.date.min, f.accession))


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


# ── A filer's list ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Listed:
    """One report the index lists for a filer."""

    #: The index's own id, `<LEI>-<period end>-ESEF-<country>-<n>`: a re-filing raises `n`.
    fxo_id: str
    period_end: str
    country: str | None
    #: The day the report was published, else the day the index added it: our filing date.
    filed: str | None
    added: str | None
    json_url: str | None


def parse_filings(body: bytes) -> list[Listed] | None:
    """The reports a list names, or None when the body is no JSON:API list (the index answered
    something else: an error page, a gateway)."""
    try:
        data = json.loads(body)
    except ValueError:
        return None
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return None
    out: list[Listed] = []
    for row in rows:
        attributes = row.get("attributes") if isinstance(row, dict) else None
        if not isinstance(attributes, dict):
            continue
        fxo = attributes.get("fxo_id")
        end = attributes.get("period_end")
        if not isinstance(fxo, str) or not isinstance(end, str):
            continue
        published = attributes.get("published")
        added = attributes.get("date_added")
        out.append(
            Listed(
                fxo_id=fxo,
                period_end=end[:10],
                country=attributes.get("country"),
                filed=(str(published)[:10] if published else None)
                or (str(added)[:10] if added else None),
                added=str(added) if added else None,
                json_url=attributes.get("json_url"),
            )
        )
    return out


#: How far a report's period end may fall from the fiscal year end and still be the annual report:
#: a 52/53-week year moves its end by up to a week.
YEAR_END_SLACK_DAYS = 10


def is_annual(period_end: str, year_end_md: tuple[int, int] | None) -> bool:
    """Whether a listed report is an annual one. The index lists half-year and quarterly reports
    beside annual ones (Novo Nordisk's newest is its 2026-09-30 interim) and does not say which is
    which, so the company's fiscal year end decides; a company with no calendar is taken to close
    on 31 December. A year end on day 0 (none recorded) is the month's last day."""
    try:
        end = dt.date.fromisoformat(period_end)
    except ValueError:
        return False
    month, day = year_end_md or (12, 31)
    for year in (end.year - 1, end.year, end.year + 1):
        try:
            anchor = (
                dt.date(year, month, day)
                if day
                else dt.date(year + month // 12, month % 12 + 1, 1) - dt.timedelta(days=1)
            )
        except ValueError:
            continue
        if abs((end - anchor).days) <= YEAR_END_SLACK_DAYS:
            return True
    return False


def annual_reports(listed: Sequence[Listed], year_end_md: tuple[int, int] | None) -> list[Listed]:
    """The annual reports of a list, ONE PER PERIOD: a re-filing of a period supersedes the
    report before it (the newest added wins, then the higher `fxo_id`), so the index's corrections
    are read and its superseded versions are not fetched at all."""
    by_period: dict[str, Listed] = {}
    for item in listed:
        if not item.json_url or not is_annual(item.period_end, year_end_md):
            continue
        prior = by_period.get(item.period_end)
        if prior is None or (item.added or "", item.fxo_id) > (prior.added or "", prior.fxo_id):
            by_period[item.period_end] = item
    return sorted(by_period.values(), key=lambda f: f.period_end, reverse=True)


def filing_rows(security_ids: Sequence[str], found: Sequence[Listed]) -> list[dict[str, Any]]:
    """`security_filing` rows for each annual report, for every security of the filer. Only these
    columns are written, so the marks stand: a re-listing must never make a filing look unread."""
    return [
        {
            "security_id": sid,
            "accession_number": f.fxo_id,
            "report_type": ANNUAL,
            "filing_date": f.filed,
            "source_code": SOURCE,
            "is_xbrl": True,
        }
        for sid in security_ids
        for f in found
    ]


# ── Raw ────────────────────────────────────────────────────────────────────────────────────────

#: What stage 1 adds to a document's row, and why each may be added (raw-layer rule):
#:   `document`     `filings` (a filer's list) or `report` (an xBRL-JSON report);
#:   `accession`    `list`, or the report's `fxo_id`: the key to our model;
#:   `http_status`  the index's status line;
#:   `outcome`      how the fetch ENDED (`answered`, `absent`, `too_large`, `not_json`).
CONTEXT_COLUMNS = ("document", "accession", "http_status", "outcome")
MERGE_ON = ("document", "accession")

FetchOutcome = Literal["answered", "absent", "too_large", "not_json"]
FETCH_OUTCOMES: frozenset[str] = frozenset(("answered", "absent", "too_large", "not_json"))


def list_outcome(answer: xbrl_org.Answer) -> FetchOutcome:
    if answer.status != 200:
        return "absent"
    return "answered" if parse_filings(answer.document.body) is not None else "not_json"


def list_row(answer: xbrl_org.Answer, *, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "filings",
        "accession": "list",
        "http_status": answer.status,
        "outcome": list_outcome(answer),
    }


def report_outcome(answer: xbrl_org.Answer) -> FetchOutcome:
    """`answered` only for a body that opens as JSON: a report is never parsed in stage 1, and a
    gateway page served with a 200 is asked for again."""
    if answer.too_large:
        return "too_large"
    if answer.status != 200:
        return "absent"
    return "answered" if answer.document.body.lstrip()[:1] == b"{" else "not_json"


def report_row(answer: xbrl_org.Answer, *, fxo_id: str, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "report",
        "accession": fxo_id,
        "http_status": answer.status,
        "outcome": report_outcome(answer),
    }


def newest_list(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """The newest answered list a filer's file holds."""
    answered = [
        r
        for r in rows
        if r.get("document") == "filings" and r.get("outcome") == "answered" and r.get("body")
    ]
    return max(answered, key=lambda r: str(r.get("fetched_at")), default=None)


@dataclass(frozen=True)
class Held:
    """A report's newest finished fetch."""

    outcome: FetchOutcome
    fetched_at: str
    run_id: str


def held_report(rows: Sequence[Mapping[str, Any]], fxo_id: str) -> Held | None:
    done = [
        r
        for r in rows
        if r.get("document") == "report"
        and r.get("accession") == fxo_id
        and r.get("outcome") in FETCH_OUTCOMES
        and r.get("fetched_at") is not None
    ]
    if not done:
        return None
    newest = max(done, key=lambda r: str(r["fetched_at"]))
    return Held(newest["outcome"], str(newest["fetched_at"]), str(newest.get("run_id") or ""))


def report_body(rows: Sequence[Mapping[str, Any]], held: Held) -> bytes | None:
    """The report the given fetch wrote, or None."""
    if held.outcome != "answered":
        return None
    for row in rows:
        if (
            row.get("document") == "report"
            and str(row.get("run_id") or "") == held.run_id
            and row.get("body") is not None
        ):
            return bytes(row["body"])
    return None
