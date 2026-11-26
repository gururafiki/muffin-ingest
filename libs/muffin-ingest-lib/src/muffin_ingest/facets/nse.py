"""India's segment lane: NSE's filers, their annual results, and the normaliser.

PORTED FROM THE EDGE's `in-filings` and `security-in-segments` handlers and `in.ts` (Phase 6,
decided 2026-11-09). The parser is `facets.segments`, unchanged; `normalise` rewrites an NSE
instance into ordinary XBRL first, because India differs in three ways, each producing a confident
wrong number rather than an error (measured on Reliance, HDFC Bank and Infosys by the edge,
2026-09-06):

  1. MEMBERS ARE ANONYMOUS POSITIONAL SLOTS (`OneReportableSegmentRevenue03Member`). The name lives
     in a sibling fact, `in-bse-fin:DescriptionOfReportableSegment`, on the same context.
  2. THE PERIOD IS NOT IN THE CONTEXT'S DATES. A Q4 filing tags the quarter and the year with the
     same 90-day dates; only the `One`/`Four` prefix of the CONTEXT ID tells them apart. Read as
     dated, Reliance's split came to 14.01 trillion against a true 11.09.
  3. EVERY COMPANY FILES TWICE, standalone and consolidated. `NatureOfReportStandaloneConsolidated`
     decides, and a standalone filing is read as "nothing to take", never as a failure.

WHAT CHANGED FROM THE EDGE, AND WHY:
  * a filer is the NSE symbol `security_nse_filer` maps by ISIN, walked on a 30-day hash floor;
  * only `.xml` links are filings. NSE lists a result with no XBRL as `.../corporate/xbrl/-`, and
    the edge filed that placeholder once per company: 440 rows, none ever readable;
  * links are kept on the `.com` archives host (`nse.canonical`), the form of every stored filing,
    although NSE now writes `.in`;
  * a refused or failed fetch never stamps a filing read (the edge stamped any non-200 as "no
    instance"; only a 404 is an absence here).
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Literal

from muffin_ingest.facets import segments
from muffin_ingest.facets.segments import _trim
from muffin_ingest.providers import nse

REGULATOR = "nse"
#: `market.data_source` code of every row this lane writes (filings and segments); the edge's.
SOURCE = "nse"
#: `filing_form` code of a result filed under `period=Annual`, as the edge filed them.
ANNUAL = "Annual"

_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


# ── The normaliser (the edge's `in.ts`, line for line) ──────────────────────────────────────────


class NotConsolidated(Exception):
    """A standalone filing: a good document this lane does not want."""


@dataclass(frozen=True)
class Normalised:
    xml: str
    #: Segment members whose name was resolved, so a silent regression is visible.
    named: int
    #: Contexts whose period was rewritten from a quarter to the year.
    annualised: int


def member_code_for(name: str) -> str:
    """A stable member code from the filer's own segment name: the positional slot is Oil to
    Chemicals in one filing and anything in the next, so the NAME is the key."""
    words = _trim(re.sub(r"[^A-Za-z0-9]+", " ", name)).split(" ")
    slug = "".join(w[:1].upper() + w[1:] for w in words)
    return f"nse:{slug or 'Unnamed'}Member"


def year_start_for(end: str) -> str:
    """The first day of the twelve months ending at `end`. JavaScript's `Date.UTC` rolls an invalid
    day over (29 February in a common year is 1 March), and so does this."""
    y, m, d = (int(x) for x in end.split("-"))
    start = date(y - 1, m, 1) + timedelta(days=d - 1) + timedelta(days=1)
    return start.isoformat()


_NATURE = re.compile(r"NatureOfReportStandaloneConsolidated[^>]*>([^<]+)<")
_CONTEXT = re.compile(r'<xbrli:context id="([^"]+)"([\s\S]*?)</xbrli:context>')
_MEMBER = re.compile(r'ReportableSegment[A-Za-z]*Axis">([^<]+)<')
_END = re.compile(r"<xbrli:endDate>([^<]+)<")
_NAME = re.compile(
    r'<in-bse-fin:DescriptionOfReportableSegment\b[^>]*contextRef="([^"]+)"[^>]*>([^<]+)<'
)
_FOUR = re.compile(r'<xbrli:context id="(Four[^"]*)"([\s\S]*?)</xbrli:context>')
_START = re.compile(r"<xbrli:startDate>[^<]+<")


def normalise(xml: str) -> Normalised:
    """An NSE instance rewritten into XBRL `segments.segment_facts_from` reads unchanged. Raises
    `NotConsolidated` for a standalone filing (or one that does not say which it is)."""
    nature = _NATURE.search(xml)
    if not nature or _trim(nature.group(1)).lower() != "consolidated":
        raise NotConsolidated

    ctx_member: dict[str, str] = {}
    ctx_end: dict[str, str] = {}
    for m in _CONTEXT.finditer(xml):
        context_id, body = m.group(1), m.group(2)
        found = _MEMBER.search(body)
        if found:
            ctx_member[context_id] = found.group(1)
        end = _END.search(body)
        if end:
            ctx_end[context_id] = _trim(end.group(1))

    name_by_context: dict[str, str] = {}
    for m in _NAME.finditer(xml):
        name_by_context[m.group(1)] = _trim(m.group(2))

    out = xml
    named = 0
    for context_id, member in ctx_member.items():
        name = name_by_context.get(context_id)
        if not name:
            continue
        named += 1
        out = out.replace(f">{member}<", f">{member_code_for(name)}<")

    annualised = 0

    def annualise(m: re.Match[str]) -> str:
        nonlocal annualised
        context_id, body = m.group(1), m.group(2)
        end = ctx_end.get(context_id)
        if not end:
            return m.group(0)
        annualised += 1
        start = year_start_for(end)
        return (
            f'<xbrli:context id="{context_id}"'
            + _START.sub(lambda _: f"<xbrli:startDate>{start}<", body, count=1)
            + "</xbrli:context>"
        )

    out = _FOUR.sub(annualise, out)
    return Normalised(out, named, annualised)


# ── A company's results ────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Filed:
    #: The XBRL link on the `.com` archives host: the filing's accession.
    link: str
    #: `YYYY-MM-DD`, the period end NSE reports (`toDate`).
    period_end: str | None
    #: `YYYY-MM-DD`, from NSE's `filingDate`.
    filed: str | None


def iso_from_nse_date(value: Any) -> str | None:
    """NSE's `31-Mar-2024` (a `filingDate` carries a time after it) as an ISO date."""
    m = re.match(r"^(\d{2})-([A-Za-z]{3})-(\d{4})", str(value or "").strip())
    if not m or m.group(2) not in _MONTHS:
        return None
    return f"{m.group(3)}-{_MONTHS.index(m.group(2)) + 1:02d}-{m.group(1)}"


def parse_filings(body: bytes) -> list[Filed] | None:
    """A results answer: a bare array, or `{"data": [...]}`. None when it is neither (not an
    answer). A result whose `xbrl` is not an `.xml` link has nothing to read and is not a filing."""
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None
    rows = data if isinstance(data, list) else data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return None
    out: list[Filed] = []
    seen: set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        link = str(r.get("xbrl") or "")
        if not (link.startswith("http") and link.lower().endswith(".xml")):
            continue
        link = nse.canonical(link)
        if link in seen:
            continue
        seen.add(link)
        out.append(
            Filed(link, iso_from_nse_date(r.get("toDate")), iso_from_nse_date(r.get("filingDate")))
        )
    return out


# ── Queries ────────────────────────────────────────────────────────────────────────────────────

#: The run's filers: NSE symbol -> every security holding it (`security_nse_filer`, by ISIN).
FILERS = """
select sf.filer_id, array_agg(distinct sf.security_id::text)
  from market.security_filer sf
 where sf.source_code = 'nse' and sf.filer_id = any(%s)
 group by sf.filer_id
"""

_UNREAD = segments.unread_sql(REGULATOR)

#: Of some filings, those read at the current parser version: the rest are due.
READ_ALREADY = f"""
select distinct f.accession_number
  from market.security_filing f
 where f.source_code = 'nse' and f.accession_number = any(%s) and not {_UNREAD}
"""

#: A filer's `.xml` results that are due, or named: each with the securities whose row is unread.
FILINGS = f"""
select f.accession_number, min(f.filing_date),
       array_agg(f.security_id::text order by f.security_id) filter (where {_UNREAD}),
       array_agg(f.security_id::text order by f.security_id)
  from market.security_filing f
 where f.source_code = 'nse' and f.report_type = '{ANNUAL}'
   and f.accession_number ~ '\\.xml$'
   and f.security_id = any(%s::uuid[])
 group by f.accession_number
having bool_or({_UNREAD}) or f.accession_number = any(%s)
"""

#: Results never read that we have held for N days, and those behind the parser.
UNREAD = f"""
select count(distinct f.accession_number) filter (where {{never}}),
       count(distinct f.security_id) filter (where {{never}}),
       count(distinct f.accession_number) filter (where f.segments_parsed_at is not null)
  from market.security_filing f
 where f.source_code = 'nse' and f.report_type = '{ANNUAL}'
   and f.accession_number ~ '\\.xml$' and {_UNREAD}
""".format(
    never="f.segments_parsed_at is null and f.as_of <= now() - make_interval(days => %(days)s)"
)


def filers(conn: Any, symbols: Sequence[str]) -> dict[str, tuple[str, ...]]:
    if not symbols:
        return {}
    with conn.cursor() as cur:
        cur.execute(FILERS, (list(symbols),))
        return {str(c): tuple(sorted(str(s) for s in ids)) for c, ids in cur.fetchall()}


def read_already(conn: Any, accessions: Sequence[str]) -> set[str]:
    if not accessions:
        return set()
    with conn.cursor() as cur:
        cur.execute(READ_ALREADY, (list(accessions),))
        return {str(r[0]) for r in cur.fetchall()}


@dataclass(frozen=True)
class Filing:
    accession: str
    #: When NSE received it (`filingDate`), which decides which of two filings stating one period
    #: owns it. Null for a result no list has named since the edge filed it without one.
    filing_date: date | None
    security_ids: tuple[str, ...]
    due: bool


def filings(conn: Any, security_ids: Sequence[str], named: Sequence[str] = ()) -> list[Filing]:
    """A filer's due results and any named ones, OLDEST FILED FIRST: a later result restates the
    periods an earlier one stated, and it must be written last."""
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


def filing_rows(security_ids: Sequence[str], found: Sequence[Filed]) -> list[dict[str, Any]]:
    """`security_filing` rows for every result, for every security of the filer, as the edge wrote
    them plus the filing date it dropped. Only these columns are written, so the marks stand."""
    return [
        {
            "security_id": sid,
            "accession_number": f.link,
            "report_type": ANNUAL,
            "report_date": f.period_end,
            "filing_date": f.filed,
            "report_url": f.link,
            "source_code": SOURCE,
            "is_xbrl": True,
        }
        for sid in security_ids
        for f in found
    ]


# ── Raw ────────────────────────────────────────────────────────────────────────────────────────

#: What stage 1 adds to a document's row, and why each may be added (raw-layer rule):
#:   `document`     `filings` (a results answer) or `instance`;
#:   `accession`    `results` for the list, or the filing's link: the key to our model;
#:   `http_status`  NSE's status line;
#:   `outcome`      how the fetch ENDED (`answered`, `absent`, `not_answer`).
CONTEXT_COLUMNS = ("document", "accession", "http_status", "outcome")
MERGE_ON = ("document", "accession")

Outcome = Literal["answered", "absent", "not_answer"]
OUTCOMES: frozenset[str] = frozenset(("answered", "absent", "not_answer"))


def list_outcome(answer: nse.Answer) -> Outcome:
    """`answered` for a results answer (`[]` included: NSE lists nothing for the symbol), `absent`
    for a 404, `not_answer` for a 200 that is not one (a gateway page), which is asked again."""
    if answer.status != 200:
        return "absent"
    return "answered" if parse_filings(answer.document.body) is not None else "not_answer"


def list_row(answer: nse.Answer, *, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "filings",
        "accession": "results",
        "http_status": answer.status,
        "outcome": list_outcome(answer),
    }


def instance_outcome(answer: nse.Answer) -> Outcome:
    if answer.status != 200:
        return "absent"
    return "answered" if b"<xbrli:xbrl" in answer.document.body[:4000] else "not_answer"


def instance_row(answer: nse.Answer, *, link: str, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "instance",
        "accession": link,
        "http_status": answer.status,
        "outcome": instance_outcome(answer),
    }


def newest_list(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """The newest answered results list a filer's file holds."""
    done = [
        r
        for r in rows
        if r.get("document") == "filings" and r.get("outcome") == "answered" and r.get("body")
    ]
    return max(done, key=lambda r: str(r.get("fetched_at"))) if done else None


@dataclass(frozen=True)
class Held:
    """An instance's newest finished fetch."""

    outcome: Outcome
    fetched_at: str
    run_id: str


def held_instance(rows: Sequence[Mapping[str, Any]], link: str) -> Held | None:
    done = [
        r
        for r in rows
        if r.get("document") == "instance"
        and r.get("accession") == link
        and r.get("outcome") in OUTCOMES
        and r.get("fetched_at") is not None
    ]
    if not done:
        return None
    newest = max(done, key=lambda r: str(r["fetched_at"]))
    return Held(newest["outcome"], str(newest["fetched_at"]), str(newest.get("run_id") or ""))


def instance_body(rows: Sequence[Mapping[str, Any]], link: str, held: Held) -> bytes | None:
    """The instance the given fetch wrote, or None."""
    if held.outcome != "answered":
        return None
    for row in rows:
        if (
            row.get("document") == "instance"
            and row.get("accession") == link
            and str(row.get("run_id") or "") == held.run_id
            and row.get("body") is not None
        ):
            return bytes(row["body"])
    return None
