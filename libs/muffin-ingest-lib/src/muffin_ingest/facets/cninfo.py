"""China's segment lane: CNINFO's filers, its annual-report announcements, and which are due.

PORTED FROM THE EDGE's `cn-filings` and `security-cn-segments` handlers and their queues,
`market.pending_cn_filings` and `pending_cn_segments` (Phase 6, decided 2026-11-09). The PDF parser
is `facets.cn_pdf`; this module is the CNINFO side of it.

WHAT CHANGED FROM THE EDGE, AND WHY:
  * the org id comes from CNINFO's stock lists (two requests a week) instead of one lookup per
    company; all 2,173 agree (measured 2026-11-15);
  * a company is walked on a 30-day hash-spread floor instead of every 180 days;
  * a refusal or a failed fetch never stamps a report read (the edge stamped every non-200);
  * an unreadable report (the parsing child died or raised) is counted on its own and stamped, as
    the edge stamped an oversized one: it is a settled fact about that file until the parser
    changes;
  * `filing_date` is written beside `report_date`. CNINFO's rows carried the announcement date in
    `report_date` and no `filing_date`, which left the newer-filing-owns-the-rows rule
    (`segment_tables.HELD_BY_NEWER`) unable to order an original report and its correction.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from muffin_ingest.facets import segment_note as sn
from muffin_ingest.facets import segments
from muffin_ingest.facets.segments import SegmentFact
from muffin_ingest.providers import cninfo
from muffin_ingest.providers.documents import Document

REGULATOR = "cninfo"
#: `market.data_source` code of every row this lane writes (filings and segments); the edge's.
SOURCE = "cninfo"

#: `filing_form` codes. The category files three documents and only the full report carries the
#: mandated table; the summary omits it and the English edition has English headings.
FULL = "年度报告"
SUMMARY = "年度报告摘要"
ENGLISH = "年度报告（英文版）"

Kind = Literal["full", "summary", "english"]
_ANNUAL_REPORT = re.compile(r"annual\s+report", re.IGNORECASE)
FORM_FOR: dict[str, str] = {"full": FULL, "summary": SUMMARY, "english": ENGLISH}


def classify(title: str) -> Kind:
    """Which document an announcement is, from its own title (measured 2026-09-06): `摘要` is the
    summary, `英文` or `annual report` the English edition, anything else the full report (a
    corrected one included). The title often omits the company name, so nothing depends on it."""
    t = title or ""
    if "摘要" in t:
        return "summary"
    if "英文" in t or _ANNUAL_REPORT.search(t):
        return "english"
    return "full"


#: CNINFO dates an announcement at midnight Beijing time. China keeps no daylight saving, so a
#: fixed offset is exact and needs no time-zone database in the image.
BEIJING = timezone(timedelta(hours=8))


def iso_from_epoch_ms(value: Any) -> str | None:
    """CNINFO's dates are epoch milliseconds at Beijing midnight; a non-number is an absence, not a
    zero. Read in UTC, as the edge read them, every date was the day before: 长江电力's FY2025
    report, filed under `finalpage/2026-04-30/`, is stamped 1777478400000, 16:00 UTC on the 29th."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return datetime.fromtimestamp(value / 1000, BEIJING).date().isoformat()


@dataclass(frozen=True)
class Announcement:
    title: str
    #: The PDF's public URL: the document's identity, stored as the filing's accession.
    url: str
    date: str | None
    kind: Kind
    size_kb: float | None

    @property
    def is_report(self) -> bool:
        """A full report the parser can read. The annual category also lists `主要财务指标` (a
        `.js` data file) and `.html` pages under report-like titles: measured 2026-11-15, 6,903 and
        606 of the 32,275 rows the edge typed `年度报告`. They stay in `security_filing` as filed,
        and are never fetched."""
        return self.kind == "full" and self.url.lower().endswith(".pdf")


def parse_announcements(body: bytes) -> list[Announcement]:
    """A search answer -> its announcements. `adjunctUrl` is a PATH on the static host."""
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return []
    rows = data.get("announcements") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    out: list[Announcement] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        path = row.get("adjunctUrl")
        if not isinstance(path, str) or not path:
            continue
        title = str(row.get("announcementTitle") or "")[:300]
        size = row.get("adjunctSize")
        out.append(
            Announcement(
                title=title,
                url=f"{cninfo.STATIC_ORIGIN}/{path.lstrip('/')}",
                date=iso_from_epoch_ms(row.get("announcementTime")),
                kind=classify(title),
                size_kb=float(size)
                if isinstance(size, (int, float)) and not isinstance(size, bool)
                else None,
            )
        )
    return out


def org_ids(bodies: Sequence[bytes]) -> dict[str, str]:
    """The stock lists -> six-digit code -> org id. A code listed twice keeps its first org id."""
    out: dict[str, str] = {}
    for body in bodies:
        data = json.loads(body)
        rows = data.get("stockList") if isinstance(data, dict) else data
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            code, org = str(row.get("code") or ""), row.get("orgId")
            if re.fullmatch(r"\d{6}", code) and isinstance(org, str) and org:
                out.setdefault(code, org)
    return out


def hk_org_ids(bodies: Sequence[bytes]) -> dict[int, str]:
    """Hong Kong code (as a number: `00939` -> 939) -> org id, for the H-shares whose org id is an
    A-share's. A Hong Kong line of a company with no A-share has no CNINFO filer to map onto."""
    a_shares = set(org_ids(bodies).values())
    out: dict[int, str] = {}
    for body in bodies:
        data = json.loads(body)
        rows = data.get("stockList") if isinstance(data, dict) else data
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            code, org = str(row.get("code") or ""), row.get("orgId")
            if re.fullmatch(r"\d{4,5}", code) and isinstance(org, str) and org in a_shares:
                out.setdefault(int(code), org)
    return out


# ── Queries ────────────────────────────────────────────────────────────────────────────────────

#: The A-share equities the lane serves: as the edge's `pending_cn_filings` chose them.
SECURITIES = """
select s.security_id::text, min(l.symbol)
  from market.security s
  join market.listing l on l.security_id = s.security_id and l.symbol ~ '^[0-9]{6}$'
 where s.security_type_code = 'equity' and s.country_iso2 = 'CN'
 group by s.security_id
"""

#: Equities with a Hong Kong line, and its code as a number: from a listing on `HK` or a `.HK`
#: provider symbol. An H-share is often held under an OTC line only, so this reaches some of them.
HK_SECURITIES = r"""
select s.security_id::text, min(c.code::int)
  from market.security s
  join lateral (
        select substring(l.symbol from '^0*([0-9]{1,5})$') code
          from market.listing l where l.security_id = s.security_id and l.exch_code = 'HK'
        union all
        select substring(p.symbol from '^0*([0-9]{1,5})\.HK$')
          from market.security_provider_symbol p where p.security_id = s.security_id
       ) c on c.code is not null
 where s.security_type_code = 'equity'
 group by s.security_id
"""

#: The run's filers: org id -> the code the search is asked with, and EVERY security holding it.
#: The code comes from any holder's six-digit listing, so a holder with none (an H-share line, a
#: sibling) still receives the filer's reports: joining each holder to its own six-digit listing
#: dropped it from the security list without a word.
FILERS = """
select sf.filer_id,
       (select min(l.symbol)
          from market.security_filer o
          join market.listing l on l.security_id = o.security_id and l.symbol ~ '^[0-9]{6}$'
         where o.source_code = 'cninfo' and o.filer_id = sf.filer_id),
       array_agg(distinct sf.security_id::text)
  from market.security_filer sf
 where sf.source_code = 'cninfo' and sf.filer_id = any(%s)
 group by sf.filer_id
"""

_UNREAD = segments.unread_sql(REGULATOR)

#: Only a PDF link is a report the parser can read (`Announcement.is_report`). Both `%` are
#: escaped: every query this joins takes parameters.
_PDF = "lower(f.accession_number) like '%%.pdf'"

#: Of some reports, those read at the current parser version: the rest are due.
READ_ALREADY = f"""
select distinct f.accession_number
  from market.security_filing f
 where f.source_code = 'cninfo' and f.accession_number = any(%s) and not {_UNREAD}
"""

#: A filer's full reports that are due, or named: each with the securities whose row is unread.
FILINGS = f"""
select f.accession_number, min(coalesce(f.filing_date, f.report_date)),
       array_agg(f.security_id::text order by f.security_id) filter (where {_UNREAD}),
       array_agg(f.security_id::text order by f.security_id)
  from market.security_filing f
 where f.source_code = 'cninfo' and f.report_type = '{FULL}' and {_PDF}
   and f.security_id = any(%s::uuid[])
 group by f.accession_number
having bool_or({_UNREAD}) or f.accession_number = any(%s)
"""

#: Full reports never read that we have held for N days, and those behind the parser.
UNREAD = f"""
select count(distinct f.accession_number) filter (where {{never}}),
       count(distinct f.security_id) filter (where {{never}}),
       count(distinct f.accession_number) filter (where f.segments_parsed_at is not null)
  from market.security_filing f
 where f.source_code = 'cninfo' and f.report_type = '{FULL}' and {_PDF} and {_UNREAD}
""".format(
    never="f.segments_parsed_at is null and f.as_of <= now() - make_interval(days => %(days)s)"
)


@dataclass(frozen=True)
class Filer:
    org_id: str
    code: str
    security_ids: tuple[str, ...]


def filers(conn: Any, org: Sequence[str]) -> dict[str, Filer]:
    if not org:
        return {}
    with conn.cursor() as cur:
        cur.execute(FILERS, (list(org),))
        return {
            str(o): Filer(str(o), str(code), tuple(sorted(str(s) for s in ids)))
            for o, code, ids in cur.fetchall()
        }


def read_already(conn: Any, urls: Sequence[str]) -> set[str]:
    if not urls:
        return set()
    with conn.cursor() as cur:
        cur.execute(READ_ALREADY, (list(urls),))
        return {str(r[0]) for r in cur.fetchall()}


@dataclass(frozen=True)
class Filing:
    accession: str
    filing_date: date | None
    security_ids: tuple[str, ...]
    due: bool


def filings(conn: Any, security_ids: Sequence[str], named: Sequence[str] = ()) -> list[Filing]:
    """A filer's due full reports and any named ones, OLDEST FIRST: a correction restates the
    year its original stated, and the newer one must be written last."""
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


def filing_rows(security_ids: Sequence[str], found: Sequence[Announcement]) -> list[dict[str, Any]]:
    """`security_filing` rows for every announcement, for every security of the filer.

    Only these columns are written, so `segments_parsed_at` and `segments_parser_version` stand: a
    re-walk must never make a report look unread.
    """
    return [
        {
            "security_id": sid,
            "accession_number": a.url,
            "report_type": FORM_FOR[a.kind],
            "report_date": a.date,
            "filing_date": a.date,
            "report_url": a.url,
            "source_code": SOURCE,
            # LOAD-BEARING: the SEC queue selects `is_xbrl` filings to parse.
            "is_xbrl": False,
        }
        for sid in security_ids
        for a in found
    ]


# ── Raw ────────────────────────────────────────────────────────────────────────────────────────

#: What stage 1 adds to a document's row, and why each may be added (raw-layer rule):
#:   `document`     `announcements` (the search, page `accession`) or `report`;
#:   `accession`    the page asked (`page-1`) or the report's public URL: the key to our model;
#:   `code`         the stock code the search was asked with, which the body does not echo;
#:   `http_status`  200 or 404;
#:   `outcome`      how a report fetch ENDED (`answered`, `absent`, `too_large`, `not_pdf`).
CONTEXT_COLUMNS = ("document", "accession", "code", "http_status", "outcome")
MERGE_ON = ("document", "accession")

ReportOutcome = Literal["answered", "absent", "too_large", "not_pdf"]
REPORT_OUTCOMES: frozenset[str] = frozenset(("answered", "absent", "too_large", "not_pdf"))


def list_row(answer: cninfo.Answer, *, code: str, page: int, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "announcements",
        "accession": f"page-{page}",
        "code": code,
        "http_status": answer.status,
        "outcome": "answered" if answer.status == 200 else "absent",
    }


def report_outcome(answer: cninfo.Answer) -> ReportOutcome:
    if answer.too_large:
        return "too_large"
    if answer.status != 200:
        return "absent"
    return "answered" if cninfo.is_pdf(answer.document.body) else "not_pdf"


def report_row(answer: cninfo.Answer, *, url: str, code: str, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "report",
        "accession": url,
        "code": code,
        "http_status": answer.status,
        "outcome": report_outcome(answer),
    }


def has_more(body: bytes) -> bool:
    """Whether a search page says the category holds more announcements than it showed.

    `hasMore` is the field to trust: measured 2026-11-23, `totalpages` reads 1 for 689 of the
    1,418 filers whose `totalAnnouncement` is above one page (38 to 57 against a page of 30)."""
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return False
    return isinstance(data, dict) and data.get("hasMore") is True


def history_pages(rows: Sequence[Mapping[str, Any]]) -> dict[int, Mapping[str, Any]]:
    """The answered search pages after the first a filer's file holds, by page number.

    History is walked once: a report reaches a later page only by being pushed down by newer ones,
    which page 1 has already listed, so a stored page never needs asking again."""
    out: dict[int, Mapping[str, Any]] = {}
    for r in rows:
        accession = str(r.get("accession") or "")
        if (
            r.get("document") == "announcements"
            and accession.startswith("page-")
            and accession[5:].isdigit()
            and int(accession[5:]) > 1
            and r.get("http_status") == 200
            and r.get("body") is not None
        ):
            out[int(accession[5:])] = r
    return out


def newest_list(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """The newest answered search page 1 a filer's file holds."""
    answered = [
        r
        for r in rows
        if r.get("document") == "announcements"
        and r.get("accession") == "page-1"
        and r.get("http_status") == 200
        and r.get("body") is not None
    ]
    return max(answered, key=lambda r: str(r.get("fetched_at"))) if answered else None


@dataclass(frozen=True)
class Held:
    """A report's newest finished fetch."""

    outcome: ReportOutcome
    fetched_at: str
    run_id: str
    #: True once retention dropped the body: the report is no longer held, only recorded.
    pruned: bool


def held_report(rows: Sequence[Mapping[str, Any]], url: str) -> Held | None:
    done = [
        r
        for r in rows
        if r.get("document") == "report"
        and r.get("accession") == url
        and r.get("outcome") in REPORT_OUTCOMES
        and r.get("fetched_at") is not None
    ]
    if not done:
        return None
    newest = max(done, key=lambda r: str(r["fetched_at"]))
    pruned = newest.get("outcome") == "answered" and newest.get("pruned_at") is not None
    return Held(
        newest["outcome"], str(newest["fetched_at"]), str(newest.get("run_id") or ""), pruned
    )


def report_body(rows: Sequence[Mapping[str, Any]], held: Held) -> bytes | None:
    """The PDF the given fetch wrote, or None."""
    if held.outcome != "answered" or held.pruned:
        return None
    for row in rows:
        if (
            row.get("document") == "report"
            and str(row.get("run_id") or "") == held.run_id
            and row.get("body") is not None
        ):
            return bytes(row["body"])
    return None


def as_document(row: Mapping[str, Any]) -> Document:
    return Document(
        url=str(row["url"]),
        body=bytes(row["body"]),
        content_type=str(row.get("content_type") or ""),
        fetched_at=datetime.fromisoformat(str(row["fetched_at"])),
    )


# ── Retention (Phase 6 decision 4) ─────────────────────────────────────────────────────────────

#: Reports held whole above this are pruned, read ones oldest-read first, down to `TARGET_BYTES`.
CAP_BYTES = 5 * 1024**3
TARGET_BYTES = 4 * 1024**3
#: A read report is pruned this long after it was read, whatever the total.
KEEP_DAYS = 30

#: Of some reports, when each was read at the current parser version (the newest mark of any
#: security holding it). A report not returned is unread and is never pruned.
READ_AT = f"""
select f.accession_number, max(f.segments_parsed_at)
  from market.security_filing f
 where f.source_code = 'cninfo' and f.accession_number = any(%s)
 group by f.accession_number
having not bool_or({_UNREAD})
"""


@dataclass(frozen=True)
class HeldReport:
    key: str
    url: str
    bytes: int
    read_at: datetime | None


def read_at(conn: Any, urls: Sequence[str]) -> dict[str, datetime]:
    if not urls:
        return {}
    with conn.cursor() as cur:
        cur.execute(READ_AT, (list(urls),))
        return {str(url): at for url, at in cur.fetchall() if at is not None}


def choose_prunes(
    held: Sequence[HeldReport],
    *,
    now: datetime,
    cap: int = CAP_BYTES,
    target: int = TARGET_BYTES,
    keep_days: int = KEEP_DAYS,
) -> list[HeldReport]:
    """Which held reports lose their body: every read one older than `keep_days`, then, while the
    total is over `cap`, read ones oldest-read first until it is at or under `target`. An unread
    report is never chosen, so a backlog fetched faster than it is read can exceed the cap; the
    check reports that rather than losing a report nobody has read."""
    read = sorted((h for h in held if h.read_at is not None), key=lambda h: (h.read_at, h.url))
    chosen = [h for h in read if h.read_at is not None and (now - h.read_at).days >= keep_days]
    total = sum(h.bytes for h in held) - sum(h.bytes for h in chosen)
    if total > cap:
        taken = {id(h) for h in chosen}
        for h in read:
            if total <= target:
                break
            if id(h) in taken:
                continue
            chosen.append(h)
            total -= h.bytes
    return chosen


# ── A table whose stated unit is wrong ─────────────────────────────────────────────────────────
#
# 航天长峰 (600855) heads its FY2025 table `单位：元` over `军工电子 54,294.31`, two lines below
# its own text stating `军工电子业务主营业务收入54,294.31万元`. The reader takes the unit a table
# states, as it must, so the split was stored at 1/10,000: 0.12 m yuan for a 1.23 bn line. In the
# table nothing separates that from a small company. The company's own revenue does: it is stated
# apart from the table (the user's decision, 2026-11-23).
#
# Measured on production's 25 such reports: one more is off by exactly 1,000 (千元 figures under a
# 元 line), and the nine whose year has a stated revenue sit at 0.47 to 1.14 of it once corrected.
# A main-business split (主营业务) is not 营业收入, so a band of 1% would have caught two. The band
# is wide because the alternative is four orders of magnitude away: a split that is right sits near
# 1.0 of revenue unscaled, and the two factors' bands cannot overlap.
UNIT_FACTORS = (10_000, 1_000)
UNIT_REVENUE_BAND = (0.3, 1.2)
#: How far a stated revenue's date may sit from the split's period end and still be that year.
UNIT_REVENUE_DAYS = 10

ANNUAL_REVENUE = """
select distinct on (m.as_of) m.as_of, m.value::float8
  from market.security_metric m
 where m.security_id = any(%s::uuid[]) and m.metric_code = 'revenue'
   and m.period_type = 'annual' and m.value > 0
 order by m.as_of, market.source_priority(m.source_code) desc, m.security_id
"""


def annual_revenue(conn: Any, security_ids: Sequence[str]) -> dict[date, float]:
    """The company's stated annual revenue by period end, from the best source per year."""
    if not security_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(ANNUAL_REVENUE, (list(security_ids),))
        return {as_of: float(value) for as_of, value in cur.fetchall()}


def _revenue_for(revenue: Mapping[date, float], ending: str) -> float | None:
    try:
        end = date.fromisoformat(ending)
    except ValueError:
        return None
    near = [
        (abs((as_of - end).days), value)
        for as_of, value in revenue.items()
        if abs((as_of - end).days) <= UNIT_REVENUE_DAYS
    ]
    return min(near)[1] if near else None


def rescale_misstated_units(
    facts: Sequence[SegmentFact], revenue: Mapping[date, float]
) -> tuple[list[SegmentFact], dict[tuple[str, str], int]]:
    """Facts with every table read in the wrong unit put right, and which (axis, period) were.

    A table is judged by its placed revenue split: annual, top level, `partition_id > 0`. When its
    total times a factor lands in `UNIT_REVENUE_BAND` of that year's stated revenue, every fact of
    that axis and period, of every metric, is multiplied by the factor. A year with no stated
    revenue is left as read: no evidence, no change.
    """
    totals: dict[tuple[str, str], float] = {}
    for f in facts:
        if (
            f.metric_code == "revenue"
            and f.period_type == "annual"
            and f.partition_id > 0
            and f.parent_member is None
        ):
            key = (f.axis, f.period_ending)
            totals[key] = totals.get(key, 0.0) + f.value
    factor_of: dict[tuple[str, str], int] = {}
    low, high = UNIT_REVENUE_BAND
    for (axis, ending), total in totals.items():
        stated = _revenue_for(revenue, ending)
        if stated is None or total <= 0:
            continue
        for factor in UNIT_FACTORS:
            if low <= total * factor / stated <= high:
                factor_of[(axis, ending)] = factor
                break
    if not factor_of:
        return list(facts), {}
    out: list[SegmentFact] = []
    for f in facts:
        scale = factor_of.get((f.axis, f.period_ending))
        if scale is None:
            out.append(f)
            continue
        out.append(
            replace(
                f,
                value=round(f.value * scale, 2),
                reconciled_to=None
                if f.reconciled_to is None
                else round(f.reconciled_to * scale, 2),
            )
        )
    return out, factor_of


# ── The segment note's vocabulary ──────────────────────────────────────────────────────────────

#: `market.segment_note_rule` rows of the note reader (`cn_note`), in the order it tries them.
NOTE_RULES = """
select rule, pattern, role
  from market.segment_note_rule
 where regulator = %s and rule in ('heading', 'label', 'member')
 order by rule, priority desc, pattern
"""


def build_note_vocabulary(rules: Sequence[tuple[str, str, str]]) -> sn.NoteVocabulary | None:
    """The note reader's vocabulary, or None while no heading or label row exists: the stage stays
    off until muffin-deployment seeds the rows, so the image can roll before the deploy."""
    headings = tuple(sn.Heading(p, role) for rule, p, role in rules if rule == "heading")
    labels = tuple(sn.Label(p, role) for rule, p, role in rules if rule == "label")
    members = tuple(sn.Heading(p, role) for rule, p, role in rules if rule == "member")
    if not headings or not labels:
        return None
    return sn.NoteVocabulary(headings=headings, labels=labels, members=members)


def note_vocabulary(conn: Any) -> sn.NoteVocabulary | None:
    with conn.cursor() as cur:
        cur.execute(NOTE_RULES, (REGULATOR,))
        rules = [(str(a), str(b), str(c)) for a, b, c in cur.fetchall()]
    return build_note_vocabulary(rules)
