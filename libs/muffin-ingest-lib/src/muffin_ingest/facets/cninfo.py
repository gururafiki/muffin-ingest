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
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

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


# ── Queries ────────────────────────────────────────────────────────────────────────────────────

#: The A-share equities the lane serves: as the edge's `pending_cn_filings` chose them.
SECURITIES = """
select s.security_id::text, min(l.symbol)
  from market.security s
  join market.listing l on l.security_id = s.security_id and l.symbol ~ '^[0-9]{6}$'
 where s.security_type_code = 'equity' and s.country_iso2 = 'CN'
 group by s.security_id
"""

#: The run's filers: org id -> the code the search is asked with, and every security holding it.
FILERS = """
select sf.filer_id, min(l.symbol), array_agg(distinct sf.security_id::text)
  from market.security_filer sf
  join market.listing l on l.security_id = sf.security_id and l.symbol ~ '^[0-9]{6}$'
 where sf.source_code = 'cninfo' and sf.filer_id = any(%s)
 group by sf.filer_id
"""

_UNREAD = """
       (f.segments_parsed_at is null
        or coalesce(f.segments_parser_version, 0)
           < (select max(version) from market.segment_parser))
"""

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
