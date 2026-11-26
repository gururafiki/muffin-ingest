"""Korea's segment lane: DART's filers, their annual reports, and which are due.

PORTED FROM THE EDGE's `kr-filings` and `security-kr-segments` handlers and their queues,
`market.pending_kr_history` and `pending_kr_segments` (Phase 6, decided 2026-11-09). The parser is
`facets.segments`, unchanged: a DART instance carries the same `ifrs-full` segment axes as an SEC
20-F, so it reads through the same code as SEC's.

WHAT CHANGED FROM THE EDGE, AND WHY:
  * the corp code comes from DART's registry (`corpCode.xml`, one request a week) instead of a
    two-year sweep of three-month `list.json` windows. Measured 2026-11-16: 894 Korean equities hold
    a six-digit code, against the 443 filers the sweep had found by 2026-09;
  * a company is walked on a 30-day hash-spread floor instead of every 90 days;
  * a refusal or a failed fetch never stamps a filing read;
  * the credential is redacted from every URL raw keeps.
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from muffin_ingest.facets import segments
from muffin_ingest.providers import dart

REGULATOR = "dart"
#: `market.data_source` code of every row this lane writes (filings and segments); the edge's.
SOURCE = "dart"

#: `filing_form` code: the annual report (사업보고서), its corrections (`[기재정정]사업보고서`)
#: included, as the edge filed them. Only these carry the archive `reprt_code=11011` serves.
ANNUAL = "사업보고서"

#: The archive entry holding the instance.
_INSTANCE = re.compile(r"\.xbrl$", re.IGNORECASE)
#: An instance larger than this is refused unread (the edge's and SEC's gate).
MAX_INSTANCE_BYTES = 32 * 1024 * 1024


# ── The registry ───────────────────────────────────────────────────────────────────────────────


def corp_codes(archive: bytes) -> dict[str, str]:
    """The registry ZIP -> six-digit stock code -> eight-digit corp code. A filer with no stock
    code is unlisted and skipped; a stock code listed twice keeps its first corp code."""
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".xml")]
        if not names:
            raise ValueError("DART's registry archive holds no XML")
        xml = z.read(names[0]).decode("utf-8", errors="replace")
    out: dict[str, str] = {}
    for block in re.finditer(r"<list>([\s\S]*?)</list>", xml):
        corp = re.search(r"<corp_code>\s*(\d{8})\s*</corp_code>", block.group(1))
        stock = re.search(r"<stock_code>\s*(\d{6})\s*</stock_code>", block.group(1))
        if corp and stock:
            out.setdefault(stock.group(1), corp.group(1))
    return out


# ── A company's filings ────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Filed:
    rcept_no: str
    report_name: str
    #: `YYYY-MM-DD`, from DART's `rcept_dt`.
    filed: str | None

    @property
    def is_annual(self) -> bool:
        return ANNUAL in self.report_name


@dataclass(frozen=True)
class FilingsPage:
    #: DART's own status: `000` answered, `013` no data (an answer), anything else a fault.
    status: str
    filed: tuple[Filed, ...]
    total_pages: int

    @property
    def answered(self) -> bool:
        return self.status in ("000", "013")


def _iso(yyyymmdd: Any) -> str | None:
    s = str(yyyymmdd or "")
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}" if re.fullmatch(r"\d{8}", s) else None


def parse_filings(body: bytes) -> FilingsPage:
    """A `list.json` answer. DART signals a fault with HTTP 200 and a `status`, so a body that is
    not JSON, or carries no status, is a fault too (`status` empty)."""
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return FilingsPage("", (), 0)
    if not isinstance(data, dict):
        return FilingsPage("", (), 0)
    status = str(data.get("status") or "")
    if status != "000":
        return FilingsPage(status, (), 0)
    listed = data.get("list")
    rows = listed if isinstance(listed, list) else []
    filed = tuple(
        Filed(str(r.get("rcept_no")), str(r.get("report_nm") or "")[:300], _iso(r.get("rcept_dt")))
        for r in rows
        if isinstance(r, dict) and r.get("rcept_no")
    )
    pages = data.get("total_page")
    return FilingsPage(status, filed, int(pages) if isinstance(pages, (int, float)) else 1)


# ── A filing's archive ─────────────────────────────────────────────────────────────────────────

ArchiveKind = Literal["zip", "dart_says_no", "not_zip"]


def classify(body: bytes) -> ArchiveKind:
    """A ZIP, DART saying no (`{"status":"013"}` or `<status>014</status>`, both with HTTP 200), or
    something else. Status 014, "the file does not exist", is the commonest answer for an older
    report with no XBRL archive and is a settled fact about an immutable filing (the edge's
    `classifyBody`, after it held a backlog's head for 143 runs by knowing only the JSON form)."""
    if body[:2] == b"PK":
        return "zip"
    m = re.search(rb'"status"\s*:\s*"(\d+)"|<status>\s*(\d+)\s*</status>', body[:300])
    status = (m.group(1) or m.group(2)) if m else None
    return "dart_says_no" if status and status != b"000" else "not_zip"


class TooLarge(Exception):
    """The archive's instance is over the gate."""


def instance_of(archive: bytes) -> bytes | None:
    """The instance (the `.xbrl` entry) of a filing's archive, or None when it holds none."""
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        for info in z.infolist():
            if _INSTANCE.search(info.filename):
                if info.file_size > MAX_INSTANCE_BYTES:
                    raise TooLarge(info.filename)
                return z.read(info)
    return None


# ── Queries ────────────────────────────────────────────────────────────────────────────────────

#: Korean equities and the six-digit code DART lists them under: the bare listing symbol, or the
#: digits of the `.KS`/`.KQ` price symbol. Measured 2026-11-16: 894, no code shared by two.
SECURITIES = r"""
select s.security_id::text, min(c.code)
  from market.security s
  join lateral (
        select substring(l.symbol from '^([0-9]{6})$') code
          from market.listing l where l.security_id = s.security_id
        union all
        select substring(p.symbol from '^([0-9]{6})\.(?:KS|KQ)$')
          from market.security_provider_symbol p where p.security_id = s.security_id
       ) c on c.code is not null
 where s.security_type_code = 'equity' and s.country_iso2 = 'KR'
 group by s.security_id
"""

#: The run's filers: corp code -> every security holding it.
FILERS = """
select sf.filer_id, array_agg(distinct sf.security_id::text)
  from market.security_filer sf
 where sf.source_code = 'dart' and sf.filer_id = any(%s)
 group by sf.filer_id
"""

_UNREAD = segments.unread_sql(REGULATOR)

#: Of some filings, those read at the current parser version: the rest are due.
READ_ALREADY = f"""
select distinct f.accession_number
  from market.security_filing f
 where f.source_code = 'dart' and f.accession_number = any(%s) and not {_UNREAD}
"""

#: A filer's annual reports that are due, or named: each with the securities whose row is unread.
FILINGS = f"""
select f.accession_number, min(f.filing_date),
       array_agg(f.security_id::text order by f.security_id) filter (where {_UNREAD}),
       array_agg(f.security_id::text order by f.security_id)
  from market.security_filing f
 where f.source_code = 'dart' and f.report_type = '{ANNUAL}'
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
 where f.source_code = 'dart' and f.report_type = '{ANNUAL}' and {_UNREAD}
""".format(
    never="f.segments_parsed_at is null and f.as_of <= now() - make_interval(days => %(days)s)"
)


def filers(conn: Any, corp: Sequence[str]) -> dict[str, tuple[str, ...]]:
    if not corp:
        return {}
    with conn.cursor() as cur:
        cur.execute(FILERS, (list(corp),))
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
    filing_date: date | None
    security_ids: tuple[str, ...]
    due: bool


def filings(conn: Any, security_ids: Sequence[str], named: Sequence[str] = ()) -> list[Filing]:
    """A filer's due annual reports and any named ones, OLDEST FIRST: a correction restates the
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


def filing_rows(security_ids: Sequence[str], found: Sequence[Filed]) -> list[dict[str, Any]]:
    """`security_filing` rows for every annual report, for every security of the filer, as the edge
    wrote them. Only these columns are written, so the marks stand: a re-walk must never make a
    filing look unread."""
    return [
        {
            "security_id": sid,
            "accession_number": f.rcept_no,
            "report_type": ANNUAL,
            "filing_date": f.filed,
            "source_code": SOURCE,
            "is_xbrl": True,
        }
        for sid in security_ids
        for f in found
        if f.is_annual
    ]


# ── Raw ────────────────────────────────────────────────────────────────────────────────────────

#: What stage 1 adds to a document's row, and why each may be added (raw-layer rule):
#:   `document`     `filings` (a `list.json` page) or `instance` (an archive);
#:   `accession`    the page asked (`page-1`) or the filing's receipt number: the key to our model;
#:   `http_status`  DART's status line;
#:   `outcome`      how an archive fetch ENDED (`answered`, `absent`, `too_large`, `not_zip`).
CONTEXT_COLUMNS = ("document", "accession", "http_status", "outcome")
MERGE_ON = ("document", "accession")

ArchiveOutcome = Literal["answered", "absent", "too_large", "not_zip"]
ARCHIVE_OUTCOMES: frozenset[str] = frozenset(("answered", "absent", "too_large", "not_zip"))


ListOutcome = Literal["answered", "absent", "refused"]


def list_outcome(answer: dart.Answer) -> ListOutcome:
    """`answered` for DART's `000` or `013` (no data, an answer); `absent` for a 404; `refused` for
    any other status in the body (`020` is its daily request limit, `800` maintenance)."""
    if answer.status != 200:
        return "absent"
    return "answered" if parse_filings(answer.document.body).answered else "refused"


def list_row(answer: dart.Answer, *, page: int, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "filings",
        "accession": f"page-{page}",
        "http_status": answer.status,
        "outcome": list_outcome(answer),
    }


def archive_outcome(answer: dart.Answer) -> ArchiveOutcome:
    if answer.too_large:
        return "too_large"
    if answer.status != 200:
        return "absent"
    kind = classify(answer.document.body)
    if kind == "zip":
        return "answered"
    return "absent" if kind == "dart_says_no" else "not_zip"


def archive_row(answer: dart.Answer, *, rcept_no: str, run_id: str) -> dict[str, Any]:
    return {
        **answer.document.as_row(run_id),
        "document": "instance",
        "accession": rcept_no,
        "http_status": answer.status,
        "outcome": archive_outcome(answer),
    }


def newest_lists(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """The answered `list.json` pages of the newest walk a filer's file holds, by page."""
    walks: dict[str, list[Mapping[str, Any]]] = {}
    for r in rows:
        if r.get("document") == "filings" and r.get("outcome") == "answered" and r.get("body"):
            walks.setdefault(str(r.get("run_id") or ""), []).append(r)
    if not walks:
        return []
    newest = max(walks.values(), key=lambda rs: max(str(r.get("fetched_at")) for r in rs))
    return sorted(newest, key=lambda r: str(r.get("accession")))


@dataclass(frozen=True)
class Held:
    """An archive's newest finished fetch."""

    outcome: ArchiveOutcome
    fetched_at: str
    run_id: str


def held_archive(rows: Sequence[Mapping[str, Any]], rcept_no: str) -> Held | None:
    done = [
        r
        for r in rows
        if r.get("document") == "instance"
        and r.get("accession") == rcept_no
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
            row.get("document") == "instance"
            and str(row.get("run_id") or "") == held.run_id
            and row.get("body") is not None
        ):
            return bytes(row["body"])
    return None
