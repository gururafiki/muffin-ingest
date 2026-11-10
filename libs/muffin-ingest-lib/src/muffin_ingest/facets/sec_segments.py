"""SEC's segment lane: which filings are due, and what raw holds for one.

PORTED FROM THE EDGE's `security-segments` handler and its queue, `market.pending_segments`
(Phase 6, decided 2026-11-09). The parser is `facets.segments`; this module is the SEC side of it.

THE QUEUE IS THE FILING TABLE'S OWN MARKERS, read per filer. A filing is due when it is an accounts
form (`filing_form.carries_segments`: 10-K, 10-Q, 20-F, 40-F) that SEC says carries XBRL, and it has
never been read or was read by an older parser version. The edge's view ranked every filing of
every filer on each call, which is what timed out 17% of its runs (343 of 2,015 in the week to
2026-11-09); asked per filer through the primary key, the same question needs no ranking.

`is_xbrl` IS NEW HERE, AND MEASURED: 77,948 accounts filings carry `is_xbrl = false` (1993 to
2026-08) and not one of them has a segment row, so the two index requests the edge spent on each
bought nothing.

A CIK FANS OUT to every equity holding it, as in the SEC filer lane: GOOG and GOOGL share
Alphabet's filings, the instance is fetched once, and each security's rows are written from it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, get_args

from muffin_ingest.providers.sec_edgar import InstanceFetch, InstanceOutcome

REGULATOR = "sec"
#: `market.data_source` code of every row this lane writes; the edge wrote the same.
SOURCE = "sec-segments"

_ACCOUNTS_FILING = """
       s.cik is not null
   and s.security_type_code = 'equity'
   and f.is_xbrl
   and f.report_type in (select form_code from market.filing_form
                          where source_code = 'sec' and carries_segments)
"""
_UNREAD = """
       (f.segments_parsed_at is null
        or coalesce(f.segments_parser_version, 0)
           < (select max(version) from market.segment_parser))
"""

#: Every filer with a filing to read, and how many: what the due sensor asks.
DUE_BY_FILER = f"""
select s.cik, count(distinct f.accession_number)
  from market.security_filing f
  join market.security s on s.security_id = f.security_id
 where {_ACCOUNTS_FILING}
   and {_UNREAD}
 group by s.cik
"""

#: The filings of some filers that are due, or that are named: each with the securities whose row
#: is unread, and every security holding it.
FILINGS = f"""
select s.cik, f.accession_number, min(f.filing_date),
       array_agg(f.security_id::text order by f.security_id) filter (where {_UNREAD}),
       array_agg(f.security_id::text order by f.security_id)
  from market.security_filing f
  join market.security s on s.security_id = f.security_id
 where {_ACCOUNTS_FILING}
   and s.cik = any(%s)
 group by s.cik, f.accession_number
having bool_or({_UNREAD}) or f.accession_number = any(%s)
"""

#: Accounts filings filed at least this many days ago and still unread: the completeness check.
UNREAD_OLDER_THAN = f"""
select count(distinct f.accession_number), count(distinct s.cik), min(f.filing_date)
  from market.security_filing f
  join market.security s on s.security_id = f.security_id
 where {_ACCOUNTS_FILING}
   and {_UNREAD}
   and f.filing_date <= current_date - %s::int
"""


@dataclass(frozen=True)
class Filing:
    """One accounts filing of a filer, and the securities to write it for."""

    cik: int
    accession: str
    filing_date: date
    #: The securities whose row is unread; for a filing named but not due, every one holding it.
    security_ids: tuple[str, ...]
    due: bool


def due_by_filer(conn: Any) -> dict[int, int]:
    """CIK -> its count of filings to read."""
    with conn.cursor() as cur:
        cur.execute(DUE_BY_FILER)
        return {int(cik): int(n) for cik, n in cur.fetchall()}


def filings(conn: Any, ciks: Sequence[int], named: Sequence[str] = ()) -> dict[int, list[Filing]]:
    """Per filer, its due filings and any `named` accession, OLDEST FIRST.

    Oldest first because the row's key leaves the accession out: when two filings of one run state
    the same period, the last one written owns it, and that must be the newer filing.
    """
    if not ciks:
        return {}
    with conn.cursor() as cur:
        cur.execute(FILINGS, (list(ciks), list(named)))
        found = [
            Filing(
                cik=int(cik),
                accession=str(accession),
                filing_date=filed,
                security_ids=tuple(unread or holding),
                due=bool(unread),
            )
            for cik, accession, filed, unread, holding in cur.fetchall()
        ]
    out: dict[int, list[Filing]] = {}
    for filing in sorted(found, key=lambda f: (f.filing_date, f.accession)):
        out.setdefault(filing.cik, []).append(filing)
    return out


def unread_older_than(conn: Any, days: int) -> tuple[int, int, date | None]:
    """(filings, filers, the oldest filing date) still unread `days` after they were filed."""
    with conn.cursor() as cur:
        cur.execute(UNREAD_OLDER_THAN, (days,))
        row = cur.fetchone()
    return (0, 0, None) if row is None else (int(row[0]), int(row[1]), row[2])


# ── Raw ────────────────────────────────────────────────────────────────────────────────────────

#: What stage 1 adds to a document's row, and why each may be added (raw-layer rule):
#:   `accession`    the filing the document answers for: the key to our model;
#:   `document`     which of the fetch's requests this is (`index_json`, `index_html`, `instance`);
#:   `http_status`  200 or 404, which the body does not say;
#:   `outcome`      how the fetch ENDED (`answered`, `absent`, `too_large`), on every row of it. A
#:                  refusal by size leaves no instance body to tell it from an absence.
CONTEXT_COLUMNS = ("accession", "document", "http_status", "outcome")

#: The key a filer's file is merged on: a re-fetch replaces the same document of the same filing.
MERGE_ON = ("accession", "document")

#: How a finished fetch can end. A refusal or a transport failure files nothing.
OUTCOMES: frozenset[str] = frozenset(get_args(InstanceOutcome))


def raw_rows(fetch: InstanceFetch, *, accession: str, run_id: str) -> list[dict[str, Any]]:
    """Every document one fetch was answered with, whole, each with the fetch's outcome."""
    return [
        {
            **answer.document.as_row(run_id),
            "accession": accession,
            "document": kind,
            "http_status": answer.status,
            "outcome": fetch.outcome,
        }
        for kind, answer in fetch.answers
    ]


@dataclass(frozen=True)
class Held:
    """What raw holds for one filing: how its newest fetch ended, and its instance if answered."""

    outcome: InstanceOutcome
    body: bytes | None
    fetched_at: str
    run_id: str


def held(rows: Sequence[Mapping[str, Any]]) -> Held | None:
    """One filing's stored rows -> its newest fetch, or None when raw holds no finished fetch.

    THE NEWEST ROW DECIDES THE OUTCOME. A re-fetch replaces the same document of the same filing
    and cannot remove a row its own path did not produce: a filing first found through the HTML
    index and later through `index.json` keeps the old HTML row. So an instance body is used only
    when the newest fetch answered, and only if that fetch wrote it.
    """
    finished = [r for r in rows if r.get("outcome") in OUTCOMES and r.get("fetched_at") is not None]
    if not finished:
        return None
    newest = max(finished, key=lambda r: str(r["fetched_at"]))
    outcome: InstanceOutcome = newest["outcome"]
    body: bytes | None = None
    if outcome == "answered":
        instance = [
            r
            for r in finished
            if r.get("document") == "instance" and r.get("run_id") == newest.get("run_id")
        ]
        if not instance or instance[0].get("body") is None:
            return None
        body = bytes(instance[0]["body"])
    return Held(outcome, body, str(newest["fetched_at"]), str(newest.get("run_id") or ""))


def held_accessions(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    """The filings a filer's file holds a finished fetch for. Reads no body, so stage 1 can ask it
    of every filer it visits."""
    return {
        str(r["accession"])
        for r in rows
        if r.get("accession") is not None and r.get("outcome") in OUTCOMES
    }
