"""Writing one filing's segments: its rows, what it no longer states, and the mark that it was read.

FOUR LANES WRITE SEGMENTS (SEC, DART, NSE, CNINFO), so the rule lives here once. The edge carried a
copy in each of its four handlers.

A PARSER RULE THAT STOPS EMITTING A FACT CANNOT RETRACT IT BY UPSERTING. A member the parser now
drops is never offered again, so its last row would stand for ever in a served partition. Measured
2026-09-05, the day the subtotal rule shipped: 41 subtotal rows still sat in partition 1 beside
their own children. So a filing's rows are RETRACTED PER ACCESSION: after the upsert, every row
this filing owns that the parse no longer produces is deleted. Per accession and never on the
row's key, which leaves the accession out so that a newer filing restating a period takes the row
over; a row another filing owns is not this filing's to delete.

A ROW IS REPLACED ONLY BY A FILING FILED NO EARLIER THAN THE ONE HOLDING IT. The key leaves the
accession out, so whichever filing was written last owned a period both state, and the lane relied
on writing a filer's filings oldest first within a run. That held for a wave and failed for a
re-read: reading an older filing again on its own (a hand `reread`, or the 2026-11-14 re-queue of
filings the edge had read as having no instance) would have taken its periods back from the newer
filing, with values from before the newer one restated them. So the newer filing keeps the row,
and the older filing's row for that key is counted `kept_by_newer` and not written. Ties on the
filing date go to the larger accession, as a later submission sorts.

ONLY WHEN THE DOCUMENT WAS READ. A filing with no instance, or one refused for its size, says
nothing about its segments: its rows stand, and it is still marked as read, because both are facts
about an immutable document. The edge's first retraction ran on those too and removed 91 rows in
three hours.
"""

from collections import Counter
from collections.abc import Collection, Sequence
from datetime import UTC, date, datetime
from typing import Any

from muffin_ingest.facets import segments
from muffin_ingest.facets.segments import SegmentFact
from muffin_ingest.writers import DbCursor, retract_unproduced, upsert

TABLE = "market.security_segment"

#: The keys of `security_id`'s rows that a filing filed after `accession` holds, for the periods
#: given. A row whose filing is not in `security_filing` (no date) never outranks this one.
HELD_BY_NEWER = """
select g.axis, g.member_code, g.parent_key, g.metric_code, g.period_type, g.period_ending::text
  from market.security_segment g
  join market.security_filing f
    on f.security_id = g.security_id and f.accession_number = g.accession_number
 where g.security_id = %s
   and g.accession_number <> %s
   and g.period_ending = any(%s::date[])
   and (f.filing_date > %s or (f.filing_date = %s and g.accession_number > %s))
"""

#: `SEGMENT_KEY` without `security_id`, in `HELD_BY_NEWER`'s column order.
_KEY = [k for k in segments.SEGMENT_KEY if k != "security_id"]


def _held_by_newer(
    cur: DbCursor, *, security_id: str, accession: str, filed: date, rows: Sequence[dict[str, Any]]
) -> set[tuple[str, ...]]:
    periods = sorted({str(row["period_ending"]) for row in rows})
    if not periods:
        return set()
    cur.execute(HELD_BY_NEWER, (security_id, accession, periods, filed, filed, accession))
    return {tuple(str(v) for v in held) for held in cur.fetchall()}


def file_filing(
    cur: DbCursor,
    *,
    security_ids: Sequence[str],
    accession: str,
    filed: date,
    facts: Sequence[SegmentFact] | None,
    source: str,
    as_of: str,
    known_currencies: Collection[str],
    parser_version: int,
) -> Counter[str]:
    """One filing, for each security it is written for, in the caller's transaction.

    `facts` is None when no document was read; an empty list is a document read that discloses no
    segments, which retracts what the filing owned.
    """
    stats: Counter[str] = Counter()
    if facts is not None:
        for security_id in security_ids:
            rows, built = segments.segment_rows(
                facts,
                security_id=security_id,
                accession=accession,
                source=source,
                as_of=as_of,
                known_currencies=known_currencies,
            )
            stats.update(built)
            newer = _held_by_newer(
                cur, security_id=security_id, accession=accession, filed=filed, rows=rows
            )
            ours = [r for r in rows if tuple(str(r[k]) for k in _KEY) not in newer]
            stats["kept_by_newer"] += len(rows) - len(ours)
            written = upsert(
                cur,
                TABLE,
                ours,
                conflict=segments.SEGMENT_KEY,
                update=segments.SEGMENT_UPDATE,
                compare=segments.SEGMENT_COMPARE,
                computed=["parent_key"],
            )
            stats["rows"] += written.written
            stats["changed"] += 0 if written.changed is None else written.changed
            stats["collapsed"] += written.collapsed
            retracted = retract_unproduced(
                cur,
                TABLE,
                rows,
                scope={"security_id": security_id, "accession_number": accession},
                key=segments.SEGMENT_RETRACT_KEY,
                types=segments.SEGMENT_RETRACT_TYPES,
            )
            stats["retracted"] += retracted.retracted
    cur.execute(
        segments.STAMP_PARSED,
        (datetime.now(UTC), parser_version, accession, list(security_ids)),
    )
    stats["stamped"] += len(security_ids)
    return stats
