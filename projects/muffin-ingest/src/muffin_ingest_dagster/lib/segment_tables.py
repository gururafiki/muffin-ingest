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

ONLY WHEN THE DOCUMENT WAS READ. A filing with no instance, or one refused for its size, says
nothing about its segments: its rows stand, and it is still marked as read, because both are facts
about an immutable document. The edge's first retraction ran on those too and removed 91 rows in
three hours.
"""

from collections import Counter
from collections.abc import Collection, Sequence
from datetime import UTC, datetime

from muffin_ingest.facets import segments
from muffin_ingest.facets.segments import SegmentFact
from muffin_ingest.writers import DbCursor, retract_unproduced, upsert

TABLE = "market.security_segment"


def file_filing(
    cur: DbCursor,
    *,
    security_ids: Sequence[str],
    accession: str,
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
            written = upsert(
                cur,
                TABLE,
                rows,
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
