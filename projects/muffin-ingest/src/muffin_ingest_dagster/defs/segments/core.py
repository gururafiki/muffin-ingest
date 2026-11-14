"""Stage 2 of the segments family: stored documents read into `security_segment`.

No provider call: a parser change re-reads files already on disk.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import fx, sec_segments, segments

from muffin_ingest_dagster.defs.sec.partitions import FILERS_PER_RUN, sec_filers
from muffin_ingest_dagster.defs.segments.raw import FRESH_WEEK, SEC_INSTANCE
from muffin_ingest_dagster.lib import segment_tables
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: What a filing considered in a run came to. They sum to `filings`.
#:   `read`         its instance was parsed, its rows written, and it was marked;
#:   `no_instance`  SEC holds no instance for it: marked, nothing written or retracted;
#:   `too_large`    its instance is over the size gate: marked, nothing written or retracted;
#:   `not_held`     stage 1 has not fetched it (refused, or not asked yet): left due;
#:   `failed`       the parse or the write raised: rolled back and left due.
SEGMENT_OUTCOMES = ("read", "no_instance", "too_large", "not_held", "failed")

#: The columns that say what a filer's file holds, without a document's body.
_LIGHT = ("accession", "document", "outcome", "fetched_at", "run_id")


def _by_accession(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    out: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        if row.get("accession") is not None:
            out.setdefault(str(row["accession"]), []).append(row)
    return out


@dg.asset(
    partitions_def=sec_filers,
    deps=[SEC_INSTANCE],
    backfill_policy=dg.BackfillPolicy.multi_run(FILERS_PER_RUN),
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_WEEK,
    metadata={"table": "market.security_segment"},
    description=(
        "Each SEC accounts filing's segments (`sec-segments`), and the mark that it was read."
    ),
)
def sec_segment(
    context: AssetExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.MaterializeResult[None]:
    """Every filing of the run's filers that is due, or that this run fetched, read from raw.

    A FILING IS READ FROM WHAT RAW HOLDS, whichever run fetched it: after a parser version is
    raised, every filing is due again and this costs no request for the ones held. A filing this
    run fetched is read even when it is not due, which is how one filing is read again by hand.

    ONE TRANSACTION PER FILING: its rows, its retraction and its mark land together. A filing
    whose parse or write raises is rolled back, counted and left due, the rest of the run is
    written, and the run then fails naming it.

    `changed` COUNTS A MOVE OF OWNER AS A CHANGE. The row's key leaves the accession out, so when a
    newer filing restates a period an older one stated, writing it moves the row to the newer
    accession even with every value equal. Measured 2026-11-14 on five filers read again against
    production (rolled back): 270 rows identical and 68 changed, all of them Equinor's 2024
    comparatives, equal in value, moving from its FY2024 20-F to its FY2025 one. The edge had read
    those two filings in the other order.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    light = {
        key: _by_accession(raw_store.stored_rows_for(SEC_INSTANCE, key, _LIGHT)) for key in keys
    }
    fetched_now = sorted(
        accession
        for stored in light.values()
        for accession, rows in stored.items()
        if any(r.get("run_id") == run_id for r in rows)
    )
    with postgres.connect() as conn:
        vocabulary = segments.vocabulary(conn)
        known = fx.known_currencies(conn)
        wanted = sec_segments.filings(conn, [int(k) for k in keys], fetched_now)

    stats: Counter[str] = Counter({o: 0 for o in SEGMENT_OUTCOMES})
    errors: list[str] = []
    for key in keys:
        filings = wanted.get(int(key), [])
        if not filings:
            continue
        stats["filers_with_filings"] += 1
        # THE BODIES ARE READ ONLY FOR A FILER WITH A FILING TO PARSE: a filer's file holds every
        # instance fetched for it, megabytes each.
        full: dict[str, list[Mapping[str, Any]]] | None = None
        with postgres.connect() as conn:
            for filing in filings:
                stats["filings"] += 1
                held = sec_segments.finished(light[key].get(filing.accession, []))
                if held is None:
                    stats["not_held"] += 1
                    continue
                facts: list[segments.SegmentFact] | None = None
                try:
                    if held.outcome == "answered":
                        if full is None:
                            full = _by_accession(raw_store.stored_rows_for(SEC_INSTANCE, key))
                        body = sec_segments.instance_body(full.get(filing.accession, []), held)
                        if body is None:
                            stats["not_held"] += 1
                            continue
                        facts = segments.segment_facts_from(
                            segments.text_of(body),
                            vocabulary.axes,
                            vocabulary.concepts,
                            vocabulary.roles,
                        )
                    with conn.cursor() as cur:
                        written = segment_tables.file_filing(
                            cur,
                            security_ids=filing.security_ids,
                            accession=filing.accession,
                            filed=filing.filing_date,
                            facts=facts,
                            source=sec_segments.SOURCE,
                            as_of=held.fetched_at,
                            known_currencies=known,
                            parser_version=vocabulary.parser_version,
                        )
                    conn.commit()
                except Exception as exc:  # one filing must not take the run's others with it
                    conn.rollback()
                    stats["failed"] += 1
                    errors.append(f"{key} {filing.accession}: {type(exc).__name__}: {exc}"[:300])
                    context.log.error("filing %s of %s failed: %s", filing.accession, key, exc)
                    continue
                stats.update(written)
                if facts is None:
                    stats["no_instance" if held.outcome == "absent" else "too_large"] += 1
                else:
                    stats["read"] += 1
                    stats["no_segments"] += not facts

    metadata = {
        "filers_requested": len(keys),
        "parser_version": vocabulary.parser_version,
        "outcomes": sum(stats[o] for o in SEGMENT_OUTCOMES),
        **{
            name: stats[name]
            for name in (
                "filers_with_filings",
                "filings",
                *SEGMENT_OUTCOMES,
                "no_segments",
                "rows",
                "changed",
                "kept_by_newer",
                "collapsed",
                "retracted",
                "placed",
                "unplaced",
                "currency_unknown",
                "stamped",
            )
        },
    }
    if errors:
        raise dg.Failure(
            description=f"{len(errors)} filing(s) could not be read; first: {errors[0]}",
            metadata={**metadata, "first_error": errors[0], "last_error": errors[-1]},
        )
    return dg.MaterializeResult(metadata=metadata)
