"""Stage 2 of Japan's segment lane: the stored code list, day lists and archives into the database.

No provider call: a reader change re-reads files already on disk.
"""

from collections import Counter
from datetime import date
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import edinet as fe
from muffin_ingest.facets import fx, segments
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.edinet.partitions import (
    DAYS_PER_RUN,
    EDINET_FILERS_PER_RUN,
    edinet_days,
    edinet_filers,
)
from muffin_ingest_dagster.defs.edinet.raw import (
    CODE_LIST,
    DAYS,
    FILINGS,
    FRESH_MONTH,
    FRESH_TWO_DAYS,
    FRESH_TWO_WEEKS,
)
from muffin_ingest_dagster.lib import segment_tables
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: What the registry releases when a company becomes the SEC's (Phase 6c decision 2): its filer row
#: and what was written under it, so one regulator speaks for it.
RELEASE = """
with released as (
  delete from market.security_filer
   where source_code = 'edinet' and security_id = any(%s::uuid[])
  returning security_id
),
segments as (
  delete from market.security_segment g using released r
   where g.security_id = r.security_id and g.source_code = 'edinet'
  returning 1
),
filings as (
  delete from market.security_filing f using released r
   where f.security_id = r.security_id and f.source_code = 'edinet'
  returning 1
)
select (select count(*) from released), (select count(*) from segments),
       (select count(*) from filings)
"""


@dg.asset(
    ins={"raw_edinet_code_list": dg.AssetIn(key=CODE_LIST)},
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_TWO_WEEKS,
    metadata={"table": "market.security_filer"},
    description="Each Japanese equity's EDINET code, from EDINET's own code list.",
)
def security_edinet_filer(
    context: AssetExecutionContext, postgres: Postgres, raw_edinet_code_list: list[dict[str, Any]]
) -> dg.MaterializeResult[None]:
    """Every Japanese equity whose local code the code list holds gets a filer row, unless the SEC
    already gives it a split, and every mapped filer's annual reports the catalogue holds are filed.

    A ROW IS NEVER DELETED FOR A COMPANY MISSING FROM THIS WEEK'S LIST: it keeps its history. One
    that became the SEC's is released with what was written under it.
    """
    archive = next(bytes(r["body"]) for r in raw_edinet_code_list if r.get("body") is not None)
    codes = fe.code_list(archive)
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(fe.SECURITIES)
            securities = [(str(sid), str(code), bool(sec)) for sid, code, sec in cur.fetchall()]
        rows = [
            {
                "security_id": sid,
                "source_code": fe.SOURCE,
                "filer_id": codes[code].edinet_code,
                "via": "registry",
            }
            for sid, code, sec in securities
            if code in codes and not sec
        ]
        the_sec_s = [sid for sid, code, sec in securities if code in codes and sec]
        with conn.cursor() as cur:
            # CLAIMED AS THE REGISTRY'S: an issuer-sibling row for the same security becomes ours.
            written = upsert(
                cur,
                "market.security_filer",
                rows,
                conflict=["security_id", "source_code"],
                update=["filer_id", "via"],
            )
            cur.execute(RELEASE, (the_sec_s,))
            released = cur.fetchone() or (0, 0, 0)
        filed = fe.file_filings(conn, sorted({str(r["filer_id"]) for r in rows}))
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            "listed": len(codes),
            "securities": len(securities),
            "mapped": len(rows),
            "the_sec_s": len(the_sec_s),
            "unmapped": len(securities) - len(rows) - len(the_sec_s),
            "changed": -1 if written.changed is None else written.changed,
            "released": int(released[0]),
            "released_segments": int(released[1]),
            "released_filings": int(released[2]),
            "filed": filed.filed,
            "withdrawn_filings": filed.withdrawn_filings,
            "withdrawn_segments": filed.withdrawn_segments,
        }
    )


#: What a catalogue row may change. `withdrawn` is left out: a withdrawal is only ever set (see
#: `MARK_WITHDRAWN`), so a re-read of the day that listed the original never restores it. So is
#: `listed_on`: the first day that listed a document keeps it.
DOCUMENT_UPDATE = (
    "edinet_code",
    "sec_code",
    "doc_type_code",
    "ordinance_code",
    "form_code",
    "period_start",
    "period_end",
    "filed_on",
    "xbrl",
    "parent_doc_id",
)


@dg.asset(
    partitions_def=edinet_days,
    deps=[DAYS],
    backfill_policy=dg.BackfillPolicy.multi_run(DAYS_PER_RUN),
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_TWO_DAYS,
    metadata={"table": "market.edinet_document"},
    description=(
        "EDINET's annual reports and amendments by day, in `market.edinet_document`, and the "
        "mapped companies' reports filed into `security_filing`."
    ),
)
def edinet_document(
    context: AssetExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.MaterializeResult[None]:
    """Each day's stored list: its annual reports and amendments into the catalogue, its
    withdrawals marked, then `file_edinet_filings` for the filers the days touched."""
    keys = list(context.partition_keys)
    stats: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    withdrawn: list[str] = []
    for key in keys:
        held = fe.held_day(raw_store.stored_rows_for(DAYS, key))
        if held is None:
            stats["days_without_list"] += 1
            continue
        day = fe.parse_day(bytes(held["body"]))
        stats["days"] += 1
        stats["listed"] += len(day.listed)
        if day.count != len(day.listed):
            # EDINET's own count disagrees with what it listed: kept, and counted, because the
            # documents it did list are still its statement.
            stats["count_mismatch"] += 1
        found = fe.document_rows(day.listed, listed_on=key)
        rows.extend(found.rows)
        withdrawn.extend(found.withdrawn)
        withdrawn.extend(r["doc_id"] for r in found.rows if r["withdrawn"])
        stats["incomplete"] += found.incomplete
    # THE NEWEST LISTING LAST: the writer keeps the last row of a key.
    rows.sort(key=lambda r: str(r["listed_on"]))
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            written = upsert(
                cur,
                "market.edinet_document",
                rows,
                conflict=["doc_id"],
                update=DOCUMENT_UPDATE,
            )
            marked: list[str] = []
            if withdrawn:
                cur.execute(fe.MARK_WITHDRAWN, (sorted(set(withdrawn)),))
                marked = [str(r[0]) for r in cur.fetchall()]
        codes = sorted({str(r["edinet_code"]) for r in rows} | set(marked))
        filed = fe.file_filings(conn, codes)
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            "requested": len(keys),
            "days": stats["days"],
            "days_without_list": stats["days_without_list"],
            "listed": stats["listed"],
            "count_mismatch": stats["count_mismatch"],
            "catalogued": len(rows),
            "annual_reports": sum(
                (r["doc_type_code"], r["ordinance_code"], r["form_code"])
                == ("120", "010", "030000")
                for r in rows
            ),
            "incomplete": stats["incomplete"],
            "changed": -1 if written.changed is None else written.changed,
            "collapsed": written.collapsed,
            "withdrawals_named": len(set(withdrawn)),
            "withdrawals_marked": len(marked),
            "filers": len(codes),
            "filed": filed.filed,
            "withdrawn_filings": filed.withdrawn_filings,
            "withdrawn_segments": filed.withdrawn_segments,
        }
    )


#: What an annual report considered in a run came to. They sum to `filings`.
#:   `read`         its note was read against its figures, its rows written (or none) and marked;
#:   `no_instance`  EDINET holds no archive (404) or the archive holds no instance: marked;
#:   `too_large`    over a size gate: marked, unread;
#:   `not_zip`      EDINET answered something else: left due, asked again;
#:   `not_held`     stage 1 has not fetched it (refused or not asked yet): left due;
#:   `failed`       the read or the write raised: rolled back and left due.
SEGMENT_OUTCOMES = ("read", "no_instance", "too_large", "not_zip", "not_held", "failed")

#: How the notes read came to: each table counted once, by `facets.segment_note`.
NOTE_COUNTS = ("tables", "tables_read", "no_kind", "no_anchor", "no_unit", "skipped_kind")

_LIGHT = ("document", "accession", "outcome", "fetched_at", "run_id")


@dg.asset(
    partitions_def=edinet_filers,
    deps=[FILINGS],
    backfill_policy=dg.BackfillPolicy.multi_run(EDINET_FILERS_PER_RUN),
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_MONTH,
    metadata={"table": "market.security_segment"},
    description=(
        "The segments each EDINET filer's annual reports state in their segment note (`edinet`), "
        "read against the report's own tagged figures."
    ),
)
def edinet_segment(
    context: AssetExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.MaterializeResult[None]:
    """Per filer, every due report raw holds, one transaction per report.

    A REPORT IS MARKED READ ONLY WHEN SOMETHING SETTLED HAPPENED TO IT: read, absent or too large.
    A report stage 1 did not fetch, or fetched as something unrecognisable, is left due.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    stats: Counter[str] = Counter({o: 0 for o in SEGMENT_OUTCOMES})
    errors: list[str] = []
    with postgres.connect() as conn:
        found = fe.filers(conn, keys)
        known = fx.known_currencies(conn)
        vocabulary = fe.vocabulary(conn)

    for key in keys:
        security_ids = found.get(key)
        if security_ids is None:
            stats["not_askable"] += 1
            continue
        light = raw_store.stored_rows_for(FILINGS, key, _LIGHT)
        fetched_now = sorted(
            {
                str(r["accession"])
                for r in light
                if r.get("document") == "archive" and r.get("run_id") == run_id
            }
        )
        with postgres.connect() as conn:
            due = fe.filings(conn, security_ids, fetched_now)
            for filing in due:
                stats["filings"] += 1
                held = fe.held_archive(light, filing.accession)
                if held is None:
                    stats["not_held"] += 1
                    continue
                if held.outcome == "not_zip":
                    stats["not_zip"] += 1
                    continue
                facts: list[segments.SegmentFact] | None = None
                outcome = {"absent": "no_instance"}.get(held.outcome, held.outcome)
                try:
                    if held.outcome == "answered":
                        documents = raw_store.stored_rows_where(
                            FILINGS, key, {"accession": filing.accession}
                        )
                        archive = fe.archive_body(documents, held)
                        if archive is None:
                            stats["not_held"] += 1
                            continue
                        try:
                            instance = fe.instance_of(archive)
                        except fe.TooLarge:
                            instance, outcome = None, "too_large"
                        else:
                            outcome = "read" if instance is not None else "no_instance"
                        if instance is not None:
                            report = fe.read_report(segments.text_of(instance), vocabulary)
                            facts = report.note.facts
                            stats["no_note"] += report.block is None
                            stats["nonconsolidated"] += report.nonconsolidated
                            stats["no_segments"] += not facts
                            stats["tables"] += report.note.tables
                            stats["tables_read"] += report.note.read
                            stats["no_kind"] += report.note.no_kind
                            stats["no_anchor"] += report.note.no_anchor
                            stats["no_unit"] += report.note.no_unit
                            stats["skipped_kind"] += report.note.skipped_kind
                    with conn.cursor() as cur:
                        written = segment_tables.file_filing(
                            cur,
                            security_ids=filing.security_ids,
                            accession=filing.accession,
                            filed=filing.filing_date or date.min,
                            facts=facts,
                            source=fe.SOURCE,
                            as_of=held.fetched_at,
                            known_currencies=known,
                            parser_version=vocabulary.parser_version,
                        )
                    conn.commit()
                except Exception as exc:  # one report must not take the run's others with it
                    conn.rollback()
                    stats["failed"] += 1
                    errors.append(f"{key} {filing.accession}: {type(exc).__name__}: {exc}"[:300])
                    continue
                stats.update(written)
                stats[outcome] += 1

    metadata = {
        "filers_requested": len(keys),
        "parser_version": vocabulary.parser_version,
        "outcomes": sum(stats[o] for o in SEGMENT_OUTCOMES),
        **{
            name: stats[name]
            for name in (
                "not_askable",
                "filings",
                *SEGMENT_OUTCOMES,
                "no_note",
                "nonconsolidated",
                "no_segments",
                *NOTE_COUNTS,
                "rows",
                "changed",
                "kept_by_newer",
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
            description=f"{len(errors)} report(s) could not be read; first: {errors[0]}",
            metadata={**metadata, "first_error": errors[0], "last_error": errors[-1]},
        )
    return dg.MaterializeResult(metadata=metadata)
