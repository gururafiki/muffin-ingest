"""Stage 2 of Europe's segment lane: the stored lists and reports into the database.

No provider call: a reader change re-reads files already on disk.
"""

from collections import Counter
from datetime import date, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import esef, fx, segments
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.esef.partitions import ESEF_FILERS_PER_RUN, esef_filers
from muffin_ingest_dagster.defs.esef.raw import ESEF_FILINGS
from muffin_ingest_dagster.defs.segments.raw import FRESH_FLOOR
from muffin_ingest_dagster.lib import segment_tables
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: The registry runs weekly; a fortnight with no run is a stopped schedule.
FRESH_TWO_WEEKS = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=14))

#: What the registry releases when a company becomes the SEC's (Phase 6c decision 2): its filer row
#: and what was written under it, so one regulator speaks for it.
RELEASE = f"""
with released as (
  delete from market.security_filer
   where source_code = '{esef.REGULATOR}' and security_id = any(%s::uuid[])
  returning security_id
),
segments as (
  delete from market.security_segment g using released r
   where g.security_id = r.security_id and g.source_code = '{esef.SOURCE}'
  returning 1
),
filings as (
  delete from market.security_filing f using released r
   where f.security_id = r.security_id and f.source_code = '{esef.SOURCE}'
  returning 1
)
select (select count(*) from released), (select count(*) from segments),
       (select count(*) from filings)
"""


@dg.asset(
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_TWO_WEEKS,
    metadata={"table": "market.security_filer"},
    description="Each European equity's LEI, from N-PORT's issuer, where the index covers it.",
)
def security_esef_filer(postgres: Postgres) -> dg.MaterializeResult[None]:
    """Every equity in a country the index covers whose issuer holds an LEI gets a filer row,
    unless the SEC already gives it a split.

    NO PROVIDER CALL: the LEI is N-PORT's, and the index is asked about a filer only in stage 1.
    A row is never deleted for a company whose LEI is no longer held: it keeps its history. One
    that became the SEC's is released with what was written under it.
    """
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(esef.SECURITIES)
            securities = [(str(sid), str(lei), bool(sec)) for sid, lei, sec in cur.fetchall()]
        rows = [
            {"security_id": sid, "source_code": esef.REGULATOR, "filer_id": lei, "via": "registry"}
            for sid, lei, sec in securities
            if not sec
        ]
        the_sec_s = [sid for sid, _, sec in securities if sec]
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
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            "securities": len(securities),
            "mapped": len(rows),
            "filers": len({r["filer_id"] for r in rows}),
            "the_sec_s": len(the_sec_s),
            "changed": -1 if written.changed is None else written.changed,
            "released": int(released[0]),
            "released_segments": int(released[1]),
            "released_filings": int(released[2]),
        }
    )


#: What an annual report considered in a run came to. They sum to `filings`.
#:   `read`        its note was read, its rows written (or none) and marked;
#:   `absent`      the index no longer serves it (404): marked;
#:   `too_large`   over the size gate: marked, unread;
#:   `not_json`    the index answered something else: left due, asked again;
#:   `not_held`    stage 1 has not fetched it (refused or not asked yet): left due;
#:   `failed`      the parse or the write raised: rolled back and left due.
ESEF_OUTCOMES = ("read", "absent", "too_large", "not_json", "not_held", "failed")
#: How the notes read came to, summed over the run's reports.
NOTE_COUNTS = ("tables", "tables_read", "no_kind", "no_anchor", "no_unit", "skipped_kind")

_LIGHT = ("document", "accession", "outcome", "fetched_at", "run_id")


@dg.asset(
    partitions_def=esef_filers,
    deps=[ESEF_FILINGS],
    backfill_policy=dg.BackfillPolicy.multi_run(ESEF_FILERS_PER_RUN),
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_FLOOR,
    metadata={"table": "market.security_segment"},
    description=(
        "Each ESEF filer's annual reports in `security_filing`, and the segments each report's "
        "segment note states (`xbrl-org`)."
    ),
)
def esef_segment(
    context: AssetExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.MaterializeResult[None]:
    """Per filer: the newest list's annual reports into `security_filing`, then every due report
    raw holds, its note read against its own figures, one transaction per report.

    A REPORT IS MARKED READ ONLY WHEN SOMETHING SETTLED HAPPENED TO IT: read, absent or too large.
    A report stage 1 did not fetch, or fetched as something unrecognisable, is left due.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    stats: Counter[str] = Counter({o: 0 for o in ESEF_OUTCOMES})
    errors: list[str] = []
    with postgres.connect() as conn:
        found = esef.filers(conn, keys)
        known = fx.known_currencies(conn)
        vocabulary = esef.vocabulary(conn)
        year_ends = {key: esef.year_end(conn, ids) for key, ids in found.items()}

    for key in keys:
        security_ids = found.get(key)
        if security_ids is None:
            stats["not_askable"] += 1
            continue
        light = raw_store.stored_rows_for(ESEF_FILINGS, key, _LIGHT)
        if not light:
            continue
        stats["filers_held"] += 1
        fetched_now = sorted(
            {
                str(r["accession"])
                for r in light
                if r.get("document") == "report" and r.get("run_id") == run_id
            }
        )
        with postgres.connect() as conn:
            held_list = esef.newest_list(
                raw_store.stored_rows_where(ESEF_FILINGS, key, {"document": "filings"})
            )
            if held_list is not None:
                listed = esef.parse_filings(bytes(held_list["body"])) or []
                annual = esef.annual_reports(listed, year_ends.get(key))
                with conn.cursor() as cur:
                    written_list = upsert(
                        cur,
                        "market.security_filing",
                        esef.filing_rows(security_ids, annual),
                        conflict=["security_id", "accession_number"],
                        update=["report_type", "filing_date", "source_code", "is_xbrl"],
                    )
                conn.commit()
                stats["listed"] += len(listed)
                stats["annual_reports_listed"] += len(annual)
                stats["filing_rows_changed"] += written_list.changed or 0
            due = esef.filings(conn, security_ids, fetched_now)

        with postgres.connect() as conn:
            for filing in due:
                stats["filings"] += 1
                held = esef.held_report(light, filing.accession)
                if held is None:
                    stats["not_held"] += 1
                    continue
                if held.outcome == "not_json":
                    stats["not_json"] += 1
                    continue
                facts: list[segments.SegmentFact] | None = None
                outcome: str = held.outcome
                try:
                    if held.outcome == "answered":
                        documents = raw_store.stored_rows_where(
                            ESEF_FILINGS, key, {"accession": filing.accession}
                        )
                        body = esef.report_body(documents, held)
                        if body is None:
                            stats["not_held"] += 1
                            continue
                        report = esef.read_report(body, vocabulary)
                        facts = report.note.facts
                        outcome = "read"
                        stats["no_note"] += report.block is None
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
                            source=esef.SOURCE,
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

    metadata: dict[str, Any] = {
        "filers_requested": len(keys),
        "parser_version": vocabulary.parser_version,
        "outcomes": sum(stats[o] for o in ESEF_OUTCOMES),
        **{
            name: stats[name]
            for name in (
                "filers_held",
                "not_askable",
                "listed",
                "annual_reports_listed",
                "filing_rows_changed",
                "filings",
                *ESEF_OUTCOMES,
                "no_note",
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
