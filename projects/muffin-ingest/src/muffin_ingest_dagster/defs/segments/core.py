"""Stage 2 of the segments family: stored documents read into `security_segment`.

No provider call: a parser change re-reads files already on disk.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import cn_child, fx, sec_segments, segments
from muffin_ingest.facets import cninfo as cn
from muffin_ingest.facets import dart as fd
from muffin_ingest.facets import nse as fn
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.sec.partitions import FILERS_PER_RUN, sec_filers
from muffin_ingest_dagster.defs.segments.partitions import (
    CNINFO_FILERS_PER_RUN,
    DART_FILERS_PER_RUN,
    NSE_FILERS_PER_RUN,
    cninfo_filers,
    dart_filers,
    nse_filers,
)
from muffin_ingest_dagster.defs.segments.raw import (
    CNINFO_FILINGS,
    CNINFO_STOCK_LIST,
    DART_CORP_CODES,
    DART_FILINGS,
    FRESH_FLOOR,
    FRESH_TWO_WEEKS,
    FRESH_WEEK,
    NSE_FILINGS,
    SEC_INSTANCE,
)
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
        vocabulary = segments.vocabulary(conn, sec_segments.REGULATOR)
        known = fx.known_currencies(conn)
        wanted = sec_segments.filings(conn, [int(k) for k in keys], fetched_now)

    stats: Counter[str] = Counter({o: 0 for o in SEGMENT_OUTCOMES})
    errors: list[str] = []
    for key in keys:
        filings = wanted.get(int(key), [])
        if not filings:
            continue
        stats["filers_with_filings"] += 1
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
                        # ONE FILING'S DOCUMENTS AT A TIME: a filer's file holds every instance
                        # fetched for it, and Principal Financial's 21 hold 413 MB, which read at
                        # once OOM-killed this step on 2026-11-14 (`row_group_rows` on stage 1).
                        documents = raw_store.stored_rows_where(
                            SEC_INSTANCE, key, {"accession": filing.accession}
                        )
                        body = sec_segments.instance_body(documents, held)
                        if body is None:
                            stats["not_held"] += 1
                            continue
                        facts = segments.segment_facts(segments.text_of(body), vocabulary)
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


# ── CNINFO (China) ─────────────────────────────────────────────────────────────────────────────


@dg.asset(
    ins={"raw_cninfo_stock_list": dg.AssetIn(key=CNINFO_STOCK_LIST)},
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_TWO_WEEKS,
    metadata={"table": "market.security_filer"},
    description="Each A-share's CNINFO org id, from CNINFO's own stock lists.",
)
def security_cninfo_filer(
    context: AssetExecutionContext, postgres: Postgres, raw_cninfo_stock_list: list[dict[str, Any]]
) -> dg.MaterializeResult[None]:
    """Every Chinese equity with a six-digit listing whose code the lists name gets a filer row.

    A row is never deleted here: a company missing from this week's list (a delisting, a list CNINFO
    served short) keeps its history, and the lane simply finds nothing new for it.
    """
    bodies = [bytes(r["body"]) for r in raw_cninfo_stock_list if r.get("body") is not None]
    org = cn.org_ids(bodies)
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(cn.SECURITIES)
            securities = [(str(sid), str(code)) for sid, code in cur.fetchall()]
        rows = [
            {"security_id": sid, "source_code": cn.SOURCE, "filer_id": org[code]}
            for sid, code in securities
            if code in org
        ]
        with conn.cursor() as cur:
            written = upsert(
                cur,
                "market.security_filer",
                rows,
                conflict=["security_id", "source_code"],
                update=["filer_id"],
            )
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            "listed": len(org),
            "securities": len(securities),
            "mapped": len(rows),
            "unmapped": len(securities) - len(rows),
            "changed": -1 if written.changed is None else written.changed,
        }
    )


#: What a full report considered in a run came to. They sum to `filings`.
#:   `read`        parsed; its rows written (or none, a report with no table) and marked;
#:   `no_report`   CNINFO holds no PDF there (404, or a page that is not a PDF): marked;
#:   `too_large`   over the size gate: marked, unread;
#:   `unreadable`  the parsing child raised, died or ran out of memory or time: marked;
#:   `not_held`    stage 1 has not fetched it (refused, unasked, or pruned): left due;
#:   `failed`      the write raised: rolled back and left due.
CNINFO_OUTCOMES = ("read", "no_report", "too_large", "unreadable", "not_held", "failed")

#: The columns that say what a filer's file holds, without a report's body.
_CN_LIGHT = ("document", "accession", "outcome", "fetched_at", "run_id", "pruned_at")


@dg.asset(
    partitions_def=cninfo_filers,
    deps=[CNINFO_FILINGS],
    backfill_policy=dg.BackfillPolicy.multi_run(CNINFO_FILERS_PER_RUN),
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_FLOOR,
    metadata={"table": "market.security_segment"},
    description=(
        "Each CNINFO filer's annual reports in `security_filing`, and the segments each full "
        "report's mandated tables state (`cninfo`)."
    ),
)
def cninfo_segment(
    context: AssetExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.MaterializeResult[None]:
    """Per filer: the newest search's announcements, and those of every later page the history walk
    stored, into `security_filing`, then every due full report raw holds, each read in its own
    child process (`cn_child`), one transaction per report.

    A REPORT IS MARKED READ ONLY WHEN SOMETHING SETTLED HAPPENED TO IT: parsed, absent, too large or
    unreadable. A report stage 1 did not fetch is left due, and a refusal never reaches here.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    stats: Counter[str] = Counter({o: 0 for o in CNINFO_OUTCOMES})
    errors: list[str] = []
    with postgres.connect() as conn:
        found = cn.filers(conn, keys)
        known = fx.known_currencies(conn)
        vocabulary = segments.vocabulary(conn, cn.REGULATOR)

    for key in keys:
        filer = found.get(key)
        if filer is None:
            stats["not_askable"] += 1
            continue
        light = raw_store.stored_rows_for(CNINFO_FILINGS, key, _CN_LIGHT)
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
            searches = raw_store.stored_rows_where(
                CNINFO_FILINGS, key, {"document": "announcements"}
            )
            newest = cn.newest_list(searches)
            if newest is not None:
                # Page 1 as last asked, and every later page the history walk stored, each report
                # once: pages shift as reports are added, and one key twice in an upsert fails it.
                once: dict[str, cn.Announcement] = {}
                for page in [newest, *dict(sorted(cn.history_pages(searches).items())).values()]:
                    for a in cn.parse_announcements(bytes(page["body"])):
                        once.setdefault(a.url, a)
                announced = list(once.values())
                with conn.cursor() as cur:
                    listed = upsert(
                        cur,
                        "market.security_filing",
                        cn.filing_rows(filer.security_ids, announced),
                        conflict=["security_id", "accession_number"],
                        update=[
                            "report_type",
                            "report_date",
                            "filing_date",
                            "report_url",
                            "source_code",
                            "is_xbrl",
                        ],
                    )
                conn.commit()
                stats["announcements"] += len(announced)
                stats["filing_rows_changed"] += listed.changed or 0
            due = cn.filings(conn, filer.security_ids, fetched_now)

        with postgres.connect() as conn:
            revenue = cn.annual_revenue(conn, filer.security_ids) if due else {}
            for filing in due:
                stats["filings"] += 1
                held = cn.held_report(light, filing.accession)
                if held is None or held.pruned:
                    stats["not_held"] += 1
                    continue
                facts: list[segments.SegmentFact] | None = None
                outcome = {"absent": "no_report", "not_pdf": "no_report"}.get(
                    held.outcome, held.outcome
                )
                if held.outcome == "answered":
                    documents = raw_store.stored_rows_where(
                        CNINFO_FILINGS, key, {"accession": filing.accession}
                    )
                    body = cn.report_body(documents, held)
                    if body is None:
                        stats["not_held"] += 1
                        continue
                    result = cn_child.read(body)
                    if isinstance(result, cn_child.Unreadable):
                        outcome = "unreadable"
                        context.log.warning(
                            "report %s of %s unreadable: %s", filing.accession, key, result.reason
                        )
                    else:
                        facts, rescaled = cn.rescale_misstated_units(result.facts, revenue)
                        outcome = "read"
                        stats["no_table"] += not facts
                        stats["units_rescaled"] += len(rescaled)
                        for (axis, ending), factor in sorted(rescaled.items()):
                            context.log.info(
                                "report %s of %s: %s %s read as x%s, the table's unit line "
                                "disagreeing with the company's revenue",
                                filing.accession,
                                key,
                                axis,
                                ending,
                                factor,
                            )
                try:
                    with conn.cursor() as cur:
                        written = segment_tables.file_filing(
                            cur,
                            security_ids=filing.security_ids,
                            accession=filing.accession,
                            filed=filing.filing_date or date.min,
                            facts=facts,
                            source=cn.SOURCE,
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
        "outcomes": sum(stats[o] for o in CNINFO_OUTCOMES),
        **{
            name: stats[name]
            for name in (
                "filers_held",
                "not_askable",
                "announcements",
                "filing_rows_changed",
                "filings",
                *CNINFO_OUTCOMES,
                "no_table",
                "units_rescaled",
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
            description=f"{len(errors)} report(s) could not be written; first: {errors[0]}",
            metadata={**metadata, "first_error": errors[0], "last_error": errors[-1]},
        )
    return dg.MaterializeResult(metadata=metadata)


# ── DART (Korea) ───────────────────────────────────────────────────────────────────────────────


@dg.asset(
    ins={"raw_dart_corp_codes": dg.AssetIn(key=DART_CORP_CODES)},
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_TWO_WEEKS,
    metadata={"table": "market.security_filer"},
    description="Each Korean equity's DART corp code, from DART's own registry.",
)
def security_dart_filer(
    context: AssetExecutionContext, postgres: Postgres, raw_dart_corp_codes: list[dict[str, Any]]
) -> dg.MaterializeResult[None]:
    """Every Korean equity whose six-digit code the registry lists gets a filer row. A row is never
    deleted here: a company missing from this week's registry keeps its history."""
    archive = next(bytes(r["body"]) for r in raw_dart_corp_codes if r.get("body") is not None)
    corp = fd.corp_codes(archive)
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(fd.SECURITIES)
            securities = [(str(sid), str(code)) for sid, code in cur.fetchall()]
        rows = [
            {"security_id": sid, "source_code": fd.SOURCE, "filer_id": corp[code]}
            for sid, code in securities
            if code in corp
        ]
        with conn.cursor() as cur:
            written = upsert(
                cur,
                "market.security_filer",
                rows,
                conflict=["security_id", "source_code"],
                update=["filer_id"],
            )
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            "listed": len(corp),
            "securities": len(securities),
            "mapped": len(rows),
            "unmapped": len(securities) - len(rows),
            "changed": -1 if written.changed is None else written.changed,
        }
    )


#: What an annual report considered in a run came to. They sum to `filings`.
#:   `read`         its instance was parsed, its rows written (or none) and marked;
#:   `no_instance`  DART holds no archive (014) or the archive holds no instance: marked;
#:   `too_large`    over the size gate: marked, unread;
#:   `not_zip`      DART answered something else: left due, asked again;
#:   `not_held`     stage 1 has not fetched it (refused or not asked yet): left due;
#:   `failed`       the parse or the write raised: rolled back and left due.
DART_OUTCOMES = ("read", "no_instance", "too_large", "not_zip", "not_held", "failed")

#: The columns that say what a filer's file holds, without a document's body.
_DART_LIGHT = ("document", "accession", "outcome", "fetched_at", "run_id")


@dg.asset(
    partitions_def=dart_filers,
    deps=[DART_FILINGS],
    backfill_policy=dg.BackfillPolicy.multi_run(DART_FILERS_PER_RUN),
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_FLOOR,
    metadata={"table": "market.security_segment"},
    description=(
        "Each DART filer's annual reports in `security_filing`, and the segments each report's "
        "XBRL instance states (`dart`)."
    ),
)
def dart_segment(
    context: AssetExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.MaterializeResult[None]:
    """Per filer: the newest list's annual reports into `security_filing`, then every due report
    raw holds, read through the same parser as SEC's, one transaction per report.

    A REPORT IS MARKED READ ONLY WHEN SOMETHING SETTLED HAPPENED TO IT: parsed, absent or too
    large. A report stage 1 did not fetch, or fetched as something unrecognisable, is left due.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    stats: Counter[str] = Counter({o: 0 for o in DART_OUTCOMES})
    errors: list[str] = []
    with postgres.connect() as conn:
        found = fd.filers(conn, keys)
        known = fx.known_currencies(conn)
        vocabulary = segments.vocabulary(conn, fd.REGULATOR)

    for key in keys:
        security_ids = found.get(key)
        if security_ids is None:
            stats["not_askable"] += 1
            continue
        light = raw_store.stored_rows_for(DART_FILINGS, key, _DART_LIGHT)
        if not light:
            continue
        stats["filers_held"] += 1
        fetched_now = sorted(
            {
                str(r["accession"])
                for r in light
                if r.get("document") == "instance" and r.get("run_id") == run_id
            }
        )
        with postgres.connect() as conn:
            pages = fd.newest_lists(
                raw_store.stored_rows_where(DART_FILINGS, key, {"document": "filings"})
            )
            filed = [f for page in pages for f in fd.parse_filings(bytes(page["body"])).filed]
            if pages:
                with conn.cursor() as cur:
                    listed = upsert(
                        cur,
                        "market.security_filing",
                        fd.filing_rows(security_ids, filed),
                        conflict=["security_id", "accession_number"],
                        update=["report_type", "filing_date", "source_code", "is_xbrl"],
                    )
                conn.commit()
                stats["annual_reports_listed"] += sum(f.is_annual for f in filed)
                stats["filing_rows_changed"] += listed.changed or 0
            due = fd.filings(conn, security_ids, fetched_now)

        with postgres.connect() as conn:
            for filing in due:
                stats["filings"] += 1
                held = fd.held_archive(light, filing.accession)
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
                            DART_FILINGS, key, {"accession": filing.accession}
                        )
                        archive = fd.archive_body(documents, held)
                        if archive is None:
                            stats["not_held"] += 1
                            continue
                        try:
                            instance = fd.instance_of(archive)
                        except fd.TooLarge:
                            instance, outcome = None, "too_large"
                        else:
                            outcome = "read" if instance is not None else "no_instance"
                        if instance is not None:
                            facts = segments.segment_facts(segments.text_of(instance), vocabulary)
                            stats["no_segments"] += not facts
                    with conn.cursor() as cur:
                        written = segment_tables.file_filing(
                            cur,
                            security_ids=filing.security_ids,
                            accession=filing.accession,
                            filed=filing.filing_date or date.min,
                            facts=facts,
                            source=fd.SOURCE,
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
        "outcomes": sum(stats[o] for o in DART_OUTCOMES),
        **{
            name: stats[name]
            for name in (
                "filers_held",
                "not_askable",
                "annual_reports_listed",
                "filing_rows_changed",
                "filings",
                *DART_OUTCOMES,
                "no_segments",
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


# ── NSE (India) ────────────────────────────────────────────────────────────────────────────────

#: How a due result can end in stage 2. They partition `filings`, so they sum to it. `standalone`
#: is read and wanted nothing: every company files twice, and the consolidated filing is the one.
NSE_OUTCOMES = ("read", "standalone", "no_instance", "not_answer", "not_held", "failed")

_NSE_LIGHT = ("document", "accession", "outcome", "fetched_at", "run_id")


@dg.asset(
    partitions_def=nse_filers,
    deps=[NSE_FILINGS],
    backfill_policy=dg.BackfillPolicy.multi_run(NSE_FILERS_PER_RUN),
    pool="sql",
    group_name="segments",
    kinds={"postgres"},
    freshness_policy=FRESH_FLOOR,
    metadata={"table": "market.security_segment"},
    description=(
        "Each NSE filer's annual results in `security_filing`, and the segments each consolidated "
        "result's XBRL instance states (`nse`), normalised first."
    ),
)
def nse_segment(
    context: AssetExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.MaterializeResult[None]:
    """Per filer: the newest list's `.xml` results into `security_filing`, then every due result
    raw holds, normalised (`nse.normalise`) and read through the same parser as SEC's, one
    transaction per result.

    A RESULT IS MARKED READ ONLY WHEN SOMETHING SETTLED HAPPENED TO IT: parsed, standalone, or
    absent (404). A result stage 1 did not fetch, or fetched as something that is not an instance,
    is left due.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    stats: Counter[str] = Counter({o: 0 for o in NSE_OUTCOMES})
    errors: list[str] = []
    with postgres.connect() as conn:
        found = fn.filers(conn, keys)
        known = fx.known_currencies(conn)
        vocabulary = segments.vocabulary(conn, fn.REGULATOR)

    for key in keys:
        security_ids = found.get(key)
        if security_ids is None:
            stats["not_askable"] += 1
            continue
        light = raw_store.stored_rows_for(NSE_FILINGS, key, _NSE_LIGHT)
        if not light:
            continue
        stats["filers_held"] += 1
        fetched_now = sorted(
            {
                str(r["accession"])
                for r in light
                if r.get("document") == "instance" and r.get("run_id") == run_id
            }
        )
        with postgres.connect() as conn:
            newest = fn.newest_list(
                raw_store.stored_rows_where(NSE_FILINGS, key, {"document": "filings"})
            )
            filed = fn.parse_filings(bytes(newest["body"])) or [] if newest is not None else []
            if filed:
                with conn.cursor() as cur:
                    listed = upsert(
                        cur,
                        "market.security_filing",
                        fn.filing_rows(security_ids, filed),
                        conflict=["security_id", "accession_number"],
                        update=[
                            "report_type",
                            "report_date",
                            "filing_date",
                            "report_url",
                            "source_code",
                            "is_xbrl",
                        ],
                    )
                conn.commit()
                stats["results_listed"] += len(filed)
                stats["filing_rows_changed"] += listed.changed or 0
            due = fn.filings(conn, security_ids, fetched_now)

        with postgres.connect() as conn:
            for filing in due:
                stats["filings"] += 1
                held = fn.held_instance(light, filing.accession)
                if held is None:
                    stats["not_held"] += 1
                    continue
                if held.outcome == "not_answer":
                    stats["not_answer"] += 1
                    continue
                facts: list[segments.SegmentFact] | None = None
                outcome = "no_instance"
                try:
                    if held.outcome == "answered":
                        documents = raw_store.stored_rows_where(
                            NSE_FILINGS, key, {"accession": filing.accession}
                        )
                        body = fn.instance_body(documents, filing.accession, held)
                        if body is None:
                            stats["not_held"] += 1
                            continue
                        try:
                            normalised = fn.normalise(segments.text_of(body))
                        except fn.NotConsolidated:
                            outcome = "standalone"
                        else:
                            outcome = "read"
                            stats["members_named"] += normalised.named
                            stats["contexts_annualised"] += normalised.annualised
                            facts = segments.segment_facts(normalised.xml, vocabulary)
                            stats["no_segments"] += not facts
                    with conn.cursor() as cur:
                        written = segment_tables.file_filing(
                            cur,
                            security_ids=filing.security_ids,
                            accession=filing.accession,
                            filed=filing.filing_date or date.min,
                            facts=facts,
                            source=fn.SOURCE,
                            as_of=held.fetched_at,
                            known_currencies=known,
                            parser_version=vocabulary.parser_version,
                        )
                    conn.commit()
                except Exception as exc:  # one result must not take the run's others with it
                    conn.rollback()
                    stats["failed"] += 1
                    errors.append(f"{key} {filing.accession}: {type(exc).__name__}: {exc}"[:300])
                    continue
                stats.update(written)
                stats[outcome] += 1

    metadata = {
        "filers_requested": len(keys),
        "parser_version": vocabulary.parser_version,
        "outcomes": sum(stats[o] for o in NSE_OUTCOMES),
        **{
            name: stats[name]
            for name in (
                "filers_held",
                "not_askable",
                "results_listed",
                "filing_rows_changed",
                "filings",
                *NSE_OUTCOMES,
                "no_segments",
                "members_named",
                "contexts_annualised",
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
            description=f"{len(errors)} result(s) could not be read; first: {errors[0]}",
            metadata={**metadata, "first_error": errors[0], "last_error": errors[-1]},
        )
    return dg.MaterializeResult(metadata=metadata)
