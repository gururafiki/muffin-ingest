"""Stage 1 of the discovery family: what the provider sent, kept whole."""

from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import openfigi
from muffin_ingest.providers import openfigi as figi
from muffin_ingest.providers import sec_nport

from muffin_ingest_dagster.defs.discovery.partitions import (
    NPORT_PER_RUN,
    VENUE_STALE_AFTER,
    exchange_sweeps,
    nport_filings,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore

#: How many `/v3/filter` pages one venue may fetch in a run. Each page is 100 rows and a mid-size
#: venue is ~22 pages (AU measured at 2,124 listings), so 40 finishes most venues in one run — at
#: `SWEEP_PACING` that is ~8 minutes holding the `openfigi_filter` pool, which nothing else wants.
#: A venue needing more is resumed by the next run, whose partition file carries the cursor.
SWEEP_MAX_PAGES = 40


#: SECONDS BETWEEN PAGES, AND THE NUMBER IT USED TO BE IS THE POINT.
#:
#: This was 2.5 s, derived from "the anonymous rate limit is 25 requests per minute" — a ceiling
#: measured on `/v3/mapping` and no longer true of `/v3/filter`. MEASURED 2026-09-20 from this
#: machine, after 65 s of silence each time:
#:
#:     paced 2.5 s   5 pages in 14.6 s, then 429 on request 6
#:     paced 12 s    7 pages in 74.3 s, no 429 at all
#:
#: So the allowance is ~5 requests a minute, and at 2.5 s a run spent four fifths of its page
#: budget being refused: two consecutive sweeps of AU each got exactly 5 pages, and a third
#: launched ~35 s later was refused on its FIRST request. The lane recovered correctly every time
#: — that is what the cursor and the merge are for — but a 429 on every run is not a resumption
#: mechanism working, it is a pacing constant that has stopped being true.
#:
#: A pool bounds CONCURRENCY (one process touches `openfigi_filter` at a time); this bounds the
#: MINUTE, which a pool cannot express. An API key would raise the allowance and is a credential
#: decision, not one to take here.
SWEEP_PACING = 12.0

#: What a page fetched with no cursor records as the cursor it was fetched WITH. A string rather
#: than NULL so every page carries a hashable merge key — see `merge_on` on `raw_exchange_sweep`.
FIRST_PAGE_CURSOR = ""


def _cik_accession(key: str) -> tuple[str, str]:
    cik, accession = key.split(":", 1)
    return cik, accession


def _nport_partition(row: dict[str, Any]) -> str:
    return f"{row['cik']}:{row['accession']}"


# --- DISCOVERY ----------------------------------------------------------------------------------


@dg.asset(
    pool="sec",
    io_manager_key="parquet_io",
    group_name="universe",
    kinds={"sec", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=7)),
    description="SEC's company_tickers_mf.json, exactly as served. ~1.2 MB, ~28,500 fund lines.",
)
def raw_fund_directory(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """The only keyless map from a tracked fund's symbol to the `(cik, seriesId)` its filing needs.

    A DOCUMENT IS A ROW WITH A `body` COLUMN — same storage class as every other raw lane.
    """
    doc = sec_nport.fund_directory()
    context.add_output_metadata({"bytes": len(doc.body), "sha256": doc.sha256, "url": doc.url})
    return [doc.as_row(context.run.run_id)]


@dg.asset(
    partitions_def=nport_filings,
    backfill_policy=dg.BackfillPolicy.multi_run(NPORT_PER_RUN),
    pool="sec",
    io_manager_key="parquet_io",
    group_name="universe",
    kinds={"sec", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=45)),
    description="One N-PORT primary_doc.xml, exactly as SEC served it.",
)
def raw_nport_filing(context: AssetExecutionContext) -> Any:
    """One filing per partition, keyed `{cik}:{accession}`.

    THE KEY CARRIES THE FILER because the Archives path needs it and the accession alone cannot
    produce it — measured 2026-09-12, the same document 404s under the CIK the accession embeds and
    is served under the directory's. `cik` and `accession` are PROVENANCE columns added beside the
    body (never derived into it), so stage 2 can re-group files without re-fetching.
    """
    keys = list(context.partition_keys)
    rows: list[dict[str, Any]] = []
    for key in keys:
        cik, accession = _cik_accession(key)
        doc = sec_nport.primary_doc(cik, accession)
        row = doc.as_row(context.run.run_id)
        row["cik"] = cik
        row["accession"] = accession
        rows.append(row)
    context.add_output_metadata(
        {"filings": len(keys), "bytes": sum(len(row["body"]) for row in rows)}
    )
    # THROUGH THE SHARED SEAM, not a local `if len(keys) > 1`. The hand-rolled version was right
    # here by luck — every partition of this lane always produces exactly one row — and wrong in
    # the lane that copied it, where a key with no answer vanished from the mapping and Dagster
    # recorded the partition materialized with no file behind it.
    return partitioned.by_partition(context, rows, key=_nport_partition)


@dg.asset(
    partitions_def=exchange_sweeps,
    backfill_policy=dg.BackfillPolicy.multi_run(4),
    pool="openfigi_filter",
    io_manager_key="parquet_io",
    group_name="universe",
    kinds={"openfigi", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=VENUE_STALE_AFTER),
    metadata={"merge_on": ["exch_code", "cursor_from"]},
    description="Pages of one venue's /v3/filter answer, verbatim, resumed by cursor.",
)
def raw_exchange_sweep(context: AssetExecutionContext, raw_store: RawStore) -> Any:
    """One page of `/v3/filter` per fetched page, filed under the VENUE.

    THE PARTITION HOLDS ONE WALK — the current one, finished or in progress. OpenFIGI has no as-of,
    so a venue's directory can only be refreshed by walking it again from the beginning, and a
    partition that accumulated every walk it had ever done would grow without bound and could not
    answer which listings are current.

    THE CURSOR LIVES IN THE FILE: each page records the cursor it was fetched WITH (`cursor_from`,
    empty for a walk's first page) beside the cursor the provider handed back (`cursor_at`). The
    last row's `cursor_at` is therefore both the resume point and the answer to "did this walk
    finish?" — null means it did. No second control table, and
    `venue_sweep_reached_its_last_page` reads exactly that.

    SO A RE-MATERIALISATION IS ONE OF TWO THINGS, and the asset says which per partition:

    * the stored walk FINISHED (or there is none) — this run starts a NEW walk from page one, and
      its rows are the whole answer, so the partition is marked `partitioned.Complete` and the
      manager REPLACES the file. Without that the two walks' pages would coexist for ever.
    * the stored walk STOPPED mid-way — this run RESUMES it from the stored cursor, and its pages
      merge into the ones already held. Replacing here is the defect this change fixes: the old
      code returned only the pages of the current run against a manager that replaces, so a
      backfill of a half-swept venue kept the tail and silently discarded everything before it,
      while `_last_cursor`'s docstring claimed the file was "REBUILT … with every page fetched
      since the beginning".

    AN EMPTY WALK REPLACES NOTHING. A venue throttled on its very first page returns no rows, and
    the manager's own guard keeps the stored directory rather than deleting a venue to record a
    refusal.

    A throttled run STOPS and still files the pages it fetched; the venue is not complete, its
    check fails, and an operator resumes it by re-materialising the partition — which is what
    `new_exchange_sweeps`' own comment has always said ("re-materialising them is an operator's
    call"). A 429 must never make a venue read as exhausted — the DART windowed-sweep lesson.
    """
    keys = list(context.partition_keys)
    rows: list[dict[str, Any]] = []
    fresh: set[str] = set()
    for exch_code in keys:
        stored = raw_store.stored_rows_for(context.asset_key, exch_code)
        cursor, next_page = _resume_state(stored)
        if cursor is None:
            fresh.add(exch_code)
        rows += _sweep_venue(context, exch_code, cursor=cursor, first_page=next_page)
    context.add_output_metadata(
        {
            "venues": len(keys),
            "pages": len(rows),
            "new_walks": len(fresh),
            "resumed_walks": len(keys) - len(fresh),
        }
    )
    return partitioned.by_partition(
        context, rows, key=lambda r: str(r["exch_code"]), complete=fresh
    )


def _sweep_venue(
    context: AssetExecutionContext, exch_code: str, *, cursor: str | None, first_page: int
) -> list[dict[str, Any]]:
    import time

    from muffin_ingest.providers.openfigi import OpenFigiThrottled

    rows: list[dict[str, Any]] = []
    for step in range(SWEEP_MAX_PAGES):
        if step:
            time.sleep(SWEEP_PACING)
        asked_with = cursor if cursor is not None else FIRST_PAGE_CURSOR
        try:
            doc = figi.filter_exchange(exch_code, cursor=cursor)
        except OpenFigiThrottled:
            # A REFUSAL IS NOT COMPLETION and not a failure: the venue's pages so far are still
            # the provider's answer and are filed; the next materialisation resumes from the
            # file's cursor. Failing the run would leave every venue it touched needing a human.
            context.log.warning(
                "openfigi throttled %s after %s pages; the venue resumes from its cursor next run",
                exch_code,
                len(rows),
            )
            break
        # PARSING THE PAGE IS ALSO VALIDATING IT: an error-shaped 200 (`{"error": "…"}`) must
        # refuse the run rather than be stored as a page.
        _, next_cursor, _ = openfigi.parse_filter(doc.body, exch_code=exch_code)
        row = doc.as_row(context.run.run_id)
        row["exch_code"] = exch_code
        # THE CURSOR THE PAGE WAS FETCHED WITH IS THE PAGE'S OWN IDENTITY, and `cursor_at` is not:
        # the LAST page of a walk has `cursor_at` null, so keying the merge on it would make every
        # walk's final page collide with every other's. `cursor_from` is distinct per page within
        # a walk by construction, because a repeat would be an infinite loop.
        row["cursor_from"] = asked_with
        row["cursor_at"] = next_cursor
        row["page"] = first_page + step
        rows.append(row)
        if not next_cursor:
            break
        cursor = next_cursor
    # THE RESUME POINT IS THE STORED ROW'S, NOT THE LOOP VARIABLE'S. `cursor` is only advanced
    # when the provider hands back a next cursor, so a walk that finishes naturally leaves it
    # holding the cursor the LAST page was fetched with — and the first live AU sweep therefore
    # logged "resumes at 'QW9Fc1FrSkhNREl6UjB...'" for a venue that had reached its last page and
    # whose check correctly passed. A message that says the opposite of the truth is worse than no
    # message: it sends a reader to re-walk a directory that is complete. Read the same field the
    # check reads, so the two cannot disagree.
    resumes_at = rows[-1]["cursor_at"] if rows else cursor
    context.log.info(
        "venue %s: %s pages from page %s, %s",
        exch_code,
        len(rows),
        first_page,
        f"resumes at {resumes_at!r}" if resumes_at else "reached its last page",
    )
    return rows


def _resume_state(stored: Any) -> tuple[str | None, int]:
    """Where the stored walk left off: `(cursor to resume from, the next page number)`.

    `(None, 0)` means START A NEW WALK — the venue has never been swept, or its stored walk reached
    its last page and a re-materialisation is a refresh rather than a resume. Both cases are the
    same instruction, which is why they return the same thing.

    Read from the rows the manager hands back rather than from a path built here: the price lane
    already paid for that drift, and a lookup that silently misses reads as "nothing stored" and
    re-walks a venue we already hold.
    """
    rows = [row for row in stored if "cursor_at" in row]
    if not rows:
        return None, 0
    last = rows[-1]
    cursor = last.get("cursor_at") or None
    if cursor is None:
        return None, 0
    page = last.get("page")
    return cursor, (int(page) + 1 if isinstance(page, int) else len(rows))
