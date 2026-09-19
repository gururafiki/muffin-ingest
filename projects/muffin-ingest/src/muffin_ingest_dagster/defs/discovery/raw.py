"""Stage 1 of the discovery family: what the provider sent, kept whole."""

from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest import settings
from muffin_ingest.facets import openfigi
from muffin_ingest.providers import openfigi as figi
from muffin_ingest.providers import sec_nport

from muffin_ingest_dagster.defs.discovery.partitions import (
    NPORT_PER_RUN,
    VENUE_STALE_AFTER,
    exchange_sweeps,
    nport_filings,
)

#: How many `/v3/filter` pages one venue may fetch in a run. 25/min anonymous is the ceiling; each
#: page is 100 rows, and a mid-size venue is ~22 pages (the AU capture reports 2,117 rows). A
#: venue that needs more is resumed by the next run, whose partition file carries the cursor.
SWEEP_MAX_PAGES = 40


#: The anonymous rate limit is 25 requests per minute, so consecutive pages must be ~2.4 s apart —
#: the MEASURED 429 arrived on request 21 of a paced sweep, and an unpaced loop would hit it on
#: every mid-size venue. A pool bounds concurrency (one process touches openfigi_filter at once);
#: this bounds the MINUTE, which a pool cannot express.
SWEEP_PACING = 2.5


def _cik_accession(key: str) -> tuple[str, str]:
    cik, accession = key.split(":", 1)
    return cik, accession


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
    out: dict[str, list[dict[str, Any]]] = {}
    for key in keys:
        cik, accession = _cik_accession(key)
        doc = sec_nport.primary_doc(cik, accession)
        row = doc.as_row(context.run.run_id)
        row["cik"] = cik
        row["accession"] = accession
        out[key] = [row]
    context.add_output_metadata(
        {"filings": len(keys), "bytes": sum(len(rows[0]["body"]) for rows in out.values())}
    )
    return out if len(keys) > 1 else out[keys[0]]


@dg.asset(
    partitions_def=exchange_sweeps,
    backfill_policy=dg.BackfillPolicy.multi_run(4),
    pool="openfigi_filter",
    io_manager_key="parquet_io",
    group_name="universe",
    kinds={"openfigi", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=VENUE_STALE_AFTER),
    description="Pages of one venue's /v3/filter answer, verbatim, resumed by cursor.",
)
def raw_exchange_sweep(context: AssetExecutionContext) -> Any:
    """One page of `/v3/filter` per fetched page, filed under the VENUE.

    THE CURSOR LIVES IN THE FILE: each page row carries `cursor_at` (that page's `next`), and the
    last row's cursor is where a re-materialisation resumes. That makes the partition a self-
    contained record of "how far we got" — the honest answer to the design's question "is our copy
    of venue V current, and how far did we get?" — without a second control table.

    A throttled run STOPS and still files the pages it fetched; the venue is not complete, and the
    next materialisation (or a sensor re-seed) resumes it. A 429 must never make a venue read as
    exhausted — the DART windowed-sweep lesson, one venue at a time.
    """
    keys = list(context.partition_keys)
    out: dict[str, list[dict[str, Any]]] = {}
    for exch_code in keys:
        out[exch_code] = _sweep_venue(context, exch_code)
    return out if len(keys) > 1 else out[keys[0]]


def _sweep_venue(context: AssetExecutionContext, exch_code: str) -> list[dict[str, Any]]:
    import time

    from muffin_ingest.providers.openfigi import OpenFigiThrottled

    cursor = _last_cursor(exch_code)
    rows: list[dict[str, Any]] = []
    for page_no in range(SWEEP_MAX_PAGES):
        if page_no:
            time.sleep(SWEEP_PACING)
        try:
            doc = figi.filter_exchange(exch_code, cursor=cursor)
        except OpenFigiThrottled:
            # A REFUSAL IS NOT COMPLETION and not a failure: the venue's pages so far are still
            # the provider's answer and are filed; the next materialisation (or a sensor re-seed)
            # resumes from the file's cursor. Failing the run would leave every venue it touched
            # needing a human.
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
        row["cursor_at"] = next_cursor
        row["page"] = page_no
        rows.append(row)
        if not next_cursor:
            break
        cursor = next_cursor
    context.log.info("venue %s: %s pages, resumes at %r", exch_code, len(rows), cursor)
    return rows


def _last_cursor(exch_code: str) -> str | None:
    """The resume cursor from the last materialisation, read off the partition's own file.

    The file is REBUILT each sweep with every page fetched since the beginning, so it needs no
    delta against the run's width — the last row's `cursor_at` is the one true cursor.
    """
    from pathlib import Path

    import pyarrow.parquet as pq

    path = Path(settings.raw_root()) / "raw_exchange_sweep" / f"{exch_code}.parquet"
    if not path.exists():
        return None
    try:
        table = pq.read_table(path)
    except Exception:
        return None  # a broken file is a fresh start's concern, not a crash's
    if table.column_names == ["collected_nothing"] or table.num_rows == 0:
        return None
    rows = table.to_pylist()
    return rows[-1].get("cursor_at") or None
