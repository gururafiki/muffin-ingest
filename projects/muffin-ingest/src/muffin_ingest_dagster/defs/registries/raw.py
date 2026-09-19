"""Stage 1 of the registries family: what the provider sent, kept whole."""

from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.providers import documents

from muffin_ingest_dagster.defs.registries.partitions import STALE_AFTER


@dg.asset(
    io_manager_key="parquet_io",
    pool="sec",
    group_name="registries",
    kinds={"sec", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=STALE_AFTER),
    description="SEC's company_tickers.json, exactly as served. ~776 KB, ~10,400 filers.",
)
def raw_sec_cik_map(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """One request, one document, no paging.

    A DOCUMENT IS A ROW WITH A `body` COLUMN — measured, pyarrow infers `binary` for `bytes` and
    round-trips it identically, and this 776 KB of JSON compresses to a fraction on the way. So
    raw keeps one storage class and one I/O manager, and the provenance travels as columns rather
    than a sidecar file that can go missing or go stale relative to the bytes it describes.
    """
    doc = documents.sec_company_tickers()
    context.add_output_metadata({"bytes": len(doc.body), "sha256": doc.sha256, "url": doc.url})
    return [doc.as_row(context.run.run_id)]


@dg.asset(
    io_manager_key="parquet_io",
    pool="nse",
    group_name="registries",
    kinds={"nse", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=STALE_AFTER),
    description="NSE's published EQUITY_L.csv, exactly as served. ~2,570 equities.",
)
def raw_nse_equity_list(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """NSE refuses a bare client: both a browser User-Agent and a `Referer` are required."""
    doc = documents.nse_equity_list()
    context.add_output_metadata({"bytes": len(doc.body), "sha256": doc.sha256, "url": doc.url})
    return [doc.as_row(context.run.run_id)]
