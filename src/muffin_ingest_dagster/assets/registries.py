"""The two whole-file registries: SEC's ticker->CIK map and NSE's equity list.

THE SIMPLEST SHAPE IN THE STANDARD, AND THE ONE THAT DELETES THE MOST. Both are backlog-driven
resources in the edge function purely because of the 90-second worker — `sec-cik-map` reached
6,645 of ~27,000 rows and *restarted from zero every run*, which is why it was rewritten as a
whole-file apply there and why `in-symbols` was built the same way beside it. Neither is
incremental by nature: the file IS the answer, there is nothing to page and nothing to resume.

Under the partition decision table that is the "one whole file" row: **no partition at all**, a
schedule, and a freshness policy carrying everything the backlog used to. No `pending_*` view, no
cursor, no negative cache, no `remaining` to report — because none of those questions exists.

WHY THESE WRITE THROUGH AN RPC RATHER THAN `postgres_io`, which is a deliberate exception to
"assets return records, they do not open connections". The Postgres I/O manager exists to make
`dedupe_by`, chunking and `require_currency` unavoidable for ROW writes. Neither of these is a row
write: each hands a whole map to a function that resolves identity across two sources, applies a
PRECEDENCE LADDER, and REFUSES AN AMBIGUOUS MATCH rather than breaking the tie. That logic is
exactly what rule 8 says must live in SQL — a wrong CIK is far worse than no CIK, because it makes
every downstream number look populated and fiction — so the asset calls it and reports what it
returned.
"""

# No `from __future__ import annotations` — Dagster resolves `context` by comparing the class.

import json
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest.facets import registries
from muffin_ingest.providers import documents
from muffin_ingest_dagster.resources import Postgres

#: Both files change slowly — SEC's is byte-identical most days, NSE's moves when a company lists.
#: Weekly is far more often than either needs and still nothing next to the old rotation, which
#: asked for them every ten minutes and got a `skipped` for its trouble.
WEEKLY = "0 4 * * 1"

#: Generous, because the failure this catches is "the schedule stopped", not "the file is stale".
#: Tighter than the cadence would make a single missed Monday a page.
STALE_AFTER = timedelta(days=16)


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
    pool="sql",
    group_name="registries",
    kinds={"postgres"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=STALE_AFTER),
    automation_condition=dg.AutomationCondition.eager(),
    description="Resolve SEC filers onto securities. Never calls a provider, so a fix is free.",
)
def security_cik(
    context: AssetExecutionContext, postgres: Postgres, raw_sec_cik_map: list[dict[str, Any]]
) -> "dg.MaterializeResult[None]":
    """`market.apply_cik_map` does the resolving, and that is where it belongs.

    It carries a precedence ladder (the `ticker` identifier first, a US venue's listing symbol
    second), REFUSES a security whose candidates disagree rather than taking `min()`, and is
    idempotent so a weekly re-run of an unchanged file rewrites nothing. Berkshire had no SEC data
    at all until that fallback existed — OpenFIGI spells the B share `BRK/B` and SEC spells it
    `BRK-B`, so the ticker join missed and every SEC-gated resource skipped the heaviest holding
    in the universe in silence, while the resource reported `updated: 0` and looked healthy.
    """
    body = bytes(raw_sec_cik_map[0]["body"])
    pairs = registries.cik_map(body)
    mapping = {row["ticker"]: row["cik"] for row in pairs}

    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute("select market.apply_cik_map(%s::jsonb)", (json.dumps(mapping),))
        row = cur.fetchone()
        conn.commit()
    updated = int(row[0]) if row else 0

    return dg.MaterializeResult(
        metadata={
            "filers": len(pairs),
            # ZERO IS THE EXPECTED STEADY STATE and is never filtered out of a chart: the function
            # only writes where the CIK actually changed, so a week with no new filer is `0`. It
            # is `filers` going to zero that would mean the file stopped parsing.
            "securities_updated": updated,
            "distinct_tickers": len(mapping),
        }
    )


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


@dg.asset(
    pool="sql",
    group_name="registries",
    kinds={"postgres"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=STALE_AFTER),
    automation_condition=dg.AutomationCondition.eager(),
    description="Resolve NSE symbols onto Indian securities, joined on ISIN.",
)
def security_nse_filer(
    context: AssetExecutionContext, postgres: Postgres, raw_nse_equity_list: list[dict[str, Any]]
) -> "dg.MaterializeResult[None]":
    """JOINED ON ISIN, NOT ON THE LISTING SYMBOL, AND THAT WAS MEASURED.

    India first shipped joining on `market.listing.symbol` because RELIANCE, HDFCBANK and INFY all
    work — and only **239 of 645** Indian equities carry a symbol NSE recognises; the rest hold a
    vendor abbreviation. `in-filings` reported `walked: 6, mapped: 0, failed: 0`: a resource
    succeeding at asking the wrong question, with no count in the system able to show it.
    Coverage 37% -> 97% on the ISIN.
    """
    body = bytes(raw_nse_equity_list[0]["body"])
    equities = registries.nse_equities(body)
    mapping = {row["isin"]: row["symbol"] for row in equities}

    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute("select market.apply_nse_symbol_map(%s::jsonb)", (json.dumps(mapping),))
        row = cur.fetchone()
        conn.commit()
    updated = int(row[0]) if row else 0

    return dg.MaterializeResult(metadata={"equities": len(equities), "securities_updated": updated})


#: ONE JOB FOR BOTH, because they share a cadence and nothing else. Kept out of the price lane's
#: schedules so a registry refresh can never delay a trading-day collection.
weekly_registries = dg.ScheduleDefinition(
    name="weekly_registries",
    target=dg.AssetSelection.assets(
        raw_sec_cik_map, security_cik, raw_nse_equity_list, security_nse_filer
    ),
    cron_schedule=WEEKLY,
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
