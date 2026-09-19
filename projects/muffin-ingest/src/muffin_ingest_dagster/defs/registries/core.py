"""Stage 2 of the registries family: raw parsed and normalised into core rows."""

import json
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import registries

from muffin_ingest_dagster.defs.registries.partitions import STALE_AFTER
from muffin_ingest_dagster.lib.resources import Postgres


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
