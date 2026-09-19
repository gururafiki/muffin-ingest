"""Stage 2 of the discovery family: raw parsed and normalised into core rows."""

from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import nport, openfigi
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.discovery.partitions import (
    NPORT_PER_RUN,
    VENUE_STALE_AFTER,
    exchange_sweeps,
    nport_filings,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

SOURCE = "sec-nport"


# --- database reads (the facet's answers, the asset's queries) ----------------------------------


def _known_identifiers(conn: Any) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "select kind_code, value, security_id from market.security_identifier "
            "where kind_code in ('isin', 'cusip', 'figi', 'other')"
        )
        return {f"{kind}:{value}": security_id for kind, value, security_id in cur.fetchall()}


def _known_countries(conn: Any) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("select iso2 from market.countries")
        return {row[0] for row in cur.fetchall()}


def _tracked_funds(conn: Any) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select symbol, name, cik, series_id from market.tracked_fund "
            "where enabled and retired_at is null order by symbol"
        )
        return [
            {"symbol": r[0], "name": r[1], "cik": r[2], "series_id": r[3]} for r in cur.fetchall()
        ]


def _fund_securities(conn: Any) -> dict[str, str]:
    """tracked_fund.symbol → the fund's security_id, via its ticker identifier."""
    with conn.cursor() as cur:
        cur.execute(
            "select i.value, i.security_id from market.security_identifier i "
            "join market.tracked_fund t on t.symbol = i.value "
            "where i.kind_code = 'ticker'"
        )
        return dict(cur.fetchall())


def _fund_security_by_series(conn: Any) -> dict[str, str]:
    """series_id → the fund's security_id, so a filing's own `<seriesId>` picks its fund."""
    with conn.cursor() as cur:
        cur.execute(
            "select t.series_id, i.security_id from market.tracked_fund t "
            "join market.security_identifier i on i.value = t.symbol "
            "where i.kind_code = 'ticker' and t.series_id is not null"
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def _exchanges(conn: Any) -> dict[str, tuple[str | None, str | None]]:
    """exch_code → (suffix, country_iso2). The suffix names the price provider's address; the bare
    case (US) is `''`."""
    with conn.cursor() as cur:
        cur.execute("select exch_code, suffix, country_iso2 from market.exchange where enabled")
        return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


@dg.asset(
    partitions_def=nport_filings,
    backfill_policy=dg.BackfillPolicy.multi_run(NPORT_PER_RUN),
    pool="sql",
    group_name="universe",
    kinds={"postgres"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=45)),
    description="The securities a filing names, resolved onto the identity tables.",
)
def discovered_security(
    context: AssetExecutionContext, postgres: Postgres, raw_nport_filing: Any
) -> "dg.MaterializeResult[None]":
    """Every holding's ISIN/CUSIP → security, identifier, issuer — THE PLACEHOLDER GUARD APPLIED.

    Reads what `market.security_identifier` already knows so a re-run of a filing is a no-op, and
    writes the NEW securities, identifiers and issuers in one transaction (the registries exception:
    three tables that must land together). A holding whose only identifier is `000000000` is
    SKIPPED by `facets/nport.is_usable_identifier` rather than invented — accepting it is how four
    companies collapse into one.
    """
    raw = partitioned.loaded_rows(raw_nport_filing)
    with postgres.connect() as conn:
        known = _known_identifiers(conn)
        countries = _known_countries(conn)
        fund_sids = _fund_securities(conn)
        tracked = _tracked_funds(conn)

        security_rows: list[dict[str, Any]] = []
        identifier_rows: list[dict[str, Any]] = []
        issuer_rows: list[dict[str, Any]] = []
        for row in raw:
            holdings = nport.parse_holdings(bytes(row["body"]))
            secs, idents, issus, _ = nport.plan_holdings(
                holdings,
                known_identifiers=known,
                countries=countries,
                source=SOURCE,
            )
            security_rows += secs
            identifier_rows += idents
            issuer_rows += issus

        # THE FUNDS THEMSELVES ARE SECURITIES TOO, and a filing's `fund_id` is the fund's
        # security_id: resolve each tracked fund to its ticker-identified security, creating it when
        # this is the first filing ever ingested. `is_tradeable=True` because an ETF really is.
        fund_secs, fund_idents = _ensure_fund_securities(tracked, fund_sids)

        with conn.cursor() as cur:
            upsert(
                cur,
                "market.issuer",
                issuer_rows,
                conflict=["issuer_id"],
                update=["name", "lei", "country_iso2"],
            )
            upsert(cur, "market.security", security_rows + fund_secs, conflict=["security_id"])
            if identifier_rows + fund_idents:
                upsert(
                    cur,
                    "market.security_identifier",
                    identifier_rows + fund_idents,
                    conflict=["kind_code", "value"],
                    update=None,  # DO NOTHING: the first source of an identifier owns it
                )
        conn.commit()

    return dg.MaterializeResult(
        metadata={
            "filings": len(raw),
            "securities": len(security_rows),
            "funds": len(fund_secs),
            "identifiers": len(identifier_rows),
            "issuers": len(issuer_rows),
        }
    )


def _ensure_fund_securities(
    tracked: list[dict[str, Any]], existing: dict[str, str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    securities: list[dict[str, Any]] = []
    identifiers: list[dict[str, Any]] = []
    for fund in tracked:
        symbol = fund["symbol"]
        if symbol in existing:
            continue
        security_id = nport.new_security_id()
        securities.append(
            {
                "security_id": security_id,
                "name": fund.get("name") or symbol,
                "security_type_code": "etf",
                "is_tradeable": True,
            }
        )
        identifiers.append(
            {
                "kind_code": "ticker",
                "value": symbol,
                "security_id": security_id,
                "source_code": SOURCE,
            }
        )
    return securities, identifiers


@dg.asset(
    partitions_def=nport_filings,
    backfill_policy=dg.BackfillPolicy.multi_run(NPORT_PER_RUN),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="universe",
    kinds={"postgres"},
    metadata={"table": "market.fund_holding", "conflict": ["fund_id", "security_id", "as_of"]},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=45)),
    automation_condition=dg.AutomationCondition.eager(),
    deps=[discovered_security],
    description="The holdings snapshot of each filing, keyed (fund, security, as_of).",
)
def fund_holding(
    context: AssetExecutionContext,
    postgres: Postgres,
    raw_nport_filing: Any,
) -> list[dict[str, Any]]:
    """Each file's holdings as `market.fund_holding` rows.

    RESOLVED AFTER `discovered_security` (a declared DEP, not a loaded input — that asset writes
    through the connection, so there is no stored artifact to read): the security_ids a filing
    maps to are the database's saying after resolution, not this asset's. Combining two lots into
    one row (the fund-holding PK cannot store the same position twice) happens in the facet.
    """
    parts = partitioned.rows_per_partition(context, raw_nport_filing)
    with postgres.connect() as conn:
        known = _known_identifiers(conn)
        countries = _known_countries(conn)
        fund_by_series = _fund_security_by_series(conn)

    rows: list[dict[str, Any]] = []
    for key, doc_rows in parts.items():
        body = bytes(doc_rows[0]["body"])
        holdings = nport.parse_holdings(body)
        series = nport.series_id(body)
        fund_id = fund_by_series.get(series) if series else None
        if fund_id is None:
            context.log.warning(
                "no tracked fund for series %s (%s) — holdings skipped", series, key
            )
            continue
        _, _, _, ids = nport.plan_holdings(
            holdings, known_identifiers=known, countries=countries, source=SOURCE
        )
        rows += nport.fund_holding_rows(
            holdings,
            ids,
            fund_id=fund_id,
            report_date=nport.filing_date_of(body).isoformat(),
            source=SOURCE,
        )
    context.add_output_metadata({"filings": len(parts), "rows": len(rows)})
    return rows


@dg.asset(
    partitions_def=exchange_sweeps,
    backfill_policy=dg.BackfillPolicy.multi_run(4),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="universe",
    kinds={"postgres"},
    metadata={"table": "market.venue_listing", "conflict": ["figi"]},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=VENUE_STALE_AFTER),
    description="The raw OpenFIGI directory for each swept venue.",
)
def venue_listing(
    context: AssetExecutionContext,
    postgres: Postgres,
    raw_exchange_sweep: Any,
) -> list[dict[str, Any]]:
    """The sweep's pages as directory rows. `provider_symbol` joins the venue's suffix so the
    directory is price-addressable (US has no suffix → the bare ticker)."""
    with postgres.connect() as conn:
        exchanges = _exchanges(conn)
    parts = partitioned.rows_per_partition(context, raw_exchange_sweep)
    rows: list[dict[str, Any]] = []
    for exch_code, doc_rows in parts.items():
        suffix, _ = exchanges.get(exch_code, ("", None))
        for r in doc_rows:
            listings, _, _ = openfigi.parse_filter(bytes(r["body"]), exch_code=exch_code)
            for listing in listings:
                listing["provider_symbol"] = (
                    f"{listing['ticker']}{suffix}" if suffix else listing["ticker"]
                )
                rows.append(listing)
    context.add_output_metadata({"venues": len(parts), "rows": len(rows)})
    return rows
