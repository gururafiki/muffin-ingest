"""Discovery: where the universe comes from — SEC N-PORT filings, and the OpenFIGI venue sweep.

LANE SHAPES, FROM THE PARTITION TABLE:
  N-PORT   is a DOCUMENT per accession → a dynamic partition per `(cik, accession)`, sensor-seeded
           from EDGAR full-text search. Raw is the primary_doc.xml, byte for byte.
  SWEEP    is a COLLECTION per venue → a dynamic partition per `exch_code`, its cursor carried in
           the partition file (the last page's `next`). Raw is the `/v3/filter` pages, verbatim.

STAGE 2 IS THE ONLY PLACE THAT NARROWS. `facets/nport.py` and `facets/openfigi.py` run against
bytes already on disk, so a corrected parse costs a re-parse and never a re-fetch.

`discovered_security` writes THREE tables (security, security_identifier, issuer) through the
`Postgres` resource — the registries' deliberate exception to "assets return records". A filing
resolves onto all three at once, and they must land in one transaction: a security whose issuer is
FK'd but missing is as broken as an identifier pointing at a security that is not there. The
alternative — one postgres_io asset per table — commits them separately and leaves a half-filed
filing dangling.
"""

# No `from __future__ import annotations` — Dagster resolves `context` by comparing classes.

from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest import settings
from muffin_ingest.facets import nport, openfigi
from muffin_ingest.providers import openfigi as figi
from muffin_ingest.providers import sec_nport
from muffin_ingest.writers import upsert
from muffin_ingest_dagster import partitioned
from muffin_ingest_dagster.resources import Postgres

NPORT_PARTITIONS = "nport_filing"
nport_filings = dg.DynamicPartitionsDefinition(name=NPORT_PARTITIONS)

SWEEP_PARTITIONS = "exchange_sweep"
exchange_sweeps = dg.DynamicPartitionsDefinition(name=SWEEP_PARTITIONS)

#: Filings per run — a MEMORY budget wearing a time budget's clothes. One filing is ~900 KB held as
#: Python bytes + the parquet copy + the normalised holdings at once, and this lane also holds a
#: live parse. 20 stays well inside a 2.5 GB container (the 128 MB AEP-style document would be the
#: exception that proves the width rule, and a >32 MB body is refused by the provider layer).
NPORT_PER_RUN = 20

#: How many `/v3/filter` pages one venue may fetch in a run. 25/min anonymous is the ceiling; each
#: page is 100 rows, and a mid-size venue is ~22 pages (the AU capture reports 2,117 rows). A
#: venue that needs more is resumed by the next run, whose partition file carries the cursor.
SWEEP_MAX_PAGES = 40

#: The anonymous rate limit is 25 requests per minute, so consecutive pages must be ~2.4 s apart —
#: the MEASURED 429 arrived on request 21 of a paced sweep, and an unpaced loop would hit it on
#: every mid-size venue. A pool bounds concurrency (one process touches openfigi_filter at once);
#: this bounds the MINUTE, which a pool cannot express.
SWEEP_PACING = 2.5

#: A swept venue's directory stays current for a month — venues change slowly, and a gate tighter
#: than the monthly `new_exchange_sweeps` sensor would fire on a lane that is behaving as designed.
VENUE_STALE_AFTER = timedelta(days=30)

SOURCE = "sec-nport"


def _cik_accession(key: str) -> tuple[str, str]:
    cik, accession = key.split(":", 1)
    return cik, accession


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


# --- sensors ------------------------------------------------------------------------------------


def _directory_map() -> dict[str, tuple[str, str]]:
    """The fund directory's `{symbol: (cik, series_id)`, read from its raw artefact.

    The fallback for a fund the operator has added to `tracked_fund` but the pipeline has never
    ingested — its row carries no cik/series_id yet, and the directory is the only keyless map that
    can supply them. Read off disk because a sensor has no I/O manager; the daily asset guarantees
    the file is fresh.
    """
    from pathlib import Path

    import pyarrow.parquet as pq

    path = Path(settings.raw_root()) / "raw_fund_directory.parquet"
    if not path.exists():
        return {}
    table = pq.read_table(path)
    if table.column_names == ["collected_nothing"]:
        return {}
    out: dict[str, tuple[str, str]] = {}
    for row in table.to_pylist():
        body = row.get("body")
        if not body:
            continue
        try:
            directory = nport.fund_directory(bytes(body))
        except nport.NportUnreadable:
            continue
        out.update(directory)
    return out


@dg.sensor(
    target=raw_nport_filing,
    minimum_interval_seconds=6 * 3600,
    description="A tracked fund's newest NPORT-P filing the grid has not queued becomes a "
    "partition.",
)
def new_nport_filings(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """FTS per enabled series for filings we have not queued yet.

    ADDS KEYS AND REQUESTS NOTHING — a new filing becomes VISIBLE as an unmaterialised partition
    rather than launching a run (the `new_securities_need_history` shape). The accession is fetched
    so the key can carry the directory CIK, which the Archives path needs and the accession alone
    cannot supply.
    """
    import datetime as _dt

    with postgres.connect() as conn:
        funds = _tracked_funds(conn)

    directory = _directory_map()
    existing = set(context.instance.get_dynamic_partitions(NPORT_PARTITIONS))
    add: list[str] = []
    today = _dt.date.today().isoformat()
    start = (_dt.date.today() - timedelta(days=310)).isoformat()
    for fund in funds:
        series = fund["series_id"]
        cik = fund["cik"]
        if not series or not cik:
            # A NEWLY ADDED FUND: `tracked_fund` has no cik/series_id until its first filing is
            # ingested, so the daily directory supplies them.
            resolved = directory.get(fund["symbol"])
            if not resolved:
                continue
            cik, series = resolved
        doc = sec_nport.search_index(series, start=start, end=today)
        refs = nport.filing_refs(doc.body, cik)
        if not refs:
            # A HITLESS SEARCH IS AN ABSENCE, NOT A FAILURE — a fund whose last filing predates the
            # window, or one the FTS has not indexed yet, must not take the rest of the sensor down.
            continue
        newest = refs[-1]
        if newest.key not in existing:
            add.append(newest.key)
    context.log.info("%s funds, %s new filings queued", len(funds), len(add))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[nport_filings.build_add_request(add)] if add else [],
    )


@dg.sensor(
    target=raw_exchange_sweep,
    minimum_interval_seconds=7 * 24 * 3600,
    description="The enabled venues become sweep partitions.",
)
def new_exchange_sweeps(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    with postgres.connect() as conn:
        venues = _exchanges(conn)
    existing = set(context.instance.get_dynamic_partitions(SWEEP_PARTITIONS))
    add = [exch for exch in venues if exch not in existing]
    context.log.info("%s venues, %s not yet swept", len(venues), len(add))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[exchange_sweeps.build_add_request(add)] if add else [],
    )


#: The discovery directory is the one daily schedule in the lane; the filings and sweeps are
#: seeded by their sensors, and re-materialising them is an operator's call.
fund_directory = dg.ScheduleDefinition(
    name="fund_directory_schedule",
    target=[raw_fund_directory],
    cron_schedule="0 5 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
