"""Stage 1 of the symbology family: what the provider sent, kept whole."""

from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.providers import openfigi as figi
from muffin_ingest.providers import yahoo_search

from muffin_ingest_dagster.defs.prices.partitions import security_partitions
from muffin_ingest_dagster.defs.symbology.partitions import SYMBOLOGY_PER_RUN
from muffin_ingest_dagster.lib.resources import Postgres


def _security_attributes(conn: Any) -> dict[str, dict[str, Any]]:
    """security_id → {isin, country_iso2}, for the subjects asked this run."""
    with conn.cursor() as cur:
        cur.execute(
            "select i.security_id, i.value, s.country_iso2 "
            "from market.security_identifier i "
            "join market.security s on s.security_id = i.security_id "
            "where i.kind_code = 'isin' and s.is_tradeable = false"
        )
        return {
            sid: {"isin": isin, "country_iso2": country} for sid, isin, country in cur.fetchall()
        }


def _rung_output(
    context: AssetExecutionContext, key_to_rows: dict[str, list[dict[str, Any]]]
) -> Any:
    """The single-partition/list vs multi-partition/mapping seam, shared by the three rungs."""
    keys: list[str] = list(key_to_rows)
    return key_to_rows if len(keys) > 1 else key_to_rows[keys[0]]


@dg.asset(
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="openfigi_mapping",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"openfigi", "parquet"},
    description="OpenFIGI's US lookup for each security, response whole.",
)
def raw_figi_ticker(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """Per security, the `/v3/mapping` body restricted to `exchCode: US`. ONE RUN, ONE BATCH.

    The US ticker is the identifier SEC and every non-provider consumer join on; OpenFIGI spells
    it `BRK/B` where SEC and the market spell `BRK-B`. The response body is stored WHOLE for the
    batch that covered the security, with `position` saying which entry answers it — the mapping
    response is positional and a reorder would attach one company's listing to another's.
    """
    with postgres.connect() as conn:
        attributes = _security_attributes(conn)

    wanted = [sid for sid in context.partition_keys if sid in attributes]
    out: dict[str, list[dict[str, Any]]] = {}
    # 10 jobs per request is OpenFIGI's anonymous batch; the loop keeps the provider honest.
    for i in range(0, len(wanted), 10):
        chunk = wanted[i : i + 10]
        jobs = [
            {"idType": "ID_ISIN", "idValue": attributes[s]["isin"], "exchCode": "US"} for s in chunk
        ]
        doc = figi.mapping(jobs)
        for position, sid in enumerate(chunk):
            row = doc.as_row(context.run.run_id)
            row["position"] = position
            row["asked_with"] = attributes[sid]["isin"]
            row["scheme"] = "ticker"
            out[sid] = [row]
    context.add_output_metadata({"asked": len(wanted), "filings": len(out)})
    return _rung_output(context, out)


@dg.asset(
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="openfigi_mapping",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"openfigi", "parquet"},
    description="OpenFIGI's every-venue lookup for each security, response whole.",
)
def raw_figi_local_symbol(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """The same mapping UNFILTERED, so every venue's match is on record for the local line."""
    with postgres.connect() as conn:
        attributes = _security_attributes(conn)

    wanted = [sid for sid in context.partition_keys if sid in attributes]
    out: dict[str, list[dict[str, Any]]] = {}
    for i in range(0, len(wanted), 10):
        chunk = wanted[i : i + 10]
        jobs = [{"idType": "ID_ISIN", "idValue": attributes[s]["isin"]} for s in chunk]
        doc = figi.mapping(jobs)
        for position, sid in enumerate(chunk):
            row = doc.as_row(context.run.run_id)
            row["position"] = position
            row["asked_with"] = attributes[sid]["isin"]
            row["scheme"] = "symbol"
            out[sid] = [row]
    context.add_output_metadata({"asked": len(wanted), "filings": len(out)})
    return _rung_output(context, out)


@dg.asset(
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="yahoo",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"yahoo", "parquet"},
    description="Yahoo's ISIN search answer for each security, response whole.",
)
def raw_yahoo_symbol(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """Per security, Yahoo's `/v1/finance/search?q=<ISIN>` body, response whole."""
    with postgres.connect() as conn:
        attributes = _security_attributes(conn)

    wanted = [sid for sid in context.partition_keys if sid in attributes]
    out: dict[str, list[dict[str, Any]]] = {}
    for sid in wanted:
        doc = yahoo_search.search(attributes[sid]["isin"])
        row = doc.as_row(context.run.run_id)
        row["position"] = 0
        row["asked_with"] = attributes[sid]["isin"]
        row["scheme"] = "symbol"
        out[sid] = [row]
    context.add_output_metadata({"asked": len(wanted), "filings": len(out)})
    return _rung_output(context, out)
