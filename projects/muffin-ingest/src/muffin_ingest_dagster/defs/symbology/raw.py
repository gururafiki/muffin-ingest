"""Stage 1 of the symbology family: what the provider sent, kept whole."""

from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import symbology as sym
from muffin_ingest.providers import openfigi as figi
from muffin_ingest.providers import yahoo_search

from muffin_ingest_dagster.defs.symbology.conditions import SYMBOLOGY_AUTOMATION
from muffin_ingest_dagster.defs.symbology.partitions import (
    SYMBOLOGY_PER_RUN,
    symbology_subjects,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres


#: OpenFIGI's batch, READ PER RUN because a key changes it tenfold. The provider states both
#: ceilings in its own 413 (10 unkeyed, 100 keyed), and this repo had been sending no key while
#: holding one — so 200 partitions cost 20 requests where they now cost 2. The rungs stay
#: partitioned per SECURITY rather than per batch precisely so this number can change without
#: re-keying the grid.
def mapping_jobs_per_request() -> int:
    return figi.mapping_jobs_per_request()


def _subjects(context: AssetExecutionContext, postgres: Postgres, evidence: str) -> Any:
    """The keys this rung has something to ask about, and what to ask with.

    A RUN COVERS THE FAMILY'S GRID; A RUNG COVERS THE SUBSET IT CAN HELP. The three rungs share one
    partitions definition because `security_symbology` consumes all three, so the grid holds every
    security missing ANY of their evidence — and a rung asked about a subject whose evidence we
    already hold would spend a provider request to be told what is already written down. Yahoo's
    search is one request per subject, so that is not a rounding error: measured 2026-09-20, the
    ladder's population is 5,697 securities missing a ticker against 1,618 missing a provider
    symbol.

    A SKIPPED SUBJECT STILL GETS A FILE, because `by_partition` writes one for every key in the
    run. The partition's claim stays true — this rung was asked about this subject and had nothing
    to ask — and the counters say which happened.
    """
    keys = list(context.partition_keys)
    with postgres.connect() as conn:
        needed = sym.subjects_needing(conn, evidence)
        attributes = sym.attributes_for(conn, [k for k in keys if k in needed])
    return keys, attributes


def _rung_metadata(
    context: AssetExecutionContext, keys: list[str], attributes: dict[str, Any], rows: int
) -> None:
    asked = len(attributes)
    context.add_output_metadata(
        {
            "subjects": len(keys),
            "asked": asked,
            # SUMS TO `subjects`, WHICH IS THE POINT. A gap between these and the partition count
            # is a branch that forgot to count, and this pipeline has paid for exactly that: a
            # throttled price partition once reported `unasked=0` while 5,437 securities had never
            # been asked at all.
            "skipped_have_evidence": len(keys) - asked,
            "rows": rows,
        }
    )


@dg.asset(
    partitions_def=symbology_subjects,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="openfigi_mapping",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"openfigi", "parquet"},
    automation_condition=SYMBOLOGY_AUTOMATION,
    description="OpenFIGI's US lookup for each security, response whole.",
)
def raw_figi_ticker(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """Per security, the `/v3/mapping` body restricted to `exchCode: US`.

    The US ticker is the identifier SEC and every non-provider consumer join on; OpenFIGI spells
    it `BRK/B` where SEC and the market spell `BRK-B`. The response body is stored WHOLE for the
    batch that covered the security, with `position` saying which entry answers it — the mapping
    response is positional and a reorder would attach one company's listing to another's.
    """
    keys, attributes = _subjects(context, postgres, sym.NEEDS_TICKER)
    rows = _map_in_batches(
        context,
        attributes,
        scheme="ticker",
        jobs=lambda isin: {"idType": "ID_ISIN", "idValue": isin, "exchCode": "US"},
    )
    _rung_metadata(context, keys, attributes, len(rows))
    return partitioned.by_partition(context, rows, key=lambda r: str(r["security_id"]))


@dg.asset(
    partitions_def=symbology_subjects,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="openfigi_mapping",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"openfigi", "parquet"},
    automation_condition=SYMBOLOGY_AUTOMATION,
    description="OpenFIGI's every-venue lookup for each security, response whole.",
)
def raw_figi_local_symbol(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """The same mapping UNFILTERED, so every venue's match is on record for the local line."""
    keys, attributes = _subjects(context, postgres, sym.NEEDS_SYMBOL)
    rows = _map_in_batches(
        context,
        attributes,
        scheme="symbol",
        jobs=lambda isin: {"idType": "ID_ISIN", "idValue": isin},
    )
    _rung_metadata(context, keys, attributes, len(rows))
    return partitioned.by_partition(context, rows, key=lambda r: str(r["security_id"]))


@dg.asset(
    partitions_def=symbology_subjects,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="yahoo",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"yahoo", "parquet"},
    automation_condition=SYMBOLOGY_AUTOMATION,
    description="Yahoo's ISIN search answer for each security, response whole.",
)
def raw_yahoo_symbol(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """Per security, Yahoo's `/v1/finance/search?q=<ISIN>` body, response whole.

    THE EXPENSIVE RUNG: one request per subject, where the mapping rungs get ten. It is also the
    FALLBACK — it exists for the securities OpenFIGI cannot name a local line for — so asking it
    about a security whose symbol we already hold is a whole request spent on a known answer.
    """
    keys, attributes = _subjects(context, postgres, sym.NEEDS_SYMBOL)
    rows: list[dict[str, Any]] = []
    for sid, attr in attributes.items():
        doc = yahoo_search.search(attr["isin"])
        row = doc.as_row(context.run.run_id)
        row["security_id"] = sid
        row["position"] = 0
        row["asked_with"] = attr["isin"]
        row["scheme"] = "symbol"
        rows.append(row)
    _rung_metadata(context, keys, attributes, len(rows))
    return partitioned.by_partition(context, rows, key=lambda r: str(r["security_id"]))


def _map_in_batches(
    context: AssetExecutionContext,
    attributes: dict[str, Any],
    *,
    scheme: str,
    jobs: Any,
) -> list[dict[str, Any]]:
    """One `/v3/mapping` request per ten subjects, each subject keeping its own position.

    `security_id` IS ADDED TO THE ROW, and it is the one addition rule 3 allows without argument:
    the body is a positional array of ten answers and carries nothing that says which of our
    securities job j was asked for. Without it the partition's own file could not be read back on
    its own — which is the whole reason raw is partitioned.
    """
    subjects = list(attributes)
    rows: list[dict[str, Any]] = []
    per_request = mapping_jobs_per_request()
    for i in range(0, len(subjects), per_request):
        chunk = subjects[i : i + per_request]
        doc = figi.mapping([jobs(attributes[s]["isin"]) for s in chunk])
        for position, sid in enumerate(chunk):
            row = doc.as_row(context.run.run_id)
            row["security_id"] = sid
            row["position"] = position
            row["asked_with"] = attributes[sid]["isin"]
            row["scheme"] = scheme
            rows.append(row)
    return rows
