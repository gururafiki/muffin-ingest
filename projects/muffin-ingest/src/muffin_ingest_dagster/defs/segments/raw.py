"""Stage 1 of the segments family: each regulator's segment-bearing documents, kept whole."""

import time
from collections import Counter
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import sec_segments
from muffin_ingest.providers import sec_edgar

from muffin_ingest_dagster.defs.sec.partitions import FILERS_PER_RUN, sec_filers
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: The raw assets, spelled once: stage 2 reads what each already holds.
SEC_INSTANCE = dg.AssetKey("raw_sec_instance")
#: `market.security_filing` as the SEC filer lane writes it: where the filings to read come from.
SEC_FILING = dg.AssetKey("sec_filing")

#: Accounts filings arrive every business day (80 to 506 a weekday in November 2026), so a week
#: in which the lane read none is a stopped sensor, not a quiet market.
FRESH_WEEK = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=7))

#: Consecutive transport failures, with nothing answered, that stop a run: SEC is not reachable.
TRANSPORT_STOP = 3

#: How a filing asked about in a run can end. They partition the filings named, so they sum to
#: `named`. A refusal stops the run and is never an absence; `held_already` and `unasked` cost no
#: request.
INSTANCE_OUTCOMES = (
    "answered",
    "absent",
    "too_large",
    "refused",
    "transport",
    "held_already",
    "unasked",
)


class SecInstanceRun(dg.Config):
    """What bounds an instance run, and what an operator may ask beyond the queue."""

    #: Filings to fetch again whatever their marks and raw say, by filer key
    #: (`{"0000320193": ["0000320193-26-000020"]}`). Stage 2 re-reads what a run fetched, so this
    #: is how one filing is read again by hand.
    reread: dict[str, list[str]] | None = None
    #: Wall-clock budget. A filing is two or three requests at 5 a second.
    budget_seconds: int = 900
    #: Stop asking once the run holds this many bytes of documents: a run returns everything it
    #: fetched at once, and an instance is up to 32 MB (median 1.7 MB, measured 2026-11-09).
    budget_bytes: int = 400_000_000


@dg.asset(
    partitions_def=sec_filers,
    deps=[SEC_FILING],
    backfill_policy=dg.BackfillPolicy.multi_run(FILERS_PER_RUN),
    pool="sec",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"sec", "parquet"},
    freshness_policy=FRESH_WEEK,
    # EVERY FILING'S DOCUMENTS ARE KEPT, keyed by filing and document: a filer's file only grows,
    # and a visit asks only for filings it does not hold.
    #
    # ONE DOCUMENT PER ROW GROUP, so neither a merge nor stage 2's read ever decodes more than one
    # instance at a time. Principal Financial's file (21 filings, 413 MB of instances, 14 MB on
    # disk) took 2,143 MB to read whole, measured 2026-11-14, against a 2.5 GB code location.
    metadata={
        "merge_on": list(sec_segments.MERGE_ON),
        "compression": "zstd",
        "row_group_rows": 1,
    },
    description=(
        "Each SEC accounts filing's XBRL instance and the index that named it, by filer, whole."
    ),
)
def raw_sec_instance(
    context: AssetExecutionContext, config: SecInstanceRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """The instance of every filing of the run's filers that is due and not held.

    WHAT A VISIT COSTS: one query for the run, one read of stored metadata per filer with a filing
    due, and two or three requests per filing fetched (`index.json`, the HTML index when that
    names no instance, the instance). A filer with nothing due costs nothing and its file stands.

    THE OUTCOMES PARTITION THE FILINGS NAMED (`INSTANCE_OUTCOMES`). A refusal (403, 429, 5xx) stops
    the run, and so do three transport failures before anything answered; the rest are `unasked`
    and stay due. Nothing is filed for a filing whose fetch did not finish, so raw never holds
    half of one.
    """
    keys = list(context.partition_keys)
    reread = {key: set(accessions) for key, accessions in (config.reread or {}).items()}
    with postgres.connect() as conn:
        wanted = sec_segments.filings(
            conn, [int(k) for k in keys], sorted({a for k in keys for a in reread.get(k, ())})
        )
    plan = [(key, filing) for key in keys for filing in wanted.get(int(key), [])]
    held: dict[str, set[str]] = {
        key: sec_segments.held_accessions(
            raw_store.stored_rows_for(SEC_INSTANCE, key, ("accession", "outcome"))
        )
        for key in {key for key, _ in plan}
    }

    def is_held(key: str, accession: str) -> bool:
        return accession in held[key] and accession not in reread.get(key, ())

    stats: Counter[str] = Counter({f"instance_{o}": 0 for o in INSTANCE_OUTCOMES})
    rows: list[dict[str, Any]] = []
    placed: dict[int, str] = {}
    deadline = time.monotonic() + config.budget_seconds
    transport_in_a_row = 0
    answered_any = False
    stopped: str | None = None
    for key, filing in plan:
        if is_held(key, filing.accession):
            stats["instance_held_already"] += 1
            continue
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is None and stats["bytes"] >= config.budget_bytes:
            stopped = "the byte budget is spent"
        if stopped is not None:
            stats["instance_unasked"] += 1
            continue
        try:
            fetch = sec_edgar.fetch_instance(filing.cik, filing.accession)
        except sec_edgar.SecRefused as exc:
            stats["instance_refused"] += 1
            stopped = f"SEC refused us ({exc})"
            continue
        except sec_edgar.SecTransport as exc:
            stats["instance_transport"] += 1
            transport_in_a_row += 1
            if transport_in_a_row >= TRANSPORT_STOP and not answered_any:
                stopped = f"{TRANSPORT_STOP} transport failures and nothing answered ({exc})"
            continue
        transport_in_a_row = 0
        answered_any = True
        stats[f"instance_{fetch.outcome}"] += 1
        stats["documents"] += len(fetch.answers)
        stats["bytes"] += sum(len(answer.document.body) for _, answer in fetch.answers)
        for row in sec_segments.raw_rows(
            fetch, accession=filing.accession, run_id=context.run.run_id
        ):
            rows.append(row)
            placed[id(row)] = key
    if stopped is not None:
        context.log.warning("%s; %s filings unasked", stopped, stats["instance_unasked"])

    context.add_output_metadata(
        {
            "requested": len(keys),
            "filers_with_filings": len(held),
            "named": len(plan),
            "outcomes": sum(stats[f"instance_{o}"] for o in INSTANCE_OUTCOMES),
            "documents": stats["documents"],
            "bytes": stats["bytes"],
            **{f"instance_{o}": stats[f"instance_{o}"] for o in INSTANCE_OUTCOMES},
        }
    )
    stored = context.instance.get_materialized_partitions(SEC_INSTANCE)
    return partitioned.by_partition(
        context,
        rows,
        key=lambda row: placed[id(row)],
        unchanged=[k for k in keys if k in stored],
    )
