"""Stage 1 of Europe's segment lane: each filer's list and its annual reports, kept whole."""

import time
from collections import Counter
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import esef
from muffin_ingest.providers import xbrl_org

from muffin_ingest_dagster.defs.esef.partitions import ESEF_FILERS_PER_RUN, esef_filers
from muffin_ingest_dagster.defs.segments.raw import (
    FRESH_FLOOR,
    LIST_MAX_AGE_DAYS,
    _round_robin,
    _younger_than,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: The raw asset, spelled once: stage 2 reads what it holds.
ESEF_FILINGS = dg.AssetKey("raw_esef_filings")
#: The registry asset that maps European equities to LEIs.
ESEF_FILER = dg.AssetKey("security_esef_filer")


class EsefRun(dg.Config):
    """What bounds an ESEF run, and what an operator may ask beyond the queue."""

    #: Reports to fetch again whatever their marks and raw say, by LEI
    #: (`{"529900JI1GG6F7RKVI53": ["529900JI1GG6F7RKVI53-2025-12-31-ESEF-FR-0"]}`).
    reread: dict[str, list[str]] | None = None
    #: Wall-clock budget.
    budget_seconds: int = 900
    #: Stop asking for reports once the run holds this many bytes (a report is 2-27 MB).
    budget_bytes: int = 400_000_000
    #: Seconds between requests: ~15 quick requests earn a 429, eight seconds apart held.
    pace_seconds: float = 8.0
    #: A filer whose stored list is younger than this is not listed; its reports are planned from
    #: the stored list. 0 lists every filer.
    list_max_age_days: float = LIST_MAX_AGE_DAYS


#: How a list and a report fetch can end. Each set partitions what it counts. `fresh` is a filer
#: whose stored list is young enough not to ask again: it costs no request.
LIST_OUTCOMES = (
    "answered",
    "absent",
    "not_json",
    "fresh",
    "refused",
    "transport",
    "unasked",
    "not_askable",
)
REPORT_OUTCOMES = (
    "answered",
    "absent",
    "too_large",
    "not_json",
    "refused",
    "transport",
    "held_already",
    "unasked",
)


@dg.asset(
    partitions_def=esef_filers,
    deps=[ESEF_FILER],
    backfill_policy=dg.BackfillPolicy.multi_run(ESEF_FILERS_PER_RUN),
    pool="xbrl_org",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"xbrl-org", "parquet"},
    freshness_policy=FRESH_FLOOR,
    # ONE DOCUMENT PER ROW GROUP (muffin-ingest#177): a filer's file holds every report fetched for
    # it, up to 27 MB each, and reading one must not decode the rest.
    metadata={"merge_on": list(esef.MERGE_ON), "compression": "zstd", "row_group_rows": 1},
    description="Each ESEF filer's list on filings.xbrl.org and the annual reports not yet read.",
)
def raw_esef_filings(
    context: AssetExecutionContext, config: EsefRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """A list per filer, then the annual reports due, in rounds across the run's filers.

    DUE: every annual report the list names (one per period, the newest filing of it) that is not
    read at the current parser version and that raw does not hold. The index lists interim
    reports beside annual ones and does not say which is which, so the filer's fiscal year end
    decides (`esef.is_annual`).

    A FILER WHOSE STORED LIST IS YOUNGER THAN `list_max_age_days` IS NOT LISTED: its reports are
    planned from that list, so the due reports are fetched exactly as if it had been asked.

    THE OUTCOMES PARTITION WHAT THEY COUNT (`LIST_OUTCOMES` sums to `requested`,
    `REPORT_OUTCOMES` to `reports_named`). A 429 or any other refusal stops the run and leaves
    the rest `unasked`: never an absence.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    now = datetime.now(UTC)
    reread = {k: set(v) for k, v in (config.reread or {}).items()}
    with postgres.connect() as conn:
        found = esef.filers(conn, keys)
        year_ends = {key: esef.year_end(conn, ids) for key, ids in found.items()}
    stats: Counter[str] = Counter(
        {
            **{f"list_{o}": 0 for o in LIST_OUTCOMES},
            **{f"report_{o}": 0 for o in REPORT_OUTCOMES},
        }
    )
    rows: list[dict[str, Any]] = []
    placed: dict[int, str] = {}
    deadline = time.monotonic() + config.budget_seconds
    stopped: str | None = None

    listed: dict[str, list[esef.Listed]] = {}
    for key in keys:
        if key not in found:
            stats["list_not_askable"] += 1
            continue
        held_list = esef.newest_list(
            raw_store.stored_rows_where(ESEF_FILINGS, key, {"document": "filings"})
        )
        if held_list is not None and _younger_than(held_list, now, config.list_max_age_days):
            stats["list_fresh"] += 1
            listed[key] = esef.parse_filings(bytes(held_list["body"])) or []
            continue
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is not None:
            stats["list_unasked"] += 1
            continue
        try:
            answer = xbrl_org.filings(key)
        except xbrl_org.XbrlOrgRefused as exc:
            stats["list_refused"] += 1
            stopped = f"filings.xbrl.org refused us ({exc})"
            continue
        except xbrl_org.XbrlOrgTransport:
            stats["list_transport"] += 1
            continue
        finally:
            time.sleep(config.pace_seconds)
        row = esef.list_row(answer, run_id=run_id)
        rows.append(row)
        placed[id(row)] = key
        stats[f"list_{row['outcome']}"] += 1
        if row["outcome"] == "answered":
            listed[key] = esef.parse_filings(answer.document.body) or []

    with postgres.connect() as conn:
        annual = {
            key: esef.annual_reports(items, year_ends.get(key)) for key, items in listed.items()
        }
        read = esef.read_already(conn, [f.fxo_id for items in annual.values() for f in items])
    queues: dict[str, list[str]] = {}
    urls: dict[str, str] = {}
    for key in keys:
        wanted = [
            f.fxo_id
            for f in annual.get(key, [])
            if f.fxo_id not in read or f.fxo_id in reread.get(key, ())
        ]
        for f in annual.get(key, []):
            if f.json_url:
                urls[f.fxo_id] = f.json_url
        if not wanted:
            continue
        light = raw_store.stored_rows_for(
            ESEF_FILINGS, key, ("document", "accession", "outcome", "fetched_at", "run_id")
        )
        queue = []
        for fxo in wanted:
            held = esef.held_report(light, fxo)
            # A body that was not JSON (a gateway page) is asked again: it settled nothing.
            if held is not None and held.outcome != "not_json" and fxo not in reread.get(key, ()):
                stats["report_held_already"] += 1
            else:
                queue.append(fxo)
        queues[key] = queue

    plan = _round_robin(queues)
    stats["reports_named"] = stats["report_held_already"] + len(plan)
    for key, fxo in plan:
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is None and stats["bytes"] >= config.budget_bytes:
            stopped = "the byte budget is spent"
        if stopped is not None:
            stats["report_unasked"] += 1
            continue
        try:
            report = xbrl_org.report(urls[fxo])
        except xbrl_org.XbrlOrgRefused as exc:
            stats["report_refused"] += 1
            stopped = f"filings.xbrl.org refused us ({exc})"
            continue
        except xbrl_org.XbrlOrgTransport:
            stats["report_transport"] += 1
            continue
        finally:
            time.sleep(config.pace_seconds)
        row = esef.report_row(report, fxo_id=fxo, run_id=run_id)
        rows.append(row)
        placed[id(row)] = key
        stats[f"report_{row['outcome']}"] += 1
        stats["bytes"] += len(report.document.body)
    if stopped is not None:
        context.log.warning(
            "%s; %s lists and %s reports unasked",
            stopped,
            stats["list_unasked"],
            stats["report_unasked"],
        )

    context.add_output_metadata(
        {
            "requested": len(keys),
            "list_outcomes": sum(stats[f"list_{o}"] for o in LIST_OUTCOMES),
            "reports_named": stats["reports_named"],
            "report_outcomes": sum(stats[f"report_{o}"] for o in REPORT_OUTCOMES),
            "bytes": stats["bytes"],
            **{f"list_{o}": stats[f"list_{o}"] for o in LIST_OUTCOMES},
            **{f"report_{o}": stats[f"report_{o}"] for o in REPORT_OUTCOMES},
        }
    )
    stored = context.instance.get_materialized_partitions(ESEF_FILINGS)
    return partitioned.by_partition(
        context,
        rows,
        key=lambda row: placed[id(row)],
        unchanged=[k for k in keys if k in stored],
    )
