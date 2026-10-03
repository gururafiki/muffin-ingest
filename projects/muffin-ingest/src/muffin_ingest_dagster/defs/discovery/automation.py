"""Jobs, schedules and sensors of the discovery family."""

from datetime import timedelta

import dagster as dg
from muffin_ingest import settings
from muffin_ingest.facets import nport
from muffin_ingest.providers import sec_nport

from muffin_ingest_dagster.defs.discovery.core import _tracked_funds
from muffin_ingest_dagster.defs.discovery.partitions import (
    NPORT_PARTITIONS,
    SWEEP_PARTITIONS,
    SWEEP_RESUME_TAG,
    exchange_sweeps,
    nport_filings,
)
from muffin_ingest_dagster.defs.discovery.queries import directory_queries
from muffin_ingest_dagster.defs.discovery.raw import (
    _resume_state,
    raw_exchange_sweep,
    raw_fund_directory,
    raw_nport_filing,
)
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

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


#: WHY BOTH DISCOVERY SENSORS SHIP RUNNING, AND WHY THAT IS NOT A SPEND DECISION.
#:
#: Each of them returns `run_requests=[]` — they ADD PARTITION KEYS AND MATERIALISE NOTHING. So
#: starting them makes outstanding work VISIBLE (a new filing, a newly enabled venue appears as an
#: unmaterialised partition) and asks no provider for anything; the fetch stays an operator's call,
#: which is what `new_exchange_sweeps`' own note below has always said.
#:
#: CHANGED FOR FILINGS ON 2026-09-26: `raw_nport_filing` now carries `on_missing()`, because the
#: lane replaces the edge's `fund-holdings` and had never once run by hand. A filing costs one SEC
#: request a quarter. CHANGED FOR THE DIRECTORY ON 2026-10-04: `raw_exchange_sweep` walks a new
#: query on its own and every query monthly (umbrella spec 2026-10-04, decision D3), ~1,300 keyed
#: requests a pass against a filter budget nothing else spends.
#:
#: That distinction is the whole reason these two go on while `new_symbols_needed` does not. The
#: symbology sensor seeds a grid whose rungs carry `AutomationCondition.missing()`, so the daemon
#: would begin asking the provider the moment the keys existed. These cannot: there is no condition
#: on the far side.
#:
#: Measured 2026-09-21 before the change: `dynamic_partitions` held ONE `exchange_sweep` key — the
#: one added by hand to prove the lane live — so a backfill had nothing to select, and the lane had
#: sat at zero materialisations since it was built. A sensor that ships STOPPED is a lane that does
#: not exist.


@dg.sensor(
    target=raw_nport_filing,
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="A tracked fund's newest NPORT-P filing the grid has not queued becomes a "
    "partition.",
)
def new_nport_filings(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """FTS per enabled series for filings we have not queued yet.

    ADDS KEYS AND REQUESTS NOTHING ITSELF; since 2026-09-26 the asset's `on_missing()` fetches a key
    the moment it is added, and `discovered_security` and `fund_holding` follow in the same run.
    Until then a new filing only became VISIBLE, and nothing ever fetched one: the lane had never
    run. The accession is fetched so the key can carry the directory CIK, which the Archives path
    needs and the accession alone cannot supply.
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
    minimum_interval_seconds=24 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Each question in market.directory_query becomes a sweep partition.",
)
def new_exchange_sweeps(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """One partition per row of `market.directory_query` — a venue x type, or an alias.

    STILL ADDS KEYS AND REQUESTS NOTHING ITSELF. What changed on 2026-10-04 is what follows a key:
    `raw_exchange_sweep` carries `on_missing()`, so a key added here is walked on the daemon's next
    tick. Daily rather than weekly, because a row added in Studio should be walked the same day.

    A KEY THE VIEW NO LONGER LISTS IS NAMED, NEVER DELETED HERE: deleting a partition drops its
    materialisation status, and a sensor that read a half-empty view during a deploy would erase
    the grid. The 59 per-venue keys from before the re-key are the expected case, deleted by hand
    once every query has been walked (umbrella spec 2026-10-04).
    """
    with postgres.connect() as conn:
        queries = directory_queries(conn)
    existing = set(context.instance.get_dynamic_partitions(SWEEP_PARTITIONS))
    # SORTED, so the grid reads venue by venue. Each key is its own run (`SWEEP_QUERIES_PER_RUN`).
    #
    # A KEY MUST BE ADDED AFTER THE CONDITION HAS BEEN EVALUATED ONCE. `on_missing()` requests a
    # partition that BECOMES missing between two evaluations; one already missing at the first
    # evaluation is treated as handled (measured on 1.13.22, 2026-09-20: 0 of 2 such partitions).
    # Steady state this is automatic — a row added in Studio lands days after the condition did.
    add = sorted(key for key in queries if key not in existing)
    stale = sorted(existing - set(queries))
    context.log.info(
        "%s queries, %s not yet partitions%s",
        len(queries),
        len(add),
        f"; {len(stale)} partitions no query lists: {', '.join(stale[:20])}" if stale else "",
    )
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[exchange_sweeps.build_add_request(add)] if add else [],
    )


@dg.sensor(
    target=raw_exchange_sweep,
    minimum_interval_seconds=15 * 60,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="A directory walk the provider refused part-way resumes from its file's cursor.",
)
def unfinished_sweeps(context: dg.SensorEvaluationContext, raw_store: RawStore) -> dg.SensorResult:
    """Request every walk whose stored file still ends in a cursor — at most once an hour each.

    WHY A SENSOR AND NOT `any_checks_match(check_failed())`. A check on a partitioned asset is
    unpartitioned in Dagster 1.13 unless declared with a preview `partitions_def` (and even then it
    records a partition only for a single-partition step), so its status is the WHOLE grid's: the
    check-based condition would see one unfinished walk as every query failing and re-walk all 237.
    This reads the same field `venue_sweep_reached_its_last_page` reads, per partition.

    A WALK REFUSED ON ITS FIRST PAGE HAS NO CURSOR, and is not this sensor's: its run FAILED
    (nothing fetched, nothing claimed), and the monthly tick or an operator asks again. Retrying
    those hourly would keep asking a provider that has just refused three times in three minutes.

    THE RUN KEY CARRIES THE CURSOR AND THE HOUR: a resume point is requested at most once an hour,
    a walk that advanced gets a fresh request, and a run already queued for it is not doubled.
    The run is tagged so `raw_exchange_sweep` only RESUMES: if another run finished the walk first,
    this one finds no cursor and asks nothing.
    """
    import hashlib
    import time

    hour = int(time.time() // 3600)
    requests: list[dg.RunRequest] = []
    for key in context.instance.get_dynamic_partitions(SWEEP_PARTITIONS):
        stored = raw_store.stored_rows_for(
            raw_exchange_sweep.key, key, columns=["cursor_at", "page"]
        )
        cursor, _ = _resume_state(stored)
        if cursor is None:
            continue  # finished, or never walked — `on_missing()` owns a new key
        mark = hashlib.sha1(cursor.encode()).hexdigest()[:12]
        requests.append(
            dg.RunRequest(
                partition_key=key,
                run_key=f"{key}:{mark}:{hour}",
                tags={SWEEP_RESUME_TAG: "true"},
            )
        )
    context.log.info("%s unfinished walks requested", len(requests))
    return dg.SensorResult(run_requests=requests)


#: The fund directory is the one daily schedule in the lane. Filings are fetched when their sensor
#: adds them, and the venue directory refreshes itself monthly through its automation condition.
fund_directory = dg.ScheduleDefinition(
    name="fund_directory_schedule",
    target=[raw_fund_directory],
    cron_schedule="0 5 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
