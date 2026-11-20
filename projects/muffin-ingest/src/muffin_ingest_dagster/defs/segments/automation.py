"""The segments family's job and sensor.

`sec_instances_due` RUNS, declared here so a reset of Dagster's state cannot stop it. It shipped
stopped (muffin-ingest#174) and was started on 2026-11-14 once the lane had read a tiny subset live
and the edge's `security-segments` resource was retired (muffin-deployment#511): two writers never
read one queue.
"""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime

import dagster as dg
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import cninfo as cn
from muffin_ingest.facets import dart as fd
from muffin_ingest.facets import nse as fn
from muffin_ingest.facets import sec_segments

from muffin_ingest_dagster.defs.sec.partitions import (
    FILERS_PER_RUN,
    SEC_FILER_PARTITION,
    cik_key,
    sec_filers,
)
from muffin_ingest_dagster.defs.segments.core import (
    cninfo_segment,
    dart_segment,
    nse_segment,
    sec_segment,
    security_cninfo_filer,
    security_dart_filer,
)
from muffin_ingest_dagster.defs.segments.partitions import (
    CNINFO_FILER_PARTITION,
    CNINFO_FILERS_PER_RUN,
    DART_FILER_PARTITION,
    DART_FILERS_PER_RUN,
    NSE_FILER_PARTITION,
    NSE_FILERS_PER_RUN,
    cninfo_filers,
    dart_filers,
    nse_filers,
    on_floor,
)
from muffin_ingest_dagster.defs.segments.raw import (
    CNINFO_FILINGS,
    DART_FILINGS,
    NSE_FILINGS,
    raw_cninfo_filings,
    raw_cninfo_stock_list,
    raw_dart_corp_codes,
    raw_dart_filings,
    raw_nse_filings,
    raw_sec_instance,
)
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from muffin_ingest_dagster.lib.runtime import CNINFO_RUN, DART_RUN, NSE_RUN, SEC_RUN, SHORT_RUN

#: THE NAMES ARE STATE: a job's name keys its run history, a sensor's its cursor and status.
SEC_SEGMENTS_JOB = "sec_segments"

#: THE MOST FILINGS ONE TICK MAY SET IN MOTION. The busiest measured day brought 506 accounts
#: filings (2026-11-05, the 10-Q deadline week), so four times that is still a day. Above it the
#: queue is a history: every filing after a parser version is raised (148,425 hold XBRL), whose
#: documents kept whole are ~25 GB. That is an operator's backfill, paced and sized for the disk,
#: and a sensor that started it by itself would do so at whatever hour the version changed.
MAX_DUE_PER_TICK = 2_000

#: A FILER'S INSTANCES AND THEIR SEGMENTS IN ONE RUN: stage 2 reads whatever raw holds that is due,
#: so a filing stage 1 fetched is written before the run ends, and a failed write is retried by
#: the next tick's visit without another request.
sec_segments_job = dg.define_asset_job(
    SEC_SEGMENTS_JOB,
    selection=dg.AssetSelection.assets(raw_sec_instance, sec_segment),
    partitions_def=sec_filers,
    run_tags=SEC_RUN,
)


def chunks_due(grid: Sequence[str], due: Mapping[int, int]) -> list[list[str]]:
    """The `FILERS_PER_RUN`-aligned chunks of the grid that hold a filer with a filing due.

    A run covers a contiguous range, and Dagster gives a scattered set one run per key (254 runs
    for 255 subjects, measured 2026-10-06). Chunks aligned to grid positions bound a day at 76
    runs, and each run asks only for what is due.
    """
    position = {key: n for n, key in enumerate(grid)}
    found = sorted(
        {position[k] // FILERS_PER_RUN for k in (cik_key(cik) for cik in due) if k in position}
    )
    return [list(grid[c * FILERS_PER_RUN : (c + 1) * FILERS_PER_RUN]) for c in found]


@dg.sensor(
    job=sec_segments_job,
    minimum_interval_seconds=1800,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Filers with an accounts filing whose XBRL instance has not been read.",
)
def sec_instances_due(
    context: dg.SensorEvaluationContext, postgres: Postgres
) -> dg.SensorResult | dg.SkipReason:
    """Requests the filer chunks holding a filing to read, one wave at a time.

    THE QUEUE IS `security_filing`'s OWN MARKS, so a filing a run could not read (a refusal, a
    spent budget, a failed write) is simply still due at the next tick. Nothing is requested while
    a run of the job is queued or running: a wave finishes before the next is counted, so a slow
    wave is never doubled.

    THE CURSOR COUNTS WAVES and is part of each run's key, so a tick evaluated twice launches a
    chunk once, and the next wave may ask for the same chunk again.
    """
    unfinished = context.instance.get_runs(
        filters=dg.RunsFilter(job_name=SEC_SEGMENTS_JOB, statuses=list(NOT_FINISHED_STATUSES)),
        limit=1,
    )
    if unfinished:
        return dg.SkipReason(f"run {unfinished[0].run_id[:8]} of the last wave has not finished")
    with postgres.connect() as conn:
        due = sec_segments.due_by_filer(conn)
    filings = sum(due.values())
    if not due:
        return dg.SkipReason("no filing is due")
    if filings > MAX_DUE_PER_TICK:
        return dg.SkipReason(
            f"{filings} filings of {len(due)} filers are due, more than {MAX_DUE_PER_TICK}: a "
            "queue that size is a history, which is a paced backfill and never one tick"
        )
    grid = context.instance.get_dynamic_partitions(SEC_FILER_PARTITION)
    chunks = chunks_due(grid, due)
    wave = int(context.cursor or 0) + 1
    context.log.info(
        "wave %s: %s filings of %s filers due, in %s runs", wave, filings, len(due), len(chunks)
    )
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=f"{wave}:{keys[0]}",
                tags={
                    ASSET_PARTITION_RANGE_START_TAG: keys[0],
                    ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                    **SEC_RUN,
                },
            )
            for keys in chunks
        ],
        cursor=str(wave),
    )


# ── CNINFO (China) ─────────────────────────────────────────────────────────────────────────────
#
# EVERYTHING HERE SHIPS STOPPED, as the SEC lane did: the edge's `cn-filings` and
# `security-cn-segments` read the same queue until they are retired, and two writers never read
# one queue. `new_cninfo_filers` alone runs, because it asks CNINFO nothing.

CNINFO_REGISTRY_JOB = "cninfo_registry"
CNINFO_SEGMENTS_JOB = "cninfo_segments"
CNINFO_RETENTION_JOB = "cninfo_retention"

cninfo_registry_job = dg.define_asset_job(
    CNINFO_REGISTRY_JOB,
    selection=dg.AssetSelection.assets(raw_cninfo_stock_list, security_cninfo_filer),
    # Two list requests and one upsert: a short job.
    run_tags=SHORT_RUN,
)

#: A filer's search and reports, and their segments, in one run: stage 2 reads what raw holds, so a
#: report fetched is read before the run ends.
cninfo_segments_job = dg.define_asset_job(
    CNINFO_SEGMENTS_JOB,
    selection=dg.AssetSelection.assets(raw_cninfo_filings, cninfo_segment),
    partitions_def=cninfo_filers,
    run_tags=CNINFO_RUN,
)


@dg.schedule(
    job=cninfo_registry_job,
    cron_schedule="20 7 * * 1",
    execution_timezone="UTC",
    # Started by hand on 2026-11-16 and running since: declared here so a state reset keeps it.
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="CNINFO's stock lists, Mondays 07:20 UTC: the org id of every A-share.",
)
def cninfo_stock_lists(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    day = context.scheduled_execution_time.date().isoformat()
    return dg.RunRequest(run_key=day)


@dg.sensor(
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="An org id an A-share holds becomes a CNINFO filer partition.",
)
def new_cninfo_filers(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """Keeps the grid in step with `security_filer`. ADDS KEYS AND REQUESTS NOTHING; a key is never
    deleted (a company gone from the lists keeps its history)."""
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "select distinct filer_id from market.security_filer where source_code = %s",
            (cn.SOURCE,),
        )
        held = [str(r[0]) for r in cur.fetchall()]
    existing = set(context.instance.get_dynamic_partitions(CNINFO_FILER_PARTITION))
    new = sorted(k for k in held if k not in existing)
    context.log.info("%s CNINFO filers held, %s new", len(held), len(new))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[cninfo_filers.build_add_request(new)] if new else [],
    )


def floor_chunks(
    grid: Sequence[str], day: date, collected: set[str], width: int = CNINFO_FILERS_PER_RUN
) -> list[list[str]]:
    """The `width`-aligned chunks holding a filer whose floor day it is, or one never collected.
    Aligned to grid positions, so run keys stay stable as the grid only appends."""
    position = {k: n for n, k in enumerate(grid)}
    due = {k for k in grid if on_floor(k, day) or k not in collected}
    found = sorted({position[k] // width for k in due})
    return [list(grid[c * width : (c + 1) * width]) for c in found]


@dg.schedule(
    job=cninfo_segments_job,
    cron_schedule="35 7 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="CNINFO filers on their 30-day floor, daily 07:35 UTC, outside the run windows.",
)
def cninfo_filers_floor(context: dg.ScheduleEvaluationContext) -> list[dg.RunRequest]:
    """THE DAY IS IN EVERY RUN KEY, so a chunk is asked again on its next floor day. A filer never
    collected is due on any day; the backlog of every filer is the operator's backfill."""
    day = context.scheduled_execution_time.date()
    grid = context.instance.get_dynamic_partitions(CNINFO_FILER_PARTITION)
    collected = set(context.instance.get_materialized_partitions(CNINFO_FILINGS))
    return [
        dg.RunRequest(
            run_key=f"{day.isoformat()}:{keys[0]}",
            tags={
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                **CNINFO_RUN,
            },
        )
        for keys in floor_chunks(grid, day, collected)
    ]


#: The columns retention reads of each stored row, without a body.
_HELD = ("document", "accession", "outcome", "pruned_at")


@dg.op(pool="cninfo", description="Drops the bodies of read CNINFO reports (Phase 6 decision 4).")
def prune_cninfo_reports(
    context: dg.OpExecutionContext, postgres: Postgres, raw_store: RawStore
) -> None:
    """Every read report older than 30 days, then read ones oldest-read first while the reports held
    whole exceed 5 GB (`cn.choose_prunes`). HOLDS THE `cninfo` POOL, as stage 1 does, so it never
    rewrites a filer's file while a run is merging into it."""
    grid = context.instance.get_dynamic_partitions(CNINFO_FILER_PARTITION)
    stored: list[tuple[str, str, int]] = []
    for key in grid:
        for row in raw_store.stored_rows_sized(CNINFO_FILINGS, key, _HELD):
            if (
                row.get("document") == "report"
                and row.get("outcome") == "answered"
                and row.get("pruned_at") is None
                and row.get("body_bytes", 0) > 0
            ):
                stored.append((key, str(row["accession"]), int(row["body_bytes"])))
    with postgres.connect() as conn:
        read = cn.read_at(conn, [url for _, url, _ in stored])
    held = [cn.HeldReport(key, url, size, read.get(url)) for key, url, size in stored]
    now = datetime.now(UTC)
    chosen = cn.choose_prunes(held, now=now)
    by_key: dict[str, set[tuple[str, str]]] = {}
    for h in chosen:
        by_key.setdefault(h.key, set()).add(("report", h.url))
    pruned = sum(
        raw_store.prune_bodies(
            CNINFO_FILINGS, key, rows, ("document", "accession"), pruned_at=now.isoformat()
        )
        for key, rows in by_key.items()
    )
    total = sum(h.bytes for h in held)
    freed = sum(h.bytes for h in chosen)
    context.log.info(
        "%s reports held whole (%.2f GB, %s unread); pruned %s (%.2f GB)",
        len(held),
        total / 1024**3,
        sum(1 for h in held if h.read_at is None),
        pruned,
        freed / 1024**3,
    )
    # AN OBSERVATION OF THE RAW ASSET, so the held total has a history beside its materializations.
    context.log_event(
        dg.AssetObservation(
            asset_key=CNINFO_FILINGS,
            metadata={
                "held": len(held),
                "held_bytes": total,
                "unread": sum(1 for h in held if h.read_at is None),
                "pruned": pruned,
                "freed_bytes": freed,
                "held_bytes_after": total - freed,
            },
        )
    )


@dg.job(name=CNINFO_RETENTION_JOB, tags=CNINFO_RUN)
def cninfo_retention_job() -> None:
    prune_cninfo_reports()


@dg.schedule(
    job=cninfo_retention_job,
    cron_schedule="*/15 * * * *",
    execution_timezone="UTC",
    # Started by hand on 2026-11-16 and running since: declared here so a state reset keeps it.
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Prunes read CNINFO reports every 15 minutes (a backfill fetches ~9 GB an hour).",
)
def cninfo_retention(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    return dg.RunRequest(run_key=context.scheduled_execution_time.isoformat())


# ── DART (Korea) ───────────────────────────────────────────────────────────────────────────────
#
# SHIPS STOPPED, as CNINFO did: the edge's `kr-filings` writes the same filings until it is
# retired. `new_dart_filers` alone runs, because it asks DART nothing.

DART_REGISTRY_JOB = "dart_registry"
DART_SEGMENTS_JOB = "dart_segments"

#: One six-minute request and one upsert (`DART_RUN`, not `SHORT_RUN`: the registry alone is 346 s).
dart_registry_job = dg.define_asset_job(
    DART_REGISTRY_JOB,
    selection=dg.AssetSelection.assets(raw_dart_corp_codes, security_dart_filer),
    run_tags=DART_RUN,
)

#: A filer's list and archives, and their segments, in one run: stage 2 reads what raw holds, so an
#: archive fetched is read before the run ends.
dart_segments_job = dg.define_asset_job(
    DART_SEGMENTS_JOB,
    selection=dg.AssetSelection.assets(raw_dart_filings, dart_segment),
    partitions_def=dart_filers,
    run_tags=DART_RUN,
)


@dg.schedule(
    job=dart_registry_job,
    cron_schedule="50 7 * * 1",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="DART's registry, Mondays 07:50 UTC: the corp code of every Korean equity.",
)
def dart_corp_codes(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    day = context.scheduled_execution_time.date().isoformat()
    return dg.RunRequest(run_key=day)


@dg.sensor(
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="A corp code a Korean equity holds becomes a DART filer partition.",
)
def new_dart_filers(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """Keeps the grid in step with `security_filer`. ADDS KEYS AND REQUESTS NOTHING; a key is never
    deleted (a company gone from the registry keeps its history)."""
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "select distinct filer_id from market.security_filer where source_code = %s",
            (fd.SOURCE,),
        )
        held = [str(r[0]) for r in cur.fetchall()]
    existing = set(context.instance.get_dynamic_partitions(DART_FILER_PARTITION))
    new = sorted(k for k in held if k not in existing)
    context.log.info("%s DART filers held, %s new", len(held), len(new))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[dart_filers.build_add_request(new)] if new else [],
    )


@dg.schedule(
    job=dart_segments_job,
    cron_schedule="5 8 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="DART filers on their 30-day floor, daily 08:05 UTC, outside the run windows.",
)
def dart_filers_floor(context: dg.ScheduleEvaluationContext) -> list[dg.RunRequest]:
    """THE DAY IS IN EVERY RUN KEY, so a chunk is asked again on its next floor day. A filer never
    collected is due on any day; the backlog of every filer is the operator's backfill."""
    day = context.scheduled_execution_time.date()
    grid = context.instance.get_dynamic_partitions(DART_FILER_PARTITION)
    collected = set(context.instance.get_materialized_partitions(DART_FILINGS))
    return [
        dg.RunRequest(
            run_key=f"{day.isoformat()}:{keys[0]}",
            tags={
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                **DART_RUN,
            },
        )
        for keys in floor_chunks(grid, day, collected, DART_FILERS_PER_RUN)
    ]


# ── NSE (India) ────────────────────────────────────────────────────────────────────────────────
#
# NO REGISTRY HERE: `security_nse_filer` (the registries family, weekly) maps Indian equities to NSE
# symbols by ISIN, and its `security_filer` rows are this grid's keys.

NSE_SEGMENTS_JOB = "nse_segments"

#: A filer's results list and instances, and their segments, in one run: stage 2 reads what raw
#: holds, so an instance fetched is read before the run ends.
nse_segments_job = dg.define_asset_job(
    NSE_SEGMENTS_JOB,
    selection=dg.AssetSelection.assets(raw_nse_filings, nse_segment),
    partitions_def=nse_filers,
    run_tags=NSE_RUN,
)


@dg.sensor(
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="An NSE symbol an Indian equity holds becomes an NSE filer partition.",
)
def new_nse_filers(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """Keeps the grid in step with `security_filer`. ADDS KEYS AND REQUESTS NOTHING; a key is never
    deleted (a company gone from NSE's list keeps its history)."""
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "select distinct filer_id from market.security_filer where source_code = %s",
            (fn.SOURCE,),
        )
        held = [str(r[0]) for r in cur.fetchall()]
    existing = set(context.instance.get_dynamic_partitions(NSE_FILER_PARTITION))
    new = sorted(k for k in held if k not in existing)
    context.log.info("%s NSE filers held, %s new", len(held), len(new))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[nse_filers.build_add_request(new)] if new else [],
    )


@dg.schedule(
    job=nse_segments_job,
    cron_schedule="20 8 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="NSE filers on their 30-day floor, daily 08:20 UTC, outside the run windows.",
)
def nse_filers_floor(context: dg.ScheduleEvaluationContext) -> list[dg.RunRequest]:
    """THE DAY IS IN EVERY RUN KEY, so a chunk is asked again on its next floor day. A filer never
    collected is due on any day; the backlog of every filer is the operator's backfill."""
    day = context.scheduled_execution_time.date()
    grid = context.instance.get_dynamic_partitions(NSE_FILER_PARTITION)
    collected = set(context.instance.get_materialized_partitions(NSE_FILINGS))
    return [
        dg.RunRequest(
            run_key=f"{day.isoformat()}:{keys[0]}",
            tags={
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                **NSE_RUN,
            },
        )
        for keys in floor_chunks(grid, day, collected, NSE_FILERS_PER_RUN)
    ]
