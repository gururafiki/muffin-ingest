"""Japan's segment lane: its jobs, schedules and sensors.

SHIPS STOPPED but for `new_edinet_filers`, which asks EDINET nothing: the lane goes live (Phase 6c
step 5d) on a tiny subset first, then its schedules and the due sensor are declared RUNNING here.
"""

from collections.abc import Iterable, Sequence
from datetime import timedelta

import dagster as dg
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import edinet as fe

from muffin_ingest_dagster.defs.edinet.core import (
    edinet_document,
    edinet_segment,
    security_edinet_filer,
)
from muffin_ingest_dagster.defs.edinet.partitions import (
    EDINET_FILER_PARTITION,
    EDINET_FILERS_PER_RUN,
    edinet_filers,
)
from muffin_ingest_dagster.defs.edinet.raw import (
    TOKYO,
    raw_edinet_code_list,
    raw_edinet_days,
    raw_edinet_filings,
)
from muffin_ingest_dagster.lib.resources import Postgres
from muffin_ingest_dagster.lib.runtime import EDINET_RUN, SHORT_RUN

#: THE NAMES ARE STATE: a job's name keys its run history, a sensor's its cursor and status.
EDINET_REGISTRY_JOB = "edinet_registry"
EDINET_DAYS_JOB = "edinet_days"
EDINET_SEGMENTS_JOB = "edinet_segments"
DAILY_SCHEDULE = "edinet_daily"

#: One 576 KB download and one upsert, then the catalogue filed for every mapped filer.
edinet_registry_job = dg.define_asset_job(
    EDINET_REGISTRY_JOB,
    selection=dg.AssetSelection.assets(raw_edinet_code_list, security_edinet_filer),
    run_tags=SHORT_RUN,
)

#: Days' lists and their catalogue in one run: stage 2 reads what stage 1 stored.
edinet_days_job = dg.define_asset_job(
    EDINET_DAYS_JOB,
    selection=dg.AssetSelection.assets(raw_edinet_days, edinet_document),
    run_tags=EDINET_RUN,
)

#: A filer's archives and their segments in one run: an archive fetched is read before it ends.
edinet_segments_job = dg.define_asset_job(
    EDINET_SEGMENTS_JOB,
    selection=dg.AssetSelection.assets(raw_edinet_filings, edinet_segment),
    run_tags=EDINET_RUN,
)


@dg.schedule(
    job=edinet_registry_job,
    cron_schedule="35 8 * * 1",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="EDINET's code list, Mondays 08:35 UTC: the EDINET code of every Japanese equity.",
)
def edinet_code_list(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    return dg.RunRequest(run_key=context.scheduled_execution_time.date().isoformat())


#: The days a nightly run asks: the newest ten, so a list fetched while documents still landed, or
#: before a withdrawal, is asked again until it settles (`raw.SETTLE_DAYS`).
LOOKBACK_DAYS = 10


@dg.schedule(
    job=edinet_days_job,
    name=DAILY_SCHEDULE,
    cron_schedule="40 15 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description=(
        "EDINET's lists for the last ten days in Tokyo, daily 15:40 UTC (00:40 in Tokyo, the day "
        "after), outside the run windows."
    ),
)
def edinet_daily(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """THE TICK'S DATE IS IN THE RUN KEY, so a range is asked again every night."""
    tokyo_today = context.scheduled_execution_time.astimezone(TOKYO).date()
    last = tokyo_today - timedelta(days=1)
    first = last - timedelta(days=LOOKBACK_DAYS - 1)
    return dg.RunRequest(
        run_key=f"{tokyo_today.isoformat()}:{first.isoformat()}",
        tags={
            ASSET_PARTITION_RANGE_START_TAG: first.isoformat(),
            ASSET_PARTITION_RANGE_END_TAG: last.isoformat(),
            **EDINET_RUN,
        },
    )


@dg.sensor(
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="An EDINET code a Japanese equity holds becomes an EDINET filer partition.",
)
def new_edinet_filers(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """Keeps the grid in step with `security_filer`. ADDS KEYS AND REQUESTS NOTHING; a key is never
    deleted (a company gone from the code list keeps its history)."""
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "select distinct filer_id from market.security_filer where source_code = %s",
            (fe.SOURCE,),
        )
        held = [str(r[0]) for r in cur.fetchall()]
    existing = set(context.instance.get_dynamic_partitions(EDINET_FILER_PARTITION))
    new = sorted(k for k in held if k not in existing)
    context.log.info("%s EDINET filers held, %s new", len(held), len(new))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[edinet_filers.build_add_request(new)] if new else [],
    )


#: THE MOST REPORTS ONE TICK MAY SET IN MOTION. Annual reports cluster in late June (154 on
#: 2026-06-18 alone), so a day's mapped share is a few hundred. Above this the queue is a history,
#: which is an operator's paced backfill and never one tick.
MAX_DUE_PER_TICK = 1_000


def chunks_due(grid: Sequence[str], due: Iterable[str]) -> list[list[str]]:
    """The `EDINET_FILERS_PER_RUN`-aligned chunks of the grid that hold a filer with a report due.
    A scattered set would be one run per key; aligned chunks bound a tick by the grid's size."""
    position = {key: n for n, key in enumerate(grid)}
    found = sorted({position[k] // EDINET_FILERS_PER_RUN for k in due if k in position})
    width = EDINET_FILERS_PER_RUN
    return [list(grid[c * width : (c + 1) * width]) for c in found]


@dg.sensor(
    job=edinet_segments_job,
    minimum_interval_seconds=1800,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="EDINET filers with an annual report not read at the current parser version.",
)
def edinet_reports_due(
    context: dg.SensorEvaluationContext, postgres: Postgres
) -> dg.SensorResult | dg.SkipReason:
    """Requests the filer chunks holding a report to read, one wave at a time, as
    `sec_instances_due` does: the queue is `security_filing`'s own marks, and nothing is requested
    while a run of the job is queued or running.

    ABOVE `MAX_DUE_PER_TICK` ONLY RECENT REPORTS ARE ASKED, so the ten years filed the day a
    company is mapped are a backfill, while a report filed this fortnight is still read.
    """
    unfinished = context.instance.get_runs(
        filters=dg.RunsFilter(job_name=EDINET_SEGMENTS_JOB, statuses=list(NOT_FINISHED_STATUSES)),
        limit=1,
    )
    if unfinished:
        return dg.SkipReason(f"run {unfinished[0].run_id[:8]} of the last wave has not finished")
    with postgres.connect() as conn:
        due = fe.due_by_filer(conn)
    if not due:
        return dg.SkipReason("no report is due")
    reports = sum(d.filings for d in due.values())
    if reports > MAX_DUE_PER_TICK:
        due = {code: d for code, d in due.items() if d.recent}
        if not due:
            return dg.SkipReason(
                f"{reports} reports are due, more than {MAX_DUE_PER_TICK}, and none was filed in "
                f"the last {fe.RECENT_DAYS} days: a history is a paced backfill"
            )
    grid = context.instance.get_dynamic_partitions(EDINET_FILER_PARTITION)
    chunks = chunks_due(grid, due)
    wave = int(context.cursor or 0) + 1
    context.log.info("wave %s: %s filers due, in %s runs", wave, len(due), len(chunks))
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=f"{wave}:{keys[0]}",
                tags={
                    ASSET_PARTITION_RANGE_START_TAG: keys[0],
                    ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                    **EDINET_RUN,
                },
            )
            for keys in chunks
        ],
        cursor=str(wave),
    )
