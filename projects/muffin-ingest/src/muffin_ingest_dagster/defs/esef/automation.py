"""Europe's segment lane: its jobs, schedules and sensor.

Everything RUNS (2026-11-28): the lane was driven live on a tiny subset, the whole grid was drained
and read, and the regulator is enabled in the same change (muffin-deployment,
`disclosure_source.enabled`).
"""

from datetime import date

import dagster as dg
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import esef

from muffin_ingest_dagster.defs.esef.core import esef_segment, security_esef_filer
from muffin_ingest_dagster.defs.esef.partitions import (
    ESEF_FILER_PARTITION,
    ESEF_FILERS_PER_RUN,
    esef_filers,
)
from muffin_ingest_dagster.defs.esef.raw import ESEF_FILINGS, raw_esef_filings
from muffin_ingest_dagster.defs.segments.automation import floor_chunks
from muffin_ingest_dagster.lib.resources import Postgres
from muffin_ingest_dagster.lib.runtime import SHORT_RUN, XBRL_ORG_RUN

#: THE NAMES ARE STATE: a job's name keys its run history, a sensor's its cursor and status.
ESEF_REGISTRY_JOB = "esef_registry"
ESEF_SEGMENTS_JOB = "esef_segments"

#: One query and one upsert: the LEIs are N-PORT's, and the index is asked nothing.
esef_registry_job = dg.define_asset_job(
    ESEF_REGISTRY_JOB,
    selection=dg.AssetSelection.assets(security_esef_filer),
    run_tags=SHORT_RUN,
)

#: A filer's list and reports, and their segments, in one run: stage 2 reads what raw holds, so a
#: report fetched is read before the run ends.
esef_segments_job = dg.define_asset_job(
    ESEF_SEGMENTS_JOB,
    selection=dg.AssetSelection.assets(raw_esef_filings, esef_segment),
    partitions_def=esef_filers,
    run_tags=XBRL_ORG_RUN,
)


@dg.schedule(
    job=esef_registry_job,
    cron_schedule="55 7 * * 1",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="The ESEF registry, Mondays 07:55 UTC: the LEI of every covered European equity.",
)
def esef_registry(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    day = context.scheduled_execution_time.date().isoformat()
    return dg.RunRequest(run_key=day)


@dg.sensor(
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="An LEI a European equity holds becomes an ESEF filer partition.",
)
def new_esef_filers(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """Keeps the grid in step with `security_filer`. ADDS KEYS AND REQUESTS NOTHING; a key is never
    deleted (a company gone from the registry keeps its history)."""
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "select distinct filer_id from market.security_filer where source_code = %s",
            (esef.REGULATOR,),
        )
        held = [str(r[0]) for r in cur.fetchall()]
    existing = set(context.instance.get_dynamic_partitions(ESEF_FILER_PARTITION))
    new = sorted(k for k in held if k not in existing)
    context.log.info("%s ESEF filers held, %s new", len(held), len(new))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[esef_filers.build_add_request(new)] if new else [],
    )


def floor_runs(grid: list[str], day: date, collected: set[str]) -> list[dg.RunRequest]:
    """THE DAY IS IN EVERY RUN KEY, so a chunk is asked again on its next floor day."""
    return [
        dg.RunRequest(
            run_key=f"{day.isoformat()}:{keys[0]}",
            tags={
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                **XBRL_ORG_RUN,
            },
        )
        for keys in floor_chunks(grid, day, collected, ESEF_FILERS_PER_RUN)
    ]


@dg.schedule(
    job=esef_segments_job,
    cron_schedule="15 9 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="ESEF filers on their 30-day floor, daily 09:15 UTC, outside the run windows.",
)
def esef_filers_floor(context: dg.ScheduleEvaluationContext) -> list[dg.RunRequest]:
    """A filer never collected is due on any day; the backlog of every filer is the operator's
    backfill. Stage 1 lists a filer only once its stored list is 29 days old, so a daily tick
    lists ~1/30 of the grid."""
    day = context.scheduled_execution_time.date()
    grid = list(context.instance.get_dynamic_partitions(ESEF_FILER_PARTITION))
    collected = set(context.instance.get_materialized_partitions(ESEF_FILINGS))
    return floor_runs(grid, day, collected)
