"""The macro family's job, its daily schedule, and the sensor that seeds its grid."""

from collections.abc import Sequence

import dagster as dg
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import macro

from muffin_ingest_dagster.defs.macro.core import macro_observation
from muffin_ingest_dagster.defs.macro.partitions import MACRO_PARTITIONS, macro_series
from muffin_ingest_dagster.defs.macro.raw import raw_macro_series
from muffin_ingest_dagster.lib.resources import Postgres
from muffin_ingest_dagster.lib.runtime import SHORT_RUN

#: THE NAME IS STATE: the schedule's and the job's name key Dagster's run history.
MACRO_SCHEDULE = "macro_daily"

#: STAGE 1 AND STAGE 2 IN ONE RUN, so a series' answer and its points land together.
macro_daily_job = dg.define_asset_job(
    MACRO_SCHEDULE,
    selection=dg.AssetSelection.assets(raw_macro_series, macro_observation),
    run_tags=SHORT_RUN,
)


@dg.schedule(
    name=MACRO_SCHEDULE,
    job=macro_daily_job,
    # 07:40 UTC: after the company lane (03:30, holding `yfinance` until ~07:20) and the nasdaq
    # window (07:10), before the SEC wave (11:00). FRED and OECD publish on their own days, so one
    # read a day is ample; the edge asked every six hours, and its TTL skipped 93% of those.
    cron_schedule="40 7 * * *",
    # STOPPED UNTIL ITS LIVE RUN AND PARITY with what the edge's `macro-indicators` wrote.
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="Every catalogued macro series' last 400 days, in one run.",
)
def macro_daily(context: dg.ScheduleEvaluationContext) -> Sequence[dg.RunRequest] | dg.SkipReason:
    """One run over the whole grid, as a range of its keys.

    THE RUN KEY CARRIES THE TICK'S DAY: Dagster launches a key once per schedule.
    """
    keys = context.instance.get_dynamic_partitions(MACRO_PARTITIONS)
    if not keys:
        return dg.SkipReason("no catalogued series has a partition yet")
    day = context.scheduled_execution_time.date().isoformat()
    return [
        dg.RunRequest(
            run_key=f"{MACRO_SCHEDULE}:{day}",
            tags={
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
            },
        )
    ]


@dg.sensor(
    target=raw_macro_series,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Each enabled row of market.macro_indicator becomes a macro_indicator partition.",
)
def new_macro_series(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """Adds keys and requests nothing: the daily schedule asks every key.

    A KEY IS NEVER DELETED HERE. A row disabled in Studio keeps its partition, and the run counts it
    `disabled` without asking; deleting it would drop its history and its status.
    """
    with postgres.connect() as conn:
        enabled = [s.code for s in macro.catalogue(conn) if s.enabled]
    existing = set(context.instance.get_dynamic_partitions(MACRO_PARTITIONS))
    add = [code for code in enabled if code not in existing]
    context.log.info("%s enabled series, %s not yet partitions", len(enabled), len(add))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[macro_series.build_add_request(add)] if add else [],
    )
