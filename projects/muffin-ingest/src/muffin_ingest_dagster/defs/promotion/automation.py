"""The promotion family's schedule: stage 1 weekly; stage 2 follows it by `eager()`."""

import dagster as dg

from muffin_ingest_dagster.defs.promotion.partitions import WAVE_CRON, wave_weeks
from muffin_ingest_dagster.defs.promotion.raw import raw_promotion_probe
from muffin_ingest_dagster.lib.runtime import PROMOTION_RUN

#: THE JOB'S AND THE SCHEDULE'S NAMES, spelled once: Dagster keys run history and schedule state on
#: them.
WAVE_JOB = "promotion_probes"
PROBE_SCHEDULE = "weekly_promotion_wave"

promotion_probes_job = dg.define_asset_job(
    WAVE_JOB,
    selection=dg.AssetSelection.assets(raw_promotion_probe),
    run_tags=PROMOTION_RUN,
)


@dg.schedule(
    name=PROBE_SCHEDULE,
    job=promotion_probes_job,
    cron_schedule=WAVE_CRON,
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Asks Yahoo about the next candidate lines; the wave follows when the probe lands.",
)
def weekly_promotion_wave(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """THE WEEK THAT STARTS AT THE TICK. `end_offset=1` makes it a partition from that minute, and
    the run key is the week, so a retried tick never asks Yahoo twice for one wave."""
    with dg.partition_loading_context(effective_dt=context.scheduled_execution_time):
        week = wave_weeks.get_last_partition_key()
    if week is None:
        raise dg.DagsterInvariantViolationError("no promotion week exists at this tick")
    return dg.RunRequest(run_key=week, partition_key=week)
