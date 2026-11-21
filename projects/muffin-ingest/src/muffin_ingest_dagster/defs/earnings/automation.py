"""The earnings family's job and its daily schedule."""

from collections.abc import Sequence
from datetime import timedelta

import dagster as dg
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)

from muffin_ingest_dagster.defs.earnings import partitions as earnings_partitions
from muffin_ingest_dagster.defs.earnings.core import nasdaq_earnings
from muffin_ingest_dagster.defs.earnings.partitions import nasdaq_days
from muffin_ingest_dagster.defs.earnings.raw import raw_nasdaq_earnings
from muffin_ingest_dagster.lib.runtime import SHORT_RUN

#: THE NAME IS STATE: the schedule's and the job's name key Dagster's run history.
NASDAQ_SCHEDULE = "nasdaq_earnings_window"

#: STAGE 1 AND STAGE 2 IN ONE RUN, so a day's answer and its rows land together.
nasdaq_earnings_job = dg.define_asset_job(
    NASDAQ_SCHEDULE,
    selection=dg.AssetSelection.assets(raw_nasdaq_earnings, nasdaq_earnings),
    partitions_def=nasdaq_days,
    # ~29 requests at ~3.3 s each is under two minutes. NO PRIORITY: at 07:10 nothing long holds
    # the `sql` pool, and the short-lane priority belongs to the lanes queued with the price night.
    run_tags=SHORT_RUN,
)


@dg.schedule(
    name=NASDAQ_SCHEDULE,
    job=nasdaq_earnings_job,
    # 07:10 UTC: after the company lane (03:30-~06:00), before the SEC wave (11:00), and after US
    # reports of the previous evening are filled in.
    cron_schedule="10 7 * * *",
    # STOPPED UNTIL THE CUTOVER: the writers use the provider keys the cutover migration adds.
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="Nasdaq's calendar for the last seven days and the next 21, in one run.",
)
def nasdaq_earnings_window(context: dg.ScheduleEvaluationContext) -> Sequence[dg.RunRequest]:
    """One run over the window, as a range of the daily grid.

    THE RUN KEY CARRIES THE TICK'S DAY: Dagster launches a key once per schedule, and the window is
    the same range of days for 29 consecutive ticks.
    """
    today = context.scheduled_execution_time.date()
    first = today - timedelta(days=earnings_partitions.WINDOW_PAST_DAYS)
    last = today + timedelta(days=earnings_partitions.WINDOW_AHEAD_DAYS)
    return [
        dg.RunRequest(
            run_key=f"{NASDAQ_SCHEDULE}:{today.isoformat()}",
            tags={
                ASSET_PARTITION_RANGE_START_TAG: first.isoformat(),
                ASSET_PARTITION_RANGE_END_TAG: last.isoformat(),
            },
        )
    ]
