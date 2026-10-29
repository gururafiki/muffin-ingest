"""The SEC family's insider jobs and schedules. Both schedules ship STOPPED until step 7."""

from collections.abc import Sequence
from datetime import timedelta

import dagster as dg

from muffin_ingest_dagster.defs.sec.core import sec_form4_trade, sec_insider_trade
from muffin_ingest_dagster.defs.sec.partitions import (
    INDEX_LOOKBACK_DAYS,
    insider_quarters,
    sec_days,
)
from muffin_ingest_dagster.defs.sec.raw import (
    DATASET,
    INDEX,
    raw_sec_daily_index,
    raw_sec_form4,
    raw_sec_insider_dataset,
)
from muffin_ingest_dagster.lib.runtime import SEC_DATASET_RUN, SEC_RUN

#: THE NAMES ARE STATE: schedule and job names key Dagster's run history and instigator state.
INDEX_SCHEDULE = "sec_daily_index"
DATASET_SCHEDULE = "sec_insider_datasets"

#: A DAY'S INDEX, ITS FORM 4S AND THEIR TRADES IN ONE RUN, so a day's filings land together.
sec_daily_index_job = dg.define_asset_job(
    INDEX_SCHEDULE,
    selection=dg.AssetSelection.assets(raw_sec_daily_index, raw_sec_form4, sec_form4_trade),
    partitions_def=sec_days,
    run_tags=SEC_RUN,
)

sec_insider_datasets_job = dg.define_asset_job(
    DATASET_SCHEDULE,
    selection=dg.AssetSelection.assets(raw_sec_insider_dataset, sec_insider_trade),
    partitions_def=insider_quarters,
    run_tags=SEC_DATASET_RUN,
)


@dg.schedule(
    name=INDEX_SCHEDULE,
    job=sec_daily_index_job,
    # 11:00 UTC: SEC publishes a day's index at ~02:01 UTC, and the price night (00:00), the
    # symbology re-ask (03:00) and the company lane (03:30-~06:00) are done.
    cron_schedule="0 11 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="Yesterday's index and Form 4s, and any day of the last ten not collected.",
)
def sec_daily_index(context: dg.ScheduleEvaluationContext) -> Sequence[dg.RunRequest]:
    """One run per day: yesterday, plus each day of the last `INDEX_LOOKBACK_DAYS` the index lane
    has not collected. One run per day, because the Form 4 lane is asked one day at a time."""
    today = context.scheduled_execution_time.date()
    held = context.instance.get_materialized_partitions(INDEX)
    first = max(sec_days.start.date(), today - timedelta(days=INDEX_LOOKBACK_DAYS))
    days = [first + timedelta(days=n) for n in range((today - first).days)]
    return [
        dg.RunRequest(run_key=f"{INDEX_SCHEDULE}:{day.isoformat()}", partition_key=day.isoformat())
        for day in days
        if day == today - timedelta(days=1) or day.isoformat() not in held
    ]


@dg.schedule(
    name=DATASET_SCHEDULE,
    job=sec_insider_datasets_job,
    # WEEKLY, NOT DAILY: a quarter's set came ~7 days after the quarter until 2026q2, which SEC had
    # not published four months on. A 404 fails the run claiming nothing, so a daily ask would leave
    # a failed run a day for months; a quarterly product is checked weekly.
    cron_schedule="45 11 * * 1",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="The newest completed quarter's insider data set, until SEC publishes it.",
)
def sec_insider_datasets(context: dg.ScheduleEvaluationContext) -> Sequence[dg.RunRequest]:
    """The newest completed quarter, while it is not held. History is an operator backfill."""
    keys = insider_quarters.get_partition_keys(
        current_time=context.scheduled_execution_time, dynamic_partitions_store=context.instance
    )
    if not keys:
        return []
    newest = keys[-1]
    if newest in context.instance.get_materialized_partitions(DATASET):
        return []
    return [dg.RunRequest(run_key=f"{DATASET_SCHEDULE}:{newest}", partition_key=newest)]
