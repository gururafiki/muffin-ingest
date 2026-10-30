"""The companies family's job and its weekly rotation."""

import math
from collections.abc import Sequence

import dagster as dg

from muffin_ingest_dagster.defs.companies import partitions as company_partitions
from muffin_ingest_dagster.defs.companies.core import (
    company_facts,
    yahoo_fiscal_calendar,
    yahoo_statement,
)
from muffin_ingest_dagster.defs.companies.raw import raw_yahoo_company
from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION, security_partitions
from muffin_ingest_dagster.lib import rotation
from muffin_ingest_dagster.lib.runtime import COMPANY_RUN

#: THE SCHEDULE'S NAME, spelled once: the rotation finds the previous day's runs by it, so a rename
#: restarts the rotation.
COMPANY_SCHEDULE = "yahoo_companies"

#: The day a run belongs to and the last key that day asked for, on every run of the day.
COMPANY_DAY_TAG = "muffin/companies_day"
COMPANY_LAST_TAG = "muffin/companies_last"

#: Two days of runs and then some: a day is `ceil(grid / 7 / 100)` runs, ~18 today.
RESUME_LOOKBACK = 80

#: STAGE 1 AND STAGE 2 IN ONE RUN, so a company's documents and its rows land together; the
#: derivation is its own asset, triggered by what this writes. THE CALENDAR RUNS LAST IN THE SAME
#: RUN (since 2026-10-30), so its labels are on the statements before the derivation, which waits
#: for the run's `sql` slot, reads them.
yahoo_companies_job = dg.define_asset_job(
    COMPANY_SCHEDULE,
    selection=dg.AssetSelection.assets(
        raw_yahoo_company, company_facts, yahoo_statement, yahoo_fiscal_calendar
    ),
    partitions_def=security_partitions,
    run_tags=COMPANY_RUN,
)


@dg.schedule(
    name=COMPANY_SCHEDULE,
    job=yahoo_companies_job,
    # 03:30 UTC: after the price night (00:00, ~2 hours) and the 03:00 symbology re-ask, so the
    # `yfinance` pool is free. Both lanes ask Yahoo at the same pace, one at a time.
    cron_schedule="30 3 * * *",
    # RUNNING SINCE 2026-10-17 (Phase 4 step 8). It shipped STOPPED until the lane had run live
    # on a tiny subset (step 7). That run found two defects the lane would have spread to every
    # company on a 52/53-week calendar: a TTM refused over the same quarter from two sources
    # (muffin-deployment#439) and a quarter split across two columns (#441). Both are fixed.
    # Declared here rather than toggled in the UI, so a reset of Dagster's state cannot stop it.
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="A seventh of the universe a day, so each company's summary is a week old at most.",
)
def yahoo_companies(
    context: dg.ScheduleEvaluationContext,
) -> Sequence[dg.RunRequest] | dg.SkipReason:
    """Today's slice of the grid, resumed after yesterday's last key (`lib.rotation`).

    THE SLICE IS A SEVENTH OF THE GRID, recomputed each day, so the rotation stays a week long as
    the universe grows and every company's summary is due on each visit. Stage 1 decides per company
    what is due, so a visit where nothing is due costs no request.
    """
    keys = context.instance.get_dynamic_partitions(SECURITY_PARTITION)
    if not keys:
        return dg.SkipReason("no securities have a partition yet")
    width = company_partitions.COMPANY_PARTITIONS_PER_RUN
    planned = rotation.slice_requests(
        context.instance,
        keys,
        rotation.Rotation(
            schedule=COMPANY_SCHEDULE,
            day_tag=COMPANY_DAY_TAG,
            last_tag=COMPANY_LAST_TAG,
            lookback=RESUME_LOOKBACK,
        ),
        day=context.scheduled_execution_time.date(),
        size=math.ceil(len(keys) / company_partitions.ROTATION_DAYS),
        width=width,
    )
    context.log.info(
        "visiting %s of %s companies from %s (%s) in runs of %s: %s",
        planned.size,
        len(keys),
        planned.start,
        planned.source,
        width,
        ", ".join(f"{lo}..{hi - 1}" for lo, hi in planned.segments),
    )
    return planned.requests
