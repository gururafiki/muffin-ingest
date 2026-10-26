"""Asset checks of the promotion family."""

import math

import dagster as dg

from muffin_ingest_dagster.defs.companies.partitions import (
    COMPANY_PARTITIONS_PER_RUN,
    ROTATION_DAYS,
)
from muffin_ingest_dagster.defs.prices.automation import SWEEP_SLICE
from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION
from muffin_ingest_dagster.defs.promotion.core import promotion_wave
from muffin_ingest_dagster.defs.promotion.raw import read_policy
from muffin_ingest_dagster.lib.resources import Postgres


@dg.asset_check(asset=promotion_wave, blocking=False)
def promotion_stays_within_the_sweep(
    context: dg.AssetCheckExecutionContext, postgres: Postgres
) -> dg.AssetCheckResult:
    """Can the price sweep still visit the whole universe in the nights the policy allows?

    The wave caps itself by the same arithmetic, so this fails only when the grid grew some other
    way (the Track button, discovery, a policy edited down). It also reports the company lane's
    day, which grows by a seventh of every wave: the user decided both are checked each week
    (2026-10-25), and the lane's runs are a fixed width, so its day is counted in runs.

    WARN, NOT BLOCKING: a pass a night longer than planned delays each security's next price by a
    day; it breaks nothing.
    """
    keys = len(context.instance.get_dynamic_partitions(SECURITY_PARTITION))
    with postgres.connect() as conn, conn.cursor() as cur:
        policy = read_policy(cur)
    nights = policy.max_pass_nights if policy else 0
    pass_nights = math.ceil(keys / SWEEP_SLICE)
    company_day = math.ceil(keys / ROTATION_DAYS)
    return dg.AssetCheckResult(
        passed=pass_nights <= nights,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "securities_in_sweep": keys,
            "price_pass_nights": pass_nights,
            "max_pass_nights": nights,
            "paused": policy.paused if policy else None,
            "company_lane_companies_a_day": company_day,
            "company_lane_runs_a_day": math.ceil(company_day / COMPANY_PARTITIONS_PER_RUN),
        },
    )
