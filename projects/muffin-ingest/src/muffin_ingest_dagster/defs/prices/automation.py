"""Jobs, schedules and sensors of the prices family."""

import dagster as dg
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import prices

from muffin_ingest_dagster.defs.prices.core import price_bar, price_bar_history
from muffin_ingest_dagster.defs.prices.partitions import (
    PROVIDER,
    SECURITY_PARTITION,
    security_partitions,
)
from muffin_ingest_dagster.defs.prices.raw import raw_price_bars, raw_price_history
from muffin_ingest_dagster.lib.resources import Postgres


@dg.sensor(
    target=raw_price_history,
    minimum_interval_seconds=3600,
    description="A security with no history partition yet becomes one to fill.",
)
def new_securities_need_history(
    context: dg.SensorEvaluationContext, postgres: Postgres
) -> dg.SensorResult:
    """Keeps Lane B's partition set in step with the universe.

    ADDS KEYS AND REQUESTS NOTHING. A promotion should make the work VISIBLE as an unmaterialised
    partition rather than launch a run per security — 10,894 run requests is not a backfill, and
    deciding when to spend the provider budget on a deep history is an operator's call.
    """
    with postgres.connect() as conn:
        subjects = prices.askable_subjects(conn, provider=PROVIDER.code)

    existing = set(context.instance.get_dynamic_partitions(SECURITY_PARTITION))
    new = [s.security_id for s in subjects if s.security_id not in existing]
    context.log.info(
        "%s securities askable, %s without a history partition", len(subjects), len(new)
    )
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[security_partitions.build_add_request(new)] if new else [],
    )


#: HOW MANY SECURITIES ONE NIGHT ASKS FOR. The universe is ~12,000 and the provider's allowance is
#: not a constant — measured, it served 12,021 requests on 2026-09-18 and refused after ~2,740 the
#: very next night — so a slice sized against either number is wrong on the other. 2,500 covers the
#: universe in about five nights, and a night that is refused simply leaves its partitions
#: unmaterialised for a later run to collect.
SWEEP_SLICE = 2500


nightly_prices_job = dg.define_asset_job(
    "nightly_prices",
    selection=dg.AssetSelection.assets(raw_price_history, price_bar_history),
    partitions_def=security_partitions,
)


@dg.schedule(
    job=nightly_prices_job,
    cron_schedule="0 0 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="The next slice of the universe, extended from each security's own watermark.",
)
def nightly_prices(context: dg.ScheduleEvaluationContext) -> dg.RunRequest | dg.SkipReason:
    """A ROUND-ROBIN OVER A CONTIGUOUS SLICE, because that is the only shape Dagster can express.

    A `RunRequest` covers one partition or one CONTIGUOUS RANGE — there is no way to name an
    arbitrary set. Weight order is uncorrelated with position in the partition list, so a
    weight-ordered batch is never contiguous, and asking for it would mean either one run per
    security (~12,000 runs a night) or re-creating the partition list in weight order and
    maintaining it as weights move each quarter. Both are the hand-kept subject table this
    project's rules exist to avoid.

    WEIGHT PRIORITY SURVIVES WHERE IT MATTERS ANYWAY. `askable_subjects` orders by fund weight and
    the asset filters that order to the partitions its run was given, so a run refused half-way has
    collected the heaviest securities in its slice first. What is given up is ordering ACROSS
    nights, and the cost is bounded: every security is asked every few nights whatever its size.

    THE POSITION IS DERIVED FROM THE DATE, NOT FROM A CURSOR, AND NOT BY CHOICE. **A Dagster
    schedule has no cursor** — `ScheduleEvaluationContext` exposes none and
    `build_schedule_context` takes no `cursor` argument; cursors belong to sensors. Turning this
    into a sensor to gain one would trade a cron for a polling interval on a job that genuinely
    runs once a night. Counting days since the epoch is stateless, survives a redeploy, and
    advances by exactly one slice a night on its own.
    """
    keys = context.instance.get_dynamic_partitions(SECURITY_PARTITION)
    if not keys:
        return dg.SkipReason("no securities have a price partition yet")

    day = context.scheduled_execution_time.date().toordinal()
    start = (day * SWEEP_SLICE) % len(keys)
    end = min(start + SWEEP_SLICE, len(keys))

    context.log.info("sweeping securities %s..%s of %s", start, end - 1, len(keys))
    return dg.RunRequest(
        # THE RANGE TAGS ARE HOW ONE RUN COVERS MANY PARTITIONS. `partition_key` names exactly one,
        # and a slice of 2,500 single-partition run requests would be 2,500 runs a night.
        tags={
            ASSET_PARTITION_RANGE_START_TAG: keys[start],
            ASSET_PARTITION_RANGE_END_TAG: keys[end - 1],
        },
    )


# THE DAY LANE, KEPT AND STOPPED — this is the rollback, not dead code.
#
# `nightly_prices` above replaced it on 2026-09-19 because the provider is asked once per TICKER
# whatever we batch (measured: four symbols, six `/v8/finance/chart/<ticker>` requests), so a
# day-partitioned cross-section was one partition standing for ~12,000 independent requests — all
# or nothing, and a refusal mid-way left a materialised partition whose completeness claim was
# false.
#
# IT IS DEFINED RATHER THAN DELETED SO THE CUTOVER IS REVERSIBLE BY FLIPPING A SWITCH. Starting
# this schedule and stopping the other restores the previous behaviour with no deploy — which is
# what expand/contract means here, and is why the assets, their checks and the whole offline replay
# suite over captured provider bytes are all still in place. It goes, with them, once the sweep has
# proven itself live.
daily_prices = dg.build_schedule_from_partitioned_job(
    dg.define_asset_job(
        "daily_prices",
        selection=dg.AssetSelection.assets(raw_price_bars, price_bar),
    ),
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
