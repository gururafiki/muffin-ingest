"""Jobs, schedules and sensors of the prices family."""

import dagster as dg
from muffin_ingest.facets import prices

from muffin_ingest_dagster.defs.prices.core import price_bar
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


# THE DAILY CROSS-SECTION, AND IT SHIPS STOPPED.
#
# The assets are complete and can be driven by hand from the UI; what has NOT happened yet is the
# dual-run parity comparison against what `security-prices` currently serves. Starting a schedule
# that asks yfinance for the whole universe every night before that comparison would be spending the
# provider budget on numbers nobody has checked, and would make the first real disagreement look
# like a production incident rather than a finding.
#
# Turning it on is a deliberate act after parity, which is why it is a line in this file rather than
# a default.
daily_prices = dg.build_schedule_from_partitioned_job(
    dg.define_asset_job(
        "daily_prices",
        selection=dg.AssetSelection.assets(raw_price_bars, price_bar),
    ),
    # RUNNING AS OF THE CUTOVER. It shipped STOPPED deliberately — a schedule spending the provider
    # budget on numbers nobody had compared was the wrong default — and the comparison has now
    # happened. The old resources stop in the same change, so if this does not run, nothing does.
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
