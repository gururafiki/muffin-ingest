"""Jobs, schedules and sensors of the fx family."""

import dagster as dg
from muffin_ingest.facets import fx

from muffin_ingest_dagster.defs.fx.core import fx_rate
from muffin_ingest_dagster.defs.fx.partitions import CURRENCY_PARTITION, currency_partitions
from muffin_ingest_dagster.defs.fx.raw import raw_fx_history, raw_fx_spot
from muffin_ingest_dagster.lib.resources import Postgres


#: RUNNING IN CODE, NOT ONLY IN THE DATABASE. Until 2026-09-24 this sensor ran because someone had
#: switched it on in the UI: `all_instigator_state()` reported `stored=RUNNING` while the code said
#: nothing, so a state reset or a rebuilt Dagster database would have turned this lane off silently —
#: the same way every sensor in the location once shipped stopped and the lanes behind them sat idle.
#: Declaring it here changes nothing today and makes the intent survive the database.
@dg.sensor(
    target=raw_fx_history,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="A currency with no history partition yet becomes one to fill.",
)
def new_currencies_need_history(
    context: dg.SensorEvaluationContext, postgres: Postgres
) -> dg.SensorResult:
    """Keeps Lane B's partition set in step with `market.currency`.

    ADDS KEYS AND REQUESTS NOTHING, for the same reason the price sensor does: a new currency should
    make the work VISIBLE as an unmaterialised partition, and deciding when to spend a provider
    budget is an operator's call rather than a sensor's.
    """
    with postgres.connect() as conn:
        currencies = fx.askable_currencies(conn, include_absent=True)

    existing = set(context.instance.get_dynamic_partitions(CURRENCY_PARTITION))
    new = [code for code in currencies if code not in existing]
    context.log.info("%s currencies askable, %s without a partition", len(currencies), len(new))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[currency_partitions.build_add_request(new)] if new else [],
    )


#: RUNNING. 43 requests a day is cheap; what gated it was comparing the output against
#: `market.fx_rate` — and that comparison CANNOT attribute a row, which is worth stating rather
#: than leaving as an implied clean bill of health.
#:
#: Both writers target the same table with the same key and the same `source_code`, and the old
#: edge function populates `derived_from` and the subunit rule too, so nothing on a stored row
#: says which produced it. What IS measurable: the stored 2026-09-11 rates sit 0.03%-0.57% above
#: the provider's completed 09-11 close for all six of EUR/GBP/JPY/KRW/ILS/TWD — the same
#: direction every time, which is one common USD move, i.e. the signature of a value snapshotted
#: mid-session rather than at the close. That is the OLD resource's shape and precisely what this
#: lane's `regularMarketTime` drop exists to prevent. Suggestive, not proof — but it argues for
#: the cutover rather than against it, and after it this lane is the only writer.
daily_fx = dg.build_schedule_from_partitioned_job(
    dg.define_asset_job(
        "daily_fx",
        selection=dg.AssetSelection.assets(raw_fx_spot, fx_rate),
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
