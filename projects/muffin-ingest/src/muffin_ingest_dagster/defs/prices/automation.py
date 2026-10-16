"""Jobs, schedules and sensors of the prices family."""

from collections.abc import Sequence

import dagster as dg
from muffin_ingest.facets import prices

from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices.core import price_bar_history, yahoo_corporate_action
from muffin_ingest_dagster.defs.prices.partitions import (
    PROVIDER,
    SECURITY_PARTITION,
    security_partitions,
)
from muffin_ingest_dagster.defs.prices.raw import raw_price_chart
from muffin_ingest_dagster.lib import rotation
from muffin_ingest_dagster.lib.resources import Postgres
from muffin_ingest_dagster.lib.runtime import PRICE_RUN


#: RUNNING IN CODE, NOT ONLY IN THE DATABASE. Until 2026-09-24 this sensor ran because someone
#: had switched it on in the UI: `all_instigator_state()` reported `stored=RUNNING` while the code
#: said nothing, so a state reset or a rebuilt Dagster database would have turned this lane off
#: silently — the way every sensor in the location once shipped stopped and the lanes behind them
#: sat idle. Declaring it here changes nothing today and makes the intent survive the database.
@dg.sensor(
    target=raw_price_chart,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
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


#: THE SCHEDULE'S NAME, spelled once. Resuming reads this schedule's own runs back by the tag the
#: scheduler stamps with it, so a rename would also restart the rotation (`lib.rotation.resume_at`).
SWEEP_SCHEDULE = "nightly_prices"

#: THE NIGHT A RUN BELONGS TO, and THE LAST KEY THAT NIGHT ASKED FOR. Every run of a night carries
#: both, so tomorrow can read where tonight stopped from any one of them. Ours rather than parsed
#: out of Dagster's own tags: the run key's format exists for idempotence, and the scheduler's
#: `.dagster/scheduled_execution_time` is a hidden tag, so a change to either would silently cost
#: the rotation its position.
SWEEP_NIGHT_TAG = "muffin/sweep_night"
SWEEP_LAST_TAG = "muffin/sweep_last"

#: SINCE 2026-10-10 THE NIGHT ASKS YAHOO'S CHART DIRECTLY (`raw_price_chart`), so each bar is
#: labelled with the currency the provider states. `raw_price_history`, the openbb lane, is left
#: defined and unscheduled as the rollback until its files are dropped (umbrella
#: docs/deferred/2026-10-10-the-openbb-price-raw-is-the-rollback.md).
nightly_prices_job = dg.define_asset_job(
    SWEEP_SCHEDULE,
    selection=dg.AssetSelection.assets(raw_price_chart, price_bar_history, yahoo_corporate_action),
    partitions_def=security_partitions,
    run_tags=PRICE_RUN,
)


@dg.schedule(
    name=SWEEP_SCHEDULE,
    job=nightly_prices_job,
    cron_schedule="0 0 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="The next slice of the universe, extended from each security's own watermark.",
)
def nightly_prices(
    context: dg.ScheduleEvaluationContext,
) -> Sequence[dg.RunRequest] | dg.SkipReason:
    """A ROUND-ROBIN OVER A CONTIGUOUS SLICE, in runs of `HISTORY_PARTITIONS_PER_RUN`.

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

    EACH NIGHT STARTS AFTER THE LAST KEY THE PREVIOUS NIGHT ASKED FOR. Until 2026-09-25 the start
    was `(day * SWEEP_SLICE) % len(keys)` — stateless, and it advanced one slice a night only while
    the grid did not change size. **Adding ONE security re-mapped every future slice**, because
    `day * 2500 mod N` and `day * 2500 mod (N+1)` are unrelated numbers. Measured: the grid went
    from 12,267 to 12,268 keys between two ticks, and the 2026-09-25 night swept [2300, 4800) —
    1,071 keys from the 09-23 night and 1,429 from the 09-24 night, i.e. nothing new at all — while
    6,056 securities sat 8-14 days stale. With discovery and symbology live the grid grows often.

    The partition list is in insertion order (`get_dynamic_partitions` orders by id), so growth
    appends at the end and never moves a key. Anchored on a KEY, a night's position cannot be
    disturbed by growth; a new security is simply reached when the rotation gets to the end.

    THE POSITION LIVES IN DAGSTER'S RUN STORAGE, NOT IN A TABLE OF OURS. **A Dagster schedule has
    no cursor** — `ScheduleEvaluationContext` exposes none and `build_schedule_context` takes no
    `cursor` argument; cursors belong to sensors, and turning this into one would trade a cron for
    a polling interval on a job that genuinely runs once a night. But the runs a schedule launched
    ARE durable state it can read back, and since 2026-09-19 nothing prunes them (`lib.rotation`).

    A NIGHT'S SLICE WRAPS PAST THE END OF THE GRID, so every night asks for `SWEEP_SLICE` keys and
    the provider allowance is spent evenly. A run never straddles the wrap: the slice is cut at the
    end of the list, so each run stays one contiguous range, which is all a RunRequest can name.
    """
    keys = context.instance.get_dynamic_partitions(SECURITY_PARTITION)
    if not keys:
        return dg.SkipReason("no securities have a price partition yet")

    # THE SLICE IS CUT INTO RUNS OF THE MEASURED WIDTH.
    #
    # This asked for the entire slice in ONE run, and the first scheduled night died:
    # 2026-09-21 00:06:57, `Killed process (python) anon-rss 2,199,804 kB` against the container's
    # 2.5 GiB, and the run failed at 00:07:08 with `ChildProcessCrashException`.
    #
    # `raw_price_history` carries `BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN)` precisely
    # to bound that memory — 96 securities of full history measured 683,391 bars and OOM-killed at
    # 2.4 GB, 25 measured ~178k rows and ~600 MB. **A backfill policy does not apply to a
    # schedule's RunRequest.** It governs how a BACKFILL is split, so naming a 2,500-key range in a
    # RunRequest walks straight past the one number that was measured to stop this, and nothing
    # warns.
    #
    # READ THROUGH THE MODULE THAT DEFINES IT, not bound at import. One number governs both the
    # asset's backfill policy and this schedule, and a copy here would be free to drift from the
    # measurement it came from — which is how the run width stopped being enforced in the first
    # place. (It is also what makes it patchable in a test: mypy refuses a re-exported attribute.)
    # The rotation is built per tick for the same reason: its constants are read when it runs.
    planned = rotation.slice_requests(
        context.instance,
        keys,
        rotation.Rotation(
            schedule=SWEEP_SCHEDULE,
            day_tag=SWEEP_NIGHT_TAG,
            last_tag=SWEEP_LAST_TAG,
            lookback=RESUME_LOOKBACK,
        ),
        day=context.scheduled_execution_time.date(),
        size=SWEEP_SLICE,
        width=prices_partitions.HISTORY_PARTITIONS_PER_RUN,
    )
    context.log.info(
        "sweeping %s of %s securities from %s (%s) in runs of %s: %s",
        planned.size,
        len(keys),
        planned.start,
        planned.source,
        prices_partitions.HISTORY_PARTITIONS_PER_RUN,
        ", ".join(f"{lo}..{hi - 1}" for lo, hi in planned.segments),
    )
    return planned.requests


#: HOW MANY OF THE SCHEDULE'S NEWEST RUNS THE ROTATION READS. A night is
#: `ceil(SWEEP_SLICE / HISTORY_PARTITIONS_PER_RUN)` runs (100 today), and a re-evaluation of
#: tonight's tick can already have launched all of tonight's, so the page must hold two nights and
#: then some. One run of the previous night is enough when it carries `SWEEP_LAST_TAG`.
RESUME_LOOKBACK = 400
