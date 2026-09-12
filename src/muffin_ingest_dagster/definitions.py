"""The Dagster code location.

Deliberately almost empty: Phase 1 stands the three services up and proves the image loads, the
code server is reachable and a run can reach the database. Facets arrive one family at a time, and
the edge function keeps running everything until each is cut over.

TWO CONVENTIONS THAT ARE LOAD-BEARING AND EASY TO LOSE:

* AN ASSET IS A TABLE, never a security. Dagster's per-item primitive is partitions, bounded around
  25,000 per asset and meant for time windows, against ~12,350 securities times ~40 facets here —
  so the per-item grain lives in `ingest.task` and an asset materialises "as much of a backlog as
  fits in this run".
* A POOL IS A PROVIDER. `concurrency.pools` in `dagster.yaml` gives each provider a limit of one,
  which is what replaced the five-minute rotation as the guarantee that nothing bursts. Requests
  per second and per day are a separate concern and belong to the limiter, because a pool cannot
  express a rate.
"""

# NO `from __future__ import annotations` IN THIS MODULE, DELIBERATELY.
#
# It stringifies every annotation, and Dagster resolves the `context` parameter by comparing the
# actual CLASS — so with it present, validation fails with "Cannot annotate `context` parameter
# with type AssetExecutionContext" while the annotation plainly IS `AssetExecutionContext`. The
# message names the parameter and not the cause, and qualifying or unqualifying the name changes
# nothing because both are strings by then.
#
# Caught by the `definitions` CI job on its first run, which is what that job exists for: this
# module is loaded by the gRPC server at startup, so the failure would otherwise have been a
# restart loop with the reason only in `docker service logs`.
from datetime import timedelta

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest import settings
from muffin_ingest_dagster.assets import fx, indices, prices, registries
from muffin_ingest_dagster.io_managers import ParquetIOManager, PostgresIOManager
from muffin_ingest_dagster.resources import Postgres
from muffin_ingest_dagster.retention import nightly_pruning, prune_dagster_storage


@dg.asset(
    group_name="ledger",
    description="The ingest ledger is reachable and answers. Proves the code location can see the "
    "database, which is the one thing a smoke asset should establish.",
    pool="sql",
    kinds={"postgres"},
    # HOURLY SCHEDULE, SO THREE HOURS IS TWO MISSED TICKS. This is the canary for the whole code
    # location — if it stops, the daemon or the database connection has gone, and every other
    # asset's own policy will follow it red a day later. Tighter than the daily lanes on purpose:
    # it is the cheapest asset here and the first thing that should say something is wrong.
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=3)),
)
def ledger_health(
    context: AssetExecutionContext, postgres: Postgres
) -> "dg.MaterializeResult[None]":
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from ingest.facet")
        facets = (cur.fetchone() or (0,))[0]
        cur.execute("select count(*) from ingest.task")
        tasks = (cur.fetchone() or (0,))[0]
        cur.execute("select count(*) from ingest.attempt where finished_at is null")
        open_attempts = (cur.fetchone() or (0,))[0]

    context.log.info("ledger: %s facets, %s tasks, %s open attempts", facets, tasks, open_attempts)
    return dg.MaterializeResult(
        metadata={
            "facets": facets,
            "tasks": tasks,
            # An attempt with no finish past its timeout is a run that DIED. `refresh_log` could
            # never show this: one row per resource, overwritten, so a resource failing on every
            # firing keeps `started_at` fresh and reads as just-started.
            "open_attempts": open_attempts,
        }
    )


@dg.asset_check(asset=ledger_health, blocking=False)
def every_symbol_keyed_facet_retracts(postgres: Postgres) -> dg.AssetCheckResult:
    """A mark removes the subject from the backlog, so nothing else will ever clear the stale value.

    The database enforces this with a check constraint; asserting it here as well means the
    dashboard shows it holding rather than only the migration knowing.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from ingest.facet where key_kind = 'symbol' and retract_sql is null"
        )
        offenders = (cur.fetchone() or (0,))[0]
    return dg.AssetCheckResult(
        passed=offenders == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"facets_without_a_retraction": offenders},
    )


@dg.asset_check(asset=prices.raw_price_bars, blocking=False)
def every_askable_security_was_asked(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """A DAY-PARTITIONED ASSET CLAIMS ITS WHOLE CROSS-SECTION, so a run that stopped early lies.

    This is the claim the entire partitioning argument rests on. "Did the collection run on
    Tuesday?" is unanswerable from `price_bar` — a security with no bar looks identical whether its
    market was shut, its symbol is dead, or nothing ran at all — so MATERIALISING THE PARTITION is
    the answer. A run that spends its budget with subjects still unasked materialises the partition
    anyway, and the claim quietly becomes false.

    Measured: 200 securities took 244 s, so ~1.22 s each and the 11,446 askable equities are **3.9
    hours**. The budget was one hour, which would have covered a quarter of them and reported
    success — the exact shape of `remaining: 0` against a backlog of 9,013.

    WARN, NOT ERROR. A short night is worth seeing and not worth failing a pipeline over, and a
    check that takes the lane down because a provider was slow is a check someone disables.
    """
    key = prices.raw_price_bars.key
    event = context.instance.get_latest_materialization_events([key]).get(key)
    materialization = event.asset_materialization if event is not None else None
    unasked = 0
    partition = "none"
    subjects = 0
    if materialization is not None:
        partition = materialization.partition or "none"
        unasked = int(getattr(materialization.metadata.get("unasked"), "value", 0) or 0)
        subjects = int(getattr(materialization.metadata.get("subjects"), "value", 0) or 0)

    return dg.AssetCheckResult(
        passed=unasked == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "unasked": unasked,
            "subjects": subjects,
            "partition": partition,
            "note": "a partition claims its whole cross-section; unasked subjects make that false",
        },
    )


# HOURLY, AND THE SCHEDULE IS PART OF WHAT IS BEING SMOKE-TESTED. A smoke asset nobody runs proves
# the code location parses; running it on a schedule proves the DAEMON is alive, the run queue
# accepts work, a subprocess launches and the database is reachable from inside one — which is the
# set of things that can be individually healthy and still not add up to a working orchestrator.
#
# It is also the liveness signal the old system never had. `refresh_log` holds one row per resource
# and overwrites it, so a resource dying on every firing kept a fresh `started_at` and read as
# just-started; `security-cn-segments` was dead for two days behind exactly that.
ledger_heartbeat = dg.ScheduleDefinition(
    name="ledger_heartbeat",
    target=[ledger_health],
    cron_schedule="7 * * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
        selection=dg.AssetSelection.assets(prices.raw_price_bars, prices.price_bar),
    ),
    # RUNNING AS OF THE CUTOVER. It shipped STOPPED deliberately — a schedule spending the provider
    # budget on numbers nobody had compared was the wrong default — and the comparison has now
    # happened. The old resources stop in the same change, so if this does not run, nothing does.
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

#: RUNNING, AND THE COMPARISON THAT GATED IT WAS ADJUDICATED AGAINST THE PROVIDER RATHER THAN
#: AGAINST `market.performance`.
#:
#: Compared naively the two tables disagree enormously — 453 of 626 (scope, period) pairs by more
#: than half a point, `country:KR 1d` at **-4.1933 against +3.2498**, a sign flip. That comparison
#: measures nothing: `index_return` is anchored on a TRADE DATE and `performance.as_of` is the
#: RUN's timestamp, so with this schedule stopped the new table sat at 2026-09-10 while the old
#: resource had run on 09-12. Two different days, and `performance` upserts in place, so there is
#: no history to pin them to a common one.
#:
#: Yahoo settles it. EWY closed 190.78 / 182.78 / 188.72 on 09-09 / 09-10 / 09-11, so the 1d return
#: ending 09-10 is **-4.1933%** and the one ending 09-11 is **+3.2498%** — each side matches the
#: provider EXACTLY, to four decimal places, for its own anchor. Both are right; only the anchors
#: differed. The match also confirms the live-quote drop: the new value is the completed bar, never
#: `regularMarketTime`.
daily_indices = dg.build_schedule_from_partitioned_job(
    dg.define_asset_job(
        "daily_indices",
        selection=dg.AssetSelection.assets(
            indices.raw_index_bars, indices.raw_sector_performance, indices.index_return
        ),
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
        selection=dg.AssetSelection.assets(fx.raw_fx_spot, fx.fx_rate),
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


# THE AUTOMATION SENSOR SHIPS STOPPED, AND WITHOUT IT `AutomationCondition` DOES NOTHING.
#
# `security_return` declares `AutomationCondition.eager()` and had never once fired: measured
# 2026-09-11, **`AUTO-MATERIALIZE runs ever: 0`** against 48 daemon ticks, all of them from the two
# standard sensors, while the history load wrote 20 M rows and the returns table sat at the 96
# securities a hand-run had given it. Dagster creates `default_automation_condition_sensor`
# automatically and leaves it STOPPED, so the mechanism this design uses to replace the old
# system's :24/:54/:14 cron choreography was inert.
#
# Declared here rather than started in the UI, for the same reason every other control in this
# repo is: a thing switched on by hand is a thing the next rebuild forgets, and nothing would
# report it — the runs simply would not happen.
automation = dg.AutomationConditionSensorDefinition(
    name="default_automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        ledger_health,
        # Lane A: the daily cross-section.
        prices.raw_price_bars,
        prices.price_bar,
        # Lane B: history and repair, per security.
        prices.raw_price_history,
        prices.price_bar_history,
        # Derived from the bars, eager on both lanes — no cron offset, which is what the old
        # system's :24/:54/:14 choreography was for.
        prices.security_return,
        # FX, in the same two lanes — a daily cross-section that claims completeness, and a
        # per-currency history whose subject IS the slice.
        fx.raw_fx_spot,
        fx.fx_rate,
        fx.raw_fx_history,
        fx.fx_rate_history,
        # Index returns — two acquisition shapes into one table, because the scopes differ in where
        # their numbers come from and in nothing else.
        indices.raw_index_bars,
        indices.raw_sector_performance,
        indices.index_return,
        registries.raw_sec_cik_map,
        registries.security_cik,
        registries.raw_nse_equity_list,
        registries.security_nse_filer,
    ],
    asset_checks=[every_symbol_keyed_facet_retracts, every_askable_security_was_asked],
    jobs=[prune_dagster_storage],
    schedules=[
        ledger_heartbeat,
        nightly_pruning,
        daily_prices,
        daily_fx,
        daily_indices,
        registries.weekly_registries,
    ],
    sensors=[prices.new_securities_need_history, fx.new_currencies_need_history, automation],
    resources={
        "postgres": Postgres(),
        # ONE MANAGER PER STORAGE CLASS, never one per asset — which is what makes the writers'
        # rules apply to every facet without any of them remembering.
        "parquet_io": ParquetIOManager(settings.raw_root()),
        # The writer takes the SAME resource every reader uses, rather than opening its own
        # connection — see `PostgresIOManager`'s docstring for what that silently skipped.
        "postgres_io": PostgresIOManager(postgres=Postgres()),
    },
)
