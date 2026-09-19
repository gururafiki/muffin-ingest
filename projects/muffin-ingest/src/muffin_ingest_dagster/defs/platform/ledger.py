"""The ledger heartbeat: the canary for the whole code location."""

from datetime import timedelta

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest_dagster.lib.resources import Postgres


@dg.asset(
    group_name="ledger",
    description="The ingest ledger is reachable and answers. Proves the code location can see the "
    "database, which is the one thing a smoke asset should establish.",
    # NO POOL: THE CANARY MUST NOT QUEUE BEHIND THE WORK IT WATCHES. On `sql` at run granularity it
    # started 111 s late behind `daily_prices` (2026-09-17) and sat QUEUED behind a 40-minute
    # recovery run, so any multi-hour run would turn its 3-hour window red and read as a dead
    # daemon. It is three counts, not a writer.
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
