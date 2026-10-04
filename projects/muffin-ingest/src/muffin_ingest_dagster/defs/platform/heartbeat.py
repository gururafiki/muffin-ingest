"""The heartbeat: the canary for the whole code location."""

import time
from datetime import timedelta

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest_dagster.lib.resources import Postgres
from muffin_ingest_dagster.lib.runtime import SHORT_RUN


@dg.asset(
    group_name="platform",
    description="The database answers from inside a run. Proves the daemon, the run queue, a "
    "subprocess and the database connection add up to a working orchestrator.",
    # NO POOL: THE CANARY MUST NOT QUEUE BEHIND THE WORK IT WATCHES. On `sql` at run granularity it
    # started 111 s late behind the day lane (2026-09-17) and sat QUEUED behind a 40-minute
    # recovery run, so any multi-hour run would turn its 3-hour window red and read as a dead
    # daemon. It is one round trip, not a writer.
    kinds={"postgres"},
    # HOURLY SCHEDULE, SO THREE HOURS IS TWO MISSED TICKS. This is the canary for the whole code
    # location — if it stops, the daemon or the database connection has gone, and every other
    # asset's own policy will follow it red a day later. Tighter than the daily lanes on purpose:
    # it is the cheapest asset here and the first thing that should say something is wrong.
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=3)),
)
def heartbeat(context: AssetExecutionContext, postgres: Postgres) -> "dg.MaterializeResult[None]":
    """One round trip to the database, timed.

    IT WAS `ledger_health` UNTIL 2026-10-04, counting the ingest ledger's facets, tasks and open
    attempts. The ledger was retired that day (its one user, the price lane, records symbol
    verdicts as `identifier_probe` rows now), so the canary asks the database the cheapest question
    there is. The round-trip time is recorded because a database under strain answers this slowly
    before it answers anything else wrongly.
    """
    started = time.monotonic()
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute("select now()")
        row = cur.fetchone()
    roundtrip_ms = round((time.monotonic() - started) * 1000)
    database_time = row[0].isoformat() if row and row[0] is not None else "none"
    context.log.info("database answered in %s ms (%s)", roundtrip_ms, database_time)
    return dg.MaterializeResult(
        metadata={"roundtrip_ms": roundtrip_ms, "database_time": database_time}
    )


# HOURLY, AND THE SCHEDULE IS PART OF WHAT IS BEING SMOKE-TESTED. A smoke asset nobody runs proves
# the code location parses; running it on a schedule proves the DAEMON is alive, the run queue
# accepts work, a subprocess launches and the database is reachable from inside one — which is the
# set of things that can be individually healthy and still not add up to a working orchestrator.
#
# It is also the liveness signal the old system never had. `refresh_log` holds one row per resource
# and overwrites it, so a resource dying on every firing kept a fresh `started_at` and read as
# just-started; `security-cn-segments` was dead for two days behind exactly that.
heartbeat_schedule = dg.ScheduleDefinition(
    name="heartbeat",
    target=[heartbeat],
    cron_schedule="7 * * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    tags=SHORT_RUN,
)
