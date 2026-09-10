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
import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest_dagster.resources import Postgres
from muffin_ingest_dagster.retention import nightly_pruning, prune_dagster_storage


@dg.asset(
    group_name="ledger",
    description="The ingest ledger is reachable and answers. Proves the code location can see the "
    "database, which is the one thing a smoke asset should establish.",
    pool="sql",
    kinds={"postgres"},
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


defs = dg.Definitions(
    assets=[ledger_health],
    asset_checks=[every_symbol_keyed_facet_retracts],
    jobs=[prune_dagster_storage],
    schedules=[ledger_heartbeat, nightly_pruning],
    resources={"postgres": Postgres()},
)
