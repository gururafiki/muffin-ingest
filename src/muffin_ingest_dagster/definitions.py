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

from __future__ import annotations

import dagster as dg

from muffin_ingest_dagster.resources import Postgres


@dg.asset(
    group_name="ledger",
    description="The ingest ledger is reachable and answers. Proves the code location can see the "
    "database, which is the one thing a smoke asset should establish.",
    pool="sql",
    kinds={"postgres"},
)
def ledger_health(context: dg.AssetExecutionContext, postgres: Postgres) -> dg.MaterializeResult:
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


defs = dg.Definitions(
    assets=[ledger_health],
    asset_checks=[every_symbol_keyed_facet_retracts],
    resources={"postgres": Postgres()},
)
