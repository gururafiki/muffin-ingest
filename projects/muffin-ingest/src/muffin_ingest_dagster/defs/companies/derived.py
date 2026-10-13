"""Stage 3 of the companies family: the metric layer and the TTMs, computed in the database."""

import time
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest_dagster.defs.companies.core import yahoo_statement
from muffin_ingest_dagster.lib.resources import Postgres

#: A FLOOR, because not every writer of the statements is a Dagster asset. Until Phase 5 the edge's
#: SEC and XBRL resources write `security_statement` and `security_metric` through PostgREST, and
#: nothing upstream here materialises when they do. Every half hour, at an off-minute.
EDGE_WRITES_FLOOR = "17,47 * * * *"

#: The two backlogs, counted from the indexes the functions page on.
UNDERIVED = "select count(*) from market.security_statement where derived_at is null"
PENDING_TTM = "select count(*) from market.pending_ttm"


class DeriveRun(dg.Config):
    """How much one run takes on. Each call is one statement under `ingest_rw`'s 120 s timeout."""

    #: Statements per `derive_security_metrics` call. The edge ran 2,000-4,000 under PostgREST's
    #: 8 s ceiling in ~12 s a call; the worker's ceiling is fifteen times that.
    metrics_page: int = 2000
    #: Securities per `derive_ttm` call: measured on production 2026-10-10, 1,000 took about 5 s.
    ttm_page: int = 1000
    #: Wall-clock budget for the whole run. A backlog left over is the next run's.
    budget_seconds: int = 900


def _count(conn: Any, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _drain(
    conn: Any, *, call: str, page: int, backlog: str, deadline: float
) -> dict[str, int | str]:
    """Call one derive function a page at a time until its backlog is empty or the budget is spent.

    EACH CALL IS ITS OWN TRANSACTION. Both functions build `on commit drop` temporary tables, so a
    second call in the same transaction fails on a table that already exists.

    A PAGE THAT DOES NOT SHRINK THE BACKLOG STOPS THE LOOP. Both functions stamp every row they
    evaluate, so a page always advances; if one ever does not (another writer re-queueing rows as
    fast as they drain), looping would spend the budget on the same rows.
    """
    before = _count(conn, backlog)
    out: dict[str, int | str] = {"backlog_before": before, "pages": 0, "written": 0}
    stopped = "drained" if before == 0 else "budget"
    while before > 0 and time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(call, (page,))
            row = cur.fetchone()
        conn.commit()
        out["pages"] = int(out["pages"]) + 1
        out["written"] = int(out["written"]) + (int(row[0]) if row and row[0] is not None else 0)
        after = _count(conn, backlog)
        if after == 0:
            stopped = "drained"
            before = after
            break
        if after >= before:
            stopped = "no_progress"
            before = after
            break
        before = after
    out["backlog_after"] = before
    out["stopped"] = stopped
    return out


@dg.asset(
    deps=[yahoo_statement],
    # EAGER, MINUS THE GATE THAT WOULD KEEP IT FROM EVER FIRING: an unpartitioned asset depends on
    # every `security` partition, and the rotation always leaves some unmaterialised. Plus a
    # half-hourly floor for the edge's writes, which no upstream materialisation announces.
    automation_condition=(
        dg.AutomationCondition.eager().without(~dg.AutomationCondition.any_deps_missing())
        | dg.AutomationCondition.cron_tick_passed(EDGE_WRITES_FLOOR)
    ).with_label("on new statements, or every half hour"),
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=3)),
    pool="sql",
    group_name="companies",
    kinds={"postgres"},
    description=(
        "The metric layer and the TTMs from every source's statements, paged under the worker's "
        "statement timeout. No provider call."
    ),
)
def derived_security_metrics(
    context: AssetExecutionContext, config: DeriveRun, postgres: Postgres
) -> dg.MaterializeResult[None]:
    """Calls `market.derive_security_metrics`, then `market.derive_ttm`, until both backlogs drain.

    THE RULES LIVE IN THE DATABASE, where muffin-deployment's tests hold them (a TTM is four
    consecutive quarters, the fiscal Q4 is derived, a page must advance). This only drives them.

    WHY IT MOVED HERE. The edge called both through PostgREST, under the `authenticator` role's 8 s
    statement timeout: 101 of 334 runs failed in the week to 2026-10-10, 90% of them between 00:00
    and 01:00 while the price night loaded the database, and `pending_ttm` stood still at ~4,100.
    `ingest_rw` allows 120 s a statement, and the loop keeps every call well inside it.
    """
    deadline = time.monotonic() + config.budget_seconds
    with postgres.connect() as conn:
        metrics = _drain(
            conn,
            call="select market.derive_security_metrics(%s)",
            page=config.metrics_page,
            backlog=UNDERIVED,
            deadline=deadline,
        )
        # AFTER THE METRICS, because a TTM is summed from them: new quarters land first.
        ttm = _drain(
            conn,
            call="select market.derive_ttm(null::uuid, %s)",
            page=config.ttm_page,
            backlog=PENDING_TTM,
            deadline=deadline,
        )
    context.log.info("metrics %s; ttm %s", metrics, ttm)
    return dg.MaterializeResult(
        metadata={
            **{f"metrics_{k}": v for k, v in metrics.items()},
            **{f"ttm_{k}": v for k, v in ttm.items()},
        }
    )
