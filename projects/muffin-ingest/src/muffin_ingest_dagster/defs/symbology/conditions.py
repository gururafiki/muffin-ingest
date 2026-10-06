"""When a rung asks about a subject.

TWO RULES, AND THE GRID IS WHAT MAKES THEM ENOUGH. A materialized partition means "we asked and
stored whatever came back, including nothing", so:

* `missing()` — we have never asked this subject. Covers a newly seeded security with no further
  machinery, and it covers one seeded before this condition existed, which `on_missing()` does NOT:
  measured 2026-09-20 on 1.13.22, `on_missing()` requested 0 of 2 partitions that were already in
  the grid when the first tick ran, and 2 of 2 added between ticks. A rule that silently does
  nothing for everything already there is the wrong rule for a lane being switched on.
* `ReAskAfter` — we asked, the provider had nothing, that was long enough ago to ask again, and
  today is this subject's own day of the re-ask cycle. OR the symbol a security holds was rejected
  by the price lane after OpenFIGI last answered for it (Stage 3b): the death is the evidence the
  held symbol is wrong, and the ladder is what can name another. The class keeps its name for the
  reason below; only what it selects changed.

THE STALE ARM IS SPREAD AND THE DEAD ARM IS NOT. A stale miss is spread over `REASK_SPREAD_DAYS`
because misses recorded by one drain all turn stale on the same morning, and scattered subjects are
one run each (`facets.symbology.due_on` has the measurement). A dead symbol is not spread: deaths
already arrive at the price sweep's pace (each security is priced about once in five nights), and a
dead symbol costs a price bar every day its repair waits, which a slot up to 29 days away would
multiply for nothing.

`~in_progress()` stops a partition being requested twice while its run is live.

WHY A CUSTOM CONDITION RATHER THAN A SENSOR EMITTING RUN REQUESTS. A sensor can request only a
partition or a contiguous RANGE of them, and a stale-miss set is scattered through the grid, so it
would be one run per subject. This paragraph used to say the condition avoids that because
`BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN)` re-batches the subset it hands the daemon. IT DOES
NOT, measured 2026-10-06: the daemon splits a requested subset into contiguous key ranges
(`_build_run_requests_with_backfill_policy` calls `get_partition_key_ranges`), so the first
re-ask's 255 scattered subjects became 254 runs either way. What bounds that cost is the spread
above. The condition stays because a sensor would be no better, and it shares the rungs'
`missing()` and `in_progress()` gates.

DRIVEN BEFORE IT WAS WRITTEN, because subclassing this API is not documented as supported: a
custom `AutomationCondition` was built against 1.13.22, attached to a dynamically-partitioned
asset, and evaluated — `evaluate()` is called, `context.candidate_subset` is an `EntitySubset` with
`compute_intersection_with_partition_keys`, and exactly the intended partition was requested. The
condition's identity is its CLASS NAME (`get_node_unique_id` hashes `self.name`), so renaming this
class is a state change like any other rename.
"""

from datetime import UTC
from typing import Any

import dagster as dg
from muffin_ingest.facets import symbology as sym

from muffin_ingest_dagster.defs.symbology.partitions import (
    REASK_AFTER_DAYS,
    REASK_CRON,
    REASK_SPREAD_DAYS,
)
from muffin_ingest_dagster.lib.resources import Postgres


class ReAskAfter(dg.AutomationCondition):  # type: ignore[type-arg]
    """Request the subjects whose recorded answer was `miss`, is older than the window and is due
    today, and the subjects holding a symbol the price lane has rejected since OpenFIGI last
    answered."""

    @property
    def description(self) -> str:
        return (
            f"a probe said miss more than {REASK_AFTER_DAYS} days ago and today is the subject's "
            f"day of a {REASK_SPREAD_DAYS}-day cycle, "
            "or the held symbol died since OpenFIGI last answered"
        )

    def evaluate(self, context: Any) -> Any:
        candidates = context.candidate_subset
        # NOTHING TO ASK ABOUT IS ANSWERED WITHOUT ASKING THE DATABASE. The cron gate upstream
        # leaves this empty on every tick but one a day, and an AND evaluates its later operands
        # only over what the earlier ones left true — so this branch is the common one.
        if candidates.is_empty:
            return dg.AutomationResult(context, true_subset=context.get_empty_subset())
        # THE TICK'S DATE, NOT THE MACHINE'S. Every evaluation on one tick shares this instant, and
        # a test can set it; `date.today()` would be the container's local date and untestable.
        at = context.evaluation_time
        today = (at.replace(tzinfo=UTC) if at.tzinfo is None else at.astimezone(UTC)).date()
        with Postgres().connect() as conn:
            stale = sym.stale_misses(conn, older_than_days=REASK_AFTER_DAYS)
            due = sym.due_on(stale, today, cycle_days=REASK_SPREAD_DAYS) | sym.dead_unasked(conn)
        return dg.AutomationResult(
            context, true_subset=candidates.compute_intersection_with_partition_keys(due)
        )


#: The rule every rung carries. Written once because three rungs sharing a grid must agree about
#: when a subject is asked, or one of them re-materialises a partition the others have not.
SYMBOLOGY_AUTOMATION = (
    (
        dg.AutomationCondition.missing()
        | (dg.AutomationCondition.cron_tick_passed(REASK_CRON) & ReAskAfter())
    )
    & ~dg.AutomationCondition.in_progress()
    # LABELLED SO THE DEFINITIONS SNAPSHOT READS AS SOMETHING. Without it three assets record
    # `unlabelled (<hash>)`, and a diff nobody can read is a diff nobody checks.
).with_label("never asked, a stale miss, or a dead symbol")
