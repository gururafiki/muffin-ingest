"""When a rung asks about a subject.

TWO RULES, AND THE GRID IS WHAT MAKES THEM ENOUGH. A materialized partition means "we asked and
stored whatever came back, including nothing", so:

* `missing()` — we have never asked this subject. Covers a newly seeded security with no further
  machinery, and it covers one seeded before this condition existed, which `on_missing()` does NOT:
  measured 2026-09-20 on 1.13.22, `on_missing()` requested 0 of 2 partitions that were already in
  the grid when the first tick ran, and 2 of 2 added between ticks. A rule that silently does
  nothing for everything already there is the wrong rule for a lane being switched on.
* `ReAskAfter` — we asked, the provider had nothing, and that was long enough ago to ask again.

`~in_progress()` stops a partition being requested twice while its run is live.

WHY A CUSTOM CONDITION RATHER THAN A SENSOR EMITTING RUN REQUESTS. A sensor has resources and this
does not, which would have made it the tidier place — but a sensor can only request a partition or
a contiguous RANGE of them, and a stale-miss set is scattered through a sorted key list, so it
becomes one run per subject. That turns OpenFIGI's ten-jobs-per-request batching into one job per
request: a tenfold increase in provider spend to schedule the same work. The condition hands the
daemon a subset instead, and `BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN)` re-batches it.

DRIVEN BEFORE IT WAS WRITTEN, because subclassing this API is not documented as supported: a
custom `AutomationCondition` was built against 1.13.22, attached to a dynamically-partitioned
asset, and evaluated — `evaluate()` is called, `context.candidate_subset` is an `EntitySubset` with
`compute_intersection_with_partition_keys`, and exactly the intended partition was requested. The
condition's identity is its CLASS NAME (`get_node_unique_id` hashes `self.name`), so renaming this
class is a state change like any other rename.
"""

from typing import Any

import dagster as dg
from muffin_ingest.facets import symbology as sym

from muffin_ingest_dagster.defs.symbology.partitions import REASK_AFTER_DAYS, REASK_CRON
from muffin_ingest_dagster.lib.resources import Postgres


class ReAskAfter(dg.AutomationCondition):  # type: ignore[type-arg]
    """Request the subjects whose recorded answer was `miss` and is older than the window."""

    @property
    def description(self) -> str:
        return f"a probe said miss more than {REASK_AFTER_DAYS} days ago"

    def evaluate(self, context: Any) -> Any:
        candidates = context.candidate_subset
        # NOTHING TO ASK ABOUT IS ANSWERED WITHOUT ASKING THE DATABASE. The cron gate upstream
        # leaves this empty on every tick but one a day, and an AND evaluates its later operands
        # only over what the earlier ones left true — so this branch is the common one.
        if candidates.is_empty:
            return dg.AutomationResult(context, true_subset=context.get_empty_subset())
        with Postgres().connect() as conn:
            due = sym.stale_misses(conn, older_than_days=REASK_AFTER_DAYS)
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
).with_label("never asked, or a stale miss")
