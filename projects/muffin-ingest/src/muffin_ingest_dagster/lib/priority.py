"""Which queued run the coordinator starts first.

`QueuedRunCoordinator` sorts every queued run by the `dagster/priority` tag, highest first, before
it checks pools (`_priority_sort`, in `_daemon/run_coordinator/queued_run_coordinator_daemon.py`
of dagster 1.13.22). A run with no tag counts as 0. Priority does not bypass a pool: a run whose
pool is full stays queued whatever its priority, and it is simply first in line when a slot frees.
"""

# PRIVATE MODULE, IMPORTED ON PURPOSE. The literal "dagster/priority" would keep working if Dagster
# renamed the tag, and it would then prioritise nothing, silently. An import fails loudly instead.
from dagster._core.storage.tags import PRIORITY_TAG

#: A SHORT LANE MUST NOT QUEUE BEHIND A LONG ONE. At 00:00 UTC the daily FX and index lanes are
#: queued beside ~100 runs of `nightly_prices`, and every one of them needs the `sql` pool, which
#: is held at limit 1 for a whole run. Without a priority the coordinator dequeues in creation
#: order, and that order is an accident of which schedule the daemon ticked first. Measured:
#: `daily_indices` waited 6,185 s on 2026-09-23 and `daily_fx` 6,069 s on 2026-09-24, while on
#: 2026-09-25 neither waited. A lane that runs for ~20 s should wait for at most the one price run
#: holding the slot, never the whole sweep. HIGHEST OF ALL, so nothing outranks it.
SHORT_LANE: dict[str, str] = {PRIORITY_TAG: "2"}

#: THE COMPANY LANE MUST NOT QUEUE BEHIND THE SYMBOLOGY RE-ASK (decided by the user 2026-11-04).
#: Since 2026-10-27 the 03:00 re-ask launches 170-205 one-subject runs of ~30 s each, all needing
#: the `sql` pool, and `yahoo_companies` (03:30) waited behind every one: it ran ~04:50-06:19
#: where it used to end ~04:45, past the run windows `muffin-deploy` documents. Automation runs
#: carry no tag, which counts as 0, so 1 lets the lane in at the next free slot. Below the short
#: lanes, which must outrank everything.
COMPANY_LANE: dict[str, str] = {PRIORITY_TAG: "1"}
