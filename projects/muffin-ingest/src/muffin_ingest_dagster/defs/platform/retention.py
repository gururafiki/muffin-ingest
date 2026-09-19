"""Dagster OSS does not prune its own event log, and nothing else on this node will either.

`dagster.yaml`'s `retention:` block covers schedule, sensor and auto-materialize TICKS only —
read from `dagster/_core/instance/config.py`, whose schema has exactly those three keys. Run
records and the event-log rows behind them grow without bound, on a database that shares a
1.5 GB-limited container with the `market` schema this pipeline already fills at ~25 million price
rows.

Deleting a RUN removes its event-log rows with it, so one nightly job is the whole mechanism.
"""

from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster import OpExecutionContext

# NO `from __future__ import annotations` IN THIS PACKAGE, and the error does not say why.
# It stringifies annotations, and Dagster compares the `context` parameter against the real class —
# so `dg.OpExecutionContext` arrives as the text "dg.OpExecutionContext", which resolves to nothing,
# and validation fails with "Cannot annotate `context` parameter with type dg.OpExecutionContext"
# while naming a type that is plainly correct. `definitions.py` already carried this scar; a second
# module in the same package reintroduced it within a day, which is why it is written down here too.

# NINETY DAYS, chosen against what reads it rather than by feel: Grafana's pipeline panels look
# back 30 days, and `refresh_run` — the thing Dagster's run history replaces — has held about 13
# days in practice. Ninety leaves a quarter of history for a post-mortem and still bounds the table.
KEEP_DAYS = 90

# A BOUND ON THE DELETE, NOT ONLY ON THE AGE. The first night after a long backlog could otherwise
# delete tens of thousands of runs in one op, on the same Postgres the app reads through a 3-second
# anon timeout. It runs nightly, so a cap simply means the backlog drains over a few nights.
MAX_PER_RUN = 2000


@dg.op(
    description="Delete run records older than the retention window, and the event log with them."
)
def prune_run_history(context: OpExecutionContext) -> None:
    cutoff = datetime.now(UTC) - timedelta(days=KEEP_DAYS)
    instance = context.instance

    records = instance.get_run_records(
        filters=dg.RunsFilter(created_before=cutoff),
        limit=MAX_PER_RUN,
        ascending=True,  # oldest first, so a capped pass always makes progress at the tail
    )

    deleted = 0
    for record in records:
        # THE OP'S OWN RUN IS INSIDE THE WINDOW IT IS DELETING FROM, and would not be selected here
        # because it is far newer than the cutoff — but a misconfigured KEEP_DAYS of 0 would take
        # it mid-flight and the failure would be baffling. Cheap to exclude explicitly.
        if record.dagster_run.run_id == context.run_id:
            continue
        instance.delete_run(record.dagster_run.run_id)
        deleted += 1

    context.log.info("pruned %s run records created before %s", deleted, cutoff.isoformat())
    # `hit_cap` is the signal that the window is not yet drained: a nightly op reporting it for
    # several nights running means the cap is too low for the volume, not that retention is broken.
    context.add_output_metadata(
        {"deleted": deleted, "cutoff": cutoff.isoformat(), "hit_cap": deleted >= MAX_PER_RUN}
    )


@dg.job(description="Nightly: bound Dagster's own storage.")
def prune_dagster_storage() -> None:
    prune_run_history()


nightly_pruning = dg.ScheduleDefinition(
    name="prune_dagster_storage",
    job=prune_dagster_storage,
    # 03:40 UTC: after the 04:35 functions restart would be worse, and the ingestion rotation is
    # quietest in this hour. Not on the hour, so it does not land with everything else.
    cron_schedule="40 3 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
