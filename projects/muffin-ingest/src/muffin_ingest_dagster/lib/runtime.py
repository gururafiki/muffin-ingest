"""How long a scheduled run may take before Dagster ends it.

`run_monitoring` in muffin-deployment's `stack/dagster/dagster.yaml` ends any run that outlives
`max_runtime_seconds` (six hours), unless the run carries `dagster/max_runtime`, which is read per
run (`check_run_timeout`, in `_daemon/monitoring/run_monitoring.py` of dagster 1.13.22). The clock
starts when the run STARTS, not when it is queued, so a run waiting on a pool is never ended for
waiting.

WHY A LIMIT AT ALL. Pools run at `granularity: run`, so a run holds its provider's slot until it
ends: venue walk `d89bb306` hung for 26.8 hours on 2026-09-21 holding the OpenFIGI pool. Each value
below comes from that job's measured run times, 2026-09-20..10-04.
"""

# PRIVATE MODULE, IMPORTED ON PURPOSE, as `priority.py` does: a literal would keep working if
# Dagster renamed the tag, and would then limit nothing, silently.
from dagster._core.storage.tags import MAX_RUNTIME_SECONDS_TAG

#: A SHORT JOB: the FX and index lanes, the heartbeat, the weekly registries, the fund directory.
#: Their p99 was 41 s at most. Five minutes is a floor rather than three times that, because the
#: monitor polls every 120 s: a tighter limit buys nothing but a run ended on a slow minute.
SHORT_RUN: dict[str, str] = {MAX_RUNTIME_SECONDS_TAG: "300"}

#: ONE NIGHTLY PRICE RUN, at most 25 securities. Its p99 was 79 s, but the slowest real run took
#: 579 s, because a security with nothing stored is fetched for its whole history. Three times the
#: p99 (237 s) would end those runs every night, and a new security would never get its history: a
#: hang turned into a policy. Thirty minutes is three times the slowest run.
PRICE_RUN: dict[str, str] = {MAX_RUNTIME_SECONDS_TAG: "1800"}
