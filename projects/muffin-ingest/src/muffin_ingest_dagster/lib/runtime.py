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

#: ONE COMPANY RUN, at most 100 companies. Each costs at most three requests at 1.4 s, so a full
#: run is ~7 minutes and its own budget stops asking at 15. Thirty minutes is twice that budget,
#: for stage 2 and a slow provider, and well short of the six-hour default that would let a hung
#: run hold the `yfinance` pool through the price night.
COMPANY_RUN: dict[str, str] = {MAX_RUNTIME_SECONDS_TAG: "1800"}

#: ONE PROMOTION PROBE: ~1,000 chart requests at 1.4 s is ~24 minutes, and its own budget stops
#: asking at 45. An hour is that budget and a slow provider, and short of the six-hour default that
#: would let a hung probe hold the `yfinance` pool into the price night.
PROMOTION_RUN: dict[str, str] = {MAX_RUNTIME_SECONDS_TAG: "3600"}

#: ONE SEC RUN: a day of the index and its Form 4s (~160-370 a day for tracked issuers, measured
#: 2026-10-19..23, at 5 requests a second: about a minute), or a chunk of 50 filers (company facts,
#: submissions and any older page not held, bounded by its own 15-minute budget). Thirty minutes is
#: twice that budget, for stage 2 and a slow SEC, and short of the six-hour default that would let a
#: hung run hold the `sec` pool into the next day's index.
SEC_RUN: dict[str, str] = {MAX_RUNTIME_SECONDS_TAG: "1800"}

#: ONE INSIDER DATA SET: one 8-14 MB zip (measured 2006q1-2026q1), read in 1.1-1.5 s, then a
#: quarter's ~80-125 thousand rows written. Fifteen minutes.
SEC_DATASET_RUN: dict[str, str] = {MAX_RUNTIME_SECONDS_TAG: "900"}
