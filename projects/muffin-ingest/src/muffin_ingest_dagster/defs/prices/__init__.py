"""Daily bars, collected PER SECURITY — and since 2026-09-19 that is the only lane that runs.

  `raw_price_history`  one partition PER SECURITY, swept nightly by `nightly_prices`.
          The subject IS the slice, so Dagster's grid answers "which securities are current?"
          natively, and a night the provider refuses simply leaves partitions unmaterialised for
          the next run to collect. Each run extends a security from its own newest stored bar —
          see `merge_on` on the asset — so a partition accumulates rather than being replaced.

  `raw_price_bars`     DAILY partitions. RETIRED as a collector, KEPT as the rollback.
          `daily_prices_schedule` ships STOPPED; the assets, their checks and the offline replay
          suite are all still here, so restoring the old behaviour is starting one schedule and
          stopping the other. It goes once the sweep has proven itself live.

WHY THE DAY LANE WENT, in one measurement. `openbb_yfinance` calls `yf.download(...)` with
`threads=False`, which loops per ticker and issues `/v8/finance/chart/<ticker>` for each — counted
on the wire, FOUR symbols produced SIX requests, a suffixed foreign symbol costing three. So a day
partition was ONE partition standing for ~12,000 independent requests: all-or-nothing, and a
refusal mid-way materialised a completeness claim that was false. A night reported as 602 calls
really asked the vendor ~12,021 times, and the allowance the next night was ~2,740.

WHAT A BATCH COSTS, because it sizes everything: batching collapses OUR call count, never the
vendor's. The pool bounds concurrency, the limiter bounds symbols per second, and neither is the
same thing. A wider WINDOW costs bytes rather than requests, which is why the sweep extends from a
watermark for memory reasons rather than to save calls.

"DID WE COLLECT TUESDAY?" IS NO LONGER A PARTITION. A rotation leaves any single day legitimately
partial, so the claim that still means something is per security — see
`no_security_is_far_behind_the_sweep`.
"""
