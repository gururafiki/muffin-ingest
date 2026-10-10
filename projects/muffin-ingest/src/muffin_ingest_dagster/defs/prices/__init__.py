"""Daily bars, collected PER SECURITY — and since 2026-09-19 that is the only lane that runs.

  `raw_price_chart`    one partition PER SECURITY, swept nightly by `nightly_prices` since
          2026-10-10: Yahoo's chart response for the security, stored whole, so each bar is
          labelled with the quote currency the provider states (`facets.price_chart`). The subject
          IS the slice, so Dagster's grid answers "which securities are current?" natively, and a
          night the provider refuses simply leaves partitions unmaterialised for the next run to
          collect. A first visit loads the whole history; later ones extend it by a week.

  `raw_price_history`  the same grid through openbb, the lane until 2026-10-10. Defined and
          unscheduled as the rollback; its files stay on disk until the drop date in umbrella
          docs/deferred/2026-10-10-the-openbb-price-raw-is-the-rollback.md.

  `raw_price_bars`     DAILY partitions, DELETED 2026-10-04 after two weeks of clean sweeps. It
          was kept as the rollback from the cutover; its offline replay tests now drive the
          security lane, and its Parquet stays on disk as the backup.

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
