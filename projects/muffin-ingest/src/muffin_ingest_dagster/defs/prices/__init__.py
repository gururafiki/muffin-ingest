"""Daily bars, in two lanes — and the reason there are two is the whole design.

  LANE A  `raw_price_bars`     DAILY partitions, from go-live.
          Materialising a partition CLAIMS that the whole cross-section for that window was
          collected. That claim is the only way to answer "did the collection run on Tuesday?",
          which the data itself cannot answer: a security with no bar looks identical whether its
          market was shut, its symbol is dead, or nothing ran at all.

  LANE B  `raw_price_history`  one partition PER SECURITY.
          Here the subject IS the slice, so Dagster's partition grid answers "which securities are
          loaded" natively. It cannot be date-partitioned without a newly promoted security costing
          a re-fetch of the universe, and it makes no completeness claim about any date — which is
          exactly why it is a separate asset rather than a backfill of Lane A.

WHAT A BATCH COSTS, because it sizes everything: `openbb_yfinance` calls
`yf.download(tickers="A,B,C", threads=False)`, so a batched call is ONE YAHOO REQUEST PER SYMBOL,
serially. Batching collapses our call count (545 rather than 10,894 for a full pass), not the
vendor's. The pool bounds concurrency, the limiter bounds symbols per second, and neither is the
same thing.
"""
