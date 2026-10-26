"""Promotion: the universe grows by a weekly wave of share classes Yahoo can price.

  STAGE 1  `raw_promotion_probe`  one WEEKLY partition per wave.
           Asks Yahoo's chart about the next candidate lines (`market.promotion_candidate`), about
           twice as many as the wave has places, plus one control symbol. The bodies are kept
           whole, so a corrected verdict costs a re-parse and never a re-fetch.

  STAGE 2  `promotion_wave`       the same partition.
           Reads each body (`muffin_ingest.facets.promotion.verdict`), records what Yahoo said in
           `market.promotion_probe`, and hands the priced lines to `market.promote_share_classes`.

WHY YAHOO DECIDES. OpenFIGI's directory says a line exists, not that anything trades on it: the
candidates measured on 2026-10-26 included delisted Bloomberg codes, temporary buyback and new-share
lines, Czech certificates on London's international order book and Activision on SIX. Decided by
the user 2026-10-26, after a rolled-back wave on production showed what the directory alone would
have minted. Which share classes are candidates, the wave size and the pause switch are SQL, tested
in muffin-deployment (`tests/a-wave-promotes-each-share-class-once-from-its-home.sql`).

TWO RUNS, NOT ONE JOB. Pools are held for a whole run, so a job holding both stages would keep
the `sql` pool through ~25 minutes of probing and hold up the metric derivation. The schedule
launches stage 1 on the `yfinance` pool, and stage 2 follows it by `eager()` on `sql`.
"""
