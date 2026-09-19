"""Index returns — the last of the price family, and the one with two acquisition shapes.

  `raw_index_bars`          DAILY partitions. Proxy-ETF bars for the 45 country and 17 group
                            scopes, in one batched call.
  `raw_sector_performance`  DAILY partitions. The 11 sectors from finviz, which publishes NUMBERS
                            rather than a series — there is no ETF behind a muffin sector.
  `index_return`            DAILY. Both, into one table.

TWO RAW ASSETS FEEDING ONE CORE ASSET IS THE POINT, not a compromise. The scopes differ in where
their numbers come from and in nothing else, so the difference belongs at the boundary where it is
real. Folding finviz into the bar lane would mean inventing a series it does not publish; splitting
the core table would mean a reader has to know which kind of scope it is holding before it can ask
for a return.

AND THE RETURN RULES ARE THE SAME RULES. A country's 3-month return is computed by `derive/returns`,
exactly as a security's is, so a country page and a stock page cannot disagree about what the phrase
means. What sectors get instead is finviz's own figure — stated, and never recomputed to look alike.
"""
