"""The macro family's grid, and what bounds a run."""

from datetime import timedelta

import dagster as dg

#: ONE PARTITION PER CATALOGUED SERIES, the provider's grain: each is one request. A key is a row's
#: `code` in `market.macro_indicator`, added by `new_macro_series` when the row is enabled. THE NAME
#: IS STATE.
MACRO_PARTITIONS = "macro_indicator"

macro_series = dg.DynamicPartitionsDefinition(name=MACRO_PARTITIONS)

#: Series a run covers: the whole catalogue (49 rows, 38 enabled) in one run. FRED answers in a
#: fraction of a second and OECD in a few, so the run is about a minute.
SERIES_PER_RUN = 100

#: The schedule asks every series daily, so a day and a half without a materialisation is a lane
#: that stopped.
FRESH = dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36))

#: How old a series' newest point may be before `macro_series_fresh` names it, by the catalogue's
#: `frequency`. Measured 2026-11-21 on what the edge served: daily series 0-2 days old, monthly up
#: to 143 (GB unemployment, published with a lag), quarterly 143 (US real GDP). A weekend plus a US
#: holiday is 4 days, so 7 for daily; the others sit above their worst measured lag.
FRESHNESS_DAYS = {"daily": 7, "weekly": 21, "monthly": 150, "quarterly": 240, "annual": 500}
