"""Jobs, schedules and sensors of the indices family."""

import dagster as dg

from muffin_ingest_dagster.defs.indices.core import index_return
from muffin_ingest_dagster.defs.indices.raw import raw_index_bars, raw_sector_performance

#: RUNNING, AND THE COMPARISON THAT GATED IT WAS ADJUDICATED AGAINST THE PROVIDER RATHER THAN
#: AGAINST `market.performance`.
#:
#: Compared naively the two tables disagree enormously — 453 of 626 (scope, period) pairs by more
#: than half a point, `country:KR 1d` at **-4.1933 against +3.2498**, a sign flip. That comparison
#: measures nothing: `index_return` is anchored on a TRADE DATE and `performance.as_of` is the
#: RUN's timestamp, so with this schedule stopped the new table sat at 2026-09-10 while the old
#: resource had run on 09-12. Two different days, and `performance` upserts in place, so there is
#: no history to pin them to a common one.
#:
#: Yahoo settles it. EWY closed 190.78 / 182.78 / 188.72 on 09-09 / 09-10 / 09-11, so the 1d return
#: ending 09-10 is **-4.1933%** and the one ending 09-11 is **+3.2498%** — each side matches the
#: provider EXACTLY, to four decimal places, for its own anchor. Both are right; only the anchors
#: differed. The match also confirms the live-quote drop: the new value is the completed bar, never
#: `regularMarketTime`.
daily_indices = dg.build_schedule_from_partitioned_job(
    dg.define_asset_job(
        "daily_indices",
        selection=dg.AssetSelection.assets(raw_index_bars, raw_sector_performance, index_return),
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
