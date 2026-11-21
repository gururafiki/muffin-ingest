"""Asset checks of the earnings family."""

import dagster as dg

from muffin_ingest_dagster.defs.earnings.core import nasdaq_earnings
from muffin_ingest_dagster.lib.resources import Postgres

#: NASDAQ'S FORWARD WINDOW, WRITTEN IN THE LAST 36 HOURS. The schedule re-asks it daily, so a
#: forward calendar older than a day and a half is a lane that stopped, as the edge's history did
#: for ten weeks while reporting `ok`.
FORWARD_WINDOW = """
select count(*)::int
  from market.earnings_calendar
 where source_code = 'nasdaq'
   and report_date between current_date and current_date + 21
   and as_of >= now() - interval '36 hours'
"""


@dg.asset_check(asset=nasdaq_earnings, blocking=False)
def nasdaq_window_is_current(postgres: Postgres) -> dg.AssetCheckResult:
    """Has the daily schedule refreshed the dates ahead? A warning: a backfill run of old days
    passes or fails this on the schedule's account, not its own."""
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(FORWARD_WINDOW)
        fresh = int((cur.fetchone() or (0,))[0] or 0)
    return dg.AssetCheckResult(
        passed=fresh > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"forward_rows_written_in_36h": fresh},
    )
