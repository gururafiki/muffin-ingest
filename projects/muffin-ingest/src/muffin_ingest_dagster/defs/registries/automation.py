"""Jobs, schedules and sensors of the registries family."""

import dagster as dg

from muffin_ingest_dagster.defs.registries.core import security_cik, security_nse_filer
from muffin_ingest_dagster.defs.registries.raw import raw_nse_equity_list, raw_sec_cik_map

#: Both files change slowly — SEC's is byte-identical most days, NSE's moves when a company lists.
#: Weekly is far more often than either needs and still nothing next to the old rotation, which
#: asked for them every ten minutes and got a `skipped` for its trouble.
WEEKLY = "0 4 * * 1"


#: ONE JOB FOR BOTH, because they share a cadence and nothing else. Kept out of the price lane's
#: schedules so a registry refresh can never delay a trading-day collection.
weekly_registries = dg.ScheduleDefinition(
    name="weekly_registries",
    target=dg.AssetSelection.assets(
        raw_sec_cik_map, security_cik, raw_nse_equity_list, security_nse_filer
    ),
    cron_schedule=WEEKLY,
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
