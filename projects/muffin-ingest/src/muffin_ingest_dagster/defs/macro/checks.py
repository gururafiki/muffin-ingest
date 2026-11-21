"""The macro family's checks."""

import dagster as dg

from muffin_ingest_dagster.defs.macro.core import macro_observation
from muffin_ingest_dagster.defs.macro.partitions import FRESHNESS_DAYS
from muffin_ingest_dagster.lib.resources import Postgres

#: Each enabled series, its frequency and how old its newest point is. A series with no point at
#: all reads as null and is named too.
AGES = """
select i.code, i.frequency, current_date - max(o.as_of) as age_days
  from market.macro_indicator i
  left join market.macro_observation o on o.indicator_code = i.code
 where i.enabled
 group by i.code, i.frequency
 order by i.code
"""


@dg.asset_check(asset=macro_observation, blocking=False)
def macro_series_fresh(postgres: Postgres) -> dg.AssetCheckResult:
    """Every enabled series has a point inside its frequency's window (`FRESHNESS_DAYS`).

    WARN, never ERROR: a provider publishes with its own lag, and a series it stopped publishing is
    a catalogue correction for a person, not a failed lane. It names which series, so the
    correction is a row in Studio.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(AGES)
        rows = cur.fetchall()
    stale: list[str] = []
    for code, frequency, age in rows:
        limit = FRESHNESS_DAYS.get(str(frequency), FRESHNESS_DAYS["monthly"])
        if age is None or int(age) > limit:
            stale.append(f"{code} ({'no point' if age is None else f'{age} days'})")
    return dg.AssetCheckResult(
        passed=not stale,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "enabled_series": len(rows),
            "stale": len(stale),
            "which": ", ".join(stale) or "none",
        },
    )
