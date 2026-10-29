"""The SEC family's grids and the constants its lanes share."""

import dagster as dg

#: THE NAMES ARE STATE (`tests/test_definitions_snapshot.py`): partition status is keyed on them.
SEC_FILER_PARTITION = "sec_filer"

#: ONE KEY PER CIK (`"0000320193"`), not per security: a CIK is one filer, and GOOG/GOOGL or
#: BRK-A/BRK-B share theirs, so a per-security grid would fetch ~340 filers twice. Stage 2 fans each
#: CIK out to every equity mapped to it at parse time. Keys are added by `new_sec_filers`.
sec_filers = dg.DynamicPartitionsDefinition(name=SEC_FILER_PARTITION)

#: ONE KEY PER DAY OF SEC'S DAILY INDEX, from the first day no insider data set covers. Measured
#: 2026-10-26: 2026q2's data set was still unpublished (SEC's page listed through 2026q1), so the
#: Form 4 lane covers every day after the newest data set held, and its grid starts on 2026-04-01.
sec_days = dg.DailyPartitionsDefinition(start_date="2026-04-01", timezone="UTC")

#: ONE KEY PER QUARTER OF SEC'S INSIDER TRANSACTIONS DATA SETS, every quarter since 2006
#: (decision 10). Key `2026-07` is 2026q3. Only completed quarters are partitions.
insider_quarters = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1,4,7,10 *", start="2006-01", fmt="%Y-%m", timezone="UTC"
)

#: How far back the index lane looks for a day it has not collected. Ten days covers a long weekend
#: and a week of a stopped schedule; anything older is an operator backfill.
INDEX_LOOKBACK_DAYS = 10


def quarter_of(key: str) -> tuple[int, int]:
    """`2026-07` -> (2026, 3): an `insider_quarters` key as the year and quarter it covers."""
    year, month = key.split("-")
    return int(year), (int(month) - 1) // 3 + 1


def quarter_key(year: int, quarter: int) -> str:
    """(2026, 3) -> `2026-07`."""
    return f"{year:04d}-{3 * quarter - 2:02d}"
