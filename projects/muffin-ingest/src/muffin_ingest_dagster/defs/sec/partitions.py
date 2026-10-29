"""The SEC family's grids and the constants its lanes share."""

import hashlib
from datetime import date

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


#: HOW MANY FILERS ONE RUN OF THE FILER LANE COVERS. A run returns every document it fetched at
#: once, and a filer's company facts are ~3.8 MB with submissions beside them, so 50 is ~250 MB at
#: worst: a first collection, where every filer is due. A visit that finds nothing due costs two
#: small reads of stored metadata.
FILERS_PER_RUN = 50

#: THE FLOOR: every filer is visited once in this many days, whatever it filed, on a day its CIK's
#: hash chooses. A flat TTL from one first collection would bring all ~3,420 back on one day.
FLOOR_CYCLE_DAYS = 30
#: A visit re-fetches submissions this old (a new name, ticker or address, older pages)...
SUBMISSIONS_FLOOR_DAYS = 30
#: ...and company facts this old. A filing of a `refreshes_facts` form makes them due sooner.
COMPANYFACTS_FLOOR_DAYS = 90
#: A filer SEC answered 404 for is asked again after this long. A CIK is permanent; a document for
#: it may simply not exist yet (a new registrant files its first 10-Q months after its S-1).
ABSENT_REASK_DAYS = 30

#: How many index days the due sensor reads in one tick, so a stalled week does not become one
#: tick that launches the whole grid.
DUE_DAYS_PER_TICK = 3


def cik_key(cik: int) -> str:
    """320193 -> `0000320193`: the CIK as SEC spells it in every URL, and as the partition key."""
    return f"{cik:010d}"


def floor_day(key: str) -> int:
    """The day of the `FLOOR_CYCLE_DAYS` cycle a filer is visited on, from its CIK's hash."""
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % FLOOR_CYCLE_DAYS


def on_floor(key: str, day: date) -> bool:
    """Whether `day` is the filer's floor day."""
    return floor_day(key) == day.toordinal() % FLOOR_CYCLE_DAYS
