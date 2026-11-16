"""The segments family's own grids. SEC's filer grid lives with the SEC family and is reused."""

import hashlib
from datetime import date

import dagster as dg

#: THE NAMES ARE STATE (`tests/test_definitions_snapshot.py`): partition status is keyed on them.
CNINFO_FILER_PARTITION = "cninfo_filer"

#: ONE KEY PER CNINFO ORG ID (`gssh0600900`), the company id CNINFO's search requires, which is the
#: unit it is asked about: one search per company, then that company's reports. Keys are added by
#: `new_cninfo_filers` from `market.security_filer`.
cninfo_filers = dg.DynamicPartitionsDefinition(name=CNINFO_FILER_PARTITION)

#: HOW MANY FILERS ONE RUN COVERS. A run returns every report it fetched at once and is bounded by
#: bytes (`CninfoRun.budget_bytes`), so the width only bounds the searches: 25, ~15 s of them.
CNINFO_FILERS_PER_RUN = 25

#: THE FLOOR: every filer is searched once in this many days, on a day its org id's hash chooses
#: (the edge re-walked every 180 days). Annual reports land in March and April, so a report waits at
#: most a month.
CNINFO_FLOOR_DAYS = 30


def floor_day(key: str) -> int:
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % CNINFO_FLOOR_DAYS


def on_floor(key: str, day: date) -> bool:
    return floor_day(key) == day.toordinal() % CNINFO_FLOOR_DAYS
