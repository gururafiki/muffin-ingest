"""The segments family's own grids. SEC's filer grid lives with the SEC family and is reused."""

import hashlib
from datetime import date

import dagster as dg

#: THE NAMES ARE STATE (`tests/test_definitions_snapshot.py`): partition status is keyed on them.
CNINFO_FILER_PARTITION = "cninfo_filer"
DART_FILER_PARTITION = "dart_filer"
NSE_FILER_PARTITION = "nse_filer"

#: ONE KEY PER CNINFO ORG ID (`gssh0600900`), the company id CNINFO's search requires, which is the
#: unit it is asked about: one search per company, then that company's reports. Keys are added by
#: `new_cninfo_filers` from `market.security_filer`.
cninfo_filers = dg.DynamicPartitionsDefinition(name=CNINFO_FILER_PARTITION)

#: HOW MANY FILERS ONE RUN COVERS. A run returns every report it fetched at once and is bounded by
#: bytes (`CninfoRun.budget_bytes`), so the width only bounds the searches: 25, ~15 s of them.
CNINFO_FILERS_PER_RUN = 25


#: ONE KEY PER DART CORP CODE (`00126380`), the filer id DART's filing list requires: one list per
#: company, then that company's archives. Keys are added by `new_dart_filers` from
#: `market.security_filer`.
dart_filers = dg.DynamicPartitionsDefinition(name=DART_FILER_PARTITION)

#: HOW MANY FILERS ONE RUN COVERS. DART is slow from outside Korea: a list answers in ~3 s, a cold
#: archive in 70-100 s (measured 2026-11-16, 99.0 s for 1.1 MB), so a run is bounded by its time
#: budget long before ten filers' lists are a cost.
DART_FILERS_PER_RUN = 10


#: ONE KEY PER NSE SYMBOL (`RELIANCE`), which NSE's results list requires: one list per company,
#: then that company's instances. `security_nse_filer` maps the symbol by ISIN, and keys are added
#: by `new_nse_filers` from `market.security_filer`.
nse_filers = dg.DynamicPartitionsDefinition(name=NSE_FILER_PARTITION)

#: HOW MANY FILERS ONE RUN COVERS. A results list answers in 0.2-0.3 s and an instance is 46-109 KB
#: from a cache that keeps it a year (measured 2026-11-16), so 25 filers fit well inside a run.
NSE_FILERS_PER_RUN = 25

#: THE FLOOR, for CNINFO, DART and NSE alike: every filer is asked once in this many days, on a day
#: its key's hash chooses (the edge re-walked CNINFO every 180 days, DART and NSE every 90). Annual
#: reports land between March and May, so a report waits at most a month.
FLOOR_DAYS = 30


def floor_day(key: str) -> int:
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % FLOOR_DAYS


def on_floor(key: str, day: date) -> bool:
    return floor_day(key) == day.toordinal() % FLOOR_DAYS
