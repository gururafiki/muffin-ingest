"""EDINET's grids: one per day EDINET lists documents for, one per filer."""

import dagster as dg

#: THE NAMES ARE STATE (`tests/test_definitions_snapshot.py`): partition status is keyed on them.
EDINET_DAY_PARTITION = "edinet_day"
EDINET_FILER_PARTITION = "edinet_filer"

#: ONE KEY PER DAY IN TOKYO, which is how EDINET is asked (`documents.json?date=`): it has no
#: per-company listing, so a day's list is how a filing is discovered. THE LIST KEEPS A ROLLING
#: TEN YEARS (bisected 2026-11-27 to between 2016-11-24 and 2016-11-28), so the grid starts where
#: the window did and its oldest days answer 404 as the window moves on. The last key is the
#: newest whole day in Tokyo.
edinet_days = dg.DailyPartitionsDefinition(
    start_date="2016-11-28", timezone="Asia/Tokyo", fmt="%Y-%m-%d"
)

#: HOW MANY DAYS ONE RUN COVERS. A list is ~0.7 MB in 1-2.5 s and asked a second apart, so thirty
#: days is under two minutes, and ten years is ~120 runs.
DAYS_PER_RUN = 30

#: ONE KEY PER EDINET CODE (`E02144`), the filer an archive belongs to. Keys are added by
#: `new_edinet_filers` from `market.security_filer`.
edinet_filers = dg.DynamicPartitionsDefinition(name=EDINET_FILER_PARTITION)

#: HOW MANY FILERS ONE RUN COVERS. A filer's history is up to ten annual reports of 0.6-2.6 MB at
#: 3.6-8 s each, so ten filers' histories are bounded by the run's time budget, and a day's few
#: new reports are seconds.
EDINET_FILERS_PER_RUN = 10
