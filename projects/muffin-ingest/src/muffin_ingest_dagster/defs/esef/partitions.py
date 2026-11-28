"""Europe's filer grid."""

import dagster as dg

#: THE NAME IS STATE (`tests/test_definitions_snapshot.py`): partition status is keyed on it.
ESEF_FILER_PARTITION = "esef_filer"

#: ONE KEY PER LEI (`529900JI1GG6F7RKVI53`), the id the index lists a filer's reports by: one list
#: per company, then that company's annual reports. Keys are added by `new_esef_filers` from
#: `market.security_filer`.
esef_filers = dg.DynamicPartitionsDefinition(name=ESEF_FILER_PARTITION)

#: HOW MANY FILERS ONE RUN COVERS. The index allows a request every eight seconds, and a filer
#: first visited holds ~5 annual reports (ESEF began with FY2020): six requests, under a minute of
#: pacing, plus 2-27 MB a report. Five filers are a few minutes, inside the run's own budget.
ESEF_FILERS_PER_RUN = 5
