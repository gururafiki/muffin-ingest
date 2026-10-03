"""Partitions definitions and constants the discovery family shares."""

from datetime import timedelta

import dagster as dg

NPORT_PARTITIONS = "nport_filing"


nport_filings = dg.DynamicPartitionsDefinition(name=NPORT_PARTITIONS)


#: One key per row of `market.directory_query` (`US.common`, `US.arca`, …) since 2026-10-04; one per
#: venue before it. The definition keeps its name: names are state.
SWEEP_PARTITIONS = "exchange_sweep"


exchange_sweeps = dg.DynamicPartitionsDefinition(name=SWEEP_PARTITIONS)


#: Filings per run — a MEMORY budget wearing a time budget's clothes. One filing is ~900 KB held as
#: Python bytes + the parquet copy + the normalised holdings at once, and this lane also holds a
#: live parse. 20 stays well inside a 2.5 GB container (the 128 MB AEP-style document would be the
#: exception that proves the width rule, and a >32 MB body is refused by the provider layer).
NPORT_PER_RUN = 20


#: When every directory query is walked again from page one: the first of the month, 03:37 UTC —
#: after the 00:00 lanes, off the hour. OpenFIGI has no as-of, so a refresh is a re-walk.
DIRECTORY_REFRESH_CRON = "37 3 1 * *"


#: A walk is stale once a monthly refresh has been missed: the longest month plus a pass (~65
#: minutes keyed for all 237 queries) and margin. Tighter, and it would fail on a lane behaving as
#: designed.
VENUE_STALE_AFTER = timedelta(days=35)


#: The tag `unfinished_sweeps` puts on the runs it requests. A run carrying it resumes a walk from
#: the cursor in its file and never starts a new one (`raw_exchange_sweep`).
SWEEP_RESUME_TAG = "muffin/sweep_resume"
