"""Partitions definitions and constants the discovery family shares."""

from datetime import timedelta

import dagster as dg

NPORT_PARTITIONS = "nport_filing"


nport_filings = dg.DynamicPartitionsDefinition(name=NPORT_PARTITIONS)


SWEEP_PARTITIONS = "exchange_sweep"


exchange_sweeps = dg.DynamicPartitionsDefinition(name=SWEEP_PARTITIONS)


#: Filings per run — a MEMORY budget wearing a time budget's clothes. One filing is ~900 KB held as
#: Python bytes + the parquet copy + the normalised holdings at once, and this lane also holds a
#: live parse. 20 stays well inside a 2.5 GB container (the 128 MB AEP-style document would be the
#: exception that proves the width rule, and a >32 MB body is refused by the provider layer).
NPORT_PER_RUN = 20


#: A swept venue's directory stays current for a month — venues change slowly, and a gate tighter
#: than the monthly `new_exchange_sweeps` sensor would fire on a lane that is behaving as designed.
VENUE_STALE_AFTER = timedelta(days=30)
