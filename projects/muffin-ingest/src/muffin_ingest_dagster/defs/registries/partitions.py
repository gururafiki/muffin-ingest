"""Partitions definitions and constants the registries family shares."""

from datetime import timedelta

#: Generous, because the failure this catches is "the schedule stopped", not "the file is stale".
#: Tighter than the cadence would make a single missed Monday a page.
STALE_AFTER = timedelta(days=16)
