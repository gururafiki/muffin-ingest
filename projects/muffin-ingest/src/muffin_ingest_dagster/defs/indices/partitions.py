"""Partitions definitions and constants the indices family shares."""

import dagster as dg

index_day = dg.DailyPartitionsDefinition(start_date="2026-09-01", timezone="UTC")
