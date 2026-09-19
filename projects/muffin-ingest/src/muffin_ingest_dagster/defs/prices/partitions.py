"""Partitions definitions and constants the prices family shares."""

from datetime import date

import dagster as dg
from muffin_ingest.providers.yfinance import Yfinance

PROVIDER = Yfinance()


#: FROM GO-LIVE, NOT FROM 1996. A partition here asserts that the whole cross-section for its
#: window was collected; pre-go-live history is Lane B's, which makes no such claim.
#:
#: IN THE PAST, AND THE PARITY GATE IS WHY. A daily partition is only valid once its window has
#: CLOSED, so "go-live" as a literal today leaves the asset with no materialisable partition at all
#: — which broke every test on the day it was written, and would also make the dual-run comparison
#: impossible: that comparison needs days the OLD resource has already covered.
#:
#: The partitions between here and the schedule being switched on are honestly unmaterialised.
#: Nobody collected those days, and a grid that says so is worth more than one that hides them by
#: starting later.
trading_day = dg.DailyPartitionsDefinition(start_date="2026-09-01", timezone="UTC")


#: One key per `security_id`, kept in step with the universe by `new_securities_need_history`.
#: The name is a constant because `DynamicPartitionsDefinition.name` is typed `str | None`, and
#: reading it back to look partitions up would hand the instance an optional.
SECURITY_PARTITION = "security"


security_partitions = dg.DynamicPartitionsDefinition(name=SECURITY_PARTITION)


#: A FIXED LITERAL, NOT A COMPUTED OFFSET. Every provider call is keyed by URI in `http-cache`, so a
#: start date derived from `now()` mints a new cache entry per run while an omitted one makes a
#: single key whose answer keeps growing. 1970 predates every listing in this universe.
#: How many securities one history run may cover — A MEMORY BUDGET, MEASURED, NOT A TASTE.
#: A `single_run` backfill of 96 securities loaded 683,391 raw bars (~7,119 each) and the child
#: process was OOM-killed at 2.4 GB against a 2.5 GB container: `UPathIOManager.load_input` is
#: EAGER, so the clean stage holds every partition's raw rows AND their normalised copies at once.
#: There is no arrangement of that step that makes the peak independent of the backfill's width, so
#: the width is the bound. 25 securities is ~178k rows, ~600 MB, and leaves room for the universe's
#: deeper histories. The full 10,894-security load is therefore ~436 runs, not one.
HISTORY_PARTITIONS_PER_RUN = 25


HISTORY_START = date(1970, 1, 1)
