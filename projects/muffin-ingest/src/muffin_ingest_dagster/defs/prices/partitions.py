"""Partitions definitions and constants the prices family shares."""

from datetime import date

import dagster as dg
from muffin_ingest.providers.yfinance import Yfinance

PROVIDER = Yfinance()


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


#: SECONDS BETWEEN THE STARTS OF TWO CHART REQUESTS — the price lane's pace since it called Yahoo
#: directly (2026-10-10). Not faster than the openbb lane it replaced, which is the decision taken
#: (spec decision 5): the 2026-10-06 night spent ~3,500 s on 2,500 securities, ~1.4 s each, and
#: openbb asked Yahoo at least once per security, so one request per 1.4 s is that rate or slower.
#: Going faster is how this provider's allowance was lost on 2026-09-19.
#:
#: IF THE `yfinance` POOL IS EVER WIDENED PAST 1, THIS SILENTLY BECOMES N TIMES LOOSER.
CHART_SECONDS_BETWEEN_REQUESTS = 1.4
