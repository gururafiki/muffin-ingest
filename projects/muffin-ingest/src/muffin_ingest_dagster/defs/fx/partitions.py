"""Partitions definitions and constants the fx family shares."""

import dagster as dg

#: Same start as the price lane: a partition older than go-live claims a collection that never ran.
fx_day = dg.DailyPartitionsDefinition(start_date="2026-09-01", timezone="UTC")


#: One key per currency code.
CURRENCY_PARTITION = "currency"


currency_partitions = dg.DynamicPartitionsDefinition(name=CURRENCY_PARTITION)


#: How many currencies one history run may cover. Ten years of WEEKLY points is ~524 rows each, so
#: the whole universe is ~22,000 rows — three orders of magnitude under the price lane's 683,391,
#: and comfortably one run. The bound exists so the rule is the same rule, not because 43 needs it.
HISTORY_PARTITIONS_PER_RUN = 43
