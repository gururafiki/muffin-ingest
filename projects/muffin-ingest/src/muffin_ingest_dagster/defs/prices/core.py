"""Stage 2 of the prices family: raw parsed and normalised into core rows."""

from datetime import date, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import prices

from muffin_ingest_dagster.defs.prices.partitions import (
    HISTORY_PARTITIONS_PER_RUN,
    HISTORY_START,
    PROVIDER,
    security_partitions,
    trading_day,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

_loaded_rows = partitioned.loaded_rows


@dg.asset(
    partitions_def=trading_day,
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="prices",
    kinds={"postgres"},
    metadata={"table": "market.price_bar", "conflict": ["security_id", "trade_date"]},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="Raw bars as typed core rows. Never calls a provider, so a fix here is free.",
)
def price_bar(
    context: AssetExecutionContext, postgres: Postgres, raw_price_bars: Any
) -> list[dict[str, Any]]:
    with postgres.connect() as conn:
        currencies = prices.currency_by_security(conn)

    # EACH PARTITION PUBLISHES ITS OWN DAY, FROM ITS OWN FILE — applied here, against files, so a
    # correction costs a re-parse rather than a re-fetch. PER FILE rather than over the run's
    # window, because raw keeps the bars the provider sent outside the range it was asked (see
    # `_collect`): a stray sits in one partition's file while the real bar sits in its own, and
    # windowing the flattened run would let the writer's last-wins dedupe choose between them by
    # file order. Per file, a stray is never published at all.
    parts = partitioned.rows_per_partition(context, raw_price_bars)
    raw = [row for part in parts.values() for row in part]
    rows: list[dict[str, Any]] = []
    for key, part in parts.items():
        day = trading_day.time_window_for_partition_key(key)
        rows += prices.normalise(
            part, currencies, source_code=PROVIDER.code, window=(day.start.date(), day.end.date())
        )
    # THE SECURITIES WITH NO CURRENCY ARE COUNTED, NOT HIDDEN. 425 of 10,894 have neither a listing
    # currency nor one of their own; the column is nullable so they still get a price, and this is
    # what stops that becoming normal.
    context.add_output_metadata(
        {
            "rows": len(rows),
            "dropped": len(raw) - len(rows),
            "without_a_currency": sum(1 for r in rows if not r["currency_code"]),
        }
    )
    return rows


@dg.asset(
    partitions_def=security_partitions,
    # THE STAGE THAT WAS OOM-KILLED, and the reason the width above is a budget rather than a
    # preference: this one holds the raw rows AND their normalised copies simultaneously.
    backfill_policy=dg.BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="prices",
    kinds={"postgres"},
    metadata={"table": "market.price_bar", "conflict": ["security_id", "trade_date"]},
    description="Lane B's raw history as typed core rows, into the same table Lane A writes.",
)
def price_bar_history(
    context: AssetExecutionContext, postgres: Postgres, raw_price_history: Any
) -> list[dict[str, Any]]:
    """The other half of Lane B, which was missing: raw was landing and nothing normalised it.

    SAME TABLE AS `price_bar`, DELIBERATELY, and keyed the same way — `(security_id, trade_date)`.
    Two assets writing one table is the cost of two lanes with different partition schemes, and the
    key is what makes the overlap harmless: whichever lane last collected a day writes the same
    value for it.

    NOT `replace_scope`. A security's history is APPENDED to by successive runs — a bounded page
    that fetched 2010-2015 must not retract 2016 onwards written by the last one. Retraction is for
    a source that restates a whole scope, which a paged history fetch does not.
    """
    raw = _loaded_rows(raw_price_history)
    with postgres.connect() as conn:
        currencies = prices.currency_by_security(conn)

    # UP TO BUT NOT INCLUDING TODAY. Somewhere a market is open and its bar for today is a session
    # in progress; raw keeps it because it is what the provider said, and this is where it is
    # refused. Re-running tomorrow admits it, by which time it is a close — which is the rule
    # behaving correctly rather than drifting.
    rows = prices.normalise(
        raw,
        currencies,
        source_code=PROVIDER.code,
        window=(HISTORY_START, date.today()),
    )
    context.add_output_metadata(
        {
            "rows": len(rows),
            "dropped": len(raw) - len(rows),
            "without_a_currency": sum(1 for r in rows if not r["currency_code"]),
        }
    )
    return rows
