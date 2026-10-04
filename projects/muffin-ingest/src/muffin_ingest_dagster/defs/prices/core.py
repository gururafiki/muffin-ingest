"""Stage 2 of the prices family: raw parsed and normalised into core rows."""

from datetime import date
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import prices

from muffin_ingest_dagster.defs.prices.partitions import (
    HISTORY_PARTITIONS_PER_RUN,
    HISTORY_START,
    PROVIDER,
    security_partitions,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

_loaded_rows = partitioned.loaded_rows


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

    `market.price_bar`, keyed `(security_id, trade_date)`. Until 2026-10-04 the day lane's
    `price_bar` asset wrote the same table under the same key, which is what made a cutover with a
    rollback harmless; this is the only writer now.

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
