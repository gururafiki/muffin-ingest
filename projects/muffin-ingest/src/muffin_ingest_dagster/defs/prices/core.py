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

    NOT `replace_scope`. A security's history is APPENDED to by successive runs, and the nightly
    cost of rewriting every bar is already an open decision
    (umbrella docs/deferred/2026-10-04-the-price-history-lane-rewrites-every-bar-nightly.md), which
    deleting and re-inserting each history would make permanent.

    BUT WITHIN ITS OWN RANGE, A RAW HISTORY IS THE ANSWER, so a bar on a date it lacks is retracted
    (`prices.retract_bars_absent_from_raw`). Without that, a history reloaded under a new symbol
    leaves the old listing's bars on every day the new one did not trade: 5,097 of them across 95
    securities on 2026-10-04, Hong Kong lines carrying their OTC line's dollar bars on HK holidays.
    """
    raw = _loaded_rows(raw_price_history)
    with postgres.connect() as conn:
        currencies = prices.currency_by_security(conn)
        # BEFORE THE WRITE, IN ITS OWN TRANSACTION. It deletes only dates the raw history lacks,
        # which the write below never produces, so neither order can undo the other.
        with conn.cursor() as cur:
            retracted = prices.retract_bars_absent_from_raw(cur, prices.dates_by_security(raw))
        conn.commit()

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
            # Bars removed because their date sits inside the raw history's range and the raw
            # history no longer has it: another listing's leftovers, almost always.
            "retracted": retracted,
        }
    )
    return rows
