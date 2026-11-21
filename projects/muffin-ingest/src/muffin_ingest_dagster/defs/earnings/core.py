"""Stage 2 of the earnings family: both sources' answers read into the earnings tables."""

from collections import defaultdict
from datetime import UTC, date, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import earnings
from muffin_ingest.writers import WriteResult, retract_unproduced, upsert

from muffin_ingest_dagster.defs.companies.partitions import COMPANY_PARTITIONS_PER_RUN
from muffin_ingest_dagster.defs.companies.raw import SUMMARY
from muffin_ingest_dagster.defs.earnings.partitions import DAYS_PER_RUN, FRESH, nasdaq_days
from muffin_ingest_dagster.defs.prices.partitions import security_partitions
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

RAW_NASDAQ = dg.AssetKey("raw_nasdaq_earnings")

#: THE KEYS CARRY THE PROVIDER (the cutover migration), so nasdaq's and Yahoo's rows for one company
#: sit side by side and neither overwrites the other.
CALENDAR_KEY = ["source_code", "symbol", "report_date"]
HISTORY_KEY = ["security_id", "period_ending", "source_code"]

HISTORY_VALUES = ["eps_actual", "eps_estimated", "surprise", "surprise_pct", "reported_date"]

#: The symbols every security answers to, so a nasdaq symbol finds its security, as the edge's did.
SYMBOLS = "select upper(symbol), security_id::text from market.symbol_security"

#: The edge's prune: a date more than 90 days gone serves no page.
PRUNE = "delete from market.earnings_calendar where report_date < current_date - %s"

#: A RE-PARSE MAY NAME A PARTITION STAGE 1 NEVER REACHED, whose file does not exist yet.
_TOLERATE_MISSING = {"allow_missing_partitions": True}


def _written(prefix: str, result: WriteResult) -> dict[str, Any]:
    changed = -1 if result.changed is None else result.changed
    return {
        f"{prefix}_rows": result.written,
        f"{prefix}_changed": changed,
        f"{prefix}_collapsed": result.collapsed,
    }


@dg.asset(
    ins={"raw_nasdaq_earnings": dg.AssetIn(key=RAW_NASDAQ, metadata=_TOLERATE_MISSING)},
    group_name="earnings",
    kinds={"postgres"},
    partitions_def=nasdaq_days,
    backfill_policy=dg.BackfillPolicy.multi_run(DAYS_PER_RUN),
    pool="sql",
    freshness_policy=FRESH,
    metadata={"table": "market.earnings_calendar, market.security_eps_history"},
    description="Nasdaq's dates (within 90 days) and reported quarters, under `nasdaq`.",
)
def nasdaq_earnings(
    context: AssetExecutionContext, postgres: Postgres, raw_nasdaq_earnings: Any
) -> dg.MaterializeResult[None]:
    """No provider call. One transaction.

    A DAY'S ANSWER IS THE WHOLE DAY, so nasdaq's calendar rows for that date which the answer no
    longer lists are retracted: a company that moved its report away from the date. Measured before
    building it: the edge's stored calendar still offered ZS on 11-24 beside its rescheduled 12-02.
    The history is never retracted; a quarter nasdaq states once is still that quarter's figure.
    """
    documents = partitioned.loaded_rows(raw_nasdaq_earnings)
    today = datetime.now(UTC).date()
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SYMBOLS)
            ids = {str(symbol): str(sid) for symbol, sid in cur.fetchall()}
        parsed = earnings.normalise_nasdaq(documents, ids=ids, today=today)
        by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
        for row in parsed.calendar:
            by_day[row["report_date"]].append(row)
        retracted = 0
        with conn.cursor() as cur:
            calendar = upsert(
                cur,
                "market.earnings_calendar",
                parsed.calendar,
                conflict=CALENDAR_KEY,
                update=[
                    "security_id",
                    "name",
                    "eps_consensus",
                    "eps_previous",
                    "num_estimates",
                    "period_ending",
                    "reporting_time",
                    "as_of",
                ],
                compare=[
                    "security_id",
                    "name",
                    "eps_consensus",
                    "eps_previous",
                    "num_estimates",
                    "period_ending",
                    "reporting_time",
                ],
            )
            for key in parsed.answered:
                day = date.fromisoformat(key)
                if (today - day).days > earnings.CALENDAR_DAYS:
                    continue
                retracted += retract_unproduced(
                    cur,
                    "market.earnings_calendar",
                    by_day.get(day, []),
                    scope={"report_date": day, "source_code": earnings.NASDAQ},
                    key=["symbol"],
                    types={"symbol": "text"},
                ).retracted
            history = upsert(
                cur,
                "market.security_eps_history",
                parsed.history,
                conflict=HISTORY_KEY,
                update=[*HISTORY_VALUES, "as_of"],
                compare=HISTORY_VALUES,
            )
            cur.execute(PRUNE, (earnings.CALENDAR_DAYS,))
            pruned = cur.rowcount if isinstance(cur.rowcount, int) else -1
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            **parsed.stats,
            **_written("calendar", calendar),
            "calendar_retracted": retracted,
            **_written("history", history),
            "calendar_pruned": pruned,
        }
    )


@dg.asset(
    ins={"raw_yahoo_summary": dg.AssetIn(key=SUMMARY, metadata=_TOLERATE_MISSING)},
    group_name="earnings",
    kinds={"postgres"},
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(COMPANY_PARTITIONS_PER_RUN),
    pool="sql",
    freshness_policy=FRESH,
    metadata={"table": "market.earnings_calendar, market.security_eps_history"},
    description="Yahoo's next date and last four quarters from the stored summaries, under "
    "`yfinance`.",
)
def yahoo_earnings(
    context: AssetExecutionContext, postgres: Postgres, raw_yahoo_summary: Any
) -> dg.MaterializeResult[None]:
    """No provider call: the company lane already stores these documents. One transaction.

    A COMPANY'S NEWEST SUMMARY STATES ITS ONE NEXT DATE, so Yahoo's other calendar rows for that
    company are retracted. Without that, a date Yahoo moved earlier would leave the old, later one
    standing as an upcoming report. The history accumulates: each document carries only four
    quarters, and an older one is still that quarter's figure.
    """
    documents = partitioned.loaded_rows(raw_yahoo_summary)
    parsed = earnings.normalise_yahoo(documents, today=datetime.now(UTC).date())
    by_company: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in parsed.calendar:
        by_company[row["security_id"]].append(row)
    retracted = 0
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            values = [
                "security_id",
                "name",
                "eps_consensus",
                "eps_low",
                "eps_high",
                "revenue_consensus",
                "is_date_estimate",
                "report_date_end",
            ]
            calendar = upsert(
                cur,
                "market.earnings_calendar",
                parsed.calendar,
                conflict=CALENDAR_KEY,
                update=[*values, "as_of"],
                compare=values,
            )
            for sid in parsed.answered:
                retracted += retract_unproduced(
                    cur,
                    "market.earnings_calendar",
                    by_company.get(sid, []),
                    scope={"security_id": sid, "source_code": earnings.YAHOO},
                    key=["symbol", "report_date"],
                    types={"symbol": "text", "report_date": "date"},
                ).retracted
            history = upsert(
                cur,
                "market.security_eps_history",
                parsed.history,
                conflict=HISTORY_KEY,
                update=[*HISTORY_VALUES, "as_of"],
                compare=HISTORY_VALUES,
            )
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            **parsed.stats,
            **_written("calendar", calendar),
            "calendar_retracted": retracted,
            **_written("history", history),
        }
    )
