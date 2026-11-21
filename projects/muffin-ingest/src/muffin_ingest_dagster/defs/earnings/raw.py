"""Stage 1 of the earnings family: nasdaq's calendar for each date, kept whole."""

import re
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import earnings
from muffin_ingest.providers import nasdaq
from muffin_ingest.providers.documents import DocumentRefused

from muffin_ingest_dagster.defs.earnings import partitions as earnings_partitions
from muffin_ingest_dagster.defs.earnings.partitions import DAYS_PER_RUN, FRESH, nasdaq_days
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.pacing import Pacer

#: Every outcome a day can have in a run. They partition the days asked about.
OUTCOMES = (*earnings.DAY_OUTCOMES, "refused", "transport", "unasked")

_STATUS = re.compile(r"HTTP (\d{3})$")


def status_of(exc: DocumentRefused) -> int | None:
    """The status a refusal names, read off the tail of its message (`documents._get`)."""
    match = _STATUS.search(str(exc))
    return int(match.group(1)) if match else None


@dg.asset(
    io_manager_key="parquet_io",
    group_name="earnings",
    kinds={"nasdaq", "parquet"},
    # THE NEWEST ANSWER FOR A DAY IS THE WHOLE DAY: a re-ask replaces the file.
    metadata={"merge_on": ["day", "fetched_at"]},
    partitions_def=nasdaq_days,
    backfill_policy=dg.BackfillPolicy.multi_run(DAYS_PER_RUN),
    pool="nasdaq",
    freshness_policy=FRESH,
    description="Nasdaq's earnings calendar for each date, whole: forecasts ahead, actuals behind.",
)
def raw_nasdaq_earnings(context: AssetExecutionContext) -> Any:
    """One request per day in the run.

    A RUN MATERIALISES ONLY IF EVERY DAY ANSWERED. A materialised day claims we hold nasdaq's answer
    for it, so a day refused, unreadable or unasked fails the run rather than being written as a
    day with no reports (umbrella CLAUDE.md, "A successful run claims every partition it covers").
    A day is cheap to ask again: the past through the cache, the window past it.
    """
    days = [date.fromisoformat(k) for k in context.partition_keys]
    today = datetime.now(UTC).date()
    fresh_from = today - timedelta(days=earnings_partitions.WINDOW_PAST_DAYS)
    stats = {o: 0 for o in OUTCOMES}
    calls = throttled = 0
    rows: list[dict[str, Any]] = []
    last_error: str | None = None
    pacer = Pacer(lambda: earnings_partitions.SECONDS_BETWEEN_REQUESTS)
    failures_in_a_row = 0
    anything_answered = False
    for index, day in enumerate(days):
        remaining = len(days) - index
        pacer.wait()
        calls += 1
        try:
            document = nasdaq.fetch(day, fresh=day >= fresh_from)
        except DocumentRefused as exc:
            last_error = str(exc)
            status = status_of(exc)
            if status == 429:
                # The throttled day and every day after it are unasked: none was answered.
                throttled = 1
                stats["unasked"] += remaining
                break
            stats["transport" if status is None else "refused"] += 1
            failures_in_a_row += 1
            if failures_in_a_row >= 3 and not anything_answered:
                stats["unasked"] += remaining - 1
                break
            continue
        failures_in_a_row = 0
        anything_answered = True
        outcome = earnings.read_nasdaq(document.body).outcome
        stats[outcome] += 1
        row = document.as_row(context.run.run_id)
        row.update({"day": day.isoformat(), "provider": nasdaq.PROVIDER})
        rows.append(row)

    unanswered = stats["unreadable"] + stats["refused"] + stats["transport"] + stats["unasked"]
    metadata = {"requested": len(days), **stats, "calls": calls, "throttled": throttled}
    if unanswered:
        raise dg.Failure(
            description=f"{unanswered} of {len(days)} days unanswered; last error: {last_error}",
            metadata=metadata,
        )
    context.add_output_metadata(metadata)
    return partitioned.by_partition(context, rows, key=_day_of, complete={r["day"] for r in rows})


def _day_of(row: Mapping[str, Any]) -> str:
    return str(row["day"])
