"""Daily bars, in two lanes — and the reason there are two is the whole design.

  LANE A  `raw_price_bars`     DAILY partitions, from go-live.
          Materialising a partition CLAIMS that the whole cross-section for that window was
          collected. That claim is the only way to answer "did the collection run on Tuesday?",
          which the data itself cannot answer: a security with no bar looks identical whether its
          market was shut, its symbol is dead, or nothing ran at all.

  LANE B  `raw_price_history`  one partition PER SECURITY.
          Here the subject IS the slice, so Dagster's partition grid answers "which securities are
          loaded" natively. It cannot be date-partitioned without a newly promoted security costing
          a re-fetch of the universe, and it makes no completeness claim about any date — which is
          exactly why it is a separate asset rather than a backfill of Lane A.

WHAT A BATCH COSTS, because it sizes everything: `openbb_yfinance` calls
`yf.download(tickers="A,B,C", threads=False)`, so a batched call is ONE YAHOO REQUEST PER SYMBOL,
serially. Batching collapses our call count (545 rather than 10,894 for a full pass), not the
vendor's. The pool bounds concurrency, the limiter bounds symbols per second, and neither is the
same thing.
"""

# No `from __future__ import annotations` here, for the same reason as `definitions.py`: it
# stringifies every annotation and Dagster resolves `context` by comparing the actual class.

import time
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest.facets import prices
from muffin_ingest.providers import openbb
from muffin_ingest.providers.isolation import BatchVerdict, fetch_with_isolation
from muffin_ingest.providers.yfinance import Yfinance
from muffin_ingest_dagster.resources import Postgres

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
HISTORY_START = date(1970, 1, 1)


class PriceRun(dg.Config):
    """What bounds a run, rather than what it fetches."""

    #: Cap the universe. For a dual-run comparison or a first look, never for steady state.
    limit: int | None = None
    #: Wall-clock budget. A run that stops early leaves its partition unmaterialised, which is
    #: visible; one that runs for ever is not.
    budget_seconds: int = 3600


def _fetcher(start: date, end: date) -> Any:
    """A `Fetcher` for `fetch_with_isolation`: raises on transport, returns [] on an empty answer.

    Those being different facts is the entire reason this pipeline is being rewritten, and the
    in-process hub is what keeps them distinguishable — over HTTP a throttled yfinance and a symbol
    the provider does not carry are both an empty 204.
    """

    def fetch(subjects: Sequence[str], timeout_s: float) -> list[dict[str, object]]:
        answer = openbb.fetch(
            "equity.price.historical",
            symbol=",".join(subjects),
            provider="yfinance",
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            interval="1d",
        )
        return answer.rows

    return fetch


def _collect(
    context: AssetExecutionContext,
    subjects: list[prices.Subject],
    *,
    start: date,
    end: date,
    batch_size: int,
    budget_seconds: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Ask for every subject in batches and return raw rows plus what the asking established."""
    deadline = time.monotonic() + budget_seconds
    rows: list[dict[str, Any]] = []
    stats = {
        "calls": 0,
        "answered": 0,
        "empty": 0,
        # NEVER FOLDED INTO `empty`, and the first version of this loop folded it. A batch that
        # RAISED tells us nothing about its subjects — "we never got an answer" and "the provider
        # answered and had nothing" are the distinction this whole pipeline is being rewritten
        # around, and counting the first as the second is how an outage becomes a population of
        # permanently dead securities. Found by driving it against production: openbb could not
        # import in the image, every call raised, and this reported fifty securities as having
        # answered nothing.
        "transport": 0,
        "dead": 0,
        "throttled": 0,
        "unasked": 0,
    }
    last_error: str | None = None
    consecutive_transport = 0

    for i in range(0, len(subjects), batch_size):
        batch = subjects[i : i + batch_size]
        if time.monotonic() >= deadline:
            # NOT AN ERROR AND NOT AN ABSENCE. Subjects we never asked about must not look like
            # subjects that answered nothing — that conflation is what once recorded ~8,300
            # securities as permanently unanswerable.
            stats["unasked"] += len(subjects) - i
            context.log.warning(
                "budget of %ss spent; %s subjects unasked", budget_seconds, stats["unasked"]
            )
            break

        by_symbol = {s.symbol: s for s in batch}
        stats["calls"] += 1
        verdict: BatchVerdict = fetch_with_isolation(
            _fetcher(start, end),
            list(by_symbol),
            timeout_s=min(60.0, max(5.0, deadline - time.monotonic())),
            deadline=deadline,
            control=PROVIDER.control_subject,
        )
        if verdict.throttled_out:
            stats["throttled"] += 1
            context.log.warning("provider is refusing us; stopping rather than marking anything")
            break

        parsed = prices.bars_by_symbol(verdict.rows, next(iter(by_symbol)))
        dead = {d.upper() for d in verdict.dead}
        answered_here = 0
        for symbol, subject in by_symbol.items():
            bars = parsed.get(symbol.upper(), [])
            if bars:
                answered_here += 1
                stats["answered"] += 1
                rows += prices.raw_rows(
                    subject,
                    bars,
                    provider=PROVIDER.code,
                    run_id=context.run.run_id,
                    observed=symbol,
                )
            elif symbol.upper() in dead:
                stats["dead"] += 1
            elif verdict.error is not None:
                stats["transport"] += 1
            else:
                stats["empty"] += 1

        if verdict.error is not None:
            last_error = verdict.error
            consecutive_transport = 0 if answered_here else consecutive_transport + 1
        else:
            consecutive_transport = 0

        # THREE, NOT ONE. A single failed batch is a blip, and the isolation pass has already
        # re-asked each of its subjects alone. Three in a row with nothing answered anywhere means
        # the fault is at our end or the provider's, and asking the remaining five hundred batches
        # would only produce a longer record of the same thing.
        if consecutive_transport >= 3 and stats["answered"] == 0:
            stats["unasked"] += len(subjects) - (i + len(batch))
            context.log.error(
                "three consecutive batches failed and nothing has answered: %s", last_error
            )
            break

    if last_error:
        context.log.warning("last error: %s", last_error)
    return rows, stats


@dg.asset(
    partitions_def=trading_day,
    # ONE RUN FOR A RANGE. Filling a week-long gap costs one run rather than seven, because the
    # asset reads the whole window and asks for it in a single pass.
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="yfinance",
    io_manager_key="parquet_io",
    group_name="prices",
    kinds={"yfinance", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="What yfinance said about the whole universe for this window, unchanged.",
)
def raw_price_bars(
    context: AssetExecutionContext, config: PriceRun, postgres: Postgres
) -> list[dict[str, Any]]:
    window: tuple[datetime, datetime] = context.partition_time_window
    start, end = window[0].date(), window[1].date() - timedelta(days=1)

    with postgres.connect() as conn:
        subjects = prices.askable_subjects(conn, provider=PROVIDER.code, limit=config.limit)

    context.log.info("asking %s subjects for %s..%s", len(subjects), start, end)
    rows, stats = _collect(
        context,
        subjects,
        start=start,
        end=end,
        batch_size=PROVIDER.batch_size,
        budget_seconds=config.budget_seconds,
    )
    # `rows: 0` IS A LEGITIMATE VALUE and is reported rather than filtered — a chart that cannot
    # draw a zero cannot show a collection that stopped, which is the only thing it is for.
    context.add_output_metadata({"subjects": len(subjects), "rows": len(rows), **stats})
    return rows


@dg.asset(
    partitions_def=trading_day,
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="prices",
    kinds={"postgres"},
    metadata={"table": "market.price_bar", "conflict": ["security_id", "trade_date"]},
    description="Raw bars as typed core rows. Never calls a provider, so a fix here is free.",
)
def price_bar(
    context: AssetExecutionContext, postgres: Postgres, raw_price_bars: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    with postgres.connect() as conn:
        currencies = prices.currency_by_security(conn)

    rows = prices.normalise(raw_price_bars, currencies, source_code=PROVIDER.code)
    # THE SECURITIES WITH NO CURRENCY ARE COUNTED, NOT HIDDEN. 425 of 10,894 have neither a listing
    # currency nor one of their own; the column is nullable so they still get a price, and this is
    # what stops that becoming normal.
    context.add_output_metadata(
        {
            "rows": len(rows),
            "dropped": len(raw_price_bars) - len(rows),
            "without_a_currency": sum(1 for r in rows if not r["currency_code"]),
        }
    )
    return rows


@dg.asset(
    partitions_def=security_partitions,
    # Verified in Dagster's source: `get_partition_keys_in_range` is implemented on the BASE
    # `PartitionsDefinition` by index over the ordered key list, so a single-run backfill works for
    # dynamic partitions and BATCHING SURVIVES — one run receives every key and asks in batches.
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="yfinance",
    io_manager_key="parquet_io",
    group_name="prices",
    kinds={"yfinance", "parquet"},
    description="Full history for these securities. Idles at zero; the load is one backfill.",
)
def raw_price_history(
    context: AssetExecutionContext, config: PriceRun, postgres: Postgres
) -> list[dict[str, Any]]:
    wanted = set(context.partition_keys)

    with postgres.connect() as conn:
        subjects = [
            s
            for s in prices.askable_subjects(conn, provider=PROVIDER.code)
            if s.security_id in wanted
        ]

    context.log.info("full history for %s of %s requested securities", len(subjects), len(wanted))
    rows, stats = _collect(
        context,
        subjects,
        start=HISTORY_START,
        end=date.today(),
        # SMALLER THAN THE CROSS-SECTION'S, because this is a MEMORY budget wearing a time budget's
        # clothes: twelve symbols at full history measured 11.6 MB of JSON in 8.1 s.
        batch_size=PROVIDER.history_batch_size,
        budget_seconds=config.budget_seconds,
    )
    context.add_output_metadata({"requested": len(wanted), "rows": len(rows), **stats})
    return rows


@dg.sensor(
    target=raw_price_history,
    minimum_interval_seconds=3600,
    description="A security with no history partition yet becomes one to fill.",
)
def new_securities_need_history(
    context: dg.SensorEvaluationContext, postgres: Postgres
) -> dg.SensorResult:
    """Keeps Lane B's partition set in step with the universe.

    ADDS KEYS AND REQUESTS NOTHING. A promotion should make the work VISIBLE as an unmaterialised
    partition rather than launch a run per security — 10,894 run requests is not a backfill, and
    deciding when to spend the provider budget on a deep history is an operator's call.
    """
    with postgres.connect() as conn:
        subjects = prices.askable_subjects(conn, provider=PROVIDER.code)

    existing = set(context.instance.get_dynamic_partitions(SECURITY_PARTITION))
    new = [s.security_id for s in subjects if s.security_id not in existing]
    context.log.info(
        "%s securities askable, %s without a history partition", len(subjects), len(new)
    )
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[security_partitions.build_add_request(new)] if new else [],
    )
