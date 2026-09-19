"""Stage 1 of the fx family: what the provider sent, kept whole."""

import time
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import fx
from muffin_ingest.providers import yahoo_chart

from muffin_ingest_dagster.defs.fx.partitions import (
    HISTORY_PARTITIONS_PER_RUN,
    currency_partitions,
    fx_day,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

#: Yahoo answers a five-day window so a weekend or holiday still yields the last real close.
SPOT_RANGE = "5d"


HISTORY_RANGE = "10y"


#: A minimum gap between requests. THE POOL CANNOT DO THIS — it bounds how many runs touch a
#: provider at once (one), and one run asking 43 times back to back is still a burst. Correct only
#: while that pool stays at limit 1; widen it and this silently becomes N times looser.
SECONDS_BETWEEN_CALLS = 0.5


class FxRun(dg.Config):
    """What bounds a run, rather than what it fetches."""

    #: Cap the currency list. For a first look or a dual-run comparison, never for steady state.
    limit: int | None = None
    #: Wall-clock budget. A run that stops early leaves its partition unmaterialised, which is
    #: visible; one that runs for ever is not.
    budget_seconds: int = 900
    #: Ask even for currencies marked absent. For a deliberate re-probe, not for a schedule.
    include_absent: bool = False


def _collect(
    context: AssetExecutionContext,
    currencies: list[str],
    *,
    range_: str,
    interval: str,
    budget_seconds: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Ask for each currency in turn and store WHAT CAME BACK, unopened.

    ONE CURRENCY PER REQUEST, because the endpoint takes one symbol. There is nothing to batch, and
    so — unlike the price lane — no isolation pass is needed: every call is already isolated and the
    provider's answer is already about exactly one subject.

    THE COUNTERS HERE ARE ABOUT THE CALL AND NOTHING ELSE. `answered` means a body came back;
    `transport` means we never got one, which is a fact about us or the network. Everything about
    the CONTENT of those bodies — points, live quotes, null closes, rates refused by the band — is
    counted by `fx.normalise` against files on disk, because until 2026-09-12 counting it here is
    what dragged the parse, the window filter and the live-quote rule in front of the write.
    Marking a currency absent on a transport failure is how a thirty-second outage becomes a month
    of silence, which is why the two can never share a counter.
    """
    deadline = time.monotonic() + budget_seconds
    rows: list[dict[str, Any]] = []
    stats = {
        "calls": 0,
        "answered": 0,
        "transport": 0,
        "unasked": 0,
        # What is actually on disk for this run. A body that shrinks to nothing across a whole
        # run is a provider event no row count can show, because the rows are the documents.
        "bytes": 0,
    }
    last_error: str | None = None
    consecutive_transport = 0

    for index, currency in enumerate(currencies):
        if time.monotonic() >= deadline:
            stats["unasked"] = len(currencies) - index
            context.log.warning("budget spent with %s currencies unasked", stats["unasked"])
            break
        if index:
            time.sleep(SECONDS_BETWEEN_CALLS)

        stats["calls"] += 1
        try:
            document = yahoo_chart.fetch(
                yahoo_chart.pair(currency), range_=range_, interval=interval
            )
        except yahoo_chart.YahooRefused as exc:
            stats["transport"] += 1
            last_error = str(exc)
            consecutive_transport += 1
            # THREE IN A ROW WITH NOTHING ANSWERED means the fault is at our end or the provider's,
            # and asking the remaining forty would only produce a longer record of the same thing.
            if consecutive_transport >= 3 and stats["answered"] == 0:
                stats["unasked"] = len(currencies) - (index + 1)
                context.log.error("three consecutive transport failures, stopping: %s", last_error)
                break
            continue

        consecutive_transport = 0
        stats["answered"] += 1
        stats["bytes"] += len(document.body)
        # A BODY THAT SAYS "I DO NOT CARRY THIS PAIR" IS STORED LIKE ANY OTHER. It is the
        # provider's answer, it is what makes the absence provable from disk, and deciding it
        # yielded no rate is stage 2's job — the same rule that keeps a 404 naming an absence
        # apart from a 404 that is a refusal.
        rows.extend(
            fx.raw_rows(
                currency, document, interval=interval, range_=range_, run_id=context.run.run_id
            )
        )

    if last_error:
        context.log.warning("last error: %s", last_error)
    context.log.info("fx collect: %s", stats)
    return rows, stats


# ── Lane A: today's rate for every currency ────────────────────────────────────────────────────


@dg.asset(
    partitions_def=fx_day,
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="yahoo",
    io_manager_key="parquet_io",
    group_name="fx",
    kinds={"yahoo", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="Every tracked currency's latest close against USD, as the provider gave it.",
)
def raw_fx_spot(context: AssetExecutionContext, config: FxRun, postgres: Postgres) -> Any:
    """WHAT YAHOO SERVED FOR EACH PAIR, WHOLE — one body per currency, nothing opened.

    THE PARTITION CLAIMS COVERAGE, NOT CONTENT. A five-day range is asked so the partition's day
    is certainly inside what comes back — a weekend, a holiday, or a provider that pads. It is NOT
    asked so that five days can be published, and the first version of this asset wrote the newest
    point instead: a run for the 2026-09-10 partition wrote 42 rates dated **2026-09-11**, a
    session still in progress. `fx_rate` refuses both of those now, reading the body.

    THE WINDOW FILTER USED TO LIVE HERE AND THAT WAS THE DEFECT. Points outside the partition's
    day were discarded before anything was stored, so a bug in the date rule — and there has been
    one, dating every FX bar a day early — cost ten years of refetching per currency instead of a
    re-parse. Stage 1 stores; stage 2 decides.
    """
    with postgres.connect() as conn:
        currencies = fx.askable_currencies(conn, include_absent=config.include_absent)
    if config.limit is not None:
        currencies = currencies[: config.limit]

    window = context.partition_time_window
    context.log.info(
        "spot for %s currencies covering %s..%s",
        len(currencies),
        window[0].date(),
        window[1].date(),
    )
    rows, stats = _collect(
        context,
        currencies,
        range_=SPOT_RANGE,
        interval="1d",
        budget_seconds=config.budget_seconds,
    )

    context.add_output_metadata({**stats, "rows": len(rows), "currencies": len(currencies)})
    # FILED UNDER EVERY DAY THE RUN COVERS, because a chart body covers a RANGE and belongs to no
    # single day. Keying it by a date read out of the body would put the artifact's placement back
    # inside stage 1 — which is exactly the interpretation this lane just stopped making.
    return partitioned.to_every_partition(context, rows)


# ── Lane B: ten years of weekly rates, one partition per currency ──────────────────────────────


@dg.asset(
    partitions_def=currency_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN),
    pool="yahoo",
    io_manager_key="parquet_io",
    group_name="fx",
    kinds={"yahoo", "parquet"},
    description="Ten years of WEEKLY closes per currency. Idles at zero; the load is one backfill.",
)
def raw_fx_history(context: AssetExecutionContext, config: FxRun, postgres: Postgres) -> Any:
    wanted = set(context.partition_keys)
    with postgres.connect() as conn:
        currencies = [c for c in fx.askable_currencies(conn, include_absent=True) if c in wanted]

    context.log.info("history for %s of %s requested currencies", len(currencies), len(wanted))
    rows, stats = _collect(
        context,
        currencies,
        range_=HISTORY_RANGE,
        interval="1wk",
        budget_seconds=config.budget_seconds,
    )
    context.add_output_metadata({**stats, "rows": len(rows)})
    # Keyed by CURRENCY here — this lane's partition IS the subject, so unlike the spot lane the
    # document does belong to exactly one key and `by_partition` can say which.
    return partitioned.by_partition(context, rows, key=lambda r: str(r["currency_code"]))
