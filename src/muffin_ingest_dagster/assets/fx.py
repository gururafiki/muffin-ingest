"""FX rates, in the same two lanes as prices — because the question splits the same way.

  LANE A  `raw_fx_spot`     DAILY partitions.
          Materialising a partition claims the whole cross-section of currencies was collected for
          that day. Nothing in `fx_rate` can answer that on its own: a currency with no row for
          Tuesday looks identical whether the pair is unquoted, the fetch failed, or nothing ran.

  LANE B  `raw_fx_history`  one partition PER CURRENCY.
          Ten years of weekly closes. The subject IS the slice, `min(as_of)` answers "is this one
          loaded", and a newly tracked currency costs one partition rather than a re-fetch of all
          43.

IT IS THE SAME SHAPE AS THE PRICE FAMILY ON PURPOSE. Forty-three currencies is small enough that one
lane would work, and building it the other way would make the standard something that holds only
when it is convenient. The parts that genuinely differ are the two that carry the domain: the
plausibility band, and subunits being DERIVED rather than fetched.
"""

# No `from __future__ import annotations` — Dagster resolves `context` by comparing the class.

import time
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest.facets import fx
from muffin_ingest.providers import yahoo_chart
from muffin_ingest_dagster import partitioned
from muffin_ingest_dagster.resources import Postgres

#: Same start as the price lane: a partition older than go-live claims a collection that never ran.
fx_day = dg.DailyPartitionsDefinition(start_date="2026-09-01", timezone="UTC")

#: One key per currency code.
CURRENCY_PARTITION = "currency"
currency_partitions = dg.DynamicPartitionsDefinition(name=CURRENCY_PARTITION)

#: How many currencies one history run may cover. Ten years of WEEKLY points is ~524 rows each, so
#: the whole universe is ~22,000 rows — three orders of magnitude under the price lane's 683,391,
#: and comfortably one run. The bound exists so the rule is the same rule, not because 43 needs it.
HISTORY_PARTITIONS_PER_RUN = 43

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


# Shared with every other lane — see `muffin_ingest_dagster.partitioned`. The local copy keyed
# every lane on `currency_code`, which is right for the CURRENCY-partitioned history asset and
# wrong for the DATE-partitioned spot one; passing the key per call site is what makes that
# impossible to get wrong silently.
_loaded_rows = partitioned.loaded_rows


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


@dg.asset(
    partitions_def=fx_day,
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="fx",
    kinds={"postgres"},
    metadata={"table": "market.fx_rate", "conflict": ["currency_code", "as_of"]},
    # SAME WINDOW AS ITS SOURCE. A normalise stage stale while its raw is fresh means the
    # TRANSFORM stopped — a different failure from the provider going quiet, and invisible
    # otherwise, because the raw asset's own policy would still be green.
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="Today's rates as core rows, with subunits derived from their parents.",
)
def fx_rate(
    context: AssetExecutionContext, postgres: Postgres, raw_fx_spot: Any
) -> list[dict[str, Any]]:
    """Bodies on disk to typed rates. Never calls a provider, so a fix here costs nothing to re-run.

    THE WINDOW IS APPLIED HERE. A partition publishes its own day and no other: writing the newest
    point Yahoo happens to hold would make the partition's claim false, and that newest point is
    routinely a mid-session quote whose "close" is not a close.
    """
    # PER PARTITION, FROM ITS OWN FILE. `raw_fx_spot` files the SAME body under every day a run
    # covers — a chart body covers a range and belongs to no single day — so a range run hands
    # this stage one copy per partition. Windowing the flattened set would publish every rate once
    # per copy; windowing each file to its own day publishes each day exactly once, from the
    # partition that claims it.
    rates: list[fx.Rate] = []
    stats: dict[str, int] = {}
    for key, part in partitioned.rows_per_partition(context, raw_fx_spot).items():
        day = fx_day.time_window_for_partition_key(key)
        parsed = fx.normalise(part, window=(day.start.date(), day.end.date()))
        rates += parsed.rates
        for name, count in parsed.stats.items():
            stats[name] = stats.get(name, 0) + count
    with_subunits = fx.with_subunits(rates)

    context.add_output_metadata(
        {
            **stats,
            "rows": len(with_subunits),
            "observed": len(rates),
            "derived": len(with_subunits) - len(rates),
        }
    )
    if stats.get("unreadable"):
        # A BODY WE STORED AND CANNOT READ IS OURS, NOT THE PROVIDER'S. Loud rather than fatal:
        # one malformed document must not blank the other forty-two currencies.
        context.log.error("%s stored bodies would not parse", stats["unreadable"])
    return fx.core_rows(with_subunits)


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


@dg.asset(
    partitions_def=currency_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="fx",
    kinds={"postgres"},
    metadata={"table": "market.fx_rate", "conflict": ["currency_code", "as_of"]},
    # NO FRESHNESS POLICY, AND THAT IS THE POINT OF THIS LANE. It idles at zero by design — the
    # load is one backfill and then nothing until a new subject appears. A staleness window here
    # would go red the day after the load and stay red for ever, against a lane behaving exactly
    # as intended, and this codebase has twice paid for a gate left red for a reason nobody acts
    # on: the cost is never the ignored check, it is the next true positive behind it.
    #
    # "Is this subject loaded?" is answered by the PARTITION GRID, not by a clock.
    description="The weekly history as core rows, into the same table Lane A writes.",
)
def fx_rate_history(
    context: AssetExecutionContext, postgres: Postgres, raw_fx_history: Any
) -> list[dict[str, Any]]:
    """SUBUNITS ARE DERIVED HERE, IN THE SAME PASS, and that is the rule the whole lane turns on.

    A subunit whose history is filled separately — or only by the spot lane — ends up with three
    days against its parent's ten years, and a consumer joining "the most recent rate at or before
    this bar" then silently uses a recent rate for every historical bar. That is exactly the shape
    that made Tel Aviv look like a 100x crash: wrong by every intervening move, and
    ordinary-looking.

    NOT `replace_scope`. A currency's history is APPENDED to by successive runs, and a bounded page
    must not retract what an earlier one wrote.
    """
    raw = _loaded_rows(raw_fx_history)
    # NO WINDOW. This lane's partition is the currency, so every point in the body belongs to it —
    # the ten-year range is the subject's whole history rather than a slice of a calendar.
    parsed = fx.normalise(raw)
    with_subunits = fx.with_subunits(parsed.rates)

    context.add_output_metadata(
        {
            **parsed.stats,
            "rows": len(with_subunits),
            "observed": len(parsed.rates),
            "derived": len(with_subunits) - len(parsed.rates),
        }
    )
    if parsed.stats["unreadable"]:
        context.log.error("%s stored bodies would not parse", parsed.stats["unreadable"])
    return fx.core_rows(with_subunits)


@dg.sensor(
    target=raw_fx_history,
    minimum_interval_seconds=3600,
    description="A currency with no history partition yet becomes one to fill.",
)
def new_currencies_need_history(
    context: dg.SensorEvaluationContext, postgres: Postgres
) -> dg.SensorResult:
    """Keeps Lane B's partition set in step with `market.currency`.

    ADDS KEYS AND REQUESTS NOTHING, for the same reason the price sensor does: a new currency should
    make the work VISIBLE as an unmaterialised partition, and deciding when to spend a provider
    budget is an operator's call rather than a sensor's.
    """
    with postgres.connect() as conn:
        currencies = fx.askable_currencies(conn, include_absent=True)

    existing = set(context.instance.get_dynamic_partitions(CURRENCY_PARTITION))
    new = [code for code in currencies if code not in existing]
    context.log.info("%s currencies askable, %s without a partition", len(currencies), len(new))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[currency_partitions.build_add_request(new)] if new else [],
    )
