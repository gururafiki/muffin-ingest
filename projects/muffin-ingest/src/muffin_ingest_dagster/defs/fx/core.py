"""Stage 2 of the fx family: raw parsed and normalised into core rows."""

from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import fx

from muffin_ingest_dagster.defs.fx.partitions import (
    HISTORY_PARTITIONS_PER_RUN,
    currency_partitions,
    fx_day,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

# Shared with every other lane — see `muffin_ingest_dagster.partitioned`. The local copy keyed
# every lane on `currency_code`, which is right for the CURRENCY-partitioned history asset and
# wrong for the DATE-partitioned spot one; passing the key per call site is what makes that
# impossible to get wrong silently.
_loaded_rows = partitioned.loaded_rows


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
    stale: list[str] = []
    for key, part in partitioned.rows_per_partition(context, raw_fx_spot).items():
        day = fx_day.time_window_for_partition_key(key)
        parsed = fx.normalise(part, window=(day.start.date(), day.end.date()))
        # A WEEKDAY WITH NO RATE INSIDE ITS WINDOW, FROM BODIES THAT DO CARRY RATES, IS A STALE
        # ANSWER, not a day without FX. http-cache served the 2026-09-17 00:00 run a body cached a
        # day earlier: `rows=0, outside_window=152`, and the partition materialised as a day with
        # no rates. FX trades every weekday; a weekend has no session, so there the same shape is
        # the truth. Accepted edge: 25 Dec and 1 Jan have no session and will fail, loudly.
        if not parsed.rates and parsed.stats.get("outside_window", 0) and day.start.weekday() < 5:
            stale.append(key)
        rates += parsed.rates
        for name, count in parsed.stats.items():
            stats[name] = stats.get(name, 0) + count
    if stale:
        raise dg.Failure(
            description=(
                f"no rate inside the window of weekday partition(s) {', '.join(stale)} while the "
                f"stored bodies carry rates for other days: a stale or cached answer. "
                f"Re-materialise raw_fx_spot and fx_rate for them."
            ),
            metadata={"stale_partitions": ", ".join(stale), **stats},
        )
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
    if stats.get("legacy_rows"):
        context.log.warning(
            "%s raw rows predate the stored-body format and yield nothing; re-materialise "
            "raw_fx_spot for these partitions to re-read them",
            stats["legacy_rows"],
        )
    return fx.core_rows(with_subunits)


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
    if parsed.stats["legacy_rows"]:
        context.log.warning(
            "%s raw rows predate the stored-body format and yield nothing; re-materialise "
            "raw_fx_history for these currencies to re-read them",
            parsed.stats["legacy_rows"],
        )
    return fx.core_rows(with_subunits)
