"""Stage 2 of the fx family: raw parsed and normalised into core rows."""

from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import fx
from muffin_ingest.writers import upsert

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


#: HOW FAR BACK A NIGHT'S BODY IS READ FOR A DAY STORED WITHOUT A RATE. The spot request is five
#: trading days, so a week of calendar days covers whatever it carries. A day Yahoo fills after its
#: own partition ran is then published by a later night, as the price lane re-reads its last week.
#: Yahoo left Monday 2026-10-12 out of every pair's daily series, and 19 hours later still had not.
REREAD = timedelta(days=7)


def _not_seeded(known: frozenset[str]) -> str:
    """Subunits the code knows and `market.currency` does not, so none of their rates was derived.

    Non-empty only between rolling code that knows a subunit and deploying the migration that seeds
    it. Named rather than counted, so the run says which code is waiting and on what.
    """
    return ", ".join(sorted(set(fx.SUBUNITS) - known)) or "none"


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
    earlier: list[fx.Rate] = []
    stats: dict[str, int] = {}
    stale: list[str] = []
    skipped: list[str] = []
    for key, part in partitioned.rows_per_partition(context, raw_fx_spot).items():
        day = fx_day.time_window_for_partition_key(key)
        own_day = day.start.date()
        parsed = fx.normalise(part, window=(own_day - REREAD, day.end.date()))
        own = [r for r in parsed.rates if r.as_of >= own_day]
        # A WEEKDAY WITH NO RATE OF ITS OWN IS ONE OF TWO THINGS, and the body's age says which.
        #
        # A STALE ANSWER: http-cache served the 2026-09-17 00:00 run a body cached a day earlier,
        # so every point predated the 09-16 window (`rows=0, outside_window=152`) and the partition
        # materialised as a day with no rates. Such a body was produced before the day ended. It
        # fails, naming the partition, so a retry is visible and nothing claims the day.
        #
        # A DAY THE PROVIDER LEFT OUT: Yahoo has no bar for Monday 2026-10-12 in any pair's daily
        # series, in a body stamped 2026-10-13 00:00:27 and again at 19:07. A body produced after
        # the day ended is the provider's answer for it, so the partition materialises empty and
        # names the day. Failing it, as this did until 2026-10-13, made a re-run pointless and the
        # failure permanent. The 09-17 decision was this rule; the broader one was a deviation.
        #
        # A weekend has no session, so there an empty day is simply the truth.
        if not own and parsed.stats.get("points", 0) and day.start.weekday() < 5:
            fresh = parsed.newest_stamp is not None and parsed.newest_stamp >= day.end.timestamp()
            (skipped if fresh else stale).append(key)
        rates += own
        # EARLIER DAYS ARE OFFERED, NEVER IMPOSED. A night's body also carries the days before its
        # own, so a day Yahoo filled late is written here, but only where no rate is stored: each
        # day is stated by its own partition's body, and an old body re-run in a backfill must not
        # overwrite what a newer one said.
        earlier += [r for r in parsed.rates if r.as_of < own_day]
        for name, count in parsed.stats.items():
            stats[name] = stats.get(name, 0) + count
    if stale:
        raise dg.Failure(
            description=(
                f"no rate inside the window of weekday partition(s) {', '.join(stale)}, from "
                f"bodies all produced before the day ended: a stale or cached answer. "
                f"Re-materialise raw_fx_spot and fx_rate for them."
            ),
            metadata={"stale_partitions": ", ".join(stale), **stats},
        )
    if skipped:
        context.log.warning(
            "Yahoo has no bar for weekday partition(s) %s in bodies produced after the day ended; "
            "they materialise empty, and a later night fills them if Yahoo does",
            ", ".join(skipped),
        )
    with postgres.connect() as conn:
        known = fx.known_currencies(conn)
        with conn.cursor() as cur:
            refill = upsert(
                cur,
                "market.fx_rate",
                fx.core_rows(fx.with_subunits(earlier, known=known)),
                conflict=["currency_code", "as_of"],
                update=None,
            )
        conn.commit()
    with_subunits = fx.with_subunits(rates, known=known)

    context.add_output_metadata(
        {
            **stats,
            "rows": len(with_subunits),
            "observed": len(rates),
            "derived": len(with_subunits) - len(rates),
            "subunits_not_seeded": _not_seeded(known),
            # Earlier days a body carried, offered insert-only: `refilled` is how many were missing.
            "reread_offered": refill.written,
            "refilled": -1 if refill.changed is None else refill.changed,
            "provider_skipped_days": ", ".join(skipped) or "none",
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
    with postgres.connect() as conn:
        known = fx.known_currencies(conn)
    with_subunits = fx.with_subunits(parsed.rates, known=known)

    context.add_output_metadata(
        {
            **parsed.stats,
            "rows": len(with_subunits),
            "observed": len(parsed.rates),
            "derived": len(with_subunits) - len(parsed.rates),
            "subunits_not_seeded": _not_seeded(known),
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
