"""Stage 3 of the prices family: computed from data already held."""

from datetime import date, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.derive import returns
from muffin_ingest.facets import prices

from muffin_ingest_dagster.defs.prices.core import price_bar, price_bar_history
from muffin_ingest_dagster.defs.prices.partitions import PROVIDER
from muffin_ingest_dagster.lib.resources import Postgres

#: How far back the return rules can reach. The longest lookback is `5y` at 1,826 days; the margin
#: covers a security whose anchor bar sits a few sessions before the nominal date because its market
#: was shut. Loading less would silently drop the long periods rather than fail.
LOOKBACK = timedelta(days=1900)


class ReturnsRun(dg.Config):
    """How much of the universe one run covers."""

    #: Securities per page. The bars for a page are loaded into memory at once, so this is a memory
    #: budget: 500 securities x ~1,300 daily bars over the longest lookback is ~650k rows.
    page: int = 500
    #: Cap the run. None = every security with bars.
    limit: int | None = None


@dg.asset(
    deps=[price_bar, price_bar_history],
    # EAGER, MINUS THE GATE THAT MADE IT NEVER FIRE, AND DEAF TO THE HISTORY LANE.
    #
    # Plain `eager()` requires that NO upstream partition is missing, and an unpartitioned asset
    # depends on every one. `price_bar_history` has unfilled `security` keys by design, so every
    # production materialisation of this asset was a hand-run. The daemon's evaluation on
    # 2026-09-17 named `~any_deps_missing` as the false branch.
    #
    # THE `.ignore(price_bar_history)` THAT SAT HERE WAS REMOVED WITH THE CUTOVER, 2026-09-19, and
    # removing it was not optional. It existed so a multi-day history backfill could not hold the
    # nightly rebuild, back when `price_bar` — the DAY lane — was what triggered this asset. Since
    # `nightly_prices` replaced that lane, `daily_prices_schedule` is stopped and `price_bar` is no
    # longer materialised at all: ignoring the security lane would have left this asset with
    # nothing whatsoever to fire on, and returns would have silently stopped rebuilding while every
    # run stayed green. That is the same defect the 09-17 decision fixed, arrived at from the other
    # side.
    #
    # Waiting for the nightly sweep to finish is now the CORRECT behaviour rather than a cost:
    # returns computed halfway through a sweep are returns off half a night's bars. Lineage keeps
    # both dependencies, so a rollback to the day lane needs no change here.
    automation_condition=dg.AutomationCondition.eager().without(
        ~dg.AutomationCondition.any_deps_missing()
    ),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="prices",
    kinds={"postgres"},
    metadata={
        "table": "market.security_return",
        "conflict": ["security_id", "period_code"],
        # A PERIOD THIS RUN STOPS PRODUCING MUST BE REMOVED, NOT LEFT. The rules deliberately
        # WITHHOLD a return — a window that never moved, an anchor before a discontinuity, a stale
        # series — and an upsert cannot express that. Without retraction the guard that stops
        # producing a number can never remove the one already there, which is how securities served
        # `1d = 0.00%` for four days after the fix that stopped generating it.
        "replace_scope": ["security_id"],
    },
    # DERIVED FROM BARS THAT ARRIVE DAILY, so a day without a rebuild means the eager condition
    # stopped firing — which is exactly the failure that went unseen for the whole history load
    # (`AUTO-MATERIALIZE runs ever: 0` against 48 daemon ticks, and no counter could show it).
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="Price and total return per period, computed from the bars. No provider call.",
)
def security_return(
    context: AssetExecutionContext, config: ReturnsRun, postgres: Postgres
) -> list[dict[str, Any]]:
    today = date.today()
    out: list[dict[str, Any]] = []
    stats = {"securities": 0, "with_returns": 0, "periods": 0, "with_total_return": 0}

    with postgres.connect() as conn:
        # ONE WINDOW, NAMED ONCE. The enumeration is exact only because it asks about the same
        # days `bars_for` reads, so the two must not be able to disagree.
        since = today - LOOKBACK
        for batch in prices.securities_with_bars(
            conn, since=since, page=config.page, limit=config.limit
        ):
            series = prices.bars_for(conn, [s for s in batch], since=since)
            for security_id, bars in series.items():
                stats["securities"] += 1
                priced = returns.price_returns(bars, today)
                total = returns.total_returns(bars, today)
                if not priced and not total:
                    # NOT AN ERROR AND NOT A ZERO. A series too short, too stale or flat across
                    # every window yields nothing, and writing zeros would be inventing numbers the
                    # rules exist to withhold.
                    continue
                stats["with_returns"] += 1
                # THE DATE OF THE LAST BAR ACTUALLY USED, NEVER THE RUN'S OWN DATE. Every one of
                # these returns is `series[-1].close` over an anchor, so stamping `today` claims a
                # number is current when its newest input may be days old — a market closed for a
                # holiday, a series the provider has stopped updating, or simply a run that fires
                # before a venue's close.
                #
                # Found by the returns parity gate, where it made the comparison meaningless rather
                # than merely mislabelled: the old resource refreshed at 10:55 UTC on 09-10 with
                # 09-09 as its newest US bar, ours held 09-10, and SCCO moved -7.2% on the day
                # between them. Both sides were arithmetically right and one trading day apart,
                # and with `as_of` stamped from the clock nothing in either table said so.
                as_of = bars[-1].trade_date
                for period in sorted(set(priced) | set(total)):
                    stats["periods"] += 1
                    stats["with_total_return"] += total.get(period) is not None
                    out.append(
                        {
                            "security_id": security_id,
                            "period_code": period,
                            "as_of": as_of.isoformat(),
                            "price_return_pct": priced.get(period),
                            # NEVER COALESCED TO THE PRICE RETURN. NULL means "not computed" — no
                            # dividend data, or a series ineligible for the window — and filling it
                            # would erase the difference between "paid no income" and "we do not
                            # know".
                            "total_return_pct": total.get(period),
                            "source_code": PROVIDER.code,
                        }
                    )

    context.add_output_metadata({**stats, "rows": len(out)})
    return out
