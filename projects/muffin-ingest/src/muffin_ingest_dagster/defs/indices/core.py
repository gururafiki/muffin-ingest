"""Stage 2 of the indices family: raw parsed and normalised into core rows."""

from datetime import date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.derive import returns
from muffin_ingest.facets import indices, prices

from muffin_ingest_dagster.defs.indices.partitions import index_day
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres


@dg.asset(
    partitions_def=index_day,
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="indices",
    kinds={"postgres"},
    metadata={
        "table": "market.index_return",
        "conflict": ["index_code", "period_code"],
        # A PERIOD THIS RUN STOPS PRODUCING MUST BE REMOVED. The rules withhold a return for a
        # window that never moved or a series gone stale, and an upsert cannot express that — which
        # is how instruments served `1d = 0.00%` for four days after the fix that stopped
        # generating it. Scoped per index so a bounded run retracts only what it covered.
        "replace_scope": ["index_code"],
    },
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="Country and group returns computed from proxy bars, plus finviz's sector figures.",
)
def index_return(
    context: AssetExecutionContext,
    postgres: Postgres,
    raw_index_bars: Any,
    raw_sector_performance: Any,
) -> list[dict[str, Any]]:
    """Never calls a provider, so a rule change costs nothing to re-run."""
    window: tuple[datetime, datetime] = context.partition_time_window
    as_of = window[1].date() - timedelta(days=1)

    # ONE BAR PER (SCOPE, DAY), THE NEWEST FILE'S. Every daily run stores its whole 1,900-day
    # lookback in its own partition's file, so a range re-parse — the thing stage 2 exists to make
    # free — loads each bar once per file. Appended as they come, a day would sit beside its own
    # duplicate in a series every return rule reads POSITIONALLY, so `1d` would compare a session
    # with itself. Partitions are visited oldest first, so the newest fetch of a day is kept.
    latest: dict[str, dict[date, prices.Bar]] = {}
    outside_window = 0
    null_closes = 0
    for row in (
        r for part in partitioned.rows_per_partition(context, raw_index_bars).values() for r in part
    ):
        # THE PROVIDER'S OWN DATE, PARSED HERE. Raw carries the vendor's `date` untouched.
        trade_date = prices.row_date(row)
        if trade_date is None:
            continue
        # ONLY THE TOP IS CUT. Everything before the window's end is the lookback the long periods
        # need; what must go is the bar for a day the partition does not cover, which for a daily
        # partition is today — a session still trading, whose "close" is not one.
        if trade_date >= window[1].date():
            outside_window += 1
            continue
        # A SESSION INSIDE THE WINDOW CAN STILL HAVE NO CLOSE. yfinance sent `close: NaN` for 09-16
        # to the run at 00:00:34 UTC on 09-17, for 60 of 61 proxies. A bare `isinstance` admitted
        # it as the newest bar, `_eligible` refused each whole series on `isfinite`, and 60 scopes
        # kept serving 09-15 behind `with_returns=1`. Refused here, the return comes from the last
        # session that has a close — stamped with that session's date — and the count says so.
        close = prices.close_of(row)
        if close is None:
            null_closes += 1
            continue
        latest.setdefault(str(row["index_code"]), {})[trade_date] = prices.Bar(
            trade_date=trade_date, close=close, dividend=row.get("dividend")
        )
    series = {
        code: sorted(days.values(), key=lambda b: b.trade_date) for code, days in latest.items()
    }

    out: list[dict[str, Any]] = []
    stats = {"scopes": 0, "with_returns": 0, "periods": 0, "with_total_return": 0}
    for code, bars in series.items():
        bars.sort(key=lambda b: b.trade_date)
        stats["scopes"] += 1
        priced = returns.price_returns(bars, as_of)
        total = returns.total_returns(bars, as_of)
        if not priced and not total:
            # A refusal is a result: a proxy too short, too stale or flat across every window
            # yields nothing, and a zero would be a number the rules exist to withhold.
            continue
        stats["with_returns"] += 1
        # The last bar actually used, never the run's date — the same rule the security returns
        # learned from the parity gate, where stamping the clock hid a whole-session offset.
        stamped = bars[-1].trade_date
        for period in sorted(set(priced) | set(total)):
            stats["periods"] += 1
            stats["with_total_return"] += total.get(period) is not None
            out.append(
                {
                    "index_code": code,
                    "period_code": period,
                    "as_of": stamped.isoformat(),
                    "price_return_pct": priced.get(period),
                    "total_return_pct": total.get(period),
                    "source_code": "yfinance",
                }
            )

    # THE SNAPSHOT'S OWN DATE, NOT THE PARTITION'S. `raw_sector_performance` is unpartitioned and
    # records the day it was read, so re-running an old partition cannot misdate a sector figure —
    # the date travels with the data, which is the rule the returns gate forced on the whole family.
    sectors, unmapped = indices.normalise_sectors(_rows(raw_sector_performance))
    out.extend(sectors)
    if unmapped:
        # LOUD, NOT FATAL: a provider rename should degrade one sector, not blank the screen — and
        # it must never be filed under a guessed id.
        context.log.error("unmapped provider sector labels: %s", ", ".join(unmapped))

    context.add_output_metadata(
        {
            **stats,
            "rows": len(out),
            "sector_rows": len(sectors),
            "unmapped_labels": len(unmapped),
            "outside_window": outside_window,
            # Non-zero on a scheduled night is expected, not a defect: a session the provider has
            # not closed yet. It is the count that makes a `with_returns` drop explicable.
            "null_closes": null_closes,
        }
    )
    return out


# The mirror every lane needs — shared, see `muffin_ingest_dagster.partitioned`.
_rows = partitioned.loaded_rows
