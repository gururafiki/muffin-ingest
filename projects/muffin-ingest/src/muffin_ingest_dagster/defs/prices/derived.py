"""Stage 3 of the prices family: computed from data already held."""

from datetime import date, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext, DagsterInstance
from muffin_ingest.derive import returns
from muffin_ingest.facets import prices

from muffin_ingest_dagster.defs.prices.core import price_bar_history
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
    deps=[price_bar_history],
    # EAGER, MINUS THE GATE THAT MADE IT NEVER FIRE.
    #
    # Plain `eager()` requires that NO upstream partition is missing, and an unpartitioned asset
    # depends on every one. `price_bar_history` has unfilled `security` keys by design, so every
    # production materialisation of this asset was a hand-run. The daemon's evaluation on
    # 2026-09-17 named `~any_deps_missing` as the false branch.
    #
    # THE HISTORY LANE IS THE ONLY UPSTREAM since the day lane was deleted on 2026-10-04 (it had
    # been stopped since the 09-19 cutover, which is when an `.ignore(price_bar_history)` here had
    # to go: ignoring the security lane would have left this asset nothing to fire on). Waiting for
    # the nightly sweep to finish is the CORRECT behaviour rather than a cost: returns computed
    # halfway through a sweep are returns off half a night's bars.
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
    withheld: list[str] = []
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
                    withheld.append(security_id)
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

        # WHAT THE RULES WITHHOLD IS RETRACTED, NOT LEFT. `replace_scope` rewrites only the
        # securities this run produced rows for, so a security whose returns are all withheld kept
        # the last ones written, for ever, looking current. The ingest ledger deleted them each
        # time it marked a symbol dead, this asset rewrote them from the last bars until the series
        # went stale (10 days), and from then until the next mark, 30 days on, they stood. A
        # security this run EVALUATED and found nothing for is one it has an answer about: no
        # current number. A 7-day holiday cannot trigger it; `STALE_DAYS` is 10.
        retracted = 0
        if withheld:
            with conn.cursor() as cur:
                cur.execute(
                    "delete from market.security_return where security_id = any(%s::uuid[])",
                    (withheld,),
                )
                retracted = cur.rowcount
            conn.commit()

    context.add_output_metadata(
        {**stats, "rows": len(out), "withheld": len(withheld), "retracted": retracted}
    )
    return out


#: Securities per call of `market.derive_security_price_span`. Finding a security's first bar walks
#: the yearly partitions up from 1970: measured on production 2026-09-30, 9.3 s per 500 securities
#: with a cold cache, well inside `ingest_rw`'s 120 s statement timeout. Each call commits, so a
#: failure late in a bootstrap keeps what the earlier calls wrote.
SPAN_CHUNK = 500

#: Where a run records how far into the history lane's materialisations it has read. The next run
#: reads from there, so the cursor is the asset's own materialisation and needs no table of ours.
WATERMARK = "watermark_storage_id"


def _latest_storage_id(instance: DagsterInstance, key: dg.AssetKey) -> int | None:
    records = instance.fetch_materializations(key, limit=1).records
    return records[0].storage_id if records else None


def _partitions_since(instance: DagsterInstance, key: dg.AssetKey, after: int) -> set[str]:
    """The partitions of `key` materialised after storage id `after`, paged."""
    found: set[str] = set()
    cursor = None
    while True:
        result = instance.fetch_materializations(
            dg.AssetRecordsFilter(asset_key=key, after_storage_id=after),
            limit=1000,
            cursor=cursor,
            ascending=True,
        )
        for record in result.records:
            materialization = record.asset_materialization
            if materialization is not None and materialization.partition:
                found.add(materialization.partition)
        if not result.has_more:
            return found
        cursor = result.cursor


def _previous_watermark(instance: DagsterInstance, key: dg.AssetKey) -> int | None:
    event = instance.get_latest_materialization_event(key)
    materialization = event.asset_materialization if event else None
    value = materialization.metadata.get(WATERMARK) if materialization else None
    raw = value.value if value is not None else None
    return raw if isinstance(raw, int) else None


@dg.asset(
    deps=[price_bar_history],
    # EAGER WITHOUT THE MISSING-DEPS GATE, for the reason `security_return` gives above: the
    # history lane always has unfilled `security` keys, and plain `eager()` would wait for all of
    # them. It fires after each night's sweep, once no upstream run is in progress.
    #
    # NOT ON ITS FIRST EVALUATION. `since_last_handled` counts a newly deployed asset's initial
    # evaluation as handled, so `newly_missing` cancels against it — measured on `security_listing`
    # the morning it shipped (2026-10-03). The first run comes with the next sweep, or by hand.
    automation_condition=dg.AutomationCondition.eager().without(
        ~dg.AutomationCondition.any_deps_missing()
    ),
    pool="sql",
    group_name="prices",
    kinds={"postgres"},
    # Every night's sweep updates the history lane, so a day without a run means the condition
    # stopped firing.
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description=(
        "The first and last bar each security holds (market.security_price_span), for the "
        "securities whose history partition materialised since the last run. No provider call."
    ),
)
def security_price_span(
    context: AssetExecutionContext, postgres: Postgres
) -> "dg.MaterializeResult[None]":
    """Keep `market.security_price_span` in step with the history lane.

    THE RUN'S OWN SECURITIES, NOT THE UNIVERSE. Re-deriving every span costs ~4 minutes cold; a
    night touches ~2,500. So each run asks for the partitions of `price_bar_history` materialised
    after the previous run's watermark, a storage id it recorded in its own metadata. The first run
    has none and asks for every materialised partition: the bootstrap.
    """
    instance = context.instance
    upstream = price_bar_history.key
    # Read BEFORE choosing the securities: anything materialised after this point is picked up next
    # time, and anything read twice is re-derived idempotently.
    watermark = _latest_storage_id(instance, upstream)
    since = _previous_watermark(instance, context.asset_key)
    if since is None:
        mode = "bootstrap"
        securities = sorted(instance.get_materialized_partitions(upstream))
    else:
        mode = "incremental"
        securities = sorted(_partitions_since(instance, upstream, since))

    totals = {"asked": 0, "written": 0, "without_bars": 0}
    with postgres.connect() as conn:
        for start in range(0, len(securities), SPAN_CHUNK):
            chunk = securities[start : start + SPAN_CHUNK]
            with conn.cursor() as cur:
                cur.execute("select market.derive_security_price_span(%s::uuid[])", (chunk,))
                row = cur.fetchone()
            conn.commit()
            counts = row[0] if row else {}
            for name in totals:
                totals[name] += int(counts.get(name, 0))

    context.log.info(
        "%s: %s securities asked, %s spans written, %s with no bars",
        mode,
        totals["asked"],
        totals["written"],
        totals["without_bars"],
    )
    return dg.MaterializeResult(
        metadata={
            "mode": mode,
            "securities": len(securities),
            **totals,
            WATERMARK: watermark if watermark is not None else (since or 0),
        }
    )
