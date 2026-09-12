"""Index returns — the last of the price family, and the one with two acquisition shapes.

  `raw_index_bars`          DAILY partitions. Proxy-ETF bars for the 45 country and 17 group
                            scopes, in one batched call.
  `raw_sector_performance`  DAILY partitions. The 11 sectors from finviz, which publishes NUMBERS
                            rather than a series — there is no ETF behind a muffin sector.
  `index_return`            DAILY. Both, into one table.

TWO RAW ASSETS FEEDING ONE CORE ASSET IS THE POINT, not a compromise. The scopes differ in where
their numbers come from and in nothing else, so the difference belongs at the boundary where it is
real. Folding finviz into the bar lane would mean inventing a series it does not publish; splitting
the core table would mean a reader has to know which kind of scope it is holding before it can ask
for a return.

AND THE RETURN RULES ARE THE SAME RULES. A country's 3-month return is computed by `derive/returns`,
exactly as a security's is, so a country page and a stock page cannot disagree about what the phrase
means. What sectors get instead is finviz's own figure — stated, and never recomputed to look alike.
"""

# No `from __future__ import annotations` — Dagster resolves `context` by comparing the class.

from datetime import date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest.derive import returns
from muffin_ingest.facets import indices, prices
from muffin_ingest.providers import openbb
from muffin_ingest_dagster import partitioned
from muffin_ingest_dagster.resources import Postgres

index_day = dg.DailyPartitionsDefinition(start_date="2026-09-01", timezone="UTC")

#: How far back the bar fetch reaches. The longest period a proxied scope serves is 5y, and
#: `index_at_or_before` needs a bar at or before the window start — so this is 5y plus a margin for
#: a listing that was closed on the anchor date.
LOOKBACK = timedelta(days=1900)

#: Symbols per request. THIS IS OUR CALL COUNT, NOT THE VENDOR'S: `openbb_yfinance` issues one
#: Yahoo request per symbol however many are joined. 62 scopes is 62 vendor requests either way.
BATCH = 20


class IndexRun(dg.Config):
    limit: int | None = None
    budget_seconds: int = 900


@dg.asset(
    partitions_def=index_day,
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="yfinance",
    io_manager_key="parquet_io",
    group_name="indices",
    kinds={"yfinance", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="Proxy-ETF bars for every country and group scope, as the provider gave them.",
)
def raw_index_bars(context: AssetExecutionContext, config: IndexRun, postgres: Postgres) -> Any:
    """THE EQUITY ROUTE SERVES AN ETF, VERIFIED RATHER THAN ASSUMED.

    The resource this replaces used `etf/historical`; driven against both on the same two symbols
    and the same window, `equity/price/historical` returned the identical 16 rows with identical
    closes. One call site is worth having, and it is only worth having if the answers match.
    """
    window: tuple[datetime, datetime] = context.partition_time_window
    end = window[1].date()

    with postgres.connect() as conn:
        scopes = indices.proxied_scopes(conn)
    if config.limit is not None:
        scopes = scopes[: config.limit]

    # A SYMBOL BACKS MORE THAN ONE SCOPE, AND A DICT COMPREHENSION SILENTLY KEEPS THE LAST.
    #
    # Measured in production: 62 scopes over **53 distinct symbols**. `EEM` backs
    # ftse:em-emea, msci:emerging AND msci:em-emea; `IVV` backs country:US, ftse:na and msci:na.
    # Written as `{symbol: code}` the map kept one code per symbol and nine scopes got no returns
    # at all — the run reported `answered=52` beside `empty=0`, two numbers that cannot both be
    # right, and nothing else could see it.
    #
    # It is not a modelling error: those scopes genuinely ARE the same index, which is exactly why
    # the relation is many-to-one and must be stored as one.
    by_symbol: dict[str, list[str]] = {}
    for code, symbol in scopes:
        by_symbol.setdefault(symbol, []).append(code)
    symbols = sorted(by_symbol)
    context.log.info(
        "%s proxied scopes over %s symbols (%s shared)",
        len(scopes),
        len(symbols),
        sum(1 for codes in by_symbol.values() if len(codes) > 1),
    )

    rows: list[dict[str, Any]] = []
    stats = {
        "calls": 0,
        "answered": 0,
        "empty": 0,
        "transport": 0,
        "bars": 0,
        "unattributed": 0,
        # Bars newer than the partition's window. A LOOKBACK SERIES STILL HAS A TOP, and this lane
        # forgot it: asking for 1,900 days up to `end` brings back TODAY's bar, which is a session
        # in progress, and `index_return` then stamps every country and group with today's date and
        # a mid-session price. Measured — country and group rows came out `as_of 2026-09-11` from
        # the 09-10 partition. Fourth time in this family that a date came from the wrong place.
        "outside_window": 0,
    }
    for index in range(0, len(symbols), BATCH):
        batch = symbols[index : index + BATCH]
        stats["calls"] += 1
        try:
            answer = openbb.price_history(batch, start=end - LOOKBACK, end=end)
        except Exception as exc:  # the reason is reported, never swallowed
            stats["transport"] += 1
            context.log.warning("batch failed: %s", exc)
            continue

        parsed = prices.bars_by_symbol(answer.rows, batch[0] if len(batch) == 1 else "")
        # ONLY THE TOP IS CUT. Everything before `end` is the lookback the long periods need; what
        # must go is anything the partition does not cover, which for a daily partition is today.
        for symbol, series in parsed.items():
            kept = [bar for bar in series if bar.trade_date < end]
            stats["outside_window"] += len(series) - len(kept)
            parsed[symbol] = kept

        for symbol, series in parsed.items():
            codes = by_symbol.get(symbol) or by_symbol.get(symbol.upper())
            if not codes:
                # A SYMBOL WE DID NOT ASK FOR IS NOT A BAR WE CAN FILE. Counted rather than
                # dropped: a non-zero value means the provider renamed something, which is a fact
                # about the response and not about our scopes.
                stats["unattributed"] += len(series)
                continue
            stats["answered"] += len(codes)
            stats["bars"] += len(series) * len(codes)
            rows.extend(
                {
                    "index_code": code,
                    "asked_symbol": symbol,
                    "trade_date": bar.trade_date.isoformat(),
                    "close": bar.close,
                    "dividend": bar.dividend,
                    "provider": "yfinance",
                    "run_id": context.run_id,
                }
                for code in codes
                for bar in series
            )
        stats["empty"] += sum(1 for s in batch if s.upper() not in {k.upper() for k in parsed})

    context.add_output_metadata({**stats, "rows": len(rows), "scopes": len(scopes)})
    # KEYED BY THE BAR'S OWN DAY — this asset is DATE-partitioned, so a `single_run` backfill over
    # several days must write one file per day. It returned a flat list until 2026-09-12, which
    # works for the one-partition path the daily schedule always takes and dies at the write on
    # the first multi-day backfill, after every provider call has been paid for.
    return partitioned.by_partition(context, rows, key=lambda r: str(r["trade_date"]))


@dg.asset(
    # DELIBERATELY UNPARTITIONED, WHICH IS THE SECOND ANSWER TO THIS AND THE RIGHT ONE.
    #
    # finviz answers "as of now" and carries no date, so a date partition claims something the
    # source cannot support: run on 09-11 for the 09-10 partition it returns TODAY's numbers, and
    # the parity comparison duly showed the sector rows 1.3pp from the old table with
    # `new_as_of=2026-09-10 old_as_of=2026-09-11` while both sides had read the same provider.
    #
    # The first fix was a guard refusing a window that had already closed — and it could never have
    # collected anything, because `end_offset` is 0, so the newest materialisable partition is
    # always YESTERDAY and today is always outside it. A guard that can only ever refuse is worse
    # than the defect it replaces.
    #
    # There is exactly one current snapshot. It cannot be backfilled, it cannot be asked about a
    # past day, and the materialisation event is already the record of when it was taken — so the
    # honest model has no date partition at all, and `as_of` comes from the DATA rather than from
    # a partition key. That is the same rule the returns gate forced onto `security_return`.
    # FINVIZ, NOT YFINANCE — a pool is a PROVIDER, and this asset sat on the yfinance pool while
    # calling finviz. The effect was the opposite of the one intended: it serialised against the
    # price lane it shares nothing with, and did not serialise against anything finviz-shaped.
    pool="finviz",
    io_manager_key="parquet_io",
    group_name="indices",
    kinds={"finviz", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="Sector performance as finviz publishes it — a current snapshot, not a series.",
)
def raw_sector_performance(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """THE PROVIDER'S OWN LABEL IS STORED, not the muffin id it maps to.

    Mapping is interpretation and belongs in stage 2. Keeping the raw label means a provider rename
    can be diagnosed from the bytes on disk rather than by asking again — and a rename is the
    realistic failure here, since finviz does not use GICS names and never has.

    A SNAPSHOT CANNOT BE BACKFILLED, AND THE FIRST VERSION QUIETLY PRETENDED IT COULD.

    finviz answers "as of now" and carries no date. Every other asset here can be asked for a past
    day — bars have dates on them — but this one cannot: run on 09-11 for the 09-10 partition, it
    returned TODAY's numbers and stage 2 stamped them 09-10. Caught by the parity comparison, where
    the sector rows sat 1.3pp from the old table with `new_as_of=2026-09-10 old_as_of=2026-09-11`
    while both sides had read the same provider — the gap was a day, not a disagreement.

    So a partition whose window has already closed collects NOTHING, loudly. That is the honest
    answer, and it is the same one the empty-partition marker already encodes: "we looked, there is
    nothing here" rather than a hole. The cost is that the sector lane only ever fills forward,
    which is a property of the source rather than a limitation of the design.
    """
    taken = date.today()
    answer = openbb.sector_performance()
    rows = indices.sector_rows(answer.rows, run_id=context.run_id, taken=taken)
    context.add_output_metadata(
        {
            "groups": len(answer.rows),
            "rows": len(rows),
            "warnings": len(answer.warnings),
            "taken": taken.isoformat(),
        }
    )
    return rows


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

    series: dict[str, list[prices.Bar]] = {}
    for row in _rows(raw_index_bars):
        trade_date = date.fromisoformat(str(row["trade_date"])[:10])
        series.setdefault(str(row["index_code"]), []).append(
            prices.Bar(
                trade_date=trade_date,
                close=float(row["close"]),
                dividend=row.get("dividend"),
            )
        )

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
        {**stats, "rows": len(out), "sector_rows": len(sectors), "unmapped_labels": len(unmapped)}
    )
    return out


# The mirror every lane needs — shared, see `muffin_ingest_dagster.partitioned`.
_rows = partitioned.loaded_rows
