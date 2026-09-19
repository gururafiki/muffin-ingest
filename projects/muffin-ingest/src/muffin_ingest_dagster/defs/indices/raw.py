"""Stage 1 of the indices family: what the provider sent, kept whole."""

from datetime import date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import indices, prices
from muffin_ingest.providers import openbb

from muffin_ingest_dagster.defs.indices.partitions import index_day
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

#: How far back the bar fetch reaches. The longest period a proxied scope serves is 5y, and
#: `index_at_or_before` needs a bar at or before the window start — so this is 5y plus a margin for
#: a listing that was closed on the anchor date.
LOOKBACK = timedelta(days=1900)


#: Symbols per request. THIS IS OUR CALL COUNT, NOT THE VENDOR'S: `openbb_yfinance` issues one
#: Yahoo request per symbol however many are joined. 62 scopes is 62 vendor requests either way.
BATCH = 20


def _partition_day(row: dict[str, Any]) -> str:
    """Which daily partition a raw row belongs to, read off the provider's own date field.

    FOR PLACEMENT ONLY. Raw stores no derived `trade_date`; a key is needed to decide which file
    a row is written to, which is not the same as storing an interpretation beside it.
    """
    parsed = prices.row_date(row)
    return parsed.isoformat() if parsed is not None else ""


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

        sole = batch[0] if len(batch) == 1 else ""
        parsed = prices.bars_by_symbol(answer.rows, sole)
        # GROUPED, NOT NARROWED — the `Bar` drives the window filter and the counters, while the
        # provider's own row is what reaches raw. This lane kept five fields of the ten yfinance
        # sends, discarding `open`/`high`/`low`/`volume`/`vwap` at stage 1.
        raw_by_symbol = prices.provider_rows_by_symbol(answer.rows, sole)
        # COUNTED HERE, REFUSED IN `index_return`. A lookback series still has a top: asking for
        # 1,900 days up to `end` brings back TODAY's bar, a session in progress, and stamping a
        # country with a mid-session price is the defect this counter exists for. It used to be
        # CUT here, which made stage 1 the judge of what a partition covers — and a row deleted
        # at fetch is one no re-parse can recover.
        stats["outside_window"] += sum(
            1 for series in parsed.values() for bar in series if bar.trade_date >= end
        )

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
                    # THE PROVIDER'S ROW FIRST, our context over the top — AND NOTHING DERIVED.
                    # A parsed `trade_date` used to be written in here, which is an
                    # interpretation of the vendor's own `date` made before anything was stored.
                    # `index_return` parses it, so a corrected date rule costs a re-parse.
                    **dict(row),
                    "index_code": code,
                    "asked_symbol": symbol,
                    "provider": "yfinance",
                    "run_id": context.run.run_id,
                }
                for code in codes
                for row in raw_by_symbol.get(symbol, [])
            )
        stats["empty"] += sum(1 for s in batch if s.upper() not in {k.upper() for k in parsed})

    context.add_output_metadata({**stats, "rows": len(rows), "scopes": len(scopes)})
    # KEYED BY THE BAR'S OWN DAY — this asset is DATE-partitioned, so a `single_run` backfill over
    # several days must write one file per day. It returned a flat list until 2026-09-12, which
    # works for the one-partition path the daily schedule always takes and dies at the write on
    # the first multi-day backfill, after every provider call has been paid for.
    return partitioned.by_partition(context, rows, key=_partition_day)


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
