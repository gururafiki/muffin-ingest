"""Asset checks of the prices family."""

import dagster as dg

from muffin_ingest_dagster.defs.prices.automation import SWEEP_SLICE
from muffin_ingest_dagster.defs.prices.core import price_bar_history
from muffin_ingest_dagster.defs.prices.raw import raw_price_bars
from muffin_ingest_dagster.lib.resources import Postgres


@dg.asset_check(asset=raw_price_bars, blocking=False)
def every_askable_security_was_asked(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """THE DAY LANE'S CLAIM, KEPT WHILE THE DAY LANE IS KEPT.

    It is no longer what production runs — `nightly_prices` sweeps the security grid instead — but
    the lane it checks is still defined and still rollback-able, and a check retired ahead of the
    asset it guards would make that rollback silent. It goes when `raw_price_bars` goes.
    """
    key = raw_price_bars.key
    event = context.instance.get_latest_materialization_events([key]).get(key)
    materialization = event.asset_materialization if event is not None else None
    unasked = 0
    partition = "none"
    subjects = 0
    if materialization is not None:
        partition = materialization.partition or "none"
        unasked = int(getattr(materialization.metadata.get("unasked"), "value", 0) or 0)
        subjects = int(getattr(materialization.metadata.get("subjects"), "value", 0) or 0)

    return dg.AssetCheckResult(
        passed=unasked == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "unasked": unasked,
            "subjects": subjects,
            "partition": partition,
            "note": "a partition claims its whole cross-section; unasked subjects make that false",
        },
    )


#: How stale a security's newest bar may be before the sweep is judged to be falling behind.
#:
#: DERIVED FROM THE SWEEP, NOT CHOSEN. The round-robin covers the universe in
#: `ceil(universe / SWEEP_SLICE)` nights, so anything inside that cycle is a security whose turn
#: has simply not come round, and only beyond it is something actually wrong. Doubling it leaves
#: room for weekends and for one refused night without crying wolf — which this project has paid
#: for twice, because the cost of a red gate is never the ignored check, it is the next true
#: positive behind it.
STALE_AFTER_DAYS = 14

#: What fraction of the universe may be stale before the check fails rather than warns.
STALE_FRACTION = 0.10

STALE_SECURITIES = """
select count(*)::int
  from market.security s
  left join lateral (
        select max(p.trade_date) as newest
          from market.price_bar p
         where p.security_id = s.security_id
       ) latest on true
 where s.security_type_code = 'equity'
   and (latest.newest is null or latest.newest < current_date - %s)
"""

ASKABLE_EQUITIES = """
select count(*)::int from market.security where security_type_code = 'equity'
"""


@dg.asset_check(asset=price_bar_history, blocking=False)
def no_security_is_far_behind_the_sweep(postgres: Postgres) -> dg.AssetCheckResult:
    """THE COMPLETENESS CLAIM MOVED HERE WHEN THE DAY PARTITION WENT, and it changed shape with it.

    While the lane was partitioned by DAY, a materialized partition claimed the whole cross-section
    for that day, and the check asked whether every askable security had been asked. That question
    belonged to the grid. Now the grid answers a better one per security — is this subject
    collected? — and the question the DATA cannot answer about itself is the opposite one: is any
    security being left behind by the rotation?

    A ROUND-ROBIN MAKES A SINGLE DAY LEGITIMATELY INCOMPLETE. Only ~`SWEEP_SLICE` securities are
    asked a night, so counting bars per trade date would report every day as ~20% collected and be
    red for ever against a lane working exactly as designed. Staleness per SECURITY is the honest
    measure, and it is the one a reader of the app would notice.

    WARN BELOW THE FRACTION, ERROR ABOVE IT. A handful of stale securities is ordinary — a symbol
    the provider does not carry, a delisting nobody has retracted yet — and the check exists to
    catch the rotation stopping, not to relitigate the negative cache.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(STALE_SECURITIES, (STALE_AFTER_DAYS,))
        stale = int((cur.fetchone() or (0,))[0])
        cur.execute(ASKABLE_EQUITIES)
        universe = int((cur.fetchone() or (0,))[0])

    share = (stale / universe) if universe else 0.0
    return dg.AssetCheckResult(
        passed=share <= STALE_FRACTION,
        severity=dg.AssetCheckSeverity.WARN
        if share <= STALE_FRACTION * 2
        else dg.AssetCheckSeverity.ERROR,
        metadata={
            "stale_securities": stale,
            "equities": universe,
            "stale_share": round(share, 4),
            "stale_after_days": STALE_AFTER_DAYS,
            "sweep_slice": SWEEP_SLICE,
            "note": "a round-robin leaves a day legitimately partial; staleness per security is "
            "the claim that still means something",
        },
    )
