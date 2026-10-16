"""Asset checks of the companies family."""

from typing import Any

import dagster as dg

from muffin_ingest_dagster.defs.companies.core import yahoo_statement
from muffin_ingest_dagster.defs.companies.partitions import ROTATION_DAYS
from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

FUNDAMENTALS = dg.AssetKey("security_fundamentals")

#: How old a company's facts may be before the rotation is judged to be leaving it behind: twice the
#: rotation, so one refused day or a slow week never reads as a stopped lane.
STALE_AFTER_DAYS = 2 * ROTATION_DAYS

#: What fraction of the equities may be stale before the check fails rather than warns. Equities
#: with no Yahoo symbol are never visited and are counted stale; they were ~5% on 2026-10-03.
STALE_FRACTION = 0.10

STALE_COMPANIES = """
select count(*) filter (where f.as_of is null or f.as_of < now() - make_interval(days => %s))::int,
       count(*)::int
  from market.security s
  left join market.security_fundamentals f on f.security_id = s.security_id
 where s.security_type_code = 'equity'
"""


@dg.asset_check(asset=FUNDAMENTALS, blocking=False)
def no_company_is_far_behind_the_rotation(postgres: Postgres) -> dg.AssetCheckResult:
    """Is the weekly rotation reaching every company? Staleness per company, as the price lane.

    UNTIL THE FIRST ROTATION ENDS THIS READS RED, and that is the truth: the edge fetched most
    companies' fundamentals once, in August. It should fall to the share of equities Yahoo cannot
    answer for within seven days of the schedule starting, and then stay there.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(STALE_COMPANIES, (STALE_AFTER_DAYS,))
        stale, equities = (cur.fetchone() or (0, 0))[:2]
    share = (stale / equities) if equities else 0.0
    return dg.AssetCheckResult(
        passed=share <= STALE_FRACTION,
        severity=dg.AssetCheckSeverity.WARN
        if share <= STALE_FRACTION * 2
        else dg.AssetCheckSeverity.ERROR,
        metadata={
            "stale_companies": stale,
            "equities": equities,
            "stale_share": round(share, 4),
            "stale_after_days": STALE_AFTER_DAYS,
        },
    )


#: Statement rows this run's documents stated, for this run's companies. A row's `as_of` is the
#: fetch time of the newest document that supplied it, so a row stated in this run is newer than the
#: run's start; an older period this run did not restate keeps its older stamp.
RUN_STATEMENTS = """
select count(*)::int, count(*) filter (where currency is null)::int
  from market.security_statement
 where security_id = any(%s::uuid[])
   and source_code = %s
   and as_of >= to_timestamp(%s)
"""


@dg.asset_check(asset=yahoo_statement, blocking=False)
def statements_carry_a_currency(
    context: dg.AssetCheckExecutionContext, postgres: Postgres
) -> dg.AssetCheckResult:
    """Every statement row this run wrote names its currency.

    The edge wrote 154,629 Yahoo statement rows and not one carried a currency, because openbb kept
    only the value of each point; Yahoo states `currencyCode` on every point. A null here means the
    parse lost it again.
    """
    keys = partitioned.run_partition_keys(context, SECURITY_PARTITION)
    record = context.instance.get_run_record_by_id(context.run.run_id)
    started = record.start_time if record and record.start_time else None
    if not keys or started is None:
        return dg.AssetCheckResult(passed=True, metadata={"checked": 0, "note": "no run scope"})
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(RUN_STATEMENTS, (keys, "yfinance", started))
        rows, missing = (cur.fetchone() or (0, 0))[:2]
    return dg.AssetCheckResult(
        passed=missing == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"rows_this_run": rows, "without_currency": missing, "companies": len(keys)},
    )


#: field -> (convention, low, high) for the MEDIAN, from muffin-deployment's
#: `check_fundamental_units.py` (measured 2026-08-18: profit margin 0.1275, dividend yield 2.74,
#: return on equity 0.1435). A unit flip moves every row together, so the median jumps ~100x; a
#: per-row band would flag holding companies and special dividends that are real.
BANDS: dict[str, tuple[str, float, float]] = {
    "profit_margin": ("fraction", 0.01, 1.0),
    "return_on_equity": ("fraction", 0.01, 1.0),
    "dividend_yield": ("percent", 0.30, 30.0),
}

#: Fewer values than this say nothing about a distribution.
MIN_VALUES = 50

#: The lane's own rows: written in the last rotation and a day. The edge's are months older.
LANE_MEDIANS = """
select percentile_cont(0.5) within group (order by {field}), count({field})::int
  from market.security_fundamentals
 where source_code = 'yfinance' and as_of >= now() - make_interval(days => %s)
"""


@dg.asset_check(asset=FUNDAMENTALS, blocking=False)
def fundamental_units_are_conventional(postgres: Postgres) -> dg.AssetCheckResult:
    """The median of each unit-sensitive column sits in its convention's band.

    Read over the lane's own rows, so the edge's August rows cannot hold a flip in the lane below
    the threshold. Too few values passes with a note: there is nothing yet to judge.
    """
    metadata: dict[str, Any] = {}
    failed: list[str] = []
    with postgres.connect() as conn, conn.cursor() as cur:
        for field, (convention, low, high) in BANDS.items():
            cur.execute(LANE_MEDIANS.format(field=field), (ROTATION_DAYS + 1,))
            median, n = cur.fetchone() or (None, 0)
            metadata[f"{field}_n"] = n
            if median is None or n < MIN_VALUES:
                metadata[f"{field}_median"] = "too few values"
                continue
            metadata[f"{field}_median"] = round(float(median), 4)
            if not (low <= float(median) <= high):
                hint = "a fraction served as a percent" if median > high else "the reverse"
                failed.append(f"{field} {float(median):.4f} outside [{low}, {high}]: {hint}")
                metadata[f"{field}_expected"] = f"{convention} in [{low}, {high}]"
    return dg.AssetCheckResult(
        passed=not failed,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={**metadata, "failures": "; ".join(failed) or "none"},
    )
