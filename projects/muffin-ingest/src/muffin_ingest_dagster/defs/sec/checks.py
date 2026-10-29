"""Asset checks of the SEC family's insider lanes.

A CHECK ON A PARTITIONED ASSET IS ONE RESULT FOR THE WHOLE GRID (Dagster 1.13), so each judges a
window of partitions rather than the one a run wrote.
"""

from datetime import UTC, date, datetime, timedelta

import dagster as dg
from muffin_ingest.facets import insider, sec_index

from muffin_ingest_dagster.defs.sec.partitions import INDEX_LOOKBACK_DAYS, quarter_key
from muffin_ingest_dagster.defs.sec.raw import DATASET, FORM4, INDEX
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: SEC publishes a day's index at ~02:01 UTC the next day and the lane runs at 11:00, so a day two
#: days old is due; a younger one may not be.
DUE_AFTER_DAYS = 2

#: How long after a quarter ends its data set may be missing before it counts: ~7 days measured
#: until 2026q2, which SEC had still not published four months on.
DATASET_LAG_DAYS = 120

#: The Form 4 days the completeness check reads, newest first.
FORM4_DAYS = 7


def _today() -> date:
    return datetime.now(UTC).date()


def _window(today: date) -> list[str]:
    last = today - timedelta(days=DUE_AFTER_DAYS)
    first = max(date(2026, 4, 1), today - timedelta(days=INDEX_LOOKBACK_DAYS))
    return [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]


@dg.asset_check(asset=INDEX, blocking=False)
def every_business_day_has_an_index(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Every day of the last `INDEX_LOOKBACK_DAYS` old enough to be published is collected: a
    weekend's marker, a holiday's listing or the day's index.

    ERROR, because the filer lane's floor and its filing triggers both read this index: if it
    stalls, the SEC family stops refreshing without any run failing.
    """
    held = context.instance.get_materialized_partitions(INDEX)
    due = _window(_today())
    missing = [d for d in due if d not in held]
    return dg.AssetCheckResult(
        passed=not missing,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"days_due": len(due), "missing": ", ".join(missing) or "none"},
    )


@dg.asset_check(asset=DATASET, blocking=False)
def insider_quarters_are_held(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Every quarter since 2006 whose data set is past its publication lag is held.

    WARN: the history is a backfill (step 8), and until it drains this lists what is left.
    """
    held = context.instance.get_materialized_partitions(DATASET)
    today = _today()
    due: list[str] = []
    year, quarter = 2006, 1
    while True:
        # The day after the quarter ends.
        end = date(year + (quarter == 4), quarter % 4 * 3 + 1, 1)
        if end + timedelta(days=DATASET_LAG_DAYS) > today:
            break
        due.append(quarter_key(year, quarter))
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    missing = [q for q in due if q not in held]
    return dg.AssetCheckResult(
        passed=not missing,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "quarters_due": len(due),
            "missing": len(missing),
            "newest_missing": ", ".join(missing[-4:]) or "none",
        },
    )


@dg.asset_check(asset=FORM4, blocking=False)
def form4_days_are_complete(
    context: dg.AssetCheckExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.AssetCheckResult:
    """Each of the last `FORM4_DAYS` collected days holds every Form 4 its index names for a
    tracked CIK, unless its quarter's data set is held. A gap is a stopped or refused run."""
    held_days = context.instance.get_materialized_partitions(FORM4)
    days = sorted((d for d in held_days), reverse=True)[:FORM4_DAYS]
    with postgres.connect() as conn:
        ciks = set(insider.tracked_issuers(conn))
    short: list[str] = []
    named_total = 0
    for day in days:
        when = date.fromisoformat(day)
        if raw_store.stored_rows_for(
            DATASET, quarter_key(when.year, (when.month - 1) // 3 + 1), ("fetched_at",)
        ):
            continue
        named: set[str] = set()
        for row in raw_store.stored_rows_for(INDEX, day, ("document", "body")):
            if row.get("document") == "form_index":
                lines = sec_index.parse(bytes(row["body"]))
                named |= {
                    line.accession for line in sec_index.named(lines, ciks, insider.FORM4_FORMS)
                }
        held = {str(r["accession"]) for r in raw_store.stored_rows_for(FORM4, day, ("accession",))}
        named_total += len(named)
        gap = named - held
        if gap:
            short.append(f"{day}: {len(gap)} of {len(named)}")
    return dg.AssetCheckResult(
        passed=not short,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "days_read": len(days),
            "named": named_total,
            "short_days": "; ".join(short) or "none",
        },
    )
