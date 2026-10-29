"""Asset checks of the SEC family: the insider lanes and the filer lane.

A CHECK ON A PARTITIONED ASSET IS ONE RESULT FOR THE WHOLE GRID (Dagster 1.13), so each judges a
window of partitions rather than the one a run wrote.
"""

from datetime import UTC, date, datetime, timedelta

import dagster as dg
from muffin_ingest.facets import insider, sec_filings, sec_index

from muffin_ingest_dagster.defs.sec.partitions import INDEX_LOOKBACK_DAYS, quarter_key
from muffin_ingest_dagster.defs.sec.raw import DATASET, FORM4, INDEX, SUBMISSIONS
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


# --- the filer lane ------------------------------------------------------------------------------

STATEMENT = dg.AssetKey("sec_statement")

#: How long after its index day a filing may be missing from `security_filing`: the due sensor reads
#: a day once it is collected (11:00 UTC the next day), and the filer's run follows within the hour.
FILING_DUE_DAYS = 2
#: The index days `no_filer_is_behind_its_filings` reads, newest first.
FILING_DAYS = 7

UNKNOWN_CURRENCY = """
select st.source_code, count(*) from market.security_statement st
 where st.currency is not null
   and not exists (select 1 from market.currency c where c.code = st.currency)
 group by 1 order by 1
"""

#: Two statement rows of one source with one fiscal label: one fiscal period plotted twice.
DUPLICATE_LABELS = """
select source_code, count(*) from (
  select source_code from market.security_statement
   where fiscal_period is not null
   group by security_id, source_code, statement, period_type, fiscal_year, fiscal_period
  having count(*) > 1) d
 group by 1 order by 1
"""

UNLABELLED = """
select count(*) filter (where fiscal_period is null), count(*)
  from market.security_statement where source_code = 'sec-xbrl'
"""

HELD_FILINGS = """
select distinct accession_number from market.security_filing where accession_number = any(%s)
"""


def _by_source(postgres: Postgres, sql: str) -> dict[str, int]:
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return {str(source): int(n) for source, n in cur.fetchall()}


@dg.asset_check(asset=STATEMENT, blocking=False)
def statements_carry_a_known_currency(postgres: Postgres) -> dg.AssetCheckResult:
    """No statement row names a currency `market.currency` lacks, whichever lane wrote it.

    ERROR: the metric derivation references `market.currency`, so one such row fails its page and
    every page after it. A Turkish `TRL` did exactly that from 2026-10-21 to 10-24.
    """
    found = _by_source(postgres, UNKNOWN_CURRENCY)
    return dg.AssetCheckResult(
        passed=not found,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"rows_by_source": ", ".join(f"{k} {v}" for k, v in found.items()) or "none"},
    )


@dg.asset_check(asset=STATEMENT, blocking=False)
def one_fiscal_period_one_point(postgres: Postgres) -> dg.AssetCheckResult:
    """No source holds two rows of one statement for one fiscal period.

    ERROR FOR `sec-xbrl` ONLY: its rows are placed on a canonical period of the filer's own
    calendar, so a second row with the same label is a defect in the lane. A Yahoo calendar is
    observed from Yahoo's own period ends, and its duplicates are counted here until the labelling
    drain is measured (step 8).
    """
    found = _by_source(postgres, DUPLICATE_LABELS)
    return dg.AssetCheckResult(
        passed=found.get("sec-xbrl", 0) == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "sec_xbrl_duplicates": found.get("sec-xbrl", 0),
            "by_source": ", ".join(f"{k} {v}" for k, v in found.items()) or "none",
        },
    )


@dg.asset_check(asset=STATEMENT, blocking=False)
def sec_statements_are_labelled(postgres: Postgres) -> dg.AssetCheckResult:
    """Every SEC statement row carries its fiscal period. A row is written only for a canonical
    period, so an unlabelled one is a labeller that did not run, or a calendar that changed under
    it.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(UNLABELLED)
        row = cur.fetchone()
    unlabelled, total = (int(row[0]), int(row[1])) if row else (0, 0)
    return dg.AssetCheckResult(
        passed=unlabelled == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"unlabelled": unlabelled, "rows": total},
    )


@dg.asset_check(asset=SUBMISSIONS, blocking=False)
def no_filer_is_behind_its_filings(
    context: dg.AssetCheckExecutionContext, postgres: Postgres, raw_store: RawStore
) -> dg.AssetCheckResult:
    """Every kept form a tracked filer filed on a recent index day is in `security_filing`.

    A gap is a stopped due sensor, a refused run, or a filer the lane cannot see. Days younger than
    `FILING_DUE_DAYS` are not judged: the sensor reads a day the morning after.
    """
    held_days = context.instance.get_materialized_partitions(INDEX)
    last = (_today() - timedelta(days=FILING_DUE_DAYS)).isoformat()
    days = sorted((d for d in held_days if d <= last), reverse=True)[:FILING_DAYS]
    with postgres.connect() as conn:
        ciks = set(insider.tracked_issuers(conn))
        kept = {code for code, form in sec_filings.forms(conn).items() if form.kept}
        named: dict[str, str] = {}
        for day in days:
            for row in raw_store.stored_rows_for(INDEX, day, ("document", "body")):
                if row.get("document") != "form_index":
                    continue
                for line in sec_index.named(sec_index.parse(bytes(row["body"])), ciks, kept):
                    named[line.accession] = day
        with conn.cursor() as cur:
            cur.execute(HELD_FILINGS, (sorted(named),))
            held = {str(r[0]) for r in cur.fetchall()}
    missing = sorted(a for a in named if a not in held)
    return dg.AssetCheckResult(
        passed=not missing,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "days_read": len(days),
            "filings_named": len(named),
            "missing": len(missing),
            "first_missing": ", ".join(f"{a} ({named[a]})" for a in missing[:5]) or "none",
        },
    )
