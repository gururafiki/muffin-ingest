"""Asset checks of the segments family.

A CHECK ON A PARTITIONED ASSET IS ONE RESULT FOR THE WHOLE GRID (Dagster 1.13), so each answers
for every filer on every evaluation, never for the partitions a run wrote.
"""

import dagster as dg
from muffin_ingest.facets import cninfo as cn
from muffin_ingest.facets import dart as fd
from muffin_ingest.facets import sec_segments

from muffin_ingest_dagster.defs.segments.partitions import CNINFO_FILER_PARTITION
from muffin_ingest_dagster.defs.segments.raw import CNINFO_FILINGS
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

SEC_SEGMENT = dg.AssetKey("sec_segment")

#: How long a filing may sit in `security_filing` without ever being read. The due sensor ticks
#: every half hour and a wave follows the filer lane within the hour, so a day is a stopped sensor,
#: a wave SEC keeps refusing, or a filing that fails every read.
UNREAD_AFTER_DAYS = 1


@dg.asset_check(asset=SEC_SEGMENT, blocking=False)
def every_sec_filing_was_read(postgres: Postgres) -> dg.AssetCheckResult:
    """No accounts filing carrying XBRL has been held for `UNREAD_AFTER_DAYS` and never read.

    TWO KINDS OF UNREAD, AND ONLY ONE IS A FAULT. A filing never read after a day is one the lane
    keeps failing on (a transport failure on every wave leaves no failed run to see), so it is an
    ERROR, which is what the alert reads. A filing read by an older parser version is a re-read
    an operator started by raising the version: a WARN, with the count that is left.
    """
    with postgres.connect() as conn:
        found = sec_segments.unread(conn, UNREAD_AFTER_DAYS)
    return dg.AssetCheckResult(
        passed=found.never_read == 0 and found.behind_the_parser == 0,
        severity=dg.AssetCheckSeverity.ERROR if found.never_read else dg.AssetCheckSeverity.WARN,
        metadata={
            "never_read": found.never_read,
            "filers": found.filers,
            "oldest_filed": "none"
            if found.oldest_filed is None
            else found.oldest_filed.isoformat(),
            "behind_the_parser": found.behind_the_parser,
        },
    )


CNINFO_SEGMENT = dg.AssetKey("cninfo_segment")

#: How long a full report may sit in `security_filing` never read before the check counts it.
CNINFO_UNREAD_AFTER_DAYS = 7


@dg.asset_check(asset=CNINFO_SEGMENT, blocking=False)
def every_cninfo_report_was_read(postgres: Postgres) -> dg.AssetCheckResult:
    """No full annual report has been held a week and never read, and none is behind the parser.

    A WARN, NOT AN ERROR, WHILE THE EDGE'S BACKLOG DRAINS: on 2026-11-15, 30,915 full reports were
    unread, most for two months, and an ERROR from the first evaluation would sit red for the weeks
    the drain takes, which is how a gate stops being read. It becomes an ERROR once the backlog is
    drained (docs/deferred/2026-11-15-the-china-segment-backlog-drains-by-backfill.md in
    the umbrella).
    """
    with postgres.connect() as conn:
        found = cn.unread(conn, CNINFO_UNREAD_AFTER_DAYS)
    return dg.AssetCheckResult(
        passed=found.never_read == 0 and found.behind_the_parser == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "never_read": found.never_read,
            "securities": found.securities,
            "behind_the_parser": found.behind_the_parser,
        },
    )


@dg.asset_check(asset=CNINFO_FILINGS, blocking=False)
def cninfo_reports_within_cap(
    context: dg.AssetCheckExecutionContext, raw_store: RawStore
) -> dg.AssetCheckResult:
    """The reports held whole stay within the 5 GB cap (Phase 6 decision 4).

    Retention prunes read reports every 15 minutes, and never an unread one, so the total can pass
    the cap while a backfill fetches faster than runs read: a WARN up to half again over it, an
    ERROR beyond, which is a retention that stopped or reports that fail to be read.
    """
    held = 0
    count = 0
    for key in context.instance.get_dynamic_partitions(CNINFO_FILER_PARTITION):
        for row in raw_store.stored_rows_sized(
            CNINFO_FILINGS, key, ("document", "outcome", "pruned_at")
        ):
            if row.get("document") == "report" and row.get("pruned_at") is None:
                held += int(row.get("body_bytes", 0))
                count += row.get("outcome") == "answered"
    over = held > cn.CAP_BYTES
    return dg.AssetCheckResult(
        passed=not over,
        severity=dg.AssetCheckSeverity.ERROR
        if held > cn.CAP_BYTES * 3 // 2
        else dg.AssetCheckSeverity.WARN,
        metadata={"held_bytes": held, "reports_held": count, "cap_bytes": cn.CAP_BYTES},
    )


DART_SEGMENT = dg.AssetKey("dart_segment")

#: How long an annual report may sit in `security_filing` never read before the check counts it. A
#: filer is visited on its 30-day floor, so a month and a week is a stopped lane.
DART_UNREAD_AFTER_DAYS = 37


@dg.asset_check(asset=DART_SEGMENT, blocking=False)
def every_dart_filing_was_read(postgres: Postgres) -> dg.AssetCheckResult:
    """No annual report has been held past a floor cycle and never read, and none is behind the
    parser. A WARN while the registry's newly mapped filers drain (894 Korean equities against the
    443 the edge had found, 2026-11-16); an ERROR once they have."""
    with postgres.connect() as conn:
        found = fd.unread(conn, DART_UNREAD_AFTER_DAYS)
    return dg.AssetCheckResult(
        passed=found.never_read == 0 and found.behind_the_parser == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "never_read": found.never_read,
            "securities": found.securities,
            "behind_the_parser": found.behind_the_parser,
        },
    )
