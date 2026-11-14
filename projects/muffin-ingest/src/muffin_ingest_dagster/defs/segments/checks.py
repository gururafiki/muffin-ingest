"""Asset checks of the segments family.

A CHECK ON A PARTITIONED ASSET IS ONE RESULT FOR THE WHOLE GRID (Dagster 1.13), so each answers
for every filer on every evaluation, never for the partitions a run wrote.
"""

import dagster as dg
from muffin_ingest.facets import sec_segments

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
