"""Asset checks of Japan's segment lane."""

import dagster as dg
from muffin_ingest.facets import edinet as fe

from muffin_ingest_dagster.lib.resources import Postgres

EDINET_SEGMENT = dg.AssetKey("edinet_segment")

#: How long an annual report may sit in `security_filing` never read before the check counts it.
#: The due sensor asks every half hour, so a week is a sensor stopped or a report that fails every
#: read.
UNREAD_AFTER_DAYS = 7


@dg.asset_check(asset=EDINET_SEGMENT, blocking=False)
def every_edinet_report_was_read(postgres: Postgres) -> dg.AssetCheckResult:
    """No annual report has been held a week and never read, and none is behind the parser.

    A WARN WHILE THE HISTORY DRAINS: ten years of reports are filed the day the registry maps their
    companies, and an ERROR from the first evaluation would sit red for the weeks the backfill
    takes, which is how a gate stops being read. It becomes an ERROR once the history is read.
    """
    with postgres.connect() as conn:
        found = fe.unread(conn, UNREAD_AFTER_DAYS)
    return dg.AssetCheckResult(
        passed=found.never_read == 0 and found.behind_the_parser == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "never_read": found.never_read,
            "securities": found.securities,
            "behind_the_parser": found.behind_the_parser,
        },
    )
