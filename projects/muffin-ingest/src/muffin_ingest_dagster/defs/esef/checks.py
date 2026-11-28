"""Asset checks of Europe's segment lane."""

import dagster as dg
from muffin_ingest.facets import esef

from muffin_ingest_dagster.lib.resources import Postgres

ESEF_SEGMENT = dg.AssetKey("esef_segment")

#: How long an annual report may sit in `security_filing` never read before the check counts it: a
#: filer is visited on its 30-day floor, so a month and a week is a stopped lane.
UNREAD_AFTER_DAYS = 37


@dg.asset_check(asset=ESEF_SEGMENT, blocking=False)
def every_esef_filing_was_read(postgres: Postgres) -> dg.AssetCheckResult:
    """No annual report has been held past a floor cycle and never read, and none is behind the
    parser. A WARN while the first pass over the grid drains; an ERROR once it has."""
    with postgres.connect() as conn:
        found = esef.unread(conn, UNREAD_AFTER_DAYS)
    return dg.AssetCheckResult(
        passed=found.never_read == 0 and found.behind_the_parser == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "never_read": found.never_read,
            "securities": found.securities,
            "behind_the_parser": found.behind_the_parser,
        },
    )
