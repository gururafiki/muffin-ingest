"""Stage 3 of the discovery family: derived from tables the database already holds."""

import time
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest_dagster.defs.discovery.core import fund_holding, venue_listing
from muffin_ingest_dagster.defs.symbology.core import security_symbology
from muffin_ingest_dagster.lib.resources import Postgres

#: A DAILY FLOOR, because not every writer of the inputs is a Dagster asset.
#: `market.promote_listing` (the app's Track button) mints a security with its share class outside
#: Dagster, and a preference edited in `market.exchange` re-ranks primaries; neither is an upstream
#: materialisation. It sits at an off-minute, clear of the 00:00 lanes and the 03:00 re-ask, and a
#: run over unchanged inputs writes nothing, so the floor costs one ~3 s statement a day.
DAILY_FLOOR = "43 5 * * *"


@dg.asset(
    deps=[venue_listing, security_symbology],
    # EAGER, MINUS THE GATE THAT WOULD KEEP IT FROM EVER FIRING. An unpartitioned asset depends on
    # every upstream partition, and both upstreams always have some unmaterialised: a venue added
    # to `market.exchange` waits for an operator's sweep, and a new symbology subject waits for
    # its rungs. Plain `eager()` would wait for all of them, which is the state `security_return`
    # sat in through a whole history load.
    automation_condition=(
        dg.AutomationCondition.eager().without(~dg.AutomationCondition.any_deps_missing())
        | dg.AutomationCondition.cron_tick_passed(DAILY_FLOOR)
    ).with_label("on an upstream change, or daily"),
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    pool="sql",
    group_name="discovery",
    kinds={"postgres"},
    description=(
        "Each tracked security's listings, derived from the directory by its share class, with "
        "one primary line. No provider call."
    ),
)
def security_listing(
    context: AssetExecutionContext, postgres: Postgres
) -> "dg.MaterializeResult[None]":
    """Calls `market.derive_security_listing()` and records what it did.

    THE RULES LIVE IN THE DATABASE, where CI tests them against real Postgres with fixtures that
    make them disagree (muffin-deployment `tests/a-listing-is-derived-from-the-directory.sql`). The
    function joins the directory to the tracked share classes, retracts what the directory no
    longer says, and picks the primary: the line we price, else the legacy primary venue, else
    the home venue, else none.

    Re-running over unchanged inputs writes nothing, so the asset can fire on every upstream
    change. Measured on production 2026-09-27: 33,859 lines for 11,476 securities in 2.7 s.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute("select market.derive_security_listing()")
        row = cur.fetchone()
    counts: dict[str, Any] = row[0] if row else {}
    context.log.info(
        "listings: %s lines for %s securities; inserted %s, changed %s, retracted %s; "
        "primary by symbol %s, legacy venue %s, home venue %s, none %s",
        counts.get("lines"),
        counts.get("securities"),
        counts.get("inserted"),
        counts.get("changed"),
        counts.get("retracted"),
        counts.get("primary_by_symbol"),
        counts.get("primary_by_legacy_venue"),
        counts.get("primary_by_home_venue"),
        counts.get("without_primary"),
    )
    return dg.MaterializeResult(metadata=counts)


#: Classification's floor, at the edge job's old slot (`muffin-classify`). Most inputs are written
#: outside Dagster: yfinance sectors (`security-profiles`), SEC's SIC code
#: (`security-filing-history`) and the segment tables (`security-segments`) are edge resources, so
#: `fund_holding` is the only upstream materialisation. One minute after the listing floor; both
#: hold the `sql` pool, so they run one after the other.
CLASSIFICATION_FLOOR = "44 5 * * *"

#: The three derivations, in the edge resource's order, each its own transaction as each was its
#: own RPC there. A failure keeps what the earlier ones wrote. Each is SECURITY DEFINER; EXECUTE for
#: `ingest_rw` is muffin-deployment migration 20261003130000.
CLASSIFIERS: tuple[tuple[str, str], ...] = (
    ("classified", "select market.derive_classifications()"),
    ("weighted", "select market.derive_segment_classification()"),
    ("sic", "select market.derive_sic_classification()"),
)


@dg.asset(
    deps=[fund_holding],
    # EAGER, MINUS THE GATE THAT WOULD KEEP IT FROM EVER FIRING: `fund_holding` is partitioned by
    # filing, and a filing the directory names before it has been fetched is a missing partition.
    automation_condition=(
        dg.AutomationCondition.eager().without(~dg.AutomationCondition.any_deps_missing())
        | dg.AutomationCondition.cron_tick_passed(CLASSIFICATION_FLOOR)
    ).with_label("on a fund holding, or daily"),
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    pool="sql",
    group_name="universe",
    kinds={"postgres"},
    description=(
        "Each security's sector and industry memberships, derived from fund holdings, segment "
        "filings and SEC's SIC code. No provider call."
    ),
)
def security_classification(
    context: AssetExecutionContext, postgres: Postgres
) -> "dg.MaterializeResult[None]":
    """Calls the three classification derivations and records what each wrote and how long it took.

    WHY IT MOVED HERE. The edge resource `derive-classifications` failed every daily run from
    2026-09-27: PostgREST's role stops a statement at 8 s, and `derive_segment_classification`
    measured 20.1 s on production on 2026-10-03 (`derive_classifications` 5.2 s,
    `derive_sic_classification` 52 ms). As `ingest_rw` the limit is 120 s, so the durations are
    recorded to show the walk toward it.

    THE RULES LIVE IN THE DATABASE, tested in muffin-deployment
    (`tests/a-weighted-classification-is-not-a-label.sql` and its neighbours).
    """
    recorded: dict[str, Any] = {}
    with postgres.connect() as conn:
        for key, statement in CLASSIFIERS:
            started = time.monotonic()
            with conn.cursor() as cur:
                cur.execute(statement)
                row = cur.fetchone()
            conn.commit()
            recorded[key] = row[0] if row else None
            recorded[f"{key}_ms"] = round((time.monotonic() - started) * 1000)
            context.log.info("%s: %s in %s ms", key, recorded[key], recorded[f"{key}_ms"])
    return dg.MaterializeResult(metadata=recorded)
