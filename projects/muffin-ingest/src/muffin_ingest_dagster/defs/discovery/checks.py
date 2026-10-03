"""Asset checks of the discovery family."""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
from muffin_ingest.facets import openfigi

from muffin_ingest_dagster.defs.discovery.derived import security_listing
from muffin_ingest_dagster.defs.discovery.partitions import SWEEP_PARTITIONS
from muffin_ingest_dagster.defs.discovery.queries import directory_queries
from muffin_ingest_dagster.defs.discovery.raw import raw_exchange_sweep
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: How far a walk that ENDED may fall short of the provider's own `total` and still count as
#: finished. `total` is re-counted on every page, so a venue that gains or loses a listing during a
#: walk ends a few rows off: measured 2026-09-27, GR held 14,202 against 14,205. The cap it exists
#: to catch is thousands (the US held 15,000 of 20,096).
CAP_DRIFT_ROWS = 25


@dg.asset_check(asset=raw_exchange_sweep, blocking=False)
def venue_sweep_reached_its_last_page(
    context: dg.AssetCheckExecutionContext, raw_store: RawStore
) -> dg.AssetCheckResult:
    """Did each query's walk get all the way to the end, or did it stop part-way?

    A walk that stopped on a 429 is a perfectly ordinary materialized partition, and the freshness
    policy cannot see it either, because a half-walked query is "recently materialized" the moment
    it stops. This is the `exchange-listings` 429 defect and the DART windowed-sweep lesson in the
    one form that closes them: a REFUSED SWEEP MUST NOT READ AS A FINISHED ONE. A partition whose
    last page still carries a `cursor_at` has more of the query to fetch, and says so.
    `unfinished_sweeps` resumes exactly these, from the same field.

    WARN, NOT ERROR, AND NOT BLOCKING: an unfinished walk is the expected state while a refresh or a
    resume is under way, so failing the run would make every refresh red. The names are the point.

    THE CAPPED VERDICT LIVES IN `directory_query_within_the_cap` SINCE 2026-10-04. A capped walk is
    finished — the provider issues no cursor past 15,000 results — and re-walking cannot help it,
    so it does not belong among the walks to resume.
    """
    keys = _every_query(context)
    unfinished: list[str] = []
    pages = 0
    never_swept: list[str] = []
    for key in keys:
        stored = list(raw_store.stored_rows_for(raw_exchange_sweep.key, key, columns=["cursor_at"]))
        pages += len(stored)
        if not stored:
            # NOT THE SAME FAILURE, AND NOT PASSED EITHER. A partition with no file at all is one
            # the check ran against before its asset did; saying "finished" would be a claim about
            # a query nobody has asked the provider.
            never_swept.append(key)
            continue
        if stored[-1].get("cursor_at"):
            unfinished.append(key)

    return dg.AssetCheckResult(
        passed=not (unfinished or never_swept),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "queries": len(keys),
            "pages_held": pages,
            "unfinished": len(unfinished),
            "never_swept": len(never_swept),
            # NAMED, NOT JUST COUNTED, and named APART: `unfinished_sweeps` resumes the first list
            # from each file's cursor, while a query never walked has no cursor and is the
            # automation condition's (or, after a failed run, an operator's).
            "resume_these": ", ".join(sorted(unfinished)[:20]) or "none",
            "not_yet_walked": ", ".join(sorted(never_swept)[:20]) or "none",
            "note": "a query whose last page still carries a cursor has more to fetch; "
            "unfinished_sweeps resumes it from the file within the hour",
        },
    )


@dg.asset_check(asset=raw_exchange_sweep, blocking=False)
def directory_query_within_the_cap(
    context: dg.AssetCheckExecutionContext, raw_store: RawStore, postgres: Postgres
) -> dg.AssetCheckResult:
    """Did a finished walk hold what the provider counted, and if not, does another query cover it?

    "NO CURSOR LEFT" IS NOT "FINISHED" WHEN THE PROVIDER STOPPED ISSUING THEM. OpenFIGI answers at
    most 15,000 results per `/v3/filter` query ("Max Amount of Pages: 150", ordered by FIGI), and
    its last page then carries no `next`. The US walk passed as finished on 2026-09-21 holding
    15,000 of 20,096 and missing every US FIGI newer than `BBG013JYT8V4` — about every listing
    since 2022. Only the provider's own `total` tells the two apart.

    A CAPPED QUERY IS COVERED when an alias asks the same type, is filed under the same venue and
    has FINISHED its own walk: `US.arca` (NYSE Arca) lists every exchange-listed US stock, each line
    naming its US line. That passes, naming the alias — honestly partial, because OTC lines past the
    cap stay unreached and no OpenFIGI filter narrows OTC enough (umbrella docs/deferred/2026-09-27-
    the-us-directory-stops-at-15000.md). An alias still walking covers nothing yet. A capped query
    nothing covers FAILS: the remedy is a `market.directory_alias` row, and Frankfurt (14,205 of
    15,000 in September) is the next expected.

    SEPARATE FROM `venue_sweep_reached_its_last_page` so that check names only walks a resume can
    help. Non-blocking and WARN: a cap is a coverage gap, not bad data.
    """
    keys = _every_query(context)
    with postgres.connect() as conn:
        queries = directory_queries(conn)
    covered: list[str] = []
    uncovered: list[str] = []
    for key in keys:
        query = queries.get(key)
        if query is None:
            continue  # a stale key; `query_for` names the remedy wherever a run touches it
        stored = list(raw_store.stored_rows_for(raw_exchange_sweep.key, key))
        if not stored or stored[-1].get("cursor_at"):
            continue  # not finished: the other check's subject
        held, total = _held_and_total(stored)
        if total is None or total - held <= max(CAP_DRIFT_ROWS, total // 1000):
            continue
        aliases = sorted(
            q.key
            for q in queries.values()
            if q.maps_to_composite
            and q.key != key
            and q.files_under == query.files_under
            and q.security_type2 == query.security_type2
        )
        described = f"{key} ({held} of {total})"
        finished = [a for a in aliases if _walk_finished(raw_store, a)]
        if finished:
            covered.append(f"{described} covered by {', '.join(finished)}")
        elif aliases:
            uncovered.append(f"{described}, {', '.join(aliases)} not finished")
        else:
            uncovered.append(described)

    return dg.AssetCheckResult(
        passed=not uncovered,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "queries": len(keys),
            "capped": len(covered) + len(uncovered),
            "capped_and_covered": "; ".join(sorted(covered)) or "none",
            "capped_and_uncovered": "; ".join(sorted(uncovered)) or "none",
            "note": "a capped walk ended short of the provider's own total at OpenFIGI's "
            "15,000-result limit; only a narrower query reaches the rest, added as a "
            "market.directory_alias row",
        },
    )


def _held_and_total(stored: Sequence[Mapping[str, Any]]) -> tuple[int, int | None]:
    """The results a walk's pages hold, and the provider's `total` read off the newest page."""
    held = 0
    total: int | None = None
    for row in stored:
        results, page_total = openfigi.filter_page_counts(bytes(row["body"]))
        held += results
        if page_total is not None:
            total = page_total
    return held, total


def _walk_finished(raw_store: RawStore, key: str) -> bool:
    """A walk is finished when it holds pages and the last one carries no cursor."""
    stored = list(raw_store.stored_rows_for(raw_exchange_sweep.key, key, columns=["cursor_at"]))
    return bool(stored) and not stored[-1].get("cursor_at")


def _every_query(context: dg.AssetCheckExecutionContext) -> list[str]:
    """Every registered query, whichever run evaluates the check.

    A CHECK ON A PARTITIONED ASSET RECORDS ONE RESULT FOR THE WHOLE ASSET in Dagster 1.13 — it is
    unpartitioned unless declared with a preview `partitions_def`. This used to answer for the
    run's partitions only, believing the result landed beside them; it did not, so the check's
    status was whichever query ran last, and a stalled walk passed the moment any other query
    finished. Answering for the grid every time makes the latest status the grid's status. The
    cost is reading one column of each walk's file.
    """
    instance: Any = context.instance
    return sorted(instance.get_dynamic_partitions(SWEEP_PARTITIONS))


#: Per legacy primary venue, where the derived primary landed. `market.listing` is the edge's table
#: and nothing writes it any more. Stage 2c renames it `listing_legacy`; this check then reads that
#: name, and goes when the table does.
LISTING_AGAINST_LEGACY = """
with legacy as (
  select l.security_id, l.exch_code from market.listing l where l.is_primary
), derived as (
  select sl.security_id, v.exch_code
    from market.security_listing sl
    join market.venue_listing v using (figi)
   where sl.is_primary
), lines as (
  select distinct security_id from market.security_listing
)
select lg.exch_code,
       count(*) filter (where d.exch_code = lg.exch_code),
       count(*) filter (where d.exch_code <> lg.exch_code),
       count(*) filter (where d.security_id is null and ln.security_id is not null),
       count(*) filter (where ln.security_id is null)
  from legacy lg
  left join derived d using (security_id)
  left join lines ln using (security_id)
 group by lg.exch_code
"""


@dg.asset_check(asset=security_listing, blocking=False)
def listing_covers_legacy(postgres: Postgres) -> dg.AssetCheckResult:
    """Does every security the legacy table gives a primary listing get one when derived?

    TRANSITIONAL, AND IT GATES STAGE 2C, which makes `market.listing` a view over
    `security_listing`. Two views read its primary row: `security_symbol` (the symbol the app
    shows) and `security_currency` (the currency a price is labelled in). A security the legacy
    table places and the derivation does not would lose both at the swap.

    THREE STATES, AND ONLY ONE FAILS. Measured on production before the derivation shipped:
    - PLACED, on the legacy venue (10,983) or another one (54). Another venue is expected where the
      held symbol names a sibling line: AT and AU both spell `.AX`.
    - LOST (182): the security has derived lines and none became primary. This is what the swap
      would break. 177 of the 182 are US, because the directory stops at OpenFIGI's 15,000-result
      cap and holds no US line for them.
    - LEGACY ONLY (899): no derived line at all, because the security has no share class or none of
      its lines is in the directory. Reported, not failed: 2c keeps their legacy rows.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(LISTING_AGAINST_LEGACY)
        rows = cur.fetchall()
    same = sum(int(r[1]) for r in rows)
    other = sum(int(r[2]) for r in rows)
    lost = sum(int(r[3]) for r in rows)
    legacy_only = sum(int(r[4]) for r in rows)
    lost_by_venue = sorted(
        ((str(r[0]), int(r[3])) for r in rows if r[3]), key=lambda x: (-x[1], x[0])
    )
    return dg.AssetCheckResult(
        passed=lost == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "legacy_primaries": same + other + lost + legacy_only,
            "same_venue": same,
            "other_venue": other,
            "lost": lost,
            "legacy_only": legacy_only,
            # NAMED BY VENUE, because the cause is usually a venue: the US lines past the cap.
            "lost_by_legacy_venue": ", ".join(f"{v} {n}" for v, n in lost_by_venue[:10]) or "none",
            "note": "a LOST security has derived lines and no derived primary, and would lose its "
            "display symbol and currency when market.listing becomes a view over security_listing",
        },
    )
