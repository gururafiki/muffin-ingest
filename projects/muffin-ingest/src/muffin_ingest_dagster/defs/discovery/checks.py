"""Asset checks of the discovery family."""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
from muffin_ingest.facets import openfigi

from muffin_ingest_dagster.defs.discovery.derived import security_listing
from muffin_ingest_dagster.defs.discovery.partitions import SWEEP_PARTITIONS
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
    """Did this venue's walk get all the way to the end, or did it stop part-way?

    WHY THIS EXISTS AT ALL. Resuming a sweep is an operator action — `new_exchange_sweeps` only
    ever ADDS venues, and its own comment says "re-materialising them is an operator's call". That
    is a sound design and it was missing its other half: NOTHING could tell a finished venue from a
    stalled one. A venue that stopped on page 40 or on a 429 is a perfectly ordinary materialized
    partition, and the freshness policy cannot see it either, because a half-swept venue is
    "recently materialized" the moment it stops. So the operator would have re-walked all 59 to be
    safe, or none of them.

    This is the `exchange-listings` 429 defect and the DART windowed-sweep lesson, in the one form
    that closes them: a REFUSED SWEEP MUST NOT READ AS A FINISHED ONE. A partition whose last page
    still carries a `cursor_at` has more of the venue to fetch, and says so.

    WARN, NOT ERROR, AND NOT BLOCKING. An unfinished venue is the expected state during a load — a
    large venue exceeds `SWEEP_MAX_PAGES` by design and takes several runs — so failing the run
    would make every load red. The check's job is to be READABLE: the partitions that fail are
    exactly the backfill selection.

    "NO CURSOR LEFT" IS NOT "FINISHED" WHEN THE PROVIDER STOPPED ISSUING THEM. OpenFIGI answers at
    most 15,000 results per `/v3/filter` query, ordered by FIGI, and its last page then carries no
    `next`. This check passed the US on 2026-09-21 while the walk held 15,000 of the 20,096 listings
    the provider's own `total` reported, missing every US FIGI newer than `BBG013JYT8V4` — about
    every listing since 2022. So a walk that ended short of its `total` is CAPPED. It is reported
    apart from the unfinished venues because re-walking it cannot help: the query has to be
    narrower (umbrella `docs/deferred/2026-09-27-the-us-directory-stops-at-15000.md`).
    """
    keys = _partitions(context)
    unfinished: list[str] = []
    pages = 0
    never_swept: list[str] = []
    capped: list[str] = []
    for exch_code in keys:
        stored = list(raw_store.stored_rows_for(raw_exchange_sweep.key, exch_code))
        pages += len(stored)
        if not stored:
            # NOT THE SAME FAILURE, AND NOT PASSED EITHER. A partition with no file at all is one
            # the check ran against before its asset did; saying "finished" would be a claim about
            # a venue nobody has asked the provider about.
            never_swept.append(exch_code)
            continue
        if stored[-1].get("cursor_at"):
            unfinished.append(exch_code)
            continue
        held, total = _held_and_total(stored)
        if total is not None and total - held > max(CAP_DRIFT_ROWS, total // 1000):
            capped.append(f"{exch_code} ({held} of {total})")

    stalled = sorted(unfinished + never_swept)
    return dg.AssetCheckResult(
        passed=not stalled and not capped,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "venues": len(keys),
            "pages_held": pages,
            "unfinished": len(unfinished),
            "never_swept": len(never_swept),
            # NAMED, NOT JUST COUNTED — the names are the backfill selection, and a count alone
            # sends the reader to look them up by hand.
            "resume_these": ", ".join(stalled[:20]) or "none",
            "capped": len(capped),
            "capped_venues": ", ".join(sorted(capped)) or "none",
            "note": "a venue whose last page still carries a cursor has more to fetch; "
            "re-materialise the partition and it resumes from the file. A CAPPED venue ended "
            "short of the provider's own total at OpenFIGI's 15,000-result limit, and only a "
            "narrower query reaches the rest",
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


def _partitions(context: dg.AssetCheckExecutionContext) -> list[str]:
    """The venues this evaluation is about: the run's, or every registered one.

    A check on a partitioned asset runs with the run's partition context, which is what makes the
    result land beside the partition it describes. Evaluated outside one — a bare check run — it
    answers for the whole grid, which is the question an operator asks.
    """
    if context.has_partition_key:
        return [context.partition_key]
    if context.has_partition_key_range:
        return list(context.partition_keys)
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
