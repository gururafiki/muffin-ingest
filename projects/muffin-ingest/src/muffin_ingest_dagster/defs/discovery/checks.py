"""Asset checks of the discovery family."""

from typing import Any

import dagster as dg

from muffin_ingest_dagster.defs.discovery.partitions import SWEEP_PARTITIONS
from muffin_ingest_dagster.defs.discovery.raw import raw_exchange_sweep
from muffin_ingest_dagster.lib.io_managers import RawStore


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
    """
    keys = _partitions(context)
    unfinished: list[str] = []
    pages = 0
    never_swept: list[str] = []
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

    stalled = sorted(unfinished + never_swept)
    return dg.AssetCheckResult(
        passed=not stalled,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "venues": len(keys),
            "pages_held": pages,
            "unfinished": len(unfinished),
            "never_swept": len(never_swept),
            # NAMED, NOT JUST COUNTED — the names are the backfill selection, and a count alone
            # sends the reader to look them up by hand.
            "resume_these": ", ".join(stalled[:20]) or "none",
            "note": "a venue whose last page still carries a cursor has more to fetch; "
            "re-materialise the partition and it resumes from the file",
        },
    )


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
