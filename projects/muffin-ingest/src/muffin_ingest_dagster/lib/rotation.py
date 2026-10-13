"""A round-robin over the security grid, one slice a day, resumed from the runs a schedule launched.

Two lanes rotate over the same grid: the nightly price sweep (`nightly_prices`, ~5 days) and the
company lane (`yahoo_companies`, 7 days). The mechanism is one, so it lives here once; each lane
keeps its own names, tags and measurements beside its schedule.

A `RunRequest` covers one partition or one CONTIGUOUS RANGE, so a slice is a contiguous run of keys
cut into ranges of the lane's measured width. The partition list is in insertion order
(`get_dynamic_partitions` orders by id), so growth appends at the end and never moves a key.

EACH DAY STARTS AFTER THE LAST KEY THE PREVIOUS DAY ASKED FOR. The price sweep once started at
`(day * slice) % len(keys)`, which advances one slice a day only while the grid does not change
size: adding ONE security re-mapped every future slice (measured 2026-09-25, a night swept
[2300, 4800), 1,071 keys from the night before and 1,429 from the night before that, while 6,056
securities sat 8-14 days stale). Anchored on a KEY, a day's position cannot be disturbed by growth.

THE POSITION LIVES IN DAGSTER'S RUN STORAGE. A schedule has no cursor (`ScheduleEvaluationContext`
exposes none), but the runs it launched are durable state it can read back.

A DAY'S SLICE WRAPS PAST THE END OF THE GRID, so every day asks for the same number of keys and the
provider allowance is spent evenly. A run never straddles the wrap: the slice is cut at the end of
the list, so each run stays one contiguous range.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date

import dagster as dg
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
    RUN_KEY_TAG,
    SCHEDULE_NAME_TAG,
)


@dataclass(frozen=True)
class Rotation:
    """Which schedule rotates, and the two tags its runs carry so the next day can resume."""

    #: The schedule's name. Its runs are found by the tag the scheduler stamps with it, so a rename
    #: restarts the rotation.
    schedule: str
    #: The day a run belongs to, as an ISO date. Ours rather than parsed out of Dagster's tags: the
    #: run key's format exists for idempotence, and `.dagster/scheduled_execution_time` is hidden.
    day_tag: str
    #: The last key that day asked for, so tomorrow can read where today stopped from any one run.
    last_tag: str
    #: How many of the schedule's newest runs to read. It must hold two days of runs: a
    #: re-evaluation of today's tick may already have launched all of today's.
    lookback: int


@dataclass(frozen=True)
class Slice:
    """What one tick asks for, and why it starts where it does."""

    requests: list[dg.RunRequest]
    start: int
    size: int
    #: Which rule chose `start`, for the tick's log.
    source: str
    segments: list[tuple[int, int]]


def slice_requests(
    instance: dg.DagsterInstance,
    keys: Sequence[str],
    rotation: Rotation,
    *,
    day: date,
    size: int,
    width: int,
    tags: Mapping[str, str] | None = None,
) -> Slice:
    """Today's slice of `size` keys from where yesterday stopped, as runs of at most `width`.

    THE WIDTH IS A RUN'S MEMORY OR TIME BUDGET, and it has to be applied here: a backfill policy
    does not apply to a schedule's `RunRequest`. The price sweep once asked for its whole slice in
    one run and was OOM-killed (2026-09-21, 2.2 GB against 2.5 GB).

    THE RUN KEY IS `<day ordinal>-<offset>`, unique per day and per range, so a re-tick of the same
    day is idempotent rather than a second copy of the slice.
    """
    count = len(keys)
    start, source = resume_at(instance, keys, rotation, day=day, size=size)
    size = min(size, count)
    last = keys[(start + size - 1) % count]
    segments = [(start, min(start + size, count))]
    if start + size > count:
        segments.append((0, start + size - count))
    requests = [
        dg.RunRequest(
            run_key=f"{day.toordinal()}-{lo}",
            # THE RANGE TAGS ARE HOW ONE RUN COVERS MANY PARTITIONS; `partition_key` names one.
            tags={
                ASSET_PARTITION_RANGE_START_TAG: keys[lo],
                ASSET_PARTITION_RANGE_END_TAG: keys[min(lo + width, hi) - 1],
                rotation.day_tag: day.isoformat(),
                rotation.last_tag: last,
                **(tags or {}),
            },
        )
        for seg_lo, hi in segments
        for lo in range(seg_lo, hi, width)
    ]
    return Slice(requests=requests, start=start, size=size, source=source, segments=segments)


def resume_at(
    instance: dg.DagsterInstance,
    keys: Sequence[str],
    rotation: Rotation,
    *,
    day: date,
    size: int,
) -> tuple[int, str]:
    """Where today starts, and which rule decided it.

    THE PREVIOUS DAY IS THE NEWEST ONE STRICTLY BEFORE TODAY. Today's own runs are excluded, so
    re-evaluating a tick (the daemon retries one that died half-way) computes the same start and so
    the same run keys, which Dagster then skips as already launched.

    A DAY IS ANCHORED ON WHAT IT REQUESTED, NOT ON WHAT SUCCEEDED. A failed day still advances the
    rotation; otherwise a slice that fails for a reason of its own (the 2026-09-21 OOM) would stop
    the rotation for the whole universe. Each lane's staleness check reports a slice that keeps
    failing.

    ONLY THIS SCHEDULE'S RUNS COUNT. A backfill or a hand-launched run carries no
    `dagster/schedule_name`, so an operator's run over some range does not move the rotation.

    THREE RULES, IN ORDER, AND THE LAST ONE CANNOT GET STUCK:

    1. the previous day's `last_tag`, the normal case;
    2. the highest range end among the previous day's runs, for a day whose anchor key has since
       been deleted;
    3. the date rotation, when no previous day can be read at all: a new instance, runs deleted, or
       tags this code no longer recognises. Restarting at 0 instead would ask for the same first
       slice every day for ever, with every run green.
    """
    count = len(keys)
    position = {key: i for i, key in enumerate(keys)}
    runs = instance.get_runs(
        filters=dg.RunsFilter(tags={SCHEDULE_NAME_TAG: rotation.schedule}),
        limit=rotation.lookback,
    )
    by_day: dict[date, list[dg.DagsterRun]] = {}
    for run in runs:
        ran = day_of(run, rotation)
        if ran is not None and ran < day:
            by_day.setdefault(ran, []).append(run)

    if by_day:
        previous = by_day[max(by_day)]
        for run in previous:
            anchor = run.tags.get(rotation.last_tag)
            if anchor in position:
                return (position[anchor] + 1) % count, "after the previous day's last key"
        ends = [
            position[end]
            for run in previous
            if (end := run.tags.get(ASSET_PARTITION_RANGE_END_TAG)) in position
        ]
        if ends:
            return (max(ends) + 1) % count, "after the previous day's highest range end"

    return (day.toordinal() * size) % count, "no previous day readable; by date"


def day_of(run: dg.DagsterRun, rotation: Rotation) -> date | None:
    """The day a run of this schedule belongs to, from our tag or, before it existed, the run key.

    The run key is `<date ordinal>-<offset>`; it is read only for runs that predate the tag.
    """
    tagged = run.tags.get(rotation.day_tag)
    if tagged:
        try:
            return date.fromisoformat(tagged)
        except ValueError:
            return None
    prefix = (run.tags.get(RUN_KEY_TAG) or "").split("-", 1)[0]
    if prefix.isdigit():
        try:
            return date.fromordinal(int(prefix))
        except ValueError:
            return None
    return None
