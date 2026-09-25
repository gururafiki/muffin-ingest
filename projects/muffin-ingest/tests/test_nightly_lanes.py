"""The 00:00 UTC lanes: where the price sweep resumes, and who is first in the queue.

The sweep's position used to be a pure function of the date, so it could be tested with no state at
all. It is now read back from the runs the schedule launched, so these tests do what the scheduler
does with a night's requests — record one run per request, with the tags the scheduler adds — and
then ask for the next night. Nothing is executed; only the run records matter.
"""

from __future__ import annotations

import itertools
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import Any

import dagster as dg
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
    PRIORITY_TAG,
    RUN_KEY_TAG,
    SCHEDULE_NAME_TAG,
)

from muffin_ingest_dagster.defs.prices import automation as prices_automation
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from tests import loaded_defs

SWEEP = prices_automation.SWEEP_SCHEDULE


def night(day: int) -> datetime:
    return datetime(2026, 9, day, tzinfo=UTC)


def sweep(instance: dg.DagsterInstance, when: datetime) -> Any:
    return prices_automation.nightly_prices(
        dg.build_schedule_context(instance=instance, scheduled_execution_time=when)
    )


def launch(
    instance: dg.DagsterInstance,
    requests: Sequence[dg.RunRequest],
    *,
    status: dg.DagsterRunStatus = dg.DagsterRunStatus.SUCCESS,
) -> None:
    """What the scheduler records for a night: one run per request, carrying the request's tags plus
    the schedule name and run key it adds itself (`_create_scheduler_run` in dagster 1.13.22)."""
    for request in requests:
        assert request.run_key is not None
        instance.add_run(
            dg.DagsterRun(
                job_name=SWEEP,
                run_id=str(uuid.uuid4()),
                status=status,
                tags={**request.tags, SCHEDULE_NAME_TAG: SWEEP, RUN_KEY_TAG: request.run_key},
            )
        )


def launch_old_format(
    instance: dg.DagsterInstance, keys: Sequence[str], day: int, start: int, stop: int, width: int
) -> None:
    """A night as the pre-2026-09-26 code recorded it: range tags and a run key, no sweep tags.

    The shape is production's, read off `run_tags` for the 2026-09-25 night:
    `dagster/run_key=739884-4775`, the two range tags, `dagster/schedule_name=nightly_prices`.
    """
    ordinal = date(2026, 9, day).toordinal()
    for lo in range(start, stop, width):
        instance.add_run(
            dg.DagsterRun(
                job_name=SWEEP,
                run_id=str(uuid.uuid4()),
                status=dg.DagsterRunStatus.SUCCESS,
                tags={
                    ASSET_PARTITION_RANGE_START_TAG: keys[lo],
                    ASSET_PARTITION_RANGE_END_TAG: keys[min(lo + width, stop) - 1],
                    SCHEDULE_NAME_TAG: SWEEP,
                    RUN_KEY_TAG: f"{ordinal}-{lo}",
                },
            )
        )


def spans(requests: Any, keys: Sequence[str]) -> list[tuple[int, int]]:
    """Each run request's range, as inclusive index positions in the partition list."""
    position = {key: i for i, key in enumerate(keys)}
    return [
        (
            position[r.tags[ASSET_PARTITION_RANGE_START_TAG]],
            position[r.tags[ASSET_PARTITION_RANGE_END_TAG]],
        )
        for r in requests
    ]


def covered(requests: Any, keys: Sequence[str]) -> list[int]:
    """Every index a night asks for, in the order its runs ask for them."""
    return [i for lo, hi in spans(requests, keys) for i in range(lo, hi + 1)]


@contextmanager
def grid(keys: Sequence[str], *, slice_size: int, width: int) -> Iterator[dg.DagsterInstance]:
    """An instance holding `keys` as the security grid, with the slice and run width patched."""
    held_slice = prices_automation.SWEEP_SLICE
    held_width = prices_partitions.HISTORY_PARTITIONS_PER_RUN
    prices_automation.SWEEP_SLICE = slice_size
    prices_partitions.HISTORY_PARTITIONS_PER_RUN = width
    try:
        with dg.instance_for_test() as instance:
            instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, list(keys))
            yield instance
    finally:
        prices_automation.SWEEP_SLICE = held_slice
        prices_partitions.HISTORY_PARTITIONS_PER_RUN = held_width


# ── where each night starts ────────────────────────────────────────────────────────────────────


def test_a_growing_grid_does_not_move_the_sweep() -> None:
    """THE DEFECT THIS EXISTS FOR, REPRODUCED WITH PRODUCTION'S OWN NUMBERS.

    The start used to be `(day * SWEEP_SLICE) % len(keys)`. On 2026-09-23 and 09-24 the grid held
    12,267 keys and the nights swept [871, 3371) and [3371, 5871). One security arrived, and on
    09-25 the same formula over 12,268 keys gave [2300, 4800): 1,071 keys from the first night and
    1,429 from the second, i.e. nothing new at all, while 6,056 securities were 8-14 days stale.

    The fixture makes the two rules disagree, and says so: the first two nights come out of the new
    code exactly as production had them, and the old rule's third night is computed here and shown
    to overlap them before the new rule's is shown not to.
    """
    keys = [f"sec-{i:05d}" for i in range(12_267)]
    with grid(keys, slice_size=2500, width=25) as instance:
        first = sweep(instance, night(23))
        launch(instance, first)
        second = sweep(instance, night(24))
        launch(instance, second)

        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, ["sec-new"])
        keys = [*keys, "sec-new"]
        third = sweep(instance, night(25))

    assert (covered(first, keys)[0], covered(first, keys)[-1]) == (871, 3370)
    assert (covered(second, keys)[0], covered(second, keys)[-1]) == (3371, 5870)

    old_start = (date(2026, 9, 25).toordinal() * 2500) % len(keys)
    assert old_start == 2300, "the fixture no longer reproduces the production incident"
    assert set(range(old_start, old_start + 2500)) <= set(covered(first, keys)) | set(
        covered(second, keys)
    ), "under the old rule the third night re-swept the first two, which is the defect"

    tonight = covered(third, keys)
    assert (tonight[0], tonight[-1]) == (5871, 8370), (
        f"the third night started at {tonight[0]}; it must continue after the second night's last "
        f"key (5870) however the grid has grown"
    )
    assert not set(tonight) & (set(covered(first, keys)) | set(covered(second, keys)))


def test_the_first_night_under_this_code_continues_the_old_rotation() -> None:
    """THE TRANSITION IS TONIGHT, AND ITS RUNS CARRY NO SWEEP TAGS.

    The last night the old code launched (2026-09-25) swept [2300, 4800) of 12,268 keys, recorded
    with range tags and a run key only. The first night under this code must read that and carry on
    from 4800, not restart and not fall back to the date.

    THE GRID GROWS BY ONE BEFORE TONIGHT, or the fixture proves nothing: over a constant grid the
    date rule also lands on 4800 — it advances exactly one slice a night until the size changes —
    so a version with no range-end fallback passed this test until the growth was added.
    """
    keys = [f"sec-{i:05d}" for i in range(12_268)]
    with grid(keys, slice_size=2500, width=25) as instance:
        launch_old_format(instance, keys, 25, 2300, 4800, 25)
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, ["sec-new"])
        keys = [*keys, "sec-new"]
        tonight = sweep(instance, night(26))

    by_date = (date(2026, 9, 26).toordinal() * 2500) % len(keys)
    assert by_date != 4800, "the fixture cannot tell the range-end fallback from the date rule"
    indices = covered(tonight, keys)
    assert (indices[0], indices[-1], len(indices)) == (4800, 7299, 2500)
    assert len(tonight) == 100
    assert {r.tags[prices_automation.SWEEP_NIGHT_TAG] for r in tonight} == {"2026-09-26"}
    assert {r.tags[prices_automation.SWEEP_LAST_TAG] for r in tonight} == {keys[7299]}


def test_a_re_evaluated_night_asks_for_exactly_what_it_asked_before() -> None:
    """THE DAEMON RETRIES A TICK THAT DIED HALF-WAY, and by then some of tonight's runs exist.

    If tonight's own runs counted as "the previous night", the retry would start after them and
    launch a second, different slice under new run keys, which Dagster would not recognise as
    duplicates. Excluding tonight is what keeps the run keys identical, so Dagster skips them.
    """
    keys = [f"sec-{i:02d}" for i in range(30)]
    with grid(keys, slice_size=10, width=4) as instance:
        launch(instance, sweep(instance, night(24)))
        first = sweep(instance, night(25))
        launch(instance, first[:2])  # the tick died after launching two of its three runs
        retried = sweep(instance, night(25))

    def fingerprint(requests: Any) -> list[tuple[Any, ...]]:
        return [
            (
                r.run_key,
                r.tags[ASSET_PARTITION_RANGE_START_TAG],
                r.tags[ASSET_PARTITION_RANGE_END_TAG],
            )
            for r in requests
        ]

    assert fingerprint(retried) == fingerprint(first)


def test_a_night_that_reaches_the_end_of_the_grid_wraps_without_straddling_it() -> None:
    """Every night asks for a full slice, so one reaching the end continues from the start — and a
    RunRequest can only name a contiguous range, so no run may span the end of the list."""
    keys = [f"sec-{i:02d}" for i in range(9)]
    with grid(keys, slice_size=4, width=3) as instance:
        first = sweep(instance, night(24))  # by date: (739883 * 4) % 9 = 8, so it wraps at once
        launch(instance, first)
        second = sweep(instance, night(25))
        launch(instance, second)
        third = sweep(instance, night(26))

    assert covered(first, keys) == [8, 0, 1, 2], "a slice reaching the end continues from the start"
    assert covered(second, keys) == [3, 4, 5, 6]
    assert covered(third, keys) == [7, 8, 0, 1]
    for requests in (first, second, third):
        for lo, hi in spans(requests, keys):
            assert lo <= hi, f"a run straddles the end of the grid: {lo}..{hi}"

    keys = [f"sec-{i:02d}" for i in range(10)]
    with grid(keys, slice_size=4, width=3) as instance:
        launch(instance, sweep(instance, night(24)))  # [2, 6)
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, ["sec-10", "sec-11"])
        keys = [*keys, "sec-10", "sec-11"]
        launch(instance, sweep(instance, night(25)))  # [6, 10)
        straddling = sweep(instance, night(26))

    assert covered(straddling, keys) == [10, 11, 0, 1], (
        "the new keys at the end are reached first, then the slice wraps to the start"
    )
    for lo, hi in spans(straddling, keys):
        assert lo <= hi, f"a run straddles the end of the grid: {lo}..{hi}"
    assert {r.tags[prices_automation.SWEEP_LAST_TAG] for r in straddling} == {"sec-01"}


def test_a_failed_night_still_advances_the_sweep() -> None:
    """A NIGHT IS ANCHORED ON WHAT IT REQUESTED, NOT ON WHAT SUCCEEDED.

    Retrying a failed slice every night looks safer and stops the rotation for the whole universe
    the first time a slice fails for a reason of its own — the 2026-09-21 OOM was exactly that.
    """
    keys = [f"sec-{i:02d}" for i in range(10)]
    with grid(keys, slice_size=4, width=4) as instance:
        failed = sweep(instance, night(24))  # [2, 6)
        launch(instance, failed, status=dg.DagsterRunStatus.FAILURE)
        # GROWN SO THE DATE RULE DISAGREES. Over a constant grid it lands on 6 as well, and a
        # version that ignored failed nights passed this test until the growth was added.
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, ["x-0", "x-1", "x-2"])
        keys = [*keys, "x-0", "x-1", "x-2"]
        tonight = sweep(instance, night(25))

    assert (date(2026, 9, 25).toordinal() * 4) % len(keys) != 6, "the rules agree; fixture is inert"
    assert covered(failed, keys) == [2, 3, 4, 5]
    assert covered(tonight, keys) == [6, 7, 8, 9]


def test_the_lookback_is_spent_on_this_schedules_runs_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hundreds of other runs land between two nights — symbology backfills alone are ~70 a batch.

    Reading the newest runs of every job would push last night's out of the page, and the sweep
    would fall back to the date rule and lose its position. The fixture puts more foreign runs after
    the previous night than the page holds, and the date rule's answer differs from the right one.
    """
    monkeypatch.setattr(prices_automation, "RESUME_LOOKBACK", 5)
    keys = [f"sec-{i:02d}" for i in range(10)]
    with grid(keys, slice_size=4, width=4) as instance:
        launch(instance, sweep(instance, night(24)))  # [2, 6)
        for _ in range(8):
            instance.add_run(
                dg.DagsterRun(
                    job_name="security_symbology_job",
                    run_id=str(uuid.uuid4()),
                    status=dg.DagsterRunStatus.SUCCESS,
                    tags={RUN_KEY_TAG: "739884-0"},
                )
            )
        tonight = sweep(instance, night(26))

    by_date = (date(2026, 9, 26).toordinal() * 4) % 10
    assert by_date != 6, "the fixture cannot tell the two rules apart"
    assert covered(tonight, keys)[0] == 6


def test_an_unreadable_history_falls_back_to_the_date_rather_than_to_zero() -> None:
    """Runs of the schedule exist but none can be placed in a night (a tag format this code does not
    recognise). Restarting at 0 would sweep the same first slice every night for ever, every run
    green; the date rule at least keeps moving."""
    keys = [f"sec-{i:02d}" for i in range(10)]
    with grid(keys, slice_size=4, width=4) as instance:
        instance.add_run(
            dg.DagsterRun(
                job_name=SWEEP,
                run_id=str(uuid.uuid4()),
                status=dg.DagsterRunStatus.SUCCESS,
                tags={SCHEDULE_NAME_TAG: SWEEP, RUN_KEY_TAG: "by-hand"},
            )
        )
        tonight = sweep(instance, night(25))

    assert covered(tonight, keys)[0] == (date(2026, 9, 25).toordinal() * 4) % 10 == 6


def test_every_key_is_reached_within_a_cycle_and_consecutive_nights_never_overlap() -> None:
    keys = [f"sec-{i:02d}" for i in range(10)]
    nights: list[list[int]] = []
    with grid(keys, slice_size=4, width=3) as instance:
        for day in (21, 22, 23):
            requests = sweep(instance, night(day))
            launch(instance, requests)
            nights.append(covered(requests, keys))

    for before, after in itertools.pairwise(nights):
        assert not set(before) & set(after), f"consecutive nights overlap: {before} / {after}"
    assert set().union(*nights) == set(range(10)), "a key was not reached within the cycle"


# ── how each night is cut into runs ────────────────────────────────────────────────────────────


def test_no_run_covers_more_partitions_than_the_measured_memory_budget() -> None:
    """THE DEFECT THIS EXISTS FOR, AND IT REACHED PRODUCTION.

    The sweep asked for its whole slice in ONE RunRequest, and the first scheduled night died:
    2026-09-21 00:06:57, `Killed process (python) anon-rss 2,199,804 kB` against the container's
    2.5 GiB, the run failing at 00:07:08 with `ChildProcessCrashException`. No bars were published
    for three days.

    `raw_price_history` declares `BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN)` precisely
    to bound that — 96 securities of full history measured 683,391 bars and OOM at 2.4 GB, 25
    measured ~178k rows and ~600 MB. **A backfill policy does not apply to a schedule's
    RunRequest**, so naming a wide range walks straight past the one number that was measured to
    stop this, and nothing warns.

    The fixture makes the two rules disagree: a slice FOUR TIMES the run width, so a schedule
    emitting one request per slice yields a 12-partition run and one emitting per width yields
    four of 3. The grid is one slice wide, so the slice is the whole grid.
    """
    keys = [f"sec-{i:03d}" for i in range(12)]
    with grid(keys, slice_size=12, width=3) as instance:
        requests = sweep(instance, night(19))

    found = spans(requests, keys)
    assert len(found) == 4, f"the slice was not cut into runs of the width: {found}"
    for lo, hi in found:
        assert hi - lo + 1 <= 3, (
            f"a run covers {hi - lo + 1} partitions against a measured budget of 3 — this is the "
            f"shape that OOM-killed the first scheduled night"
        )
    # AND THE SLICE IS STILL WHOLE. Bounding the runs must not silently collect less: cutting a
    # slice into runs that skip securities would look identical in every counter.
    assert sorted(covered(requests, keys)) == list(range(12))


def test_each_run_of_a_night_is_asked_for_once() -> None:
    """Two different nights must not share a run key, or Dagster skips the second night's runs.

    THE GRID IS ONE SLICE WIDE SO EVERY NIGHT COVERS THE SAME RANGES, and that is the whole
    fixture: a run key built from the range alone would then be identical across nights. The first
    version of this test used a longer grid and PASSED with the date prefix deleted.
    """
    keys = [f"sec-{i:03d}" for i in range(12)]
    with grid(keys, slice_size=12, width=3) as instance:
        first = sweep(instance, night(19))
        again = sweep(instance, night(19))
        launch(instance, first)
        next_night = sweep(instance, night(20))

    run_keys = [str(r.run_key) for r in first]
    assert len(set(run_keys)) == len(run_keys), "two runs of one night share a run key"
    assert run_keys == [str(r.run_key) for r in again]
    assert not set(run_keys) & {str(r.run_key) for r in next_night}, (
        "a later night reuses a run key, so Dagster would skip that part of the sweep"
    )


def test_the_sweep_skips_rather_than_failing_before_any_security_exists() -> None:
    with dg.instance_for_test() as instance:
        assert isinstance(sweep(instance, night(19)), dg.SkipReason)


# ── who is first in the queue at 00:00 ─────────────────────────────────────────────────────────


def test_the_short_daily_lanes_are_first_in_line_for_the_sql_pool() -> None:
    """`daily_fx` (~25 s) and `daily_indices` (~20 s) are queued beside ~100 price runs, all needing
    the `sql` pool at limit 1. The coordinator starts the highest `dagster/priority` first, so these
    two must outrank every other job — measured without it, 6,185 s and 6,069 s of waiting."""
    repository = loaded_defs().get_repository_def()
    priority = {
        name: int(repository.get_job(name).run_tags.get(PRIORITY_TAG, "0"))
        for name in repository.job_names
    }
    short = {"daily_fx", "daily_indices"}
    assert short <= set(priority), (
        f"a short lane is no longer a job: {sorted(short - set(priority))}"
    )
    lowest_short = min(priority[name] for name in short)
    outranking = sorted(n for n, p in priority.items() if n not in short and p >= lowest_short)
    assert lowest_short > 0 and not outranking, (
        f"priorities {priority}: the short lanes must outrank everything else, {outranking} do not"
    )
