"""Every scheduled run carries a time limit, sized from what its job measured.

`run_monitoring` ends a run past `dagster/max_runtime`, or past the instance's six-hour default when
the run carries none. The limit has to be on the RUN, and a schedule reaches its runs in two ways:
a job's `run_tags`, and a plain `ScheduleDefinition`'s own `tags`. Both are checked here, together
with one evaluation proving the second way really lands on the run request rather than only
labelling the schedule in the UI.
"""

from __future__ import annotations

import dagster as dg
from dagster._core.storage.tags import MAX_RUNTIME_SECONDS_TAG

from muffin_ingest_dagster.defs.platform.heartbeat import heartbeat_schedule
from muffin_ingest_dagster.defs.prices.automation import SWEEP_SCHEDULE
from muffin_ingest_dagster.lib import runtime
from tests import loaded_defs


def _limits() -> dict[str, str | None]:
    repo = loaded_defs().get_repository_def()
    limits: dict[str, str | None] = {}
    for schedule in repo.schedule_defs:
        job = repo.get_job(schedule.job_name)
        tags = {**job.run_tags, **schedule.tags}
        limits[schedule.name] = tags.get(MAX_RUNTIME_SECONDS_TAG)
    return limits


def test_every_scheduled_run_has_a_time_limit() -> None:
    """A schedule added without one would run under the six-hour default, unnoticed."""
    limits = _limits()
    assert limits, "no schedules were loaded"
    missing = sorted(name for name, limit in limits.items() if limit is None)
    assert not missing, f"scheduled runs with no time limit: {missing}"


def test_the_price_sweep_gets_room_for_a_full_history_and_the_rest_are_short() -> None:
    """THE TWO SIZES ARE DIFFERENT ON PURPOSE. A nightly price run that finds securities with
    nothing stored fetches each one's whole history, and its slowest real run took 579 s. Held to
    the short jobs' five minutes, those runs would end every night and the securities would never
    get a history, so the sweep must keep its own, longer limit.
    """
    limits = _limits()
    assert limits[SWEEP_SCHEDULE] == runtime.PRICE_RUN[MAX_RUNTIME_SECONDS_TAG]
    others = {name: limit for name, limit in limits.items() if name != SWEEP_SCHEDULE}
    assert set(others.values()) == {runtime.SHORT_RUN[MAX_RUNTIME_SECONDS_TAG]}, others
    assert float(runtime.PRICE_RUN[MAX_RUNTIME_SECONDS_TAG]) >= 3 * 579, "below 3x the slowest run"


def test_a_plain_schedule_s_tags_reach_its_run_request() -> None:
    """`ScheduleDefinition(tags=…)` labels the schedule, and reaches its runs only when the schedule
    has no execution function. Evaluated rather than read, so a Dagster change to that rule fails
    here instead of leaving the heartbeat, the registries and the fund directory unlimited.

    A CONTEXT MANAGER, WITH NO INSTANCE OF ITS OWN. Built over `instance_for_test()` and left open,
    the context kept a global definitions-state storage bound to that closed instance, and the next
    test file to load `Definitions` failed on "Attempted to resolve undefined DagsterInstance
    weakref" — in another test, for a reason it had nothing to do with.
    """
    with dg.build_schedule_context() as context:
        result = heartbeat_schedule.evaluate_tick(context)
    requests = result.run_requests or []
    assert len(requests) == 1
    limit = runtime.SHORT_RUN[MAX_RUNTIME_SECONDS_TAG]
    assert requests[0].tags.get(MAX_RUNTIME_SECONDS_TAG) == limit
