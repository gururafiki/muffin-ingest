"""The segments family's job and sensor.

`sec_instances_due` SHIPS STOPPED. It is started in code once the lane has read a tiny subset live
and the edge's `security-segments` resource is retired: two writers never read one queue.
"""

from collections.abc import Mapping, Sequence

import dagster as dg
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import sec_segments

from muffin_ingest_dagster.defs.sec.partitions import (
    FILERS_PER_RUN,
    SEC_FILER_PARTITION,
    cik_key,
    sec_filers,
)
from muffin_ingest_dagster.defs.segments.core import sec_segment
from muffin_ingest_dagster.defs.segments.raw import raw_sec_instance
from muffin_ingest_dagster.lib.resources import Postgres
from muffin_ingest_dagster.lib.runtime import SEC_RUN

#: THE NAMES ARE STATE: a job's name keys its run history, a sensor's its cursor and status.
SEC_SEGMENTS_JOB = "sec_segments"

#: THE MOST FILINGS ONE TICK MAY SET IN MOTION. The busiest measured day brought 506 accounts
#: filings (2026-11-05, the 10-Q deadline week), so four times that is still a day. Above it the
#: queue is a history: every filing after a parser version is raised (148,425 hold XBRL), whose
#: documents kept whole are ~25 GB. That is an operator's backfill, paced and sized for the disk,
#: and a sensor that started it by itself would do so at whatever hour the version changed.
MAX_DUE_PER_TICK = 2_000

#: A FILER'S INSTANCES AND THEIR SEGMENTS IN ONE RUN: stage 2 reads whatever raw holds that is due,
#: so a filing stage 1 fetched is written before the run ends, and a failed write is retried by
#: the next tick's visit without another request.
sec_segments_job = dg.define_asset_job(
    SEC_SEGMENTS_JOB,
    selection=dg.AssetSelection.assets(raw_sec_instance, sec_segment),
    partitions_def=sec_filers,
    run_tags=SEC_RUN,
)


def chunks_due(grid: Sequence[str], due: Mapping[int, int]) -> list[list[str]]:
    """The `FILERS_PER_RUN`-aligned chunks of the grid that hold a filer with a filing due.

    A run covers a contiguous range, and Dagster gives a scattered set one run per key (254 runs
    for 255 subjects, measured 2026-10-06). Chunks aligned to grid positions bound a day at 76
    runs, and each run asks only for what is due.
    """
    position = {key: n for n, key in enumerate(grid)}
    found = sorted(
        {position[k] // FILERS_PER_RUN for k in (cik_key(cik) for cik in due) if k in position}
    )
    return [list(grid[c * FILERS_PER_RUN : (c + 1) * FILERS_PER_RUN]) for c in found]


@dg.sensor(
    job=sec_segments_job,
    minimum_interval_seconds=1800,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Filers with an accounts filing whose XBRL instance has not been read.",
)
def sec_instances_due(
    context: dg.SensorEvaluationContext, postgres: Postgres
) -> dg.SensorResult | dg.SkipReason:
    """Requests the filer chunks holding a filing to read, one wave at a time.

    THE QUEUE IS `security_filing`'s OWN MARKS, so a filing a run could not read (a refusal, a
    spent budget, a failed write) is simply still due at the next tick. Nothing is requested while
    a run of the job is queued or running: a wave finishes before the next is counted, so a slow
    wave is never doubled.

    THE CURSOR COUNTS WAVES and is part of each run's key, so a tick evaluated twice launches a
    chunk once, and the next wave may ask for the same chunk again.
    """
    unfinished = context.instance.get_runs(
        filters=dg.RunsFilter(job_name=SEC_SEGMENTS_JOB, statuses=list(NOT_FINISHED_STATUSES)),
        limit=1,
    )
    if unfinished:
        return dg.SkipReason(f"run {unfinished[0].run_id[:8]} of the last wave has not finished")
    with postgres.connect() as conn:
        due = sec_segments.due_by_filer(conn)
    filings = sum(due.values())
    if not due:
        return dg.SkipReason("no filing is due")
    if filings > MAX_DUE_PER_TICK:
        return dg.SkipReason(
            f"{filings} filings of {len(due)} filers are due, more than {MAX_DUE_PER_TICK}: a "
            "queue that size is a history, which is a paced backfill and never one tick"
        )
    grid = context.instance.get_dynamic_partitions(SEC_FILER_PARTITION)
    chunks = chunks_due(grid, due)
    wave = int(context.cursor or 0) + 1
    context.log.info(
        "wave %s: %s filings of %s filers due, in %s runs", wave, filings, len(due), len(chunks)
    )
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=f"{wave}:{keys[0]}",
                tags={
                    ASSET_PARTITION_RANGE_START_TAG: keys[0],
                    ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                    **SEC_RUN,
                },
            )
            for keys in chunks
        ],
        cursor=str(wave),
    )
