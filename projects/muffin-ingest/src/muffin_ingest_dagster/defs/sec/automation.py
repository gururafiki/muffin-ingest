"""The SEC family's jobs, schedules and sensors.

Everything that asks SEC shipped STOPPED until the lanes had run live on a tiny subset (Phase 5
step 6, 2026-10-29). What runs now, and why the rest does not:
- `new_sec_filers` asks nothing; it only makes the filer grid match the CIKs we hold.
- `sec_daily_index` runs (since 2026-10-30, step 7a). Its Form 4 rows land in `insider_trade` beside
  the edge's, and `security_insider_summary` counts an edge row only on a day the lane has not
  collected (muffin-deployment#478); before that it counted both, so this waited for it.
- `sec_filers_due` waits for the CIK grid's backfill (step 8). On its first tick every filer never
  collected is due, which is the whole grid in ~75 runs holding `sql` for ~5 hours from whenever it
  starts; started by a roll in the evening, that runs into the 00:00 price night.
- `sec_insider_datasets` runs (since 2026-10-30): weekly, every quarter from the Form 4 grid's start
  that is not held. A quarter SEC has not published ends its run claiming nothing rather than
  failing it (umbrella note
  `docs/deferred/2026-10-30-the-insider-data-set-schedule-fails-until-sec-publishes.md`).
"""

from collections.abc import Mapping, Sequence
from datetime import date, timedelta

import dagster as dg
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import insider, sec_filings, sec_index

from muffin_ingest_dagster.defs.sec.core import (
    sec_filer_facts,
    sec_form4_trade,
    sec_insider_trade,
)
from muffin_ingest_dagster.defs.sec.partitions import (
    DUE_DAYS_PER_TICK,
    FILERS_PER_RUN,
    INDEX_LOOKBACK_DAYS,
    SEC_FILER_PARTITION,
    cik_key,
    insider_quarters,
    on_floor,
    quarter_key,
    sec_days,
    sec_filers,
)
from muffin_ingest_dagster.defs.sec.raw import (
    COMPANYFACTS,
    DATASET,
    FORM4,
    INDEX,
    raw_sec_daily_index,
    raw_sec_filer,
    raw_sec_form4,
    raw_sec_insider_dataset,
)
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from muffin_ingest_dagster.lib.runtime import SEC_DATASET_RUN, SEC_RUN

#: THE NAMES ARE STATE: schedule and job names key Dagster's run history and instigator state.
INDEX_SCHEDULE = "sec_daily_index"
DATASET_SCHEDULE = "sec_insider_datasets"
FILERS_JOB = "sec_filers"

#: A DAY'S INDEX, ITS FORM 4S AND THEIR TRADES IN ONE RUN, so a day's filings land together.
sec_daily_index_job = dg.define_asset_job(
    INDEX_SCHEDULE,
    selection=dg.AssetSelection.assets(raw_sec_daily_index, raw_sec_form4, sec_form4_trade),
    partitions_def=sec_days,
    run_tags=SEC_RUN,
)

#: A FILER'S DOCUMENTS AND ITS TABLES IN ONE RUN: stage 2 parses only the filers whose documents
#: this run fetched, so it must run in the same run as stage 1.
sec_filers_job = dg.define_asset_job(
    FILERS_JOB,
    selection=dg.AssetSelection.assets(raw_sec_filer, sec_filer_facts),
    partitions_def=sec_filers,
    run_tags=SEC_RUN,
)

sec_insider_datasets_job = dg.define_asset_job(
    DATASET_SCHEDULE,
    selection=dg.AssetSelection.assets(raw_sec_insider_dataset, sec_insider_trade),
    partitions_def=insider_quarters,
    run_tags=SEC_DATASET_RUN,
)


@dg.schedule(
    name=INDEX_SCHEDULE,
    job=sec_daily_index_job,
    # 11:00 UTC: SEC publishes a day's index at ~02:01 UTC, and the price night (00:00), the
    # symbology re-ask (03:00) and the company lane (03:30-~06:00) are done.
    cron_schedule="0 11 * * *",
    # RUNNING SINCE 2026-10-30 (Phase 5 step 7a), declared here so a reset of Dagster's state
    # cannot stop it, once `security_insider_summary` picked between writers (module docstring).
    # Live on 10-26..10-28: one listing and one day per run, every Form 4 answered.
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Yesterday's index and Form 4s, and any day of the last ten not collected.",
)
def sec_daily_index(context: dg.ScheduleEvaluationContext) -> Sequence[dg.RunRequest]:
    """One run per day: yesterday, plus each day of the last `INDEX_LOOKBACK_DAYS` not wholly
    collected. One run per day, because the Form 4 lane is asked one day at a time.

    NOT WHOLLY COLLECTED: the index or the trades never materialised (the trades follow the Form 4s
    in the same run, so a day without Form 4s has no trades), or the Form 4 run left some of the
    day's filings unanswered (`short_form4_days`). A run stops on SEC's first 403, 429 or 5xx, and
    a re-run asks only for what the day's file lacks.

    THE RUN KEY CARRIES THE TICK'S DAY. Dagster launches a run key once per schedule, so a key of
    the partition alone would never ask a day twice, however short it was left.
    """
    today = context.scheduled_execution_time.date()
    instance = context.instance
    first = max(sec_days.start.date(), today - timedelta(days=INDEX_LOOKBACK_DAYS))
    days = [(first + timedelta(days=n)).isoformat() for n in range((today - first).days)]
    held = [instance.get_materialized_partitions(k) for k in (INDEX, sec_form4_trade.key)]
    short = short_form4_days(instance, days)
    yesterday = (today - timedelta(days=1)).isoformat()
    return [
        dg.RunRequest(run_key=f"{INDEX_SCHEDULE}:{today.isoformat()}:{day}", partition_key=day)
        for day in days
        if day == yesterday or day in short or any(day not in h for h in held)
    ]


def short_form4_days(instance: dg.DagsterInstance, days: Sequence[str]) -> set[str]:
    """The days whose newest Form 4 run left some of the day's filings unanswered.

    READ FROM THE RUN'S OWN COUNTERS: its outcomes sum to `named`, so anything beside `answered`
    and `held_already` is a filing the day's file does not hold. Measured 2026-10-30, SEC answered
    7 of 23,033 backfill requests with a 503 or 504, each stopping its day with 30-642 Form 4s
    unasked, and all seven answered 200 when asked again.
    """
    short: set[str] = set()
    for day in days:
        records = instance.fetch_materializations(
            dg.AssetRecordsFilter(asset_key=FORM4, asset_partitions=[day]), limit=1
        ).records
        newest = records[0].asset_materialization if records else None
        if newest is None:
            continue
        count = {k: v.value for k, v in newest.metadata.items()}
        answered = _count(count, "form4_answered") + _count(count, "form4_held_already")
        if _count(count, "named") > answered:
            short.add(day)
    return short


def _count(metadata: Mapping[str, object], name: str) -> int:
    value = metadata.get(name)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


#: THE FIRST QUARTER THE SCHEDULE OWNS: the one the Form 4 grid starts in. Its data set replaces
#: the Form 4 rows for its days. Earlier quarters are history, an operator backfill (step 8), so a
#: schedule turned on can never launch eighty quarters and ~5 GB by itself.
FIRST_SCHEDULED_QUARTER = quarter_key(sec_days.start.year, (sec_days.start.month - 1) // 3 + 1)


@dg.schedule(
    name=DATASET_SCHEDULE,
    job=sec_insider_datasets_job,
    # WEEKLY, NOT DAILY: a quarter's set came ~7 days after the quarter until 2026q2, which SEC had
    # not published four months on. A quarterly product is checked weekly.
    cron_schedule="45 11 * * 1",
    # RUNNING SINCE 2026-10-30, declared here so a reset of Dagster's state cannot stop it.
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Each completed quarter's insider data set from 2026q2 on, until SEC publishes it.",
)
def sec_insider_datasets(context: dg.ScheduleEvaluationContext) -> Sequence[dg.RunRequest]:
    """Every completed quarter from `FIRST_SCHEDULED_QUARTER` that is not held, oldest first.

    EVERY QUARTER, NOT THE NEWEST: SEC can publish an older quarter after a newer one has ended, and
    asking only the newest would never ask for it again.

    THE RUN KEY CARRIES THE TICK'S DAY. Dagster launches a run key once per schedule, so a key of
    the quarter alone would ask an unpublished quarter on its first Monday and never again.
    """
    keys = insider_quarters.get_partition_keys(
        current_time=context.scheduled_execution_time, dynamic_partitions_store=context.instance
    )
    held = context.instance.get_materialized_partitions(DATASET)
    tick = context.scheduled_execution_time.date().isoformat()
    return [
        dg.RunRequest(run_key=f"{DATASET_SCHEDULE}:{tick}:{key}", partition_key=key)
        for key in keys
        if key >= FIRST_SCHEDULED_QUARTER and key not in held
    ]


@dg.sensor(
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="A CIK an equity holds becomes a filer partition.",
)
def new_sec_filers(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """Keeps the filer grid in step with the CIKs our equities hold. ADDS KEYS AND REQUESTS NOTHING.

    A key is never deleted: a CIK is permanent, and one no equity holds any more is `not_askable`
    in the lane rather than gone from its history. Running in code: it asks SEC nothing.
    """
    with postgres.connect() as conn:
        ciks = insider.tracked_issuers(conn)
    existing = set(context.instance.get_dynamic_partitions(SEC_FILER_PARTITION))
    # Sorted, so a first pass appends the grid in CIK order and its 50-filer chunks are stable.
    new = sorted(k for k in (cik_key(c) for c in ciks) if k not in existing)
    context.log.info("%s filers held, %s new", len(ciks), len(new))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[sec_filers.build_add_request(new)] if new else [],
    )


def filers_due(
    day: date,
    index_bodies: Sequence[bytes],
    grid: Sequence[str],
    ciks: Sequence[int],
    forms: dict[str, sec_filings.Form],
    collected: set[str],
) -> tuple[set[str], dict[str, str], dict[str, str]]:
    """The filers one index day makes due, and why.

    DUE: a tracked CIK the day's index names with a kept form (a `refreshes_facts` form also makes
    its company facts due), the filers whose floor day it is, and every filer never collected.
    Returns the due keys and, per key, the day its submissions and company facts were filed.
    """
    kept = {code for code, f in forms.items() if f.kept}
    facts_forms = {code for code, f in forms.items() if f.refreshes_facts}
    tracked = set(ciks)
    filed_sub: dict[str, str] = {}
    filed_facts: dict[str, str] = {}
    for body in index_bodies:
        for line in sec_index.named(sec_index.parse(body), tracked, kept):
            filed_sub[cik_key(line.cik)] = day.isoformat()
            if line.form in facts_forms:
                filed_facts[cik_key(line.cik)] = day.isoformat()
    in_grid = set(grid)
    due = {k for k in filed_sub if k in in_grid}
    due |= {k for k in grid if on_floor(k, day) or k not in collected}
    return due, filed_sub, filed_facts


@dg.sensor(
    job=sec_filers_job,
    minimum_interval_seconds=900,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Filers that filed, whose floor day it is, or that were never collected.",
)
def sec_filers_due(
    context: dg.SensorEvaluationContext, postgres: Postgres, raw_store: RawStore
) -> dg.SensorResult:
    """Reads each newly collected index day and requests the filer chunks holding a due filer.

    THE CURSOR IS THE NEWEST INDEX DAY READ, at most `DUE_DAYS_PER_TICK` a tick. If the index lane
    stalls, so does this, and `every_business_day_has_an_index` goes ERROR: the floor rides the
    index on purpose, so one alarm covers both.

    ONE RUN PER `FILERS_PER_RUN`-ALIGNED CHUNK holding a due filer: a run covers a contiguous
    range, and chunks aligned to grid positions keep run keys stable as the grid only appends. A
    scattered set requested as ranges would be one run per filer (measured 2026-10-06: 254 runs).
    Each run asks only what is due and leaves the rest of its chunk `Unchanged`.
    """
    held = context.instance.get_materialized_partitions(INDEX)
    after = context.cursor or ""
    days = sorted(d for d in held if d > after)
    if not context.cursor:
        # A FIRST TICK STARTS AT THE NEWEST DAY, not at the grid's first: the backlog of every
        # filer is the operator's backfill, and never-collected filers are due on any day anyway.
        days = days[-1:]
    days = days[:DUE_DAYS_PER_TICK]
    if not days:
        return dg.SensorResult(run_requests=[], cursor=context.cursor)

    grid = context.instance.get_dynamic_partitions(SEC_FILER_PARTITION)
    position = {k: n for n, k in enumerate(grid)}
    collected = set(context.instance.get_materialized_partitions(COMPANYFACTS))
    with postgres.connect() as conn:
        ciks = list(insider.tracked_issuers(conn))
        forms = sec_filings.forms(conn)
    due: set[str] = set()
    filed_sub: dict[str, str] = {}
    filed_facts: dict[str, str] = {}
    for day in days:
        bodies = [
            bytes(r["body"])
            for r in raw_store.stored_rows_for(INDEX, day, ("document", "body"))
            if r.get("document") == "form_index"
        ]
        found, sub, facts = filers_due(
            date.fromisoformat(day), bodies, grid, ciks, forms, collected
        )
        due |= found
        filed_sub.update(sub)
        filed_facts.update(facts)

    requests: list[dg.RunRequest] = []
    for chunk in sorted({position[k] // FILERS_PER_RUN for k in due}):
        keys = grid[chunk * FILERS_PER_RUN : (chunk + 1) * FILERS_PER_RUN]
        requests.append(
            dg.RunRequest(
                run_key=f"{days[-1]}:{keys[0]}",
                tags={
                    ASSET_PARTITION_RANGE_START_TAG: keys[0],
                    ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                    **SEC_RUN,
                },
                run_config={
                    "ops": {
                        "raw_sec_filer": {
                            "config": {
                                "filed_submissions": {
                                    k: filed_sub[k] for k in keys if k in filed_sub
                                },
                                "filed_companyfacts": {
                                    k: filed_facts[k] for k in keys if k in filed_facts
                                },
                            }
                        }
                    }
                },
            )
        )
    context.log.info(
        "index days %s: %s filers due in %s runs", ", ".join(days), len(due), len(requests)
    )
    return dg.SensorResult(run_requests=requests, cursor=days[-1])
