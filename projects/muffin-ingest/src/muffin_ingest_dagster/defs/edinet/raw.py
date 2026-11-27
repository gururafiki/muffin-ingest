"""Stage 1 of Japan's segment lane: EDINET's code list, its day lists and annual reports' archives,
kept whole (Phase 6c decision 3: kept for good, so a reader change never asks EDINET again)."""

import time
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import edinet as fe
from muffin_ingest.providers import edinet

from muffin_ingest_dagster.defs.edinet.partitions import (
    DAYS_PER_RUN,
    EDINET_FILERS_PER_RUN,
    edinet_days,
    edinet_filers,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

CODE_LIST = dg.AssetKey("raw_edinet_code_list")
DAYS = dg.AssetKey("raw_edinet_days")
FILINGS = dg.AssetKey("raw_edinet_filings")
#: `market.security_filing` as the day lane writes it (through `file_edinet_filings`): the reports
#: to fetch come from there.
DOCUMENT = dg.AssetKey("edinet_document")

TOKYO = ZoneInfo("Asia/Tokyo")

#: The code list changes when a company lists or renames, so two weeks old is a missed schedule.
FRESH_TWO_WEEKS = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=14))
#: A day's list is asked the night after it ends, so two days without one is a stopped schedule.
FRESH_TWO_DAYS = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=2))
#: Annual reports arrive every month (a fiscal year ends in every month, most in March, so most
#: land in June), so 35 days in which no archive was read is a stopped sensor.
FRESH_MONTH = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=35))

#: Consecutive transport failures, with nothing answered, that stop a run: EDINET is not reachable.
TRANSPORT_STOP = 3


@dg.asset(
    pool="edinet",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"edinet", "parquet"},
    freshness_policy=FRESH_TWO_WEEKS,
    description="EDINET's code list (`Edinetcode.zip`), whole: each filer and its securities code.",
)
def raw_edinet_code_list(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """One document, the whole registry (576 KB). Anything but a ZIP fails the run: an empty
    registry would read as every company gone."""
    answer = edinet.code_list()
    if answer.document.body[:2] != b"PK":
        raise dg.Failure(f"EDINET's code list is not a ZIP: {answer.document.body[:120]!r}")
    context.add_output_metadata({"bytes": len(answer.document.body)})
    return [
        {
            **answer.document.as_row(context.run.run_id),
            "document": "code_list",
            "accession": "Edinetcode.zip",
            "http_status": answer.status,
            "outcome": "answered",
        }
    ]


#: A DAY'S LIST IS SETTLED once it was fetched this many days after the day ended: documents land on
#: the day they are submitted, and the window covers a late withdrawal or correction of its entries.
#: A settled list is not asked again, so a backfill over held days costs nothing.
SETTLE_DAYS = 10


class EdinetDayRun(dg.Config):
    """What bounds a day-list run."""

    #: Ask every day in the run again, settled or not (a re-capture after EDINET restates a list).
    force: bool = False
    #: A list fetched this many days after its day is settled (`SETTLE_DAYS`).
    settle_days: int = SETTLE_DAYS
    #: Seconds between requests.
    pace_seconds: float = 1.0
    #: Wall-clock budget: thirty lists take under two minutes.
    budget_seconds: int = 900


#: How a day asked about in a run can end. They partition the days, so they sum to `requested`.
#:   `answered`   EDINET listed the day (an empty list is an answer);
#:   `absent`     EDINET said 404: the day is outside its rolling ten years, and none is held;
#:   `settled`    a list fetched `settle_days` after the day is held: not asked;
#:   `kept`       EDINET said 404 for a day whose list is held: the held list stands;
#:   `refused`    EDINET refused us (the key, a throttle, a fault): the run fails, claiming nothing;
#:   `transport`  no answer: the run fails, claiming nothing;
#:   `unasked`    the run stopped before it.
DAY_OUTCOMES = ("answered", "absent", "settled", "kept", "refused", "transport", "unasked")


def _settled(held: Any, day: date, settle_days: int) -> bool:
    """Whether a held list was fetched `settle_days` or more after the day ended in Tokyo."""
    try:
        at = datetime.fromisoformat(str(held.get("fetched_at")))
    except ValueError:
        return False
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    ended = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=TOKYO)
    return at >= ended + timedelta(days=settle_days)


@dg.asset(
    partitions_def=edinet_days,
    backfill_policy=dg.BackfillPolicy.multi_run(DAYS_PER_RUN),
    pool="edinet",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"edinet", "parquet"},
    freshness_policy=FRESH_TWO_DAYS,
    description="EDINET's list of the documents filed on a day in Tokyo (`documents.json`), whole.",
)
def raw_edinet_days(
    context: AssetExecutionContext, config: EdinetDayRun, raw_store: RawStore
) -> Any:
    """One list per day. A LIST IS NEVER REPLACED BY AN ABSENCE: EDINET answers 404 for a day
    outside its window, and the window moves, so a day whose list is held keeps it.

    A RUN THAT DID NOT HEAR EVERY DAY CLAIMS NONE (a refusal, a transport failure, a spent budget):
    it fails, and the next run asks again. A list is small, so asking again is cheap, and a day
    materialised with nothing would read as a day EDINET listed nothing for.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    stats: Counter[str] = Counter({o: 0 for o in DAY_OUTCOMES})
    rows: list[dict[str, Any]] = []
    placed: dict[int, str] = {}
    unchanged: list[str] = []
    deadline = time.monotonic() + config.budget_seconds
    stopped: str | None = None
    failing = 0
    for key in keys:
        day = date.fromisoformat(key)
        held = fe.held_day(
            raw_store.stored_rows_for(DAYS, key, ("document", "outcome", "fetched_at"))
        )
        if held is not None and not config.force and _settled(held, day, config.settle_days):
            stats["settled"] += 1
            unchanged.append(key)
            continue
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is not None:
            stats["unasked"] += 1
            continue
        try:
            answer = edinet.daily_index(day)
        except edinet.EdinetRefused as exc:
            stats["refused"] += 1
            stopped = f"EDINET refused us ({exc})"
            continue
        except edinet.EdinetTransport:
            stats["transport"] += 1
            failing += 1
            if failing >= TRANSPORT_STOP and stats["answered"] == 0:
                stopped = f"{failing} transport failures and nothing answered"
            continue
        failing = 0
        outcome = fe.day_outcome(answer)
        if outcome == "absent" and held is not None:
            stats["kept"] += 1
            unchanged.append(key)
        else:
            row = fe.day_row(answer, day=key, run_id=run_id)
            rows.append(row)
            placed[id(row)] = key
            stats[outcome] += 1
            stats["bytes"] += len(answer.document.body)
        time.sleep(config.pace_seconds)

    metadata = {
        "requested": len(keys),
        "outcomes": sum(stats[o] for o in DAY_OUTCOMES),
        "bytes": stats["bytes"],
        **{o: stats[o] for o in DAY_OUTCOMES},
    }
    if stats["refused"] or stats["transport"] or stats["unasked"]:
        raise dg.Failure(
            description=(
                f"{stats['refused'] + stats['transport'] + stats['unasked']} of {len(keys)} days "
                f"were not heard ({stopped or 'transport failures'}); the run claims none"
            ),
            metadata=metadata,
        )
    context.add_output_metadata(metadata)
    return partitioned.by_partition(
        context, rows, key=lambda row: placed[id(row)], unchanged=unchanged
    )


class EdinetFilingRun(dg.Config):
    """What bounds an archive run, and what an operator may ask beyond the queue."""

    #: Archives to fetch again whatever their marks and raw say, by EDINET code
    #: (`{"E02144": ["S100YC2P"]}`).
    reread: dict[str, list[str]] | None = None
    #: Wall-clock budget. An archive takes 3.6-8 s from the node.
    budget_seconds: int = 900
    #: Stop asking for archives once the run holds this many bytes (an archive is 0.6-2.6 MB).
    budget_bytes: int = 300_000_000
    #: Seconds between requests.
    pace_seconds: float = 1.0


#: How an archive named in a run can end. They partition the archives named, so they sum to
#: `named`. A refusal stops the run and is never an absence; `held_already` and `unasked` cost no
#: request.
ARCHIVE_OUTCOMES = (
    "answered",
    "absent",
    "too_large",
    "not_zip",
    "refused",
    "transport",
    "held_already",
    "unasked",
)


def _round_robin(queues: dict[str, list[str]]) -> list[tuple[str, str]]:
    """Each filer's newest report first, then each one's next: a spent budget leaves every filer's
    newest year read rather than one filer's whole history."""
    out: list[tuple[str, str]] = []
    depth = max((len(q) for q in queues.values()), default=0)
    for n in range(depth):
        out.extend((key, queue[n]) for key, queue in queues.items() if n < len(queue))
    return out


@dg.asset(
    partitions_def=edinet_filers,
    # EVERY DAY'S CATALOGUE FEEDS EVERY FILER: a filer's reports are filed from whichever days
    # listed them, so the dependency is on the whole day grid.
    deps=[dg.AssetDep(DOCUMENT, partition_mapping=dg.AllPartitionMapping())],
    backfill_policy=dg.BackfillPolicy.multi_run(EDINET_FILERS_PER_RUN),
    pool="edinet",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"edinet", "parquet"},
    freshness_policy=FRESH_MONTH,
    # ONE DOCUMENT PER ROW GROUP (muffin-ingest#177): a filer's file holds every archive fetched
    # for it, a megabyte or two each, and stage 2 reads one at a time.
    metadata={"merge_on": list(fe.MERGE_ON), "compression": "zstd", "row_group_rows": 1},
    description="Each EDINET filer's annual-report archives that are due, whole.",
)
def raw_edinet_filings(
    context: AssetExecutionContext, config: EdinetFilingRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """The archives due, in rounds across the run's filers, newest report first.

    DUE: every annual report `security_filing` holds for the filer's securities that is not read at
    the current parser version and that raw does not hold. The day lane files them there, so this
    asset asks EDINET for nothing but archives.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    reread = {k: set(v) for k, v in (config.reread or {}).items()}
    stats: Counter[str] = Counter({o: 0 for o in ARCHIVE_OUTCOMES})
    with postgres.connect() as conn:
        found = fe.filers(conn, keys)
        due = {
            key: fe.filings(conn, found[key], sorted(reread.get(key, ())))
            for key in keys
            if key in found
        }
    stats["not_askable"] = sum(key not in found for key in keys)

    queues: dict[str, list[str]] = {}
    for key, filings in due.items():
        wanted = [f.accession for f in reversed(filings)]
        if not wanted:
            continue
        light = raw_store.stored_rows_for(
            FILINGS, key, ("document", "accession", "outcome", "fetched_at", "run_id")
        )
        queue = []
        for doc_id in wanted:
            held = fe.held_archive(light, doc_id)
            # A body that was neither an archive nor EDINET saying no is asked again: it settled
            # nothing about the report.
            if held is not None and held.outcome != "not_zip" and doc_id not in reread.get(key, ()):
                stats["held_already"] += 1
            else:
                queue.append(doc_id)
        queues[key] = queue

    plan = _round_robin(queues)
    stats["named"] = stats["held_already"] + len(plan)
    rows: list[dict[str, Any]] = []
    placed: dict[int, str] = {}
    deadline = time.monotonic() + config.budget_seconds
    stopped: str | None = None
    failing = 0
    for key, doc_id in plan:
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is None and stats["bytes"] >= config.budget_bytes:
            stopped = "the byte budget is spent"
        if stopped is not None:
            stats["unasked"] += 1
            continue
        try:
            answer = edinet.document(doc_id)
        except edinet.EdinetRefused as exc:
            stats["refused"] += 1
            stopped = f"EDINET refused us ({exc})"
            continue
        except edinet.EdinetTransport:
            stats["transport"] += 1
            failing += 1
            if failing >= TRANSPORT_STOP and stats["answered"] == 0:
                stopped = f"{failing} transport failures and nothing answered"
            continue
        failing = 0
        row = fe.archive_row(answer, doc_id=doc_id, run_id=run_id)
        rows.append(row)
        placed[id(row)] = key
        stats[row["outcome"]] += 1
        stats["bytes"] += len(answer.document.body)
        time.sleep(config.pace_seconds)
    if stopped is not None:
        context.log.warning("%s; %s archives unasked", stopped, stats["unasked"])

    context.add_output_metadata(
        {
            "requested": len(keys),
            "not_askable": stats["not_askable"],
            "named": stats["named"],
            "outcomes": sum(stats[o] for o in ARCHIVE_OUTCOMES),
            "bytes": stats["bytes"],
            **{o: stats[o] for o in ARCHIVE_OUTCOMES},
        }
    )
    stored = context.instance.get_materialized_partitions(FILINGS)
    return partitioned.by_partition(
        context,
        rows,
        key=lambda row: placed[id(row)],
        unchanged=[k for k in keys if k in stored],
    )
