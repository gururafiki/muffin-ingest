"""Stage 1 of the segments family: each regulator's segment-bearing documents, kept whole."""

import time
from collections import Counter
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import cn_pdf, sec_segments
from muffin_ingest.facets import cninfo as cn
from muffin_ingest.providers import cninfo, sec_edgar

from muffin_ingest_dagster.defs.sec.partitions import FILERS_PER_RUN, sec_filers
from muffin_ingest_dagster.defs.segments.partitions import CNINFO_FILERS_PER_RUN, cninfo_filers
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: The raw assets, spelled once: stage 2 reads what each already holds.
SEC_INSTANCE = dg.AssetKey("raw_sec_instance")
#: `market.security_filing` as the SEC filer lane writes it: where the filings to read come from.
SEC_FILING = dg.AssetKey("sec_filing")

#: Accounts filings arrive every business day (80 to 506 a weekday in November 2026), so a week
#: in which the lane read none is a stopped sensor, not a quiet market.
FRESH_WEEK = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=7))

#: Consecutive transport failures, with nothing answered, that stop a run: SEC is not reachable.
TRANSPORT_STOP = 3

#: How a filing asked about in a run can end. They partition the filings named, so they sum to
#: `named`. A refusal stops the run and is never an absence; `held_already` and `unasked` cost no
#: request.
INSTANCE_OUTCOMES = (
    "answered",
    "absent",
    "too_large",
    "refused",
    "transport",
    "held_already",
    "unasked",
)


class SecInstanceRun(dg.Config):
    """What bounds an instance run, and what an operator may ask beyond the queue."""

    #: Filings to fetch again whatever their marks and raw say, by filer key
    #: (`{"0000320193": ["0000320193-26-000020"]}`). Stage 2 re-reads what a run fetched, so this
    #: is how one filing is read again by hand.
    reread: dict[str, list[str]] | None = None
    #: Wall-clock budget. A filing is two or three requests at 5 a second.
    budget_seconds: int = 900
    #: Stop asking once the run holds this many bytes of documents: a run returns everything it
    #: fetched at once, and an instance is up to 32 MB (median 1.7 MB, measured 2026-11-09).
    budget_bytes: int = 400_000_000


@dg.asset(
    partitions_def=sec_filers,
    deps=[SEC_FILING],
    backfill_policy=dg.BackfillPolicy.multi_run(FILERS_PER_RUN),
    pool="sec",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"sec", "parquet"},
    freshness_policy=FRESH_WEEK,
    # EVERY FILING'S DOCUMENTS ARE KEPT, keyed by filing and document: a filer's file only grows,
    # and a visit asks only for filings it does not hold.
    #
    # ONE DOCUMENT PER ROW GROUP, so neither a merge nor stage 2's read ever decodes more than one
    # instance at a time. Principal Financial's file (21 filings, 413 MB of instances, 14 MB on
    # disk) took 2,143 MB to read whole, measured 2026-11-14, against a 2.5 GB code location.
    metadata={
        "merge_on": list(sec_segments.MERGE_ON),
        "compression": "zstd",
        "row_group_rows": 1,
    },
    description=(
        "Each SEC accounts filing's XBRL instance and the index that named it, by filer, whole."
    ),
)
def raw_sec_instance(
    context: AssetExecutionContext, config: SecInstanceRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """The instance of every filing of the run's filers that is due and not held.

    WHAT A VISIT COSTS: one query for the run, one read of stored metadata per filer with a filing
    due, and two or three requests per filing fetched (`index.json`, the HTML index when that
    names no instance, the instance). A filer with nothing due costs nothing and its file stands.

    THE OUTCOMES PARTITION THE FILINGS NAMED (`INSTANCE_OUTCOMES`). A refusal (403, 429, 5xx) stops
    the run, and so do three transport failures before anything answered; the rest are `unasked`
    and stay due. Nothing is filed for a filing whose fetch did not finish, so raw never holds
    half of one.
    """
    keys = list(context.partition_keys)
    reread = {key: set(accessions) for key, accessions in (config.reread or {}).items()}
    with postgres.connect() as conn:
        wanted = sec_segments.filings(
            conn, [int(k) for k in keys], sorted({a for k in keys for a in reread.get(k, ())})
        )
    plan = [(key, filing) for key in keys for filing in wanted.get(int(key), [])]
    held: dict[str, set[str]] = {
        key: sec_segments.held_accessions(
            raw_store.stored_rows_for(SEC_INSTANCE, key, ("accession", "outcome"))
        )
        for key in {key for key, _ in plan}
    }

    def is_held(key: str, accession: str) -> bool:
        return accession in held[key] and accession not in reread.get(key, ())

    stats: Counter[str] = Counter({f"instance_{o}": 0 for o in INSTANCE_OUTCOMES})
    rows: list[dict[str, Any]] = []
    placed: dict[int, str] = {}
    deadline = time.monotonic() + config.budget_seconds
    transport_in_a_row = 0
    answered_any = False
    stopped: str | None = None
    for key, filing in plan:
        if is_held(key, filing.accession):
            stats["instance_held_already"] += 1
            continue
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is None and stats["bytes"] >= config.budget_bytes:
            stopped = "the byte budget is spent"
        if stopped is not None:
            stats["instance_unasked"] += 1
            continue
        try:
            fetch = sec_edgar.fetch_instance(filing.cik, filing.accession)
        except sec_edgar.SecRefused as exc:
            stats["instance_refused"] += 1
            stopped = f"SEC refused us ({exc})"
            continue
        except sec_edgar.SecTransport as exc:
            stats["instance_transport"] += 1
            transport_in_a_row += 1
            if transport_in_a_row >= TRANSPORT_STOP and not answered_any:
                stopped = f"{TRANSPORT_STOP} transport failures and nothing answered ({exc})"
            continue
        transport_in_a_row = 0
        answered_any = True
        stats[f"instance_{fetch.outcome}"] += 1
        stats["documents"] += len(fetch.answers)
        stats["bytes"] += sum(len(answer.document.body) for _, answer in fetch.answers)
        for row in sec_segments.raw_rows(
            fetch, accession=filing.accession, run_id=context.run.run_id
        ):
            rows.append(row)
            placed[id(row)] = key
    if stopped is not None:
        context.log.warning("%s; %s filings unasked", stopped, stats["instance_unasked"])

    context.add_output_metadata(
        {
            "requested": len(keys),
            "filers_with_filings": len(held),
            "named": len(plan),
            "outcomes": sum(stats[f"instance_{o}"] for o in INSTANCE_OUTCOMES),
            "documents": stats["documents"],
            "bytes": stats["bytes"],
            **{f"instance_{o}": stats[f"instance_{o}"] for o in INSTANCE_OUTCOMES},
        }
    )
    stored = context.instance.get_materialized_partitions(SEC_INSTANCE)
    return partitioned.by_partition(
        context,
        rows,
        key=lambda row: placed[id(row)],
        unchanged=[k for k in keys if k in stored],
    )


# ── CNINFO (China) ─────────────────────────────────────────────────────────────────────────────

CNINFO_STOCK_LIST = dg.AssetKey("raw_cninfo_stock_list")
CNINFO_FILINGS = dg.AssetKey("raw_cninfo_filings")
#: `market.security_filer` as `security_cninfo_filer` writes it: which org id each A-share has.
CNINFO_FILER = dg.AssetKey("security_cninfo_filer")

#: The stock lists change when a company lists or renames, so a week old is a missed schedule.
FRESH_TWO_WEEKS = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=14))
#: Every filer is searched within its 30-day floor, so 35 days without a visit is a stopped lane.
FRESH_FLOOR = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=35))


@dg.asset(
    pool="cninfo",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"cninfo", "parquet"},
    freshness_policy=FRESH_TWO_WEEKS,
    description="CNINFO's A-share stock lists (Shanghai, Shenzhen and Beijing), whole.",
)
def raw_cninfo_stock_list(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """Two documents, each the whole answer: the lists are the org-id registry. A refusal or a 404
    fails the run, because an empty registry would read as every company gone."""
    rows: list[dict[str, Any]] = []
    for name in cninfo.STOCK_LISTS:
        answer = cninfo.stock_list(name)
        if answer.status != 200:
            raise dg.Failure(f"CNINFO's {name} answered HTTP {answer.status}")
        rows.append(
            {**answer.document.as_row(context.run.run_id), "document": name, "http_status": 200}
        )
    context.add_output_metadata(
        {"documents": len(rows), "bytes": sum(len(r["body"]) for r in rows)}
    )
    return rows


class CninfoRun(dg.Config):
    """What bounds a CNINFO run, and what an operator may ask beyond the queue."""

    #: Reports to fetch again whatever their marks and raw say, by org id
    #: (`{"gssh0600900": ["https://static.cninfo.com.cn/finalpage/…PDF"]}`).
    reread: dict[str, list[str]] | None = None
    #: Wall-clock budget.
    budget_seconds: int = 900
    #: Stop asking for reports once the run holds this many bytes: a run returns everything it
    #: fetched at once, and a report is up to 12 MB (median ~4 MB).
    budget_bytes: int = 300_000_000
    #: Seconds between requests. 24 of 24 searches answered at 1 s and at 0.25 s (2026-11-15).
    pace_seconds: float = 0.5


#: How a search and a report fetch can end. Each set partitions what it counts.
SEARCH_OUTCOMES = ("answered", "absent", "refused", "transport", "unasked", "not_askable")
REPORT_OUTCOMES = (
    "answered",
    "absent",
    "too_large",
    "not_pdf",
    "refused",
    "transport",
    "held_already",
    "unasked",
)


def _round_robin(queues: dict[str, list[str]]) -> list[tuple[str, str]]:
    """Each filer's first report, then each one's second, and so on: a byte budget that runs out
    part-way leaves every filer with its newest report read, not a few filers with all of theirs."""
    plan: list[tuple[str, str]] = []
    depth = max((len(q) for q in queues.values()), default=0)
    for n in range(depth):
        plan.extend((key, q[n]) for key, q in queues.items() if n < len(q))
    return plan


@dg.asset(
    partitions_def=cninfo_filers,
    deps=[CNINFO_FILER],
    backfill_policy=dg.BackfillPolicy.multi_run(CNINFO_FILERS_PER_RUN),
    pool="cninfo",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"cninfo", "parquet"},
    freshness_policy=FRESH_FLOOR,
    # ONE DOCUMENT PER ROW GROUP (muffin-ingest#177): a filer's file holds every report fetched for
    # it, megabytes each, and a row group is the unit Parquet decodes.
    metadata={"merge_on": list(cn.MERGE_ON), "compression": "zstd", "row_group_rows": 1},
    description=(
        "Each CNINFO filer's annual-report search and the full reports not yet read, whole."
    ),
)
def raw_cninfo_filings(
    context: AssetExecutionContext, config: CninfoRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """A search per filer, then the full reports due, in rounds across the run's filers.

    DUE: every full report the search lists, and every one `security_filing` already holds for the
    filer's securities, that is not read at the current parser version and that raw does not hold
    (a pruned report is not held). The search's first page only, as the edge asked.

    THE OUTCOMES PARTITION WHAT THEY COUNT (`SEARCH_OUTCOMES` sums to `requested`,
    `REPORT_OUTCOMES` to `reports_named`). A refusal stops the run and leaves the rest `unasked`;
    nothing is filed for a fetch that did not finish.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    reread = {k: set(v) for k, v in (config.reread or {}).items()}
    with postgres.connect() as conn:
        found = cn.filers(conn, keys)
    stats: Counter[str] = Counter(
        {
            **{f"search_{o}": 0 for o in SEARCH_OUTCOMES},
            **{f"report_{o}": 0 for o in REPORT_OUTCOMES},
        }
    )
    rows: list[dict[str, Any]] = []
    placed: dict[int, str] = {}
    deadline = time.monotonic() + config.budget_seconds
    stopped: str | None = None

    listed: dict[str, list[cn.Announcement]] = {}
    for key in keys:
        filer = found.get(key)
        if filer is None:
            stats["search_not_askable"] += 1
            continue
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is not None:
            stats["search_unasked"] += 1
            continue
        try:
            answer = cninfo.announcements(filer.code, key)
        except cninfo.CninfoRefused as exc:
            stats["search_refused"] += 1
            stopped = f"CNINFO refused us ({exc})"
            continue
        except cninfo.CninfoTransport:
            stats["search_transport"] += 1
            continue
        row = cn.list_row(answer, code=filer.code, page=1, run_id=run_id)
        rows.append(row)
        placed[id(row)] = key
        stats["search_answered" if answer.status == 200 else "search_absent"] += 1
        if answer.status == 200:
            listed[key] = cn.parse_announcements(answer.document.body)
        time.sleep(config.pace_seconds)

    with postgres.connect() as conn:
        listed_full = [a.url for anns in listed.values() for a in anns if a.is_report]
        read = cn.read_already(conn, listed_full)
        stored_due = {
            key: [f.accession for f in cn.filings(conn, found[key].security_ids) if f.due]
            for key in keys
            if key in found
        }
    queues: dict[str, list[str]] = {}
    for key in keys:
        if key not in found:
            continue
        newest_first = [
            a.url
            for a in sorted(listed.get(key, []), key=lambda a: a.date or "", reverse=True)
            if a.is_report and a.url not in read
        ]
        wanted = list(
            dict.fromkeys([*newest_first, *reversed(stored_due.get(key, [])), *reread.get(key, ())])
        )
        if not wanted:
            continue
        light = raw_store.stored_rows_for(
            CNINFO_FILINGS,
            key,
            ("document", "accession", "outcome", "fetched_at", "run_id", "pruned_at"),
        )
        queue = []
        for url in wanted:
            held = cn.held_report(light, url)
            if held is not None and not held.pruned and url not in reread.get(key, ()):
                stats["report_held_already"] += 1
            else:
                queue.append(url)
        queues[key] = queue

    plan = _round_robin(queues)
    stats["reports_named"] = stats["report_held_already"] + len(plan)
    for key, url in plan:
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is None and stats["bytes"] >= config.budget_bytes:
            stopped = "the byte budget is spent"
        if stopped is not None:
            stats["report_unasked"] += 1
            continue
        try:
            answer = cninfo.report(url, max_bytes=cn_pdf.MAX_PDF_BYTES)
        except cninfo.CninfoRefused as exc:
            stats["report_refused"] += 1
            stopped = f"CNINFO refused us ({exc})"
            continue
        except cninfo.CninfoTransport:
            stats["report_transport"] += 1
            continue
        row = cn.report_row(answer, url=url, code=found[key].code, run_id=run_id)
        rows.append(row)
        placed[id(row)] = key
        stats[f"report_{row['outcome']}"] += 1
        stats["bytes"] += len(answer.document.body)
        time.sleep(config.pace_seconds)
    if stopped is not None:
        context.log.warning(
            "%s; %s searches and %s reports unasked",
            stopped,
            stats["search_unasked"],
            stats["report_unasked"],
        )

    context.add_output_metadata(
        {
            "requested": len(keys),
            "search_outcomes": sum(stats[f"search_{o}"] for o in SEARCH_OUTCOMES),
            "reports_named": stats["reports_named"],
            "report_outcomes": sum(stats[f"report_{o}"] for o in REPORT_OUTCOMES),
            "bytes": stats["bytes"],
            **{f"search_{o}": stats[f"search_{o}"] for o in SEARCH_OUTCOMES},
            **{f"report_{o}": stats[f"report_{o}"] for o in REPORT_OUTCOMES},
        }
    )
    stored = context.instance.get_materialized_partitions(CNINFO_FILINGS)
    return partitioned.by_partition(
        context,
        rows,
        key=lambda row: placed[id(row)],
        unchanged=[k for k in keys if k in stored],
    )
