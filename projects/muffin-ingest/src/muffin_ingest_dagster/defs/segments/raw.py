"""Stage 1 of the segments family: each regulator's segment-bearing documents, kept whole."""

import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import cn_pdf, sec_segments
from muffin_ingest.facets import cninfo as cn
from muffin_ingest.facets import dart as fd
from muffin_ingest.facets import nse as fn
from muffin_ingest.providers import cninfo, dart, nse, sec_edgar

from muffin_ingest_dagster.defs.sec.partitions import FILERS_PER_RUN, sec_filers
from muffin_ingest_dagster.defs.segments.partitions import (
    CNINFO_FILERS_PER_RUN,
    DART_FILERS_PER_RUN,
    FLOOR_DAYS,
    NSE_FILERS_PER_RUN,
    cninfo_filers,
    dart_filers,
    nse_filers,
)
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
    #: fetched at once, and an instance is up to 64 MB (median 1.7 MB, measured 2026-11-09).
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
        if fetch.filed_under is not None:
            stats["found_under_filer"] += 1
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
            # NOT AN OUTCOME: of the filings answered (or too large), those whose instance sat
            # under another filer's CIK than the security's (`sec_edgar.fetch_instance`).
            "found_under_filer": stats["found_under_filer"],
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
    description="CNINFO's stock lists (Shanghai, Shenzhen, Beijing and Hong Kong), whole.",
)
def raw_cninfo_stock_list(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """Three documents, each the whole answer: the lists are the org-id registry. A refusal or a 404
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


#: A filer's list is not asked again until the list its file holds is this many days old (decided
#: 2026-11-23). The floor makes a filer due every `FLOOR_DAYS`, but asks in width-aligned chunks,
#: and a chunk of ~45 filers holds one on its floor day about three days in four: with no age check,
#: each floor listed nearly its whole grid every day (measured 2026-11-22: `nse_filers_floor` ran
#: 16-19 runs a day). A day under the floor, so a list fetched on one floor day is due on the next.
#: The SEC filer lane has the same rule: a floor visit is not a fetch.
LIST_MAX_AGE_DAYS = FLOOR_DAYS - 1


def _younger_than(row: Any, now: datetime, days: float) -> bool:
    """Whether a stored list's `fetched_at` (ISO 8601) is less than `days` before `now`.

    No list, or a timestamp that does not parse, is not young: the list is asked rather than
    trusted. A list asked with an age of 0 days is always asked.
    """
    if row is None:
        return False
    try:
        at = datetime.fromisoformat(str(row.get("fetched_at")))
    except ValueError:
        return False
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    return now - at < timedelta(days=days)


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
    #: A filer whose stored search is younger than this is not searched; its reports are planned
    #: from the stored search. 0 searches every filer.
    list_max_age_days: float = LIST_MAX_AGE_DAYS
    #: Search pages after the first to walk for each filer, once: a page raw already holds is read
    #: from raw, never asked again. 0 (the floor) walks none; the history backfill sets it.
    history_pages: int = 0


#: How a search and a report fetch can end. Each set partitions what it counts. `fresh` is a filer
#: whose stored search is young enough not to ask again: it costs no request.
SEARCH_OUTCOMES = (
    "answered",
    "absent",
    "fresh",
    "refused",
    "transport",
    "unasked",
    "not_askable",
)
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
#: How a later search page can end: asked and answered or absent, read from raw (`held`), or not
#: asked (refused, a transport failure, or the budget). Counted apart: pages per filer vary.
HISTORY_OUTCOMES = ("answered", "absent", "held", "refused", "transport", "unasked")


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

    A FILER WHOSE STORED SEARCH IS YOUNGER THAN `list_max_age_days` IS NOT SEARCHED: its reports
    are planned from that search, so the due reports are fetched exactly as if it had been asked.

    WITH `history_pages`, THE SEARCH'S LATER PAGES ARE WALKED ONCE per filer: page 1 reaches back
    about a decade, and page 2 holds ~8 more full reports a filer (2003-2019, measured
    2026-11-23). A page raw holds is read from raw; one not held is asked while the page before it
    says there is more (`history_*` counts them, apart from the outcomes).

    THE OUTCOMES PARTITION WHAT THEY COUNT (`SEARCH_OUTCOMES` sums to `requested`,
    `REPORT_OUTCOMES` to `reports_named`). A refusal stops the run and leaves the rest `unasked`;
    nothing is filed for a fetch that did not finish.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    now = datetime.now(UTC)
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
        lists_held = raw_store.stored_rows_where(CNINFO_FILINGS, key, {"document": "announcements"})
        searched = cn.newest_list(lists_held)
        first: bytes | None = None
        if searched is not None and _younger_than(searched, now, config.list_max_age_days):
            stats["search_fresh"] += 1
            first = bytes(searched["body"])
        else:
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
                first = answer.document.body
            time.sleep(config.pace_seconds)
        if first is None:
            continue
        listed[key] = cn.parse_announcements(first)
        more, held_pages = cn.has_more(first), cn.history_pages(lists_held)
        for page in range(2, 2 + config.history_pages):
            if not more:
                break
            page_held = held_pages.get(page)
            if page_held is not None:
                stats["history_held"] += 1
                body = bytes(page_held["body"])
            else:
                if stopped is None and time.monotonic() >= deadline:
                    stopped = "the time budget is spent"
                if stopped is not None:
                    stats["history_unasked"] += 1
                    break
                try:
                    answer = cninfo.announcements(filer.code, key, page)
                except cninfo.CninfoRefused as exc:
                    stats["history_refused"] += 1
                    stopped = f"CNINFO refused us ({exc})"
                    break
                except cninfo.CninfoTransport:
                    stats["history_transport"] += 1
                    break
                row = cn.list_row(answer, code=filer.code, page=page, run_id=run_id)
                rows.append(row)
                placed[id(row)] = key
                time.sleep(config.pace_seconds)
                if answer.status != 200:
                    stats["history_absent"] += 1
                    break
                stats["history_answered"] += 1
                body = answer.document.body
            listed[key] += cn.parse_announcements(body)
            more = cn.has_more(body)

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
            **{f"history_{o}": stats[f"history_{o}"] for o in HISTORY_OUTCOMES},
        }
    )
    stored = context.instance.get_materialized_partitions(CNINFO_FILINGS)
    return partitioned.by_partition(
        context,
        rows,
        key=lambda row: placed[id(row)],
        unchanged=[k for k in keys if k in stored],
    )


# ── DART (Korea) ───────────────────────────────────────────────────────────────────────────────

DART_CORP_CODES = dg.AssetKey("raw_dart_corp_codes")
DART_FILINGS = dg.AssetKey("raw_dart_filings")
#: `market.security_filer` as `security_dart_filer` writes it: which corp code each equity has.
DART_FILER = dg.AssetKey("security_dart_filer")


@dg.asset(
    pool="dart",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"dart", "parquet"},
    freshness_policy=FRESH_TWO_WEEKS,
    description="DART's company registry (`corpCode.xml`), whole: one ZIP.",
)
def raw_dart_corp_codes(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """One document, the whole registry (3.6 MB, ~6 minutes from the node). Anything but a ZIP fails
    the run: an empty registry would read as every company gone."""
    answer = dart.corp_codes()
    kind = fd.classify(answer.document.body) if answer.status == 200 else None
    if kind != "zip":
        raise dg.Failure(
            f"DART's registry answered HTTP {answer.status} ({kind}): "
            f"{answer.document.body[:120]!r}"
        )
    context.add_output_metadata({"bytes": len(answer.document.body)})
    return [
        {
            **answer.document.as_row(context.run.run_id),
            "document": "corpCode.xml",
            "http_status": answer.status,
        }
    ]


class DartRun(dg.Config):
    """What bounds a DART run, and what an operator may ask beyond the queue."""

    #: Filings to fetch again whatever their marks and raw say, by corp code
    #: (`{"00126380": ["20260310002820"]}`).
    reread: dict[str, list[str]] | None = None
    #: Wall-clock budget. A cold archive takes 70-100 s from the node.
    budget_seconds: int = 900
    #: Stop asking for archives once the run holds this many bytes (median ~1 MB, the gate 24 MB).
    budget_bytes: int = 300_000_000
    #: Seconds between requests.
    pace_seconds: float = 0.2
    #: A filer whose stored list is younger than this is not listed; its archives are planned
    #: from the stored list. 0 lists every filer.
    list_max_age_days: float = LIST_MAX_AGE_DAYS


#: How a filing list and an archive fetch can end. Each set partitions what it counts. `fresh` is a
#: filer whose stored list is young enough not to ask again: it costs no request.
LIST_OUTCOMES = (
    "answered",
    "absent",
    "fresh",
    "refused",
    "transport",
    "unasked",
    "not_askable",
)
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

#: The most list pages asked of one filer in a run. Samsung's eleven years are 47 filings, one page
#: of 100; a filer past 300 is no company this lane serves.
MAX_LIST_PAGES = 3


@dg.asset(
    partitions_def=dart_filers,
    deps=[DART_FILER],
    backfill_policy=dg.BackfillPolicy.multi_run(DART_FILERS_PER_RUN),
    pool="dart",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"dart", "parquet"},
    freshness_policy=FRESH_FLOOR,
    # ONE DOCUMENT PER ROW GROUP (muffin-ingest#177): a filer's file holds every archive fetched
    # for it, a megabyte each.
    metadata={"merge_on": list(fd.MERGE_ON), "compression": "zstd", "row_group_rows": 1},
    description="Each DART filer's periodic filings and the annual-report archives not yet read.",
)
def raw_dart_filings(
    context: AssetExecutionContext, config: DartRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """A filing list per filer, then the archives due, in rounds across the run's filers.

    DUE: every annual report the list names, and every one `security_filing` already holds for the
    filer's securities, that is not read at the current parser version and that raw does not hold.

    A FILER WHOSE STORED LIST IS YOUNGER THAN `list_max_age_days` IS NOT LISTED: its archives are
    planned from that list's pages, so the due archives are fetched exactly as if it had been asked.

    THE OUTCOMES PARTITION WHAT THEY COUNT (`LIST_OUTCOMES` sums to `requested`,
    `ARCHIVE_OUTCOMES` to `archives_named`). DART refusing in its body (`020`, its daily limit) or
    in its status line stops the run and leaves the rest `unasked`.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    now = datetime.now(UTC)
    reread = {k: set(v) for k, v in (config.reread or {}).items()}
    with postgres.connect() as conn:
        found = fd.filers(conn, keys)
    stats: Counter[str] = Counter(
        {
            **{f"list_{o}": 0 for o in LIST_OUTCOMES},
            **{f"archive_{o}": 0 for o in ARCHIVE_OUTCOMES},
        }
    )
    rows: list[dict[str, Any]] = []
    placed: dict[int, str] = {}
    deadline = time.monotonic() + config.budget_seconds
    stopped: str | None = None

    listed: dict[str, list[fd.Filed]] = {}
    for key in keys:
        if key not in found:
            stats["list_not_askable"] += 1
            continue
        # The newest walk's pages, page 1 first. A walk a transport failure cut short holds fewer
        # pages than its first page says there are: it settled nothing, and is listed again.
        pages_held = fd.newest_lists(
            raw_store.stored_rows_where(DART_FILINGS, key, {"document": "filings"})
        )
        walk = [fd.parse_filings(bytes(page["body"])) for page in pages_held]
        whole = bool(walk) and len(walk) >= min(max(walk[0].total_pages, 1), MAX_LIST_PAGES)
        newest_page = max(pages_held, key=lambda r: str(r.get("fetched_at")), default=None)
        if whole and _younger_than(newest_page, now, config.list_max_age_days):
            stats["list_fresh"] += 1
            listed[key] = [f for parsed in walk for f in parsed.filed]
            continue
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is not None:
            stats["list_unasked"] += 1
            continue
        page, pages, outcome = 1, 1, "answered"
        filed: list[fd.Filed] = []
        while page <= min(pages, MAX_LIST_PAGES):
            try:
                answer = dart.filings(key, page)
            except dart.DartRefused as exc:
                outcome, stopped = "refused", f"DART refused us ({exc})"
                break
            except dart.DartTransport:
                outcome = "transport"
                break
            row = fd.list_row(answer, page=page, run_id=run_id)
            rows.append(row)
            placed[id(row)] = key
            if row["outcome"] != "answered":
                outcome = row["outcome"]
                if outcome == "refused":
                    status = fd.parse_filings(answer.document.body).status
                    stopped = f"DART refused us in its body (status {status or 'unreadable'})"
                break
            parsed = fd.parse_filings(answer.document.body)
            filed.extend(parsed.filed)
            pages = max(parsed.total_pages, 1)
            page += 1
            time.sleep(config.pace_seconds)
        stats[f"list_{outcome}"] += 1
        if outcome == "answered":
            listed[key] = filed

    with postgres.connect() as conn:
        named = [f.rcept_no for filed in listed.values() for f in filed if f.is_annual]
        read = fd.read_already(conn, named)
        stored_due = {
            key: [f.accession for f in fd.filings(conn, found[key]) if f.due]
            for key in keys
            if key in found
        }
    queues: dict[str, list[str]] = {}
    for key in keys:
        if key not in found:
            continue
        newest_first = [
            f.rcept_no
            for f in sorted(listed.get(key, []), key=lambda f: f.filed or "", reverse=True)
            if f.is_annual and f.rcept_no not in read
        ]
        wanted = list(
            dict.fromkeys([*newest_first, *reversed(stored_due.get(key, [])), *reread.get(key, ())])
        )
        if not wanted:
            continue
        light = raw_store.stored_rows_for(
            DART_FILINGS, key, ("document", "accession", "outcome", "fetched_at", "run_id")
        )
        queue = []
        for rcept in wanted:
            held = fd.held_archive(light, rcept)
            # A body that was neither an archive nor DART saying no is asked again: it settled
            # nothing about the filing.
            if held is not None and held.outcome != "not_zip" and rcept not in reread.get(key, ()):
                stats["archive_held_already"] += 1
            else:
                queue.append(rcept)
        queues[key] = queue

    plan = _round_robin(queues)
    stats["archives_named"] = stats["archive_held_already"] + len(plan)
    for key, rcept in plan:
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is None and stats["bytes"] >= config.budget_bytes:
            stopped = "the byte budget is spent"
        if stopped is not None:
            stats["archive_unasked"] += 1
            continue
        try:
            answer = dart.instance(rcept)
        except dart.DartRefused as exc:
            stats["archive_refused"] += 1
            stopped = f"DART refused us ({exc})"
            continue
        except dart.DartTransport:
            stats["archive_transport"] += 1
            continue
        row = fd.archive_row(answer, rcept_no=rcept, run_id=run_id)
        rows.append(row)
        placed[id(row)] = key
        stats[f"archive_{row['outcome']}"] += 1
        stats["bytes"] += len(answer.document.body)
        time.sleep(config.pace_seconds)
    if stopped is not None:
        context.log.warning(
            "%s; %s lists and %s archives unasked",
            stopped,
            stats["list_unasked"],
            stats["archive_unasked"],
        )

    context.add_output_metadata(
        {
            "requested": len(keys),
            "list_outcomes": sum(stats[f"list_{o}"] for o in LIST_OUTCOMES),
            "archives_named": stats["archives_named"],
            "archive_outcomes": sum(stats[f"archive_{o}"] for o in ARCHIVE_OUTCOMES),
            "bytes": stats["bytes"],
            **{f"list_{o}": stats[f"list_{o}"] for o in LIST_OUTCOMES},
            **{f"archive_{o}": stats[f"archive_{o}"] for o in ARCHIVE_OUTCOMES},
        }
    )
    stored = context.instance.get_materialized_partitions(DART_FILINGS)
    return partitioned.by_partition(
        context,
        rows,
        key=lambda row: placed[id(row)],
        unchanged=[k for k in keys if k in stored],
    )


# ── NSE (India) ────────────────────────────────────────────────────────────────────────────────

NSE_FILINGS = dg.AssetKey("raw_nse_filings")
#: The registry asset that maps Indian equities to NSE symbols (the registries family).
NSE_FILER = dg.AssetKey("security_nse_filer")


class NseRun(dg.Config):
    """What bounds an NSE run, and what an operator may ask beyond the queue."""

    #: Filings to fetch again whatever their marks and raw say, by symbol
    #: (`{"RELIANCE": ["https://nsearchives.nseindia.com/corporate/xbrl/INDAS_....xml"]}`).
    reread: dict[str, list[str]] | None = None
    #: Wall-clock budget.
    budget_seconds: int = 900
    #: Seconds between requests: NSE sits behind Akamai's bot management, and the edge asked six
    #: companies every fifteen minutes.
    pace_seconds: float = 1.0
    #: A filer whose stored list is younger than this is not listed; its instances are planned
    #: from the stored list. 0 lists every filer.
    list_max_age_days: float = LIST_MAX_AGE_DAYS


#: How a results list and an instance fetch can end. Each set partitions what it counts. `fresh` is
#: a filer whose stored list is young enough not to ask again: it costs no request.
NSE_LIST_OUTCOMES = (
    "answered",
    "absent",
    "not_answer",
    "fresh",
    "refused",
    "transport",
    "unasked",
    "not_askable",
)
NSE_INSTANCE_OUTCOMES = (
    "answered",
    "absent",
    "not_answer",
    "refused",
    "transport",
    "held_already",
    "unasked",
)


@dg.asset(
    partitions_def=nse_filers,
    deps=[NSE_FILER],
    backfill_policy=dg.BackfillPolicy.multi_run(NSE_FILERS_PER_RUN),
    pool="nse",
    io_manager_key="parquet_io",
    group_name="segments",
    kinds={"nse", "parquet"},
    freshness_policy=FRESH_FLOOR,
    metadata={"merge_on": list(fn.MERGE_ON), "compression": "zstd"},
    description="Each NSE filer's annual results list and the instances not yet read.",
)
def raw_nse_filings(
    context: AssetExecutionContext, config: NseRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """A results list per filer, then the instances due, in rounds across the run's filers.

    DUE: every `.xml` result the list names, and every one `security_filing` already holds for the
    filer's securities, that is not read at the current parser version and that raw does not hold.

    A FILER WHOSE STORED LIST IS YOUNGER THAN `list_max_age_days` IS NOT LISTED: its instances are
    planned from that list, so the due instances are fetched exactly as if it had been asked.

    THE OUTCOMES PARTITION WHAT THEY COUNT (`NSE_LIST_OUTCOMES` sums to `requested`,
    `NSE_INSTANCE_OUTCOMES` to `instances_named`). A refusal (403, 429, 5xx, or a redirect, which
    would leave the cache) stops the run and leaves the rest `unasked`.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    now = datetime.now(UTC)
    reread = {k: {nse.canonical(x) for x in v} for k, v in (config.reread or {}).items()}
    with postgres.connect() as conn:
        found = fn.filers(conn, keys)
    stats: Counter[str] = Counter(
        {
            **{f"list_{o}": 0 for o in NSE_LIST_OUTCOMES},
            **{f"instance_{o}": 0 for o in NSE_INSTANCE_OUTCOMES},
        }
    )
    rows: list[dict[str, Any]] = []
    placed: dict[int, str] = {}
    deadline = time.monotonic() + config.budget_seconds
    stopped: str | None = None

    listed: dict[str, list[fn.Filed]] = {}
    for key in keys:
        if key not in found:
            stats["list_not_askable"] += 1
            continue
        held_list = fn.newest_list(
            raw_store.stored_rows_where(NSE_FILINGS, key, {"document": "filings"})
        )
        if held_list is not None and _younger_than(held_list, now, config.list_max_age_days):
            stats["list_fresh"] += 1
            listed[key] = fn.parse_filings(bytes(held_list["body"])) or []
            continue
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is not None:
            stats["list_unasked"] += 1
            continue
        try:
            answer = nse.filings(key)
        except nse.NseRefused as exc:
            stats["list_refused"] += 1
            stopped = f"NSE refused us ({exc})"
            continue
        except nse.NseTransport:
            stats["list_transport"] += 1
            continue
        row = fn.list_row(answer, run_id=run_id)
        rows.append(row)
        placed[id(row)] = key
        stats[f"list_{row['outcome']}"] += 1
        if row["outcome"] == "answered":
            listed[key] = fn.parse_filings(answer.document.body) or []
        time.sleep(config.pace_seconds)

    with postgres.connect() as conn:
        named = [f.link for filed in listed.values() for f in filed]
        read = fn.read_already(conn, named)
        stored_due = {
            key: [f.accession for f in fn.filings(conn, found[key]) if f.due]
            for key in keys
            if key in found
        }
    queues: dict[str, list[str]] = {}
    for key in keys:
        if key not in found:
            continue
        newest_first = [
            f.link
            for f in sorted(listed.get(key, []), key=lambda f: f.period_end or "", reverse=True)
            if f.link not in read
        ]
        wanted = list(
            dict.fromkeys([*newest_first, *reversed(stored_due.get(key, [])), *reread.get(key, ())])
        )
        if not wanted:
            continue
        light = raw_store.stored_rows_for(
            NSE_FILINGS, key, ("document", "accession", "outcome", "fetched_at", "run_id")
        )
        queue = []
        for link in wanted:
            held = fn.held_instance(light, link)
            # A body that was not an instance settled nothing about the filing: asked again.
            if (
                held is not None
                and held.outcome != "not_answer"
                and link not in reread.get(key, ())
            ):
                stats["instance_held_already"] += 1
            else:
                queue.append(link)
        queues[key] = queue

    plan = _round_robin(queues)
    stats["instances_named"] = stats["instance_held_already"] + len(plan)
    for key, link in plan:
        if stopped is None and time.monotonic() >= deadline:
            stopped = "the time budget is spent"
        if stopped is not None:
            stats["instance_unasked"] += 1
            continue
        try:
            answer = nse.instance(link)
        except nse.NseRefused as exc:
            stats["instance_refused"] += 1
            stopped = f"NSE refused us ({exc})"
            continue
        except nse.NseTransport:
            stats["instance_transport"] += 1
            continue
        row = fn.instance_row(answer, link=link, run_id=run_id)
        rows.append(row)
        placed[id(row)] = key
        stats[f"instance_{row['outcome']}"] += 1
        stats["bytes"] += len(answer.document.body)
        time.sleep(config.pace_seconds)
    if stopped is not None:
        context.log.warning(
            "%s; %s lists and %s instances unasked",
            stopped,
            stats["list_unasked"],
            stats["instance_unasked"],
        )

    context.add_output_metadata(
        {
            "requested": len(keys),
            "list_outcomes": sum(stats[f"list_{o}"] for o in NSE_LIST_OUTCOMES),
            "instances_named": stats["instances_named"],
            "instance_outcomes": sum(stats[f"instance_{o}"] for o in NSE_INSTANCE_OUTCOMES),
            "bytes": stats["bytes"],
            **{f"list_{o}": stats[f"list_{o}"] for o in NSE_LIST_OUTCOMES},
            **{f"instance_{o}": stats[f"instance_{o}"] for o in NSE_INSTANCE_OUTCOMES},
        }
    )
    stored = context.instance.get_materialized_partitions(NSE_FILINGS)
    return partitioned.by_partition(
        context,
        rows,
        key=lambda row: placed[id(row)],
        unchanged=[k for k in keys if k in stored],
    )
