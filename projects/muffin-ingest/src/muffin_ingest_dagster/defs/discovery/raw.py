"""Stage 1 of the discovery family: what the provider sent, kept whole."""

from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import openfigi
from muffin_ingest.providers import openfigi as figi
from muffin_ingest.providers import sec_nport

from muffin_ingest_dagster.defs.discovery.partitions import (
    DIRECTORY_REFRESH_CRON,
    NPORT_PER_RUN,
    SWEEP_RESUME_TAG,
    VENUE_STALE_AFTER,
    exchange_sweeps,
    nport_filings,
)
from muffin_ingest_dagster.defs.discovery.queries import (
    DirectoryQuery,
    directory_queries,
    query_for,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: How many `/v3/filter` pages one query may fetch in a run: ALL OF THEM. OpenFIGI serves at most
#: 150 pages of 100 per query, so no walk can be longer. This was 40, sized for the anonymous
#: pacing (12 s a page); keyed it is 3 s, so the longest walk (US.common, capped at 150 pages) holds
#: the `openfigi_filter` pool for ~7.5 minutes, and nothing else wants that pool. A walk now ends in
#: the run that started it unless the provider refuses, and only then does `unfinished_sweeps`
#: resume it from the cursor in its file.
SWEEP_MAX_PAGES = openfigi.FILTER_MAX_PAGES


#: SECONDS BETWEEN PAGES — AND THE PROVIDER, NOT THIS MODULE, KNOWS WHICH NUMBER APPLIES.
#:
#: This was 2.5 s, from "the anonymous limit is 25 requests per minute" — a ceiling measured on
#: `/v3/mapping`. Re-measured on `/v3/filter` 2026-09-20 it is ~5 a minute, so it became 12 s.
#: Then, 2026-09-21: `OPENFIGI_API_KEY` was in this container's environment the whole time and the
#: provider never sent it, so BOTH of those were the ANONYMOUS budget, measured while holding a
#: key. Keyed, 15 pages sustained at 0.3 s with no 429.
#:
#: So the constant is gone and the budget is read per run. A pool bounds CONCURRENCY; this bounds
#: the MINUTE, which a pool cannot express — that part was always right.
def sweep_pacing() -> float:
    return figi.sweep_pacing_s()


#: HOW LONG A REFUSAL IS WAITED OUT, AND HOW MANY TIMES. OpenFIGI's filter bucket holds ~20
#: requests and refills at 17-25 a minute (measured 2026-09-22), so a minute's wait refills it.
#: Keyed pacing is 3 s, which is 20 a minute — AT the refill rate, not under it — so a long walk can
#: drain the bucket and a refusal is ordinary rather than a fault. Before 2026-10-04 a refusal ended
#: the query, and with four queries in a run the next three were then refused on their first page.
#: Waiting it out keeps the walk going; a refusal that outlasts three minutes is the provider
#: refusing us, not the minute.
REFUSAL_RETRIES = 2


def refusal_cooldown() -> float:
    """Seconds to wait after a refusal before asking for the same page again."""
    return 65.0


def _pause(seconds: float) -> None:
    """Every wait the walk makes goes through here, so a test can record the waits without
    sleeping them — and without replacing `time.sleep` under Dagster's own executor."""
    import time

    time.sleep(seconds)


#: What a page fetched with no cursor records as the cursor it was fetched WITH. A string rather
#: than NULL so every page carries a hashable merge key — see `merge_on` on `raw_exchange_sweep`.
FIRST_PAGE_CURSOR = ""


def _cik_accession(key: str) -> tuple[str, str]:
    cik, accession = key.split(":", 1)
    return cik, accession


def _nport_partition(row: dict[str, Any]) -> str:
    return f"{row['cik']}:{row['accession']}"


# --- DISCOVERY ----------------------------------------------------------------------------------


@dg.asset(
    pool="sec",
    io_manager_key="parquet_io",
    group_name="universe",
    kinds={"sec", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=7)),
    description="SEC's company_tickers_mf.json, exactly as served. ~1.2 MB, ~28,500 fund lines.",
)
def raw_fund_directory(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """The only keyless map from a tracked fund's symbol to the `(cik, seriesId)` its filing needs.

    A DOCUMENT IS A ROW WITH A `body` COLUMN — same storage class as every other raw lane.
    """
    doc = sec_nport.fund_directory()
    context.add_output_metadata({"bytes": len(doc.body), "sha256": doc.sha256, "url": doc.url})
    return [doc.as_row(context.run.run_id)]


@dg.asset(
    partitions_def=nport_filings,
    backfill_policy=dg.BackfillPolicy.multi_run(NPORT_PER_RUN),
    pool="sec",
    io_manager_key="parquet_io",
    group_name="universe",
    kinds={"sec", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=45)),
    # A NEW FILING IS FETCHED ONCE, AUTOMATICALLY. This lane shipped with no condition — the
    # sensor made a filing VISIBLE and an operator fetched it — and measured 2026-09-26 it had
    # therefore never run at all: `raw_nport_filing`, `discovered_security` and `fund_holding` had
    # zero materialisations while the edge's `fund-holdings` kept writing every table they own.
    # Retiring that resource needs this lane to run on its own, and the cost is one SEC request per
    # fund per quarter. `on_missing()` requests a partition when it BECOMES missing — a key the
    # sensor adds — and never again for it: a failed fetch is re-run by an operator, not every 30
    # seconds against SEC. Keys already in the grid when this shipped are not requested either
    # (measured on 1.13.22 for `on_missing()`, 2026-09-20); they were backfilled by hand.
    automation_condition=dg.AutomationCondition.on_missing(),
    description="One N-PORT primary_doc.xml, exactly as SEC served it.",
)
def raw_nport_filing(context: AssetExecutionContext) -> Any:
    """One filing per partition, keyed `{cik}:{accession}`.

    THE KEY CARRIES THE FILER because the Archives path needs it and the accession alone cannot
    produce it — measured 2026-09-12, the same document 404s under the CIK the accession embeds and
    is served under the directory's. `cik` and `accession` are PROVENANCE columns added beside the
    body (never derived into it), so stage 2 can re-group files without re-fetching.
    """
    keys = list(context.partition_keys)
    rows: list[dict[str, Any]] = []
    for key in keys:
        cik, accession = _cik_accession(key)
        doc = sec_nport.primary_doc(cik, accession)
        row = doc.as_row(context.run.run_id)
        row["cik"] = cik
        row["accession"] = accession
        rows.append(row)
    context.add_output_metadata(
        {"filings": len(keys), "bytes": sum(len(row["body"]) for row in rows)}
    )
    # THROUGH THE SHARED SEAM, not a local `if len(keys) > 1`. The hand-rolled version was right
    # here by luck — every partition of this lane always produces exactly one row — and wrong in
    # the lane that copied it, where a key with no answer vanished from the mapping and Dagster
    # recorded the partition materialized with no file behind it.
    return partitioned.by_partition(context, rows, key=_nport_partition)


#: WHEN THE DIRECTORY IS WALKED: on the first of each month (every query, from page one), and as
#: soon as a new query appears. Both built in and serialisable, so the daemon's default automation
#: sensor evaluates them. Resuming a walk the provider refused part-way is `unfinished_sweeps`' job,
#: not a condition's: in Dagster 1.13 a check on a partitioned asset is UNPARTITIONED unless
#: declared with a preview `partitions_def`, and even then it names a partition only in a
#: single-partition step — so `any_checks_match(check_failed())` would match EVERY query the moment
#: one walk stopped, and re-walk all 237 every tick (`compute_subset_with_status` returns the full
#: subset).
SWEEP_CONDITION = (
    dg.AutomationCondition.on_cron(DIRECTORY_REFRESH_CRON) | dg.AutomationCondition.on_missing()
).with_label("monthly, and when a new query appears")


#: ONE QUERY PER RUN, because a run that succeeds marks every partition it covers as materialized.
#: With four queries a run (until 2026-10-04), a refusal in the second left the third and fourth
#: unasked, and the run still claimed them: a new query read as walked with nothing stored, and a
#: monthly refresh read as done while the file held last month's walk. One query per run makes the
#: claim and the walk the same thing. The cost is a run's overhead (~5-10 s) per query, ~237 runs a
#: month on a pool nothing else uses.
SWEEP_QUERIES_PER_RUN = 1


@dg.asset(
    partitions_def=exchange_sweeps,
    backfill_policy=dg.BackfillPolicy.multi_run(SWEEP_QUERIES_PER_RUN),
    pool="openfigi_filter",
    io_manager_key="parquet_io",
    group_name="universe",
    kinds={"openfigi", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=VENUE_STALE_AFTER),
    automation_condition=SWEEP_CONDITION,
    metadata={"merge_on": ["exch_code", "cursor_from"]},
    description="Pages of one directory query's /v3/filter answer, verbatim, resumed by cursor.",
)
def raw_exchange_sweep(
    context: AssetExecutionContext, raw_store: RawStore, postgres: Postgres
) -> Any:
    """One page of `/v3/filter` per fetched page, filed under the QUERY it answers.

    ONE PARTITION PER QUESTION, since 2026-10-04. A key is a row of `market.directory_query`:
    `US.common`, `US.reit`, `US.dr`, `US.partnership`, and `US.arca`, which asks NYSE Arca and is
    filed under US. Each has its own `total`, cap, cursor and completeness, which is what a
    partition claims (`defs/discovery/queries.py`). The pages record the REQUEST (`exch_code`,
    `security_type2`) beside the cursors: an empty page carries no rows to read the type from, and
    that pair is how a run covering several queries routes each page to its own file.

    THE PARTITION HOLDS ONE WALK — the current one, finished or in progress. OpenFIGI has no as-of,
    so a venue's directory can only be refreshed by walking it again from the beginning, and a
    partition that accumulated every walk it had ever done would grow without bound and could not
    answer which listings are current.

    THE CURSOR LIVES IN THE FILE: each page records the cursor it was fetched WITH (`cursor_from`,
    empty for a walk's first page) beside the cursor the provider handed back (`cursor_at`). The
    last row's `cursor_at` is therefore both the resume point and the answer to "did this walk
    finish?" — null means it did. No second control table, and
    `venue_sweep_reached_its_last_page` reads exactly that.

    SO A RE-MATERIALISATION IS ONE OF TWO THINGS, and the asset says which per partition:

    * the stored walk FINISHED (or there is none) — this run starts a NEW walk from page one, and
      its rows are the whole answer, so the partition is marked `partitioned.Complete` and the
      manager REPLACES the file. Without that the two walks' pages would coexist for ever.
    * the stored walk STOPPED mid-way — this run RESUMES it from the stored cursor, and its pages
      merge into the ones already held. Replacing here is the defect this change fixes: the old
      code returned only the pages of the current run against a manager that replaces, so a
      backfill of a half-swept venue kept the tail and silently discarded everything before it,
      while `_last_cursor`'s docstring claimed the file was "REBUILT … with every page fetched
      since the beginning".

    AN EMPTY WALK REPLACES NOTHING. A venue throttled on its very first page returns no rows, and
    the manager's own guard keeps the stored directory rather than deleting a venue to record a
    refusal.

    A REFUSAL IS WAITED OUT, then filed. The same page is asked again after a cool-down
    (`REFUSAL_RETRIES`); a refusal that outlasts them stops the run, which still files the pages it
    fetched — the walk is unfinished, its check fails, and `unfinished_sweeps` resumes it from the
    file's cursor within the hour. If the run fetched nothing at all it FAILS instead, so a query
    the provider never answered cannot read as walked. A 429 must never make a walk read as
    exhausted — the DART windowed-sweep lesson.
    """
    with postgres.connect() as conn:
        queries = directory_queries(conn)
    keys = list(context.partition_keys)
    # A RESUME NEVER STARTS A NEW WALK. `unfinished_sweeps` asks for a walk the provider refused
    # part-way; by the time its run starts, another run may have finished that walk, and starting
    # again from page one would spend a whole query to learn nothing. Only a refresh — the monthly
    # tick, a new query, an operator — begins a walk.
    resume_only = context.run.tags.get(SWEEP_RESUME_TAG) == "true"
    rows: list[dict[str, Any]] = []
    fresh: set[str] = set()
    finished_already: list[str] = []
    asked: list[str] = []
    refused: list[str] = []
    for key in keys:
        query = query_for(queries, key)
        if refused:
            # A REFUSAL THAT OUTLASTED ITS COOL-DOWNS STOPS THE RUN. Asking the next query would be
            # refused too, and each refused request is one the provider counts against us.
            continue
        stored = raw_store.stored_rows_for(context.asset_key, key, columns=["cursor_at", "page"])
        cursor, next_page = _resume_state(stored)
        if cursor is None and resume_only and stored:
            finished_already.append(key)
            continue
        if cursor is None:
            fresh.add(key)
        walked, was_refused = _sweep_query(
            context, query, cursor=cursor, first_page=next_page, paced=bool(asked)
        )
        asked.append(key)
        rows += walked
        if was_refused:
            refused.append(key)
    unasked = [k for k in keys if k not in asked and k not in finished_already]
    if refused and not rows:
        # NOTHING WAS FETCHED, SO FAILING LOSES NOTHING, and succeeding would claim a walk that did
        # not happen: a new query would read as walked, and a refresh as done while the file still
        # holds the previous walk. A failed run leaves the partition's last materialization where it
        # was, so its freshness policy reports the age of the data rather than of the attempt.
        raise dg.Failure(
            description=f"openfigi refused {', '.join(refused)} on its first page, "
            f"{REFUSAL_RETRIES + 1} times; nothing was fetched and nothing is claimed",
            metadata={"refused": ", ".join(refused), "unasked": ", ".join(unasked) or "none"},
        )
    if unasked:
        # Only a run covering several queries can get here — an operator's range, since automation
        # runs one query at a time. Those partitions are claimed without having been asked, so say
        # which, loudly; the check names them only if nothing is stored for them.
        context.log.warning(
            "openfigi refused %s; %s were not asked and are claimed by this run without a walk",
            ", ".join(refused),
            ", ".join(unasked),
        )
    # THE FOUR OUTCOMES SUM TO `queries` — every key is a new walk, a resumed one, one a resume
    # found already finished, or one a refusal left unasked. `refused` counts the walks among the
    # first two that stopped on a refusal.
    context.add_output_metadata(
        {
            "queries": len(keys),
            "pages": len(rows),
            "new_walks": len(fresh),
            "resumed_walks": len(asked) - len(fresh),
            "already_finished": len(finished_already),
            "unasked": len(unasked),
            "refused": len(refused),
        }
    )
    by_request = {q.request: q.key for q in queries.values()}
    return partitioned.by_partition(
        context,
        rows,
        key=lambda r: by_request[(str(r["exch_code"]), str(r["security_type2"]))],
        complete=fresh,
    )


def _sweep_query(
    context: AssetExecutionContext,
    query: DirectoryQuery,
    *,
    cursor: str | None,
    first_page: int,
    paced: bool,
) -> tuple[list[dict[str, Any]], bool]:
    """Walk one query from `cursor`: `(the pages fetched, whether the provider refused us)`.

    `paced` says a request has already gone out in this run, so even this query's first page waits:
    the pacing is per request, not per query, and a gap of zero between two queries spent the
    bucket's slack until 2026-10-04.
    """
    from muffin_ingest.providers.openfigi import OpenFigiThrottled, OpenFigiUnavailable

    pacing = sweep_pacing()
    rows: list[dict[str, Any]] = []
    refused = False
    for step in range(SWEEP_MAX_PAGES):
        if step or paced:
            _pause(pacing)
        asked_with = cursor if cursor is not None else FIRST_PAGE_CURSOR
        doc = None
        for attempt in range(REFUSAL_RETRIES + 1):
            if attempt:
                _pause(refusal_cooldown())
            try:
                doc = figi.filter_exchange(
                    query.exch_code_asked, cursor=cursor, security_type2=query.security_type2
                )
                break
            except (OpenFigiThrottled, OpenFigiUnavailable) as refusal:
                # A REFUSAL IS NOT COMPLETION and not an absence. A 429 refuses the minute; a 200
                # carrying "There was an error while processing this request." is the provider's
                # transient fault, already retried once past the cache by the provider. Both mean
                # "not this time", so the same page is asked again after the bucket refills. Its
                # exact words are logged: "could not answer" is the provider's sentence.
                context.log.warning(
                    "openfigi refused %s page %s (attempt %s of %s): %s",
                    query.key,
                    first_page + step,
                    attempt + 1,
                    REFUSAL_RETRIES + 1,
                    refusal,
                )
        if doc is None:
            # STILL REFUSED AFTER THE COOL-DOWNS. The pages so far are the provider's answer and are
            # filed; the walk is unfinished, its cursor is in the file, and `unfinished_sweeps`
            # resumes it. A refusal must never make a walk read as exhausted — the DART
            # windowed-sweep lesson.
            refused = True
            break
        # PARSING THE PAGE IS ALSO VALIDATING IT: an error-shaped 200 (`{"error": "…"}`) must
        # refuse the run rather than be stored as a page.
        _, next_cursor, _ = openfigi.parse_filter(doc.body, exch_code=query.exch_code_asked)
        row = doc.as_row(context.run.run_id)
        # THE REQUEST, RECORDED: the code and type the provider was asked about (raw rule 3c — an
        # empty page has no rows to read either from). `exch_code` is what was ASKED, so a
        # `US.arca` page says `UP`; which venue it is filed under is stage 2's reading of
        # `market.directory_query`, not a fact about the page.
        row["exch_code"] = query.exch_code_asked
        row["security_type2"] = query.security_type2
        # THE CURSOR THE PAGE WAS FETCHED WITH IS THE PAGE'S OWN IDENTITY, and `cursor_at` is not:
        # the LAST page of a walk has `cursor_at` null, so keying the merge on it would make every
        # walk's final page collide with every other's. `cursor_from` is distinct per page within
        # a walk by construction, because a repeat would be an infinite loop.
        row["cursor_from"] = asked_with
        row["cursor_at"] = next_cursor
        row["page"] = first_page + step
        rows.append(row)
        if not next_cursor:
            break
        cursor = next_cursor
    # THE RESUME POINT IS THE STORED ROW'S, NOT THE LOOP VARIABLE'S. `cursor` is only advanced
    # when the provider hands back a next cursor, so a walk that finishes naturally leaves it
    # holding the cursor the LAST page was fetched with — and the first live AU sweep therefore
    # logged "resumes at 'QW9Fc1FrSkhNREl6UjB...'" for a venue that had reached its last page and
    # whose check correctly passed. A message that says the opposite of the truth is worse than no
    # message: it sends a reader to re-walk a directory that is complete. Read the same field the
    # check reads, so the two cannot disagree.
    resumes_at = rows[-1]["cursor_at"] if rows else cursor
    context.log.info(
        "query %s: %s pages from page %s, %s",
        query.key,
        len(rows),
        first_page,
        f"resumes at {resumes_at!r}" if resumes_at else "reached its last page",
    )
    return rows, refused


def _resume_state(stored: Any) -> tuple[str | None, int]:
    """Where the stored walk left off: `(cursor to resume from, the next page number)`.

    `(None, 0)` means START A NEW WALK — the venue has never been swept, or its stored walk reached
    its last page and a re-materialisation is a refresh rather than a resume. Both cases are the
    same instruction, which is why they return the same thing.

    Read from the rows the manager hands back rather than from a path built here: the price lane
    already paid for that drift, and a lookup that silently misses reads as "nothing stored" and
    re-walks a venue we already hold.
    """
    rows = [row for row in stored if "cursor_at" in row]
    if not rows:
        return None, 0
    last = rows[-1]
    cursor = last.get("cursor_at") or None
    if cursor is None:
        return None, 0
    page = last.get("page")
    return cursor, (int(page) + 1 if isinstance(page, int) else len(rows))
