"""Stage 1 of the SEC family: what SEC published, kept whole.

Two lanes. The insider lanes (the daily index, each tracked issuer's Form 4s, the quarterly data
sets) are partitioned by day and quarter. The filer lane (each CIK's company facts and submissions)
is partitioned by CIK and visited when the filer files, or on its floor day.
"""

import json
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import insider, sec_facts, sec_filings, sec_index
from muffin_ingest.providers import sec_edgar

from muffin_ingest_dagster.defs.sec.partitions import (
    ABSENT_REASK_DAYS,
    COMPANYFACTS_FLOOR_DAYS,
    FILERS_PER_RUN,
    SUBMISSIONS_FLOOR_DAYS,
    insider_quarters,
    quarter_key,
    quarter_of,
    sec_days,
    sec_filers,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: The raw assets, spelled once: later stages read what each already holds.
INDEX = dg.AssetKey("raw_sec_daily_index")
FORM4 = dg.AssetKey("raw_sec_form4")
DATASET = dg.AssetKey("raw_sec_insider_dataset")
COMPANYFACTS = dg.AssetKey("raw_sec_companyfacts")
SUBMISSIONS = dg.AssetKey("raw_sec_submissions")

#: SEC publishes a day's index at about 02:01 UTC the next day (measured 2026-10-26), and the lane
#: collects it at 11:00, so a day older than two is a stopped lane rather than a quiet one.
FRESH_DAY = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=2))

#: A quarter's data set came ~7 days after the quarter until 2026q2, which SEC had not published by
#: 2026-10-26. Four months is the most a quarter may wait before the lane is the suspect.
FRESH_QUARTER = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=120))

#: Every filer is visited at least once in `FLOOR_CYCLE_DAYS` days (30), so a filer document older
#: than this is a stopped lane, not a quiet filer.
FRESH_FILER = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=33))

#: Consecutive transport failures, with nothing answered, that stop a run: SEC is not reachable.
TRANSPORT_STOP = 3


class Form4Run(dg.Config):
    """What bounds a Form 4 run."""

    #: Ask again for Form 4s the day's file already holds, after a parser fix that needs the bodies
    #: again. Never for steady state: a filing does not change once disseminated.
    force: bool = False
    #: Wall-clock budget. A heavy day is ~370 tracked Form 4s at 5 requests a second, ~75 s.
    budget_seconds: int = 900


@dg.asset(
    partitions_def=sec_days,
    # ONE RUN FOR A RANGE: a day costs one or two requests, so a backfill since 2026-04-01 is a few
    # hundred requests, and a run per day would spend more on runs than on SEC.
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="sec",
    io_manager_key="parquet_io",
    group_name="sec",
    kinds={"sec", "parquet"},
    freshness_policy=FRESH_DAY,
    description=(
        "SEC's daily form index, one file a day, kept whole; a holiday keeps the quarter listing "
        "that proves it."
    ),
)
def raw_sec_daily_index(context: AssetExecutionContext) -> Any:
    """Each day's `form.<yyyymmdd>.idx`, decided from the quarter's listing before it is asked for.

    A DAY WITH NO INDEX ANSWERS 403, NOT 404 (measured 2026-10-26: Columbus Day, a Saturday, and a
    day before publication all return S3's `AccessDenied`), which reads as SEC refusing us. So each
    quarter's listing is asked first, once per run, and a day is requested only when the listing
    names it (`sec_index.classify_day`):
      * a weekend writes the empty marker, with no request;
      * a holiday keeps the listing, which names a later day and not this one;
      * a day not yet published fails the run, claiming nothing.
    """
    days = sorted(date.fromisoformat(k) for k in context.partition_keys)
    run_id = context.run.run_id
    stats: Counter[str] = Counter()
    listings: dict[tuple[int, int], tuple[dict[str, Any], set[date]]] = {}

    def listing(day: date) -> tuple[dict[str, Any], set[date]]:
        key = (day.year, (day.month - 1) // 3 + 1)
        if key not in listings:
            answer = sec_edgar.fetch(sec_edgar.quarter_listing_url(day), provider="sec")
            stats["calls"] += 1
            if answer.status != 200:
                raise dg.Failure(f"SEC has no listing for {key[0]}q{key[1]} (HTTP {answer.status})")
            [row] = sec_index.raw_rows(
                answer.document, kind="quarter_listing", http_status=answer.status, run_id=run_id
            )
            listings[key] = (row, sec_index.listing_days(answer.document.body))
        return listings[key]

    rows: list[dict[str, Any]] = []
    # PLACEMENT IS NOT STORAGE: a row's day chooses its file and is never written into it. A
    # holiday's evidence is the quarter's listing, whose body names no single day.
    placed: dict[int, str] = {}
    unpublished: list[date] = []
    for day in days:
        if day.weekday() >= 5:
            stats["index_weekend"] += 1
            continue
        row, listed = listing(day)
        evidence = set(listed)
        # THE LAST BUSINESS DAY OF A QUARTER is proven a holiday only by the next quarter's listing
        # (an observed New Year's Day falls on 31 December), once that quarter has begun.
        following = _next_quarter_start(day)
        if day not in listed and not any(d > day for d in listed) and following <= _today():
            _, later = listing(following)
            evidence |= later
        state = sec_index.classify_day(day, evidence)
        stats[f"index_{state}"] += 1
        if state == "holiday":
            evidence_row = dict(row)
            rows.append(evidence_row)
            placed[id(evidence_row)] = day.isoformat()
        elif state == "not_published":
            unpublished.append(day)
        elif state == "published":
            answer = sec_edgar.fetch(sec_edgar.daily_index_url(day), provider="sec")
            stats["calls"] += 1
            if answer.status != 200:
                raise dg.Failure(
                    f"SEC's listing names {day} but its index answered HTTP {answer.status}"
                )
            # Read once here, so a body that is not an index fails the run rather than reading
            # later as a day on which nobody filed.
            sec_index.parse(answer.document.body)
            stats["bytes"] += len(answer.document.body)
            for found in sec_index.raw_rows(
                answer.document, kind="form_index", http_status=answer.status, run_id=run_id
            ):
                rows.append(found)
                placed[id(found)] = day.isoformat()
    if unpublished:
        raise dg.Failure(
            f"SEC has not published {', '.join(d.isoformat() for d in unpublished)} yet; "
            "nothing in this run is claimed"
        )
    context.add_output_metadata({"days": len(days), "listings": len(listings), **stats})
    return partitioned.by_partition(context, rows, key=lambda r: placed[id(r)])


def _today() -> date:
    return datetime.now(UTC).date()


def _next_quarter_start(day: date) -> date:
    """The first day of the quarter after `day`'s."""
    q = (day.month - 1) // 3
    return date(day.year + (q == 3), (q + 1) % 4 * 3 + 1, 1)


@dg.asset(
    partitions_def=sec_days,
    deps=[INDEX],
    backfill_policy=dg.BackfillPolicy.multi_run(1),
    pool="sec",
    io_manager_key="parquet_io",
    group_name="sec",
    kinds={"sec", "parquet"},
    # EVERY FORM 4 ASKED FOR IS KEPT, keyed by its accession: a day file only grows, and a re-run
    # asks only for what it lacks.
    metadata={"merge_on": ["accession"]},
    freshness_policy=FRESH_DAY,
    description=(
        "The complete submission of each Form 4 of a tracked issuer, by the day SEC listed it."
    ),
)
def raw_sec_form4(
    context: AssetExecutionContext, config: Form4Run, postgres: Postgres, raw_store: RawStore
) -> Any:
    """Each Form 4 or 4/A the day's index names for a tracked CIK, asked once.

    SKIPPED ONCE THE QUARTER'S DATA SET IS HELD: the data set replaces this lane's rows for every
    filing of the quarter, so its Form 4s are no longer worth a request.

    THE OUTCOMES PARTITION THE FILINGS NAMED: answered, absent, refused, transport, held_already and
    unasked sum to `named`. A refusal (403, 429) stops the run; so do three transport failures in a
    row before anything answered. Neither is ever an absence.
    """
    day = date.fromisoformat(context.partition_key)
    stored_index = raw_store.stored_rows_for(
        INDEX, context.partition_key, columns=("document", "http_status", "body")
    )
    held_rows = raw_store.stored_rows_for(FORM4, context.partition_key, columns=("accession",))
    if not stored_index and day.weekday() < 5:
        raise dg.Failure(f"{day}'s index is not collected yet; run raw_sec_daily_index first")
    covered = bool(raw_store.stored_rows_for(DATASET, quarter_key(*_quarter(day)), ("fetched_at",)))
    if covered:
        context.add_output_metadata({"covered_by_dataset": True, "named": 0})
        return partitioned.Unchanged() if held_rows else []

    index = [r for r in stored_index if r.get("document") == "form_index"]
    with postgres.connect() as conn:
        ciks = set(insider.tracked_issuers(conn))
    named: dict[str, sec_index.IndexLine] = {}
    for found in index:
        for line in sec_index.named(
            sec_index.parse(bytes(found["body"])), ciks, insider.FORM4_FORMS
        ):
            named.setdefault(line.accession, line)
    held = {str(r["accession"]) for r in held_rows}

    stats: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    deadline = time.monotonic() + config.budget_seconds
    asked = list(named.values())
    transport_in_a_row = 0
    answered_any = False
    for position, line in enumerate(asked):
        if line.accession in held and not config.force:
            stats["form4_held_already"] += 1
            continue
        if time.monotonic() >= deadline:
            stats["form4_unasked"] += len(asked) - position
            context.log.warning("budget spent; %s Form 4s unasked", len(asked) - position)
            break
        try:
            answer = sec_edgar.fetch(
                sec_edgar.filing_text_url(line.cik, line.accession), provider="sec"
            )
        except sec_edgar.SecRefused as exc:
            stats["form4_refused"] += 1
            stats["form4_unasked"] += len(asked) - position - 1
            context.log.warning("SEC refused us (%s); stopping", exc)
            break
        except sec_edgar.SecTransport as exc:
            stats["form4_transport"] += 1
            transport_in_a_row += 1
            if transport_in_a_row >= TRANSPORT_STOP and not answered_any:
                stats["form4_unasked"] += len(asked) - position - 1
                context.log.warning(
                    "%s transport failures and nothing answered: %s", TRANSPORT_STOP, exc
                )
                break
            continue
        transport_in_a_row = 0
        if answer.status == 404:
            # NAMED BY TODAY'S INDEX AND ABSENT: not a filing with no trades. Counted, not filed.
            stats["form4_absent"] += 1
            continue
        answered_any = True
        stats["form4_answered"] += 1
        stats["bytes"] += len(answer.document.body)
        rows += insider.form4_raw_rows(
            answer.document, accession=line.accession, run_id=context.run.run_id
        )

    outcomes = sum(v for k, v in stats.items() if k.startswith("form4_"))
    context.add_output_metadata(
        {"covered_by_dataset": False, "named": len(named), "outcomes": outcomes, **stats}
    )
    if not rows and held_rows:
        return partitioned.Unchanged()
    return rows


def _quarter(day: date) -> tuple[int, int]:
    return day.year, (day.month - 1) // 3 + 1


@dg.asset(
    partitions_def=insider_quarters,
    backfill_policy=dg.BackfillPolicy.multi_run(1),
    pool="sec",
    io_manager_key="parquet_io",
    group_name="sec",
    kinds={"sec", "parquet"},
    freshness_policy=FRESH_QUARTER,
    description="SEC's Insider Transactions Data Set for one quarter, the zip kept whole.",
)
def raw_sec_insider_dataset(context: AssetExecutionContext) -> Any:
    """One quarter's `<yyyy>q<n>_form345.zip`. A 404 is a quarter not published yet: the run
    fails and claims nothing, rather than filing a quarter with no trades."""
    year, quarter = quarter_of(context.partition_key)
    answer = sec_edgar.fetch(
        sec_edgar.insider_dataset_url(year, quarter), provider="sec", timeout_s=180
    )
    if answer.status == 404:
        raise dg.Failure(f"SEC has not published {year}q{quarter}'s insider data set yet")
    context.add_output_metadata({"bytes": len(answer.document.body), "calls": 1})
    return insider.dataset_raw_rows(answer.document, run_id=context.run.run_id)


# --- the filer lane ------------------------------------------------------------------------------


class SecFilerRun(dg.Config):
    """What bounds a filer run, and what the due sensor knows that stored metadata cannot say."""

    #: Ask for both documents whatever raw holds: after a parser fix that needs fresh bodies.
    force: bool = False
    #: Wall-clock budget. A first collection of 50 filers is ~150 requests and ~250 MB.
    budget_seconds: int = 900
    #: THE INDEX DAY ON WHICH EACH FILER LAST FILED A KEPT FORM, from `sec_filers_due`: its
    #: submissions are due if the newest answer was fetched before that day ended.
    filed_submissions: dict[str, str] | None = None
    #: The same, for a form that changes company facts (`filing_form.refreshes_facts`).
    filed_companyfacts: dict[str, str] | None = None


#: Every outcome a filer's document can have in a run. Per document, they partition the askable
#: filers, so each set sums to `requested - not_askable`.
FILER_OUTCOMES = (
    "answered",
    "absent",
    "absent_unproven",
    "unreadable",
    "refused",
    "transport",
    "not_due",
    "unasked",
)

#: The two documents, in the order a visit asks for them: the submissions decide nothing about the
#: facts, but a filer SEC does not know answers 404 for both, and the first says so cheaply.
FILER_DOCUMENTS = ("submissions", "companyfacts")


def filer_due(
    stored: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    floor_days: int,
    filed_on: str | None,
    force: bool,
) -> str | None:
    """Why a filer document is due, or None.

    `stored` is the partition's rows for ONE document (the main submissions document, not its
    pages). The newest decides: a 404 is re-asked after `ABSENT_REASK_DAYS`; an answer is re-asked
    when the filer filed after it was fetched (`filed_on`, an index day), or on the floor.
    """
    if force:
        return "forced"
    dated = [
        (datetime.fromisoformat(str(r["fetched_at"])), int(r["http_status"]))
        for r in stored
        if r.get("fetched_at") is not None and r.get("http_status") is not None
    ]
    if not dated:
        return "never"
    newest, status = max(dated)
    age = now - newest
    if status == 404:
        return "absent_reask" if age >= timedelta(days=ABSENT_REASK_DAYS) else None
    if filed_on is not None:
        day_over = datetime.combine(
            date.fromisoformat(filed_on) + timedelta(days=1), datetime.min.time(), UTC
        )
        if newest < day_over:
            return "filed"
    return "floor" if age >= timedelta(days=floor_days) else None


def _facts_readable(body: bytes) -> bool:
    """Whether a 200 is a company facts document at all. A body that is not is never filed over
    the stored answer: one bad response must not replace the last good one."""
    try:
        payload = json.loads(body)
    except ValueError:
        return False
    return isinstance(payload, dict) and isinstance(payload.get("facts"), dict)


class _Stop(Exception):
    """SEC refused us, or three transport failures came before anything answered."""


@dataclass
class _FilerVisit:
    facts: list[dict[str, Any]] = field(default_factory=list)
    submissions: list[dict[str, Any]] = field(default_factory=list)
    #: The partition each row belongs to, by the row's identity: a row never carries its CIK.
    placed: dict[int, str] = field(default_factory=dict)
    #: 404s waiting for proof that SEC is answering in this run: (key, document, rows).
    pending_absent: list[tuple[str, str, list[dict[str, Any]]]] = field(default_factory=list)
    stats: Counter[str] = field(default_factory=Counter)
    data_answered: bool = False
    transport_in_a_row: int = 0
    last_error: str | None = None

    def file(self, key: str, document: str, rows: list[dict[str, Any]]) -> None:
        target = self.facts if document == "companyfacts" else self.submissions
        for row in rows:
            target.append(row)
            self.placed[id(row)] = key


class _FilerAsker:
    """One run's requests to data.sec.gov, counted, stopping on a refusal."""

    def __init__(self, visit: _FilerVisit) -> None:
        self.visit = visit

    def get(self, url: str) -> sec_edgar.SecAnswer | None:
        """The answer, or None for a transport failure. Raises `_Stop` to end the run."""
        self.visit.stats["calls"] += 1
        try:
            answer = sec_edgar.fetch(url, provider="sec-data")
        except sec_edgar.SecRefused as exc:
            self.visit.stats["throttled"] += 1
            self.visit.last_error = str(exc)
            raise _Stop(f"SEC refused us: {exc}") from exc
        except sec_edgar.SecTransport as exc:
            self.visit.last_error = str(exc)
            self.visit.transport_in_a_row += 1
            if self.visit.transport_in_a_row >= TRANSPORT_STOP and not self.visit.data_answered:
                raise _Stop(f"{TRANSPORT_STOP} transport failures and nothing answered") from exc
            return None
        self.visit.transport_in_a_row = 0
        self.visit.stats["bytes"] += len(answer.document.body)
        if answer.status == 200:
            self.visit.data_answered = True
        return answer


def _ask_submissions(
    asker: _FilerAsker, key: str, cik: int, stored: Sequence[Mapping[str, Any]], run_id: str
) -> str:
    """The main submissions document, then every older page it names that raw does not hold."""
    visit = asker.visit
    answer = asker.get(sec_edgar.submissions_url(cik))
    if answer is None:
        return "transport"
    rows = sec_filings.raw_rows(
        answer.document, kind="submissions", page=None, http_status=answer.status, run_id=run_id
    )
    if answer.status == 404:
        visit.pending_absent.append((key, "submissions", rows))
        return "absent"
    try:
        sec_filings.profile(answer.document.body)
        pages = sec_filings.older_pages(answer.document.body)
    except sec_filings.SubmissionsUnreadable as exc:
        visit.last_error = f"{key}: {exc}"
        return "unreadable"
    visit.file(key, "submissions", rows)
    # AN OLDER PAGE NEVER CHANGES ONCE WRITTEN (its filings are all older than the main document's
    # thousand), so a page raw holds is never asked again.
    held = {
        str(r["page"])
        for r in stored
        if r.get("page") and r.get("http_status") is not None and int(r["http_status"]) == 200
    }
    for name in pages:
        if name in held:
            visit.stats["pages_held"] += 1
            continue
        page = asker.get(sec_edgar.submissions_page_url(name))
        if page is None:
            visit.stats["pages_transport"] += 1
        elif page.status == 404:
            visit.stats["pages_absent"] += 1
        else:
            visit.stats["pages_fetched"] += 1
            visit.file(
                key,
                "submissions",
                sec_filings.raw_rows(
                    page.document,
                    kind="submissions_page",
                    page=name,
                    http_status=page.status,
                    run_id=run_id,
                ),
            )
    return "answered"


def _ask_companyfacts(asker: _FilerAsker, key: str, cik: int, run_id: str) -> str:
    visit = asker.visit
    answer = asker.get(sec_edgar.companyfacts_url(cik))
    if answer is None:
        return "transport"
    rows = sec_facts.raw_rows(answer.document, http_status=answer.status, run_id=run_id)
    if answer.status == 404:
        visit.pending_absent.append((key, "companyfacts", rows))
        return "absent"
    if not _facts_readable(answer.document.body):
        visit.last_error = f"{key}: company facts that are not a facts document"
        return "unreadable"
    visit.file(key, "companyfacts", rows)
    return "answered"


def _settle_absences(asker: _FilerAsker) -> None:
    """File the run's 404s only once SEC is shown to be answering: a 200 from data.sec.gov in the
    same run, or the control filer asked past the cache. Otherwise they are `absent_unproven`."""
    visit = asker.visit
    if not visit.pending_absent:
        return
    proven = visit.data_answered
    if not proven:
        visit.stats["control_asked"] += 1
        try:
            control = asker.get(sec_edgar.submissions_url(sec_edgar.CONTROL_CIK))
        except _Stop:
            control = None
        proven = control is not None and control.status == 200
        visit.stats["control_answered"] += int(proven)
    for key, document, rows in visit.pending_absent:
        if proven:
            visit.file(key, document, rows)
        else:
            visit.stats[f"{document}_absent"] -= 1
            visit.stats[f"{document}_absent_unproven"] += 1


@dg.multi_asset(
    name="raw_sec_filer",
    outs={
        "raw_sec_companyfacts": dg.AssetOut(
            io_manager_key="parquet_io",
            group_name="sec",
            kinds={"sec", "parquet"},
            # THE NEWEST ANSWER, AND AN ABSENCE BESIDE IT: company facts are cumulative, so an
            # answer replaces the one before it (same `http_status`), and a 404 cannot erase it.
            metadata={"merge_on": ["http_status"], "compression": "zstd"},
            freshness_policy=FRESH_FILER,
            description="Each filer's newest XBRL company facts document, kept whole.",
        ),
        "raw_sec_submissions": dg.AssetOut(
            io_manager_key="parquet_io",
            group_name="sec",
            kinds={"sec", "parquet"},
            # THE NEWEST MAIN DOCUMENT, AND EVERY OLDER PAGE, each fetched once.
            metadata={"merge_on": ["page", "http_status"], "compression": "zstd"},
            freshness_policy=FRESH_FILER,
            description="Each filer's newest submissions document and every older page it names.",
        ),
    },
    partitions_def=sec_filers,
    backfill_policy=dg.BackfillPolicy.multi_run(FILERS_PER_RUN),
    pool="sec",
)
def raw_sec_filer(
    context: AssetExecutionContext, config: SecFilerRun, postgres: Postgres, raw_store: RawStore
) -> tuple[Any, Any]:
    """Each filer's submissions and company facts, when either is due.

    WHAT A VISIT COSTS: nothing when neither document is due (two reads of stored metadata); one
    request for the submissions plus one per older page raw does not hold; one for company facts.

    THE OUTCOMES PARTITION THE FILERS, once per document: each askable filer has exactly one
    submissions outcome and one company facts outcome per run, so each set sums to
    `requested - not_askable`. A stop (a 403, a 429, or three transport failures before anything
    answered) leaves the rest `unasked`, never absent.

    A 404 IS FILED ONLY ONCE SEC IS SHOWN TO BE ANSWERING (`_settle_absences`). A filer we could
    not prove absent stays as it was.
    """
    keys = list(context.partition_keys)
    with postgres.connect() as conn:
        filers = insider.tracked_issuers(conn)
    visit = _FilerVisit()
    for document in FILER_DOCUMENTS:
        for outcome in FILER_OUTCOMES:
            visit.stats[f"{document}_{outcome}"] = 0
    asker = _FilerAsker(visit)
    now = datetime.now(UTC)
    deadline = time.monotonic() + config.budget_seconds
    run_id = context.run.run_id
    askable = [k for k in keys if int(k) in filers]

    for position, key in enumerate(askable):
        remaining = len(askable) - position
        if time.monotonic() >= deadline:
            for document in FILER_DOCUMENTS:
                visit.stats[f"{document}_unasked"] += remaining
            context.log.warning("budget spent; %s filers unasked", remaining)
            break
        cik = int(key)
        stored_sub = raw_store.stored_rows_for(
            SUBMISSIONS, key, columns=("page", "http_status", "fetched_at")
        )
        stored_facts = raw_store.stored_rows_for(
            COMPANYFACTS, key, columns=("http_status", "fetched_at")
        )
        reasons = {
            "submissions": filer_due(
                [r for r in stored_sub if not r.get("page")],
                now=now,
                floor_days=SUBMISSIONS_FLOOR_DAYS,
                filed_on=(config.filed_submissions or {}).get(key),
                force=config.force,
            ),
            "companyfacts": filer_due(
                stored_facts,
                now=now,
                floor_days=COMPANYFACTS_FLOOR_DAYS,
                filed_on=(config.filed_companyfacts or {}).get(key),
                force=config.force,
            ),
        }
        asked: list[str] = []
        try:
            for document in FILER_DOCUMENTS:
                reason = reasons[document]
                if reason is None:
                    visit.stats[f"{document}_not_due"] += 1
                    asked.append(document)
                    continue
                visit.stats[f"due_{reason}"] += 1
                outcome = (
                    _ask_submissions(asker, key, cik, stored_sub, run_id)
                    if document == "submissions"
                    else _ask_companyfacts(asker, key, cik, run_id)
                )
                visit.stats[f"{document}_{outcome}"] += 1
                asked.append(document)
        except _Stop as stop:
            # THE DOCUMENT BEING ASKED WAS REFUSED; the rest of this filer, and every filer after
            # it, were never asked.
            current = FILER_DOCUMENTS[len(asked)]
            visit.stats[f"{current}_refused"] += 1
            for document in FILER_DOCUMENTS[len(asked) + 1 :]:
                visit.stats[f"{document}_unasked"] += 1
            for document in FILER_DOCUMENTS:
                visit.stats[f"{document}_unasked"] += remaining - 1
            context.log.warning("%s; stopping with %s filers unasked", stop, remaining - 1)
            break
    _settle_absences(asker)
    if visit.last_error:
        context.log.warning("last error: %s", visit.last_error)

    common = {
        "requested": len(keys),
        "not_askable": len(keys) - len(askable),
        "calls": visit.stats["calls"],
        "bytes": visit.stats["bytes"],
        "throttled": visit.stats["throttled"],
        "control_asked": visit.stats["control_asked"],
        "control_answered": visit.stats["control_answered"],
        **{k: v for k, v in sorted(visit.stats.items()) if k.startswith("due_")},
    }
    context.add_output_metadata(
        {
            **common,
            **{k: v for k, v in sorted(visit.stats.items()) if k.startswith("companyfacts_")},
        },
        output_name="raw_sec_companyfacts",
    )
    context.add_output_metadata(
        {
            **common,
            **{k: v for k, v in sorted(visit.stats.items()) if k.startswith("submissions_")},
            **{k: v for k, v in sorted(visit.stats.items()) if k.startswith("pages_")},
        },
        output_name="raw_sec_submissions",
    )

    def place(row: Mapping[str, Any]) -> str:
        return visit.placed[id(row)]

    held_facts = context.instance.get_materialized_partitions(COMPANYFACTS)
    held_sub = context.instance.get_materialized_partitions(SUBMISSIONS)
    return (
        partitioned.by_partition(
            context, visit.facts, key=place, unchanged=[k for k in keys if k in held_facts]
        ),
        partitioned.by_partition(
            context, visit.submissions, key=place, unchanged=[k for k in keys if k in held_sub]
        ),
    )
