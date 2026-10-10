"""Stage 1 of the prices family: what the provider sent, kept whole."""

import time
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import price_chart, prices
from muffin_ingest.providers import openbb, yahoo_chart
from muffin_ingest.providers.documents import Document
from muffin_ingest.providers.isolation import BatchVerdict, fetch_with_isolation
from muffin_ingest.providers.vocab import throttled
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices.partitions import (
    HISTORY_PARTITIONS_PER_RUN,
    HISTORY_START,
    PROVIDER,
    security_partitions,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres


class PriceRun(dg.Config):
    """What bounds a run, rather than what it fetches."""

    #: Cap the universe. For a dual-run comparison or a first look, never for steady state.
    limit: int | None = None
    #: Wall-clock budget. MEASURED, NOT CHOSEN: 200 securities took 244 s, so the cross-section is
    #: ~1.22 s each and the 11,446 askable equities are **3.9 hours**. At the old default of one
    #: hour a nightly run covered a quarter of the universe and stopped — while its partition still
    #: claimed to have collected the whole cross-section, which is the false claim this design
    #: exists to make impossible. Five hours leaves headroom for a slow night.
    #:
    #: IT HOLDS THE `yfinance` POOL FOR THAT WHOLE TIME, so the history lane cannot run beside it.
    #: That is correct once history idles at zero, and is why the initial load runs before the
    #: schedule is started rather than beside it.
    budget_seconds: int = 18000


def _fetcher(start: date, end: date, warnings: list[str] | None = None) -> Any:
    """A `Fetcher` for `fetch_with_isolation`: raises on transport, returns [] on an empty answer.

    Those being different facts is the entire reason this pipeline is being rewritten, and the
    in-process hub is what keeps them distinguishable — over HTTP a throttled yfinance and a symbol
    the provider does not carry are both an empty 204.

    `warnings` COLLECTS WHAT THE PROVIDER SAID ABOUT ITSELF. `OBBject.warnings` is a provider
    declaring itself degraded while still returning 200; the `Fetcher` protocol returns rows, so
    without somewhere to put it the text was read for classification and then dropped — the one
    sentence explaining an odd value, absent from the artifact that holds the value.
    """

    def fetch(subjects: Sequence[str], timeout_s: float) -> list[dict[str, object]]:
        answer = openbb.price_history(subjects, start=start, end=end)
        if warnings is not None:
            warnings.extend(w for w in answer.warnings if w not in warnings)
        return answer.rows

    return fetch


# The two seam shapes now live in `muffin_ingest_dagster.partitioned`, shared by every lane —
# these were hand-copied into three asset modules with the key expression inlined differently
# each time, and two of those copies were wrong. See that module's header.
_by_partition = partitioned.by_partition


def _ask(
    context: AssetExecutionContext,
    by_symbol: dict[str, prices.Subject],
    *,
    start: date,
    end: date,
    deadline: float,
    postgres: Postgres | None = None,
    warnings: list[str] | None = None,
) -> BatchVerdict:
    """One provider call, and what it established about each symbol recorded as a probe.

    THE VERDICT IS AN OBSERVATION, kept in `market.identifier_probe` under this provider: a hit for
    each symbol that answered, a miss for each one the batch proved dead (asked alone, a control
    answered), nothing for the rest. `prices.symbol_probes` holds those rules; the askable query
    reads the misses back, bound to the symbol they were asked with. It replaced the ingest ledger
    on 2026-10-04, which recorded the same verdicts in a queue this lane no longer needs.

    WITHOUT A CONNECTION THIS RECORDS NOTHING, deliberately: the offline tests drive the real asset
    with no database, and a lane that could only run against Postgres would be a lane nothing could
    replay.
    """
    fetch = _fetcher(start, end, warnings)
    timeout = min(60.0, max(5.0, deadline - time.monotonic()))
    verdict = fetch_with_isolation(
        fetch,
        list(by_symbol),
        timeout_s=timeout,
        deadline=deadline,
        control=PROVIDER.control_subject,
    )
    if postgres is not None:
        probes = prices.symbol_probes(
            by_symbol, verdict, provider=PROVIDER.code, observed_at=datetime.now(UTC)
        )
        if probes:
            with postgres.connect() as conn, conn.cursor() as cur:
                upsert(
                    cur,
                    "market.identifier_probe",
                    probes,
                    conflict=["security_id", "scheme", "provider"],
                    # THE LATEST OBSERVATION WINS: a hit replaces last month's miss, and a re-asked
                    # miss refreshes its age.
                    update=["asked_with", "value", "outcome", "observed_at"],
                )
    return verdict


def _collect(
    context: AssetExecutionContext,
    subjects: list[prices.Subject],
    *,
    start: date,
    end: date,
    batch_size: int,
    budget_seconds: int,
    postgres: Postgres | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Ask for every subject in batches and return raw rows plus what the asking established.

    `postgres` IS WHAT TURNS AN OBSERVATION INTO A RECORD. Without it this counts `dead` and throws
    the fact away, so a symbol yfinance will never serve costs a real vendor request every single
    day — the vendor is asked once per SYMBOL whatever we batch. With it, each batch's verdict is
    written as `identifier_probe` rows (`_ask`), and a miss is recorded only when the subject was
    asked ALONE and a control answered.
    """
    deadline = time.monotonic() + budget_seconds
    rows: list[dict[str, Any]] = []
    stats = {
        "calls": 0,
        "answered": 0,
        "empty": 0,
        # Bars the provider returned that do not belong to the window we asked for. Counted rather
        # than dropped in silence: a non-zero value is a statement about the PROVIDER's idea of a
        # date range, and the day it becomes zero is the day this filter stopped being needed.
        "outside_window": 0,
        # NEVER FOLDED INTO `empty`, and the first version of this loop folded it. A batch that
        # RAISED tells us nothing about its subjects — "we never got an answer" and "the provider
        # answered and had nothing" are the distinction this whole pipeline is being rewritten
        # around, and counting the first as the second is how an outage becomes a population of
        # permanently dead securities. Found by driving it against production: openbb could not
        # import in the image, every call raised, and this reported fifty securities as having
        # answered nothing.
        "transport": 0,
        "dead": 0,
        "throttled": 0,
        "unasked": 0,
    }
    last_error: str | None = None
    consecutive_transport = 0

    since_last_call = 0.0
    for i in range(0, len(subjects), batch_size):
        batch = subjects[i : i + batch_size]
        if since_last_call:
            wait = PROVIDER.min_seconds_between_calls - (time.monotonic() - since_last_call)
            if wait > 0:
                time.sleep(wait)
        if time.monotonic() >= deadline:
            # NOT AN ERROR AND NOT AN ABSENCE. Subjects we never asked about must not look like
            # subjects that answered nothing — that conflation is what once recorded ~8,300
            # securities as permanently unanswerable.
            stats["unasked"] += len(subjects) - i
            context.log.warning(
                "budget of %ss spent; %s subjects unasked", budget_seconds, stats["unasked"]
            )
            break

        by_symbol = {s.symbol: s for s in batch}
        stats["calls"] += 1
        since_last_call = time.monotonic()
        # What the provider said about itself on this batch — collected by the fetcher and
        # written onto every raw row it produced.
        said: list[str] = []
        verdict: BatchVerdict = _ask(
            context,
            by_symbol,
            start=start,
            end=end,
            deadline=deadline,
            postgres=postgres,
            warnings=said,
        )
        if verdict.throttled_out:
            stats["throttled"] += 1
            # THE REST IS UNASKED, AND THIS BRANCH FORGOT TO SAY SO while the budget and transport
            # branches both did. Measured on the 2026-09-16 partition: refused at call 329 of ~601,
            # reported `unasked=0`, so 5,437 of 12,017 securities were never asked, and the day
            # lane's partition (deleted with that lane on 2026-10-04) materialised as complete. A
            # refused ask is not an answer, so the refused batch counts too.
            stats["unasked"] += len(subjects) - i
            context.log.warning(
                "provider is refusing us; stopping with %s subjects unasked rather than marking "
                "anything",
                stats["unasked"],
            )
            break

        # GROUPED, NOT NARROWED. `bars_by_symbol` parses each provider row into a `Bar` so the
        # window filter and the per-subject counters can work — but the `Bar` is used for those
        # DECISIONS only. What reaches raw is the provider's own row, whole; see
        # `prices.raw_rows`. Narrowing here is what dropped `open`/`high`/`low`/`vwap` at stage 1.
        sole = next(iter(by_symbol))
        parsed = prices.bars_by_symbol(verdict.rows, sole)
        by_symbol_rows = prices.provider_rows_by_symbol(verdict.rows, sole)
        # THE PROVIDER RETURNS BARS OUTSIDE THE WINDOW WE ASKED FOR, AND THEY ARE COUNTED HERE AND
        # REFUSED IN STAGE 2. Measured 2026-09-11 against the real hub:
        # `start_date=2026-09-09&end_date=2026-09-09` returns bars for BOTH 09-09 and 09-10 — a
        # degenerate range is widened rather than refused — and a run made while Tokyo was trading
        # also brought back a bar dated 09-11, a session still in progress whose "close" is not a
        # close and looks exactly like a real one.
        #
        # THEY USED TO BE DROPPED RIGHT HERE, which made stage 1 the place that decides what
        # belongs to a window — and a row deleted at fetch is a row no re-parse can recover. The
        # counter stays, because a non-zero value is a statement about the PROVIDER's idea of a
        # date range and the day it becomes zero is the day this rule stopped being needed.
        stats["outside_window"] += sum(
            1 for series in parsed.values() for b in series if not (start <= b.trade_date < end)
        )
        dead = {d.upper() for d in verdict.dead}
        answered_here = 0
        for symbol, subject in by_symbol.items():
            # THE PROVIDER ANSWERED, WHATEVER WINDOW THE BARS LANDED IN. This read the
            # window-filtered list until 2026-09-12, so a symbol whose only bars fell outside the
            # partition counted as `empty` — "the provider has nothing for this security" — which
            # is the one conflation this pipeline exists to remove.
            bars = parsed.get(symbol.upper(), [])
            if bars:
                answered_here += 1
                stats["answered"] += 1
                rows += prices.raw_rows(
                    subject,
                    by_symbol_rows.get(symbol.upper(), []),
                    provider=PROVIDER.code,
                    run_id=context.run.run_id,
                    observed=symbol,
                    warnings=said,
                )
            elif symbol.upper() in dead:
                stats["dead"] += 1
            elif verdict.error is not None:
                stats["transport"] += 1
            else:
                stats["empty"] += 1

        if verdict.error is not None:
            last_error = verdict.error
            consecutive_transport = 0 if answered_here else consecutive_transport + 1
        else:
            consecutive_transport = 0

        # THREE, NOT ONE. A single failed batch is a blip, and the isolation pass has already
        # re-asked each of its subjects alone. Three in a row with nothing answered anywhere means
        # the fault is at our end or the provider's, and asking the remaining five hundred batches
        # would only produce a longer record of the same thing.
        if consecutive_transport >= 3 and stats["answered"] == 0:
            stats["unasked"] += len(subjects) - (i + len(batch))
            context.log.error(
                "three consecutive batches failed and nothing has answered: %s", last_error
            )
            break

    if last_error:
        context.log.warning("last error: %s", last_error)
    return rows, stats


@dg.asset(
    partitions_def=security_partitions,
    # MULTI-RUN HERE AND SINGLE-RUN ON LANE A, AND THE PARTITION AXIS IS WHAT DECIDES.
    #
    # A DATE partition holds many securities, so one run is one batched sweep and `single_run` is
    # what makes a week-long gap cost one run instead of seven. A SECURITY partition holds one
    # subject — and `openbb_yfinance` calls `yf.download(..., threads=False)`, so the vendor is
    # asked ONCE PER SYMBOL whatever we do. There is nothing to batch ACROSS these partitions.
    #
    # That matters because the first version of this comment argued the opposite: that multi-run
    # "turns ten calls into ninety-six". It does not, for this provider — joining symbols collapses
    # OUR call count, never theirs. So `single_run` bought no provider saving here and cost an
    # unbounded memory footprint, which is exactly how it failed.
    backfill_policy=dg.BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN),
    pool="yfinance",
    io_manager_key="parquet_io",
    group_name="prices",
    kinds={"yfinance", "parquet"},
    # NO FRESHNESS POLICY, AND THAT IS THE POINT OF THIS LANE. It idles at zero by design — the
    # load is one backfill and then nothing until a new subject appears. A staleness window here
    # would go red the day after the load and stay red for ever, against a lane behaving exactly
    # as intended, and this codebase has twice paid for a gate left red for a reason nobody acts
    # on: the cost is never the ignored check, it is the next true positive behind it.
    #
    # "Is this subject loaded?" is answered by the PARTITION GRID, not by a clock — and since
    # 2026-09-19 that grid is durable, because the job that pruned the event log behind it was
    # retired for exactly this reason.
    #
    # THE PARTITION EXTENDS RATHER THAN BEING REPLACED. Without `merge_on` a run that fetched only
    # the days since the watermark would write those days ALONE over ten years of stored bars — the
    # extension deleting the history it was extending. The key is the provider's own `date` beside
    # our `security_id`; both are on every row `prices.raw_rows` emits, and a `datetime.date`
    # survives the Parquet round trip as a `datetime.date`, which is what makes it safe to key on.
    metadata={"merge_on": ["security_id", "date"]},
    description="Each security's bars, extended from where its own partition left off.",
)
def raw_price_history(
    context: AssetExecutionContext, config: PriceRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """Each security, from where its own partition left off.

    TWO COHORTS, NOT ONE WINDOW PER SUBJECT, and the reason is that a window costs bytes rather
    than requests: the vendor is asked ONCE PER SYMBOL whatever range we name (measured — four
    symbols produced six `/v8/finance/chart/<ticker>` requests), so widening a batch's window buys
    no extra call and narrowing it saves none. What a window does bound is MEMORY, which is the
    real constraint here: twelve symbols at full history measured 11.6 MB of JSON.

    So a security that has never been collected is asked for everything, and one that has is asked
    from its own newest stored bar. Splitting on that keeps a single new security from dragging the
    other twenty-four in its run through ten years each — which is what one shared window would do
    the moment a subject was added.
    """
    wanted = set(context.partition_keys)

    with postgres.connect() as conn:
        subjects = [
            s
            for s in prices.askable_subjects(conn, provider=PROVIDER.code)
            if s.security_id in wanted
        ]

    # WHAT EACH PARTITION ALREADY HOLDS, read through the manager that wrote it. Asking the raw
    # files rather than `market.price_bar` keeps stage 1 independent of stage 2 having succeeded —
    # and a watermark that reads too OLD is merely a wider window, which the merge then dedupes,
    # while one that reads too NEW would leave a hole nothing reports.
    watermarks: dict[str, date] = {}
    restarting: list[str] = []
    for subject in subjects:
        stored = raw_store.stored_rows_for(
            context.asset_key, subject.security_id, columns=("date", "asked_symbol")
        )
        # A SYMBOL CHANGE RESTARTS THE HISTORY. A partition holding a row asked with another symbol
        # is another listing's history, and extending it would append this listing to that one —
        # 69 partitions did exactly that before 2026-10-05 (`prices.asked_with_another_symbol`).
        # No watermark makes it a full load, and a full load with rows REPLACES the file. A refused
        # one replaces nothing, so the old history stands until the provider answers.
        if prices.asked_with_another_symbol(stored, subject.symbol):
            restarting.append(subject.security_id)
            continue
        dates = [d for d in (prices.row_date(row) for row in stored) if d is not None]
        if dates:
            watermarks[subject.security_id] = max(dates)
    for sid in restarting[:20]:
        context.log.info(f"{sid}: stored history was asked with another symbol; reloading it")

    loading = [s for s in subjects if s.security_id not in watermarks]
    extending = [s for s in subjects if s.security_id in watermarks]
    # EXCLUSIVE, AND THEREFORE "UP TO BUT NOT INCLUDING TODAY". Somewhere a market is open, and its
    # bar for today is a session in progress whose "close" is not a close. A lane must not write a
    # partial bar that then looks settled.
    end = date.today()

    # BUILT, NOT INLINED IN THE LOOP HEADER. `_earliest` takes a min over the watermarks, so
    # evaluating it for an empty cohort raises `min() iterable argument is empty` — which is what
    # the first run of a never-collected security does, i.e. the commonest case there is.
    cohorts: list[tuple[list[prices.Subject], date]] = []
    if loading:
        cohorts.append((loading, HISTORY_START))
    if extending:
        cohorts.append((extending, _earliest(watermarks, end)))

    rows: list[dict[str, Any]] = []
    stats: dict[str, int] = {}
    for cohort, start in cohorts:
        context.log.info("%s securities from %s..%s", len(cohort), start, end)
        got, cohort_stats = _collect(
            context,
            cohort,
            start=start,
            end=end,
            # SMALLER THAN THE CROSS-SECTION'S, because this is a MEMORY budget wearing a time
            # budget's clothes: twelve symbols at full history measured 11.6 MB of JSON in 8.1 s.
            batch_size=PROVIDER.history_batch_size,
            budget_seconds=config.budget_seconds,
            postgres=postgres,
        )
        rows += got
        for name, value in cohort_stats.items():
            stats[name] = stats.get(name, 0) + value

    context.add_output_metadata(
        {
            "requested": len(wanted),
            # A PARTITION NOBODY ASKED ABOUT IS STILL A PARTITION OF THIS RUN. `askable_subjects`
            # leaves out a security whose symbol was rejected alone within 30 days (a probe miss),
            # one with no symbol, and anything not an equity, and they fell out of every
            # counter: the 2026-09-30 night reported `requested 2500` beside `answered 2463` and no
            # other outcome, 37 securities accounted for nowhere. Counted, the outcome counters sum
            # to `requested` again, and that identity is how these runs are read.
            "not_askable": len(wanted) - len(subjects),
            "rows": len(rows),
            "loading_full_history": len(loading),
            # Of those, the ones whose stored history was another symbol's. Counted apart because a
            # burst here means symbols changed under many securities at once, which is worth a look.
            "restarting_history": len(restarting),
            "extending_from_watermark": len(extending),
            **stats,
        }
    )
    # A SUBJECT ASKED FOR EVERYTHING REPLACES ITS PARTITION; ONE ASKED FOR AN EXTENSION MERGES INTO
    # IT. Both happen in the same run, which is why this is a set of partition keys and not a flag.
    #
    # Measured 2026-09-20, on the first live run of this lane. Every partition on disk was written
    # before raw stopped adding `trade_date`, so none carried the `date` that `merge_on` keys on:
    # the watermark read nothing, every security took the full-history path, and the merge then
    # kept all 4,496 stored rows beside the 4,502 just fetched — the same history twice, in a file
    # that would have doubled for each of 12,016 partitions. Without this the merge cannot converge
    # on a partition whose stored shape predates its key; `rows_unkeyable` would stay non-zero for
    # ever and stop being a signal that anything is wrong.
    return _by_partition(
        context,
        rows,
        key=lambda r: str(r["security_id"]),
        complete={s.security_id for s in loading},
    )


#: HOW FAR BEHIND ITS OWN WATERMARK EACH EXTENSION RE-READS. The vendor is asked once per ticker
#: whatever range we name, so a wider window costs bytes, never a request: at ~one bar a day this
#: is about seven extra rows per security per extension.
#:
#: WHAT IT BUYS IS A RULE WHERE THERE WAS AN ACCIDENT. Yahoo has two kinds of gap at 00:00 UTC.
#: The just-closed US session arrives with a NaN close, which stage 2 refuses, and the next
#: extension's inclusive re-read of the newest stored day filled that. But a day can also be
#: `null` in Yahoo itself for days afterwards and then appear: measured 2026-09-24, the 09-22 bar
#: was still missing for KO, CZR, EMBC and PRAA while MSFT, SPY and JPM had it. An extension
#: starting at the cohort's oldest watermark re-read such a day only if a cohort-mate happened to
#: be behind it. The sweep reaches each security every ~5 nights, so with seven days a day still
#: missing at one extension is asked for again at the next one, whatever its cohort-mates hold.
REREAD = timedelta(days=7)


def _earliest(watermarks: dict[str, date], end: date) -> date:
    """The oldest watermark in the cohort, less the re-read window.

    THE STORED DAYS ARE RE-ASKED RATHER THAN SKIPPED, because a provider restates: a bar can be
    corrected, split-adjusted or filled in after the fact, and the merge supersedes it per key
    (raw, `merge_on`) and per `(security_id, trade_date)` (core, DO UPDATE). See `REREAD`. A
    watermark of `end` would also make a degenerate range — `start == end` returns everything from
    `start` to today on this provider, measured — so the window is held at least a day wide.
    """
    oldest = min(watermarks.values())
    return min(oldest - REREAD, end - timedelta(days=1))


# ── Yahoo's chart, asked directly: the lane since 2026-10-10 ─────────────────────────────────────


def _record_probes(
    postgres: Postgres, answered: list[prices.Subject], dead: list[prices.Subject]
) -> None:
    """What the run established about each symbol, through the one rule that may write a miss.

    `prices.symbol_probes` records a miss only for a subject asked ALONE with a control that
    answered in the same attempt. Every chart request asks one symbol, so the first proof holds by
    construction; `dead` is passed only once the caller holds the second.
    """
    by_symbol = {s.symbol: s for s in answered + dead}
    verdict = BatchVerdict(
        rows=[{"symbol": s.symbol} for s in answered],
        dead=[s.symbol for s in dead],
        isolated=True,
        control_answered=True,
    )
    probes = prices.symbol_probes(
        by_symbol, verdict, provider=PROVIDER.code, observed_at=datetime.now(UTC)
    )
    if probes:
        with postgres.connect() as conn, conn.cursor() as cur:
            upsert(
                cur,
                "market.identifier_probe",
                probes,
                conflict=["security_id", "scheme", "provider"],
                update=["asked_with", "value", "outcome", "observed_at"],
            )


def _read_chart(document: Document) -> tuple[yahoo_chart.Series | None, str]:
    """What one response was, as the outcome it is counted under.

    `absent` IS YAHOO SAYING SO — `chart.error`, a 404 reading "No data found, symbol may be
    delisted" for BDMS-F.BK and ICT.PS (measured 2026-10-10) — and `empty` is a symbol it knows with
    nothing in the window. Only the first can become a miss, and only with a healthy control.
    """
    try:
        series = yahoo_chart.parse(document.body)
    except yahoo_chart.YahooRefused:
        return None, "unreadable"
    if series.error is not None:
        return series, "absent"
    if series.granularity not in (None, price_chart.INTERVAL):
        return series, "not_daily"
    return series, ("answered" if series.points else "empty")


class _Pacer:
    """The spacing between the STARTS of two requests, read from the module so a test can set it."""

    def __init__(self) -> None:
        self._last = 0.0

    def wait(self) -> None:
        if self._last:
            gap = prices_partitions.CHART_SECONDS_BETWEEN_REQUESTS - (time.monotonic() - self._last)
            if gap > 0:
                time.sleep(gap)
        self._last = time.monotonic()


def _ask_chart(
    pacer: _Pacer, symbol: str, start: date | None, end: date
) -> tuple[Document, int, int]:
    """One chart request: everything when `start` is None, else the days from `start` to `end`.

    `period1=0`, NEVER `range=max`: measured 2026-10-10, `range=max` came back at `3mo` for AAPL
    and `1wk` for AMRM.TA whatever the interval said, while `period1=0` returned AAPL's 11,549 daily
    bars.
    """
    pacer.wait()
    period1 = 0 if start is None else yahoo_chart.day_start(start)
    period2 = yahoo_chart.day_start(end)
    document = yahoo_chart.fetch(
        symbol,
        interval=price_chart.INTERVAL,
        period1=period1,
        period2=period2,
        events="div,split",
        adjusted=True,
    )
    return document, period1, period2


def _collect_charts(
    context: AssetExecutionContext,
    plans: list[tuple[prices.Subject, price_chart.Plan]],
    *,
    end: date,
    budget_seconds: int,
    postgres: Postgres | None,
) -> tuple[list[dict[str, Any]], set[str], dict[str, int]]:
    """Ask each security once, in weight order, and return the documents and what the asking showed.

    THE OUTCOMES PARTITION THE SUBJECTS: answered, empty, absent, not_daily, unreadable, transport
    and unasked sum to the subjects asked about, and the asset adds `not_askable` to reach
    `requested`. A gap in that sum is a branch that forgot to count, which is how half a night once
    read as complete.
    """
    deadline = time.monotonic() + budget_seconds
    pacer = _Pacer()
    rows: list[dict[str, Any]] = []
    complete: set[str] = set()
    answered: list[prices.Subject] = []
    absent: list[prices.Subject] = []
    stats = {
        "calls": 0,
        "answered": 0,
        "empty": 0,
        "absent": 0,
        "not_daily": 0,
        "unreadable": 0,
        "transport": 0,
        "unasked": 0,
        "throttled": 0,
        "dead": 0,
        "reloaded_after_split": 0,
        "control_calls": 0,
        "bytes": 0,
    }
    last_error: str | None = None
    consecutive_transport = 0

    for index, (subject, chosen) in enumerate(plans):
        if time.monotonic() >= deadline:
            # NOT AN ERROR AND NOT AN ABSENCE: a subject we never asked about must not look like one
            # that answered nothing.
            stats["unasked"] += len(plans) - index
            context.log.warning("budget of %ss spent; %s unasked", budget_seconds, stats["unasked"])
            break
        stats["calls"] += 1
        try:
            document, period1, period2 = _ask_chart(pacer, subject.symbol, chosen.start, end)
        except yahoo_chart.YahooRefused as exc:
            last_error = str(exc)
            if throttled(last_error):
                stats["throttled"] += 1
                stats["unasked"] += len(plans) - index
                context.log.warning(
                    "Yahoo is refusing us (%s); stopping with %s unasked and marking nothing",
                    last_error,
                    stats["unasked"],
                )
                break
            stats["transport"] += 1
            consecutive_transport += 1
            if consecutive_transport >= 3 and stats["answered"] == 0:
                stats["unasked"] += len(plans) - index - 1
                context.log.error("three transport failures and nothing answered: %s", last_error)
                break
            continue
        consecutive_transport = 0
        series, outcome = _read_chart(document)

        # A SPLIT SINCE THE STORED HISTORY WAS LOADED MAKES ALL OF IT STALE, so the whole history is
        # asked for at once. If that is refused the extension is DROPPED rather than stored: its
        # window still covers the split, so the next visit sees the same event and tries again.
        if (
            outcome == "answered"
            and series is not None
            and chosen.start is not None
            and price_chart.stale_after_split(series, chosen.loaded_on)
        ):
            stats["calls"] += 1
            try:
                document, period1, period2 = _ask_chart(pacer, subject.symbol, None, end)
            except yahoo_chart.YahooRefused as exc:
                last_error = str(exc)
                if throttled(last_error):
                    stats["throttled"] += 1
                    stats["unasked"] += len(plans) - index
                    break
                stats["transport"] += 1
                continue
            series, outcome = _read_chart(document)
            if outcome != "answered":
                # The reload said less than the extension did, so nothing replaces anything; the
                # next visit sees the split again.
                stats["transport"] += 1
                continue
            stats["reloaded_after_split"] += 1
            chosen = price_chart.Plan(start=None, reason="reload_after_split")

        stats["bytes"] += len(document.body)
        stats[outcome] += 1
        rows += price_chart.raw_rows(
            subject, document, run_id=context.run.run_id, period1=period1, period2=period2
        )
        if outcome == "answered":
            answered.append(subject)
            # ONLY A FULL HISTORY THAT ANSWERED REPLACES THE FILE. An absence, an empty window or a
            # body we cannot read is appended: writing it over ten years of documents would delete
            # them to record one bad answer.
            if chosen.start is None:
                complete.add(subject.security_id)
        elif outcome == "absent":
            absent.append(subject)

    # A MISS NEEDS A HEALTHY PROVIDER IN THE SAME RUN. A security that answered is that proof; when
    # none did, the control is asked once. Never a tally stacked on top of the per-symbol evidence:
    # the control can always succeed when Yahoo is up, so a run of dead symbols still gets marked.
    healthy = bool(answered)
    if absent and not healthy and time.monotonic() < deadline:
        stats["control_calls"] += 1
        try:
            control, _, _ = _ask_chart(
                pacer, PROVIDER.control_subject, end - timedelta(days=7), end
            )
            healthy = _read_chart(control)[1] == "answered"
        except yahoo_chart.YahooRefused as exc:
            last_error = str(exc)
    dead = absent if healthy else []
    stats["dead"] = len(dead)
    if postgres is not None:
        _record_probes(postgres, answered, dead)
    if last_error:
        context.log.warning("last error: %s", last_error)
    return rows, complete, stats


@dg.asset(
    partitions_def=security_partitions,
    # The same memory budget as the lane it replaces: 25 securities' full histories are ~25 MB of
    # JSON at the largest, far under what the openbb rows cost, and stage 2 holds them parsed.
    backfill_policy=dg.BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN),
    pool="yfinance",
    io_manager_key="parquet_io",
    group_name="prices",
    kinds={"yahoo", "parquet"},
    # ONE ROW PER RESPONSE, AND A RESPONSE IS NEVER SUPERSEDED BY ANOTHER: each is what Yahoo said
    # at `fetched_at`. A full history that answers replaces the file (`partitioned.Complete`); an
    # extension is appended to it.
    metadata={"merge_on": ["security_id", "fetched_at"]},
    description="Each security's Yahoo chart documents: its whole history, then a week per visit.",
)
def raw_price_chart(
    context: AssetExecutionContext, config: PriceRun, postgres: Postgres, raw_store: RawStore
) -> Any:
    """Each security, asked directly of Yahoo's chart endpoint, the body stored whole.

    WHY NOT openbb ANY MORE. The quote currency is in every chart response (`meta.currency`) and
    openbb's rows do not carry it, so the lane labelled its bars with a guess that was wrong for
    seven of thirteen sampled listings. The request count is unchanged: openbb's yfinance adapter
    asks this same endpoint once per ticker.

    A SECURITY'S FIRST VISIT LOADS ITS WHOLE HISTORY, and that is the whole changeover (spec
    decision 2): the openbb raw files stay where they are as the rollback, and after one rotation
    (~5 nights) every security is on chart documents. Later visits extend from the newest stored
    point, less a week (`price_chart.plan`).
    """
    wanted = set(context.partition_keys)
    with postgres.connect() as conn:
        subjects = [
            s
            for s in prices.askable_subjects(conn, provider=PROVIDER.code)
            if s.security_id in wanted
        ]
    # EXCLUSIVE: up to but not including today, where a market is still trading.
    end = date.today()
    plans: list[tuple[prices.Subject, price_chart.Plan]] = []
    reasons: dict[str, int] = {}
    for subject in subjects:
        stored = raw_store.stored_rows_for(
            context.asset_key,
            subject.security_id,
            columns=("fetched_at", "asked_symbol", "period1", "url", "body"),
        )
        chosen = price_chart.plan(stored, subject.symbol, end)
        reasons[chosen.reason] = reasons.get(chosen.reason, 0) + 1
        plans.append((subject, chosen))

    rows, complete, stats = _collect_charts(
        context, plans, end=end, budget_seconds=config.budget_seconds, postgres=postgres
    )
    context.add_output_metadata(
        {
            "requested": len(wanted),
            # Left out by `askable_subjects`: no symbol, not an equity, or a symbol Yahoo rejected
            # alone within 30 days. Counted so the outcomes sum to `requested`.
            "not_askable": len(wanted) - len(subjects),
            "documents": len(rows),
            **{f"plan_{reason}": count for reason, count in sorted(reasons.items())},
            **stats,
        }
    )
    return _by_partition(context, rows, key=lambda r: str(r["security_id"]), complete=complete)
