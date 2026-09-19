"""Stage 1 of the prices family: what the provider sent, kept whole."""

import time
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest import ledger
from muffin_ingest.facets import prices
from muffin_ingest.providers import openbb
from muffin_ingest.providers.isolation import BatchVerdict, fetch_with_isolation
from muffin_ingest.providers.outcome import Outcome

from muffin_ingest_dagster.defs.prices.partitions import (
    HISTORY_PARTITIONS_PER_RUN,
    HISTORY_START,
    PROVIDER,
    security_partitions,
    trading_day,
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


def _partition_day(row: dict[str, Any]) -> str:
    """Which daily partition a raw row belongs to, read off the provider's own date field."""
    parsed = prices.row_date(row)
    return parsed.isoformat() if parsed is not None else ""


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
    """One provider call, wrapped in the ledger attempt that can justify a mark.

    THE ATTEMPT IS OPENED BEFORE THE CALL AND CLOSED IN A `finally`, because a worker killed
    mid-call writes nothing at all — it goes silent rather than red — so the row saying "something
    started here" has to exist before the thing that might kill us.

    WITHOUT A CONNECTION THIS IS EXACTLY THE OLD BEHAVIOUR, deliberately: the offline tests drive
    the real asset with no database, and a lane that could only run against Postgres would be a lane
    nothing could replay.
    """
    fetch = _fetcher(start, end, warnings)
    timeout = min(60.0, max(5.0, deadline - time.monotonic()))
    if postgres is None:
        return fetch_with_isolation(
            fetch,
            list(by_symbol),
            timeout_s=timeout,
            deadline=deadline,
            control=PROVIDER.control_subject,
        )

    with postgres.connect() as conn:
        started = time.monotonic()
        with ledger.attempt(
            conn, context.run_id, prices.FACET, PROVIDER.code, list(by_symbol)
        ) as att:
            verdict = fetch_with_isolation(
                fetch,
                list(by_symbol),
                timeout_s=timeout,
                deadline=deadline,
                control=PROVIDER.control_subject,
            )
            # CLOSED WITH WHAT THE ISOLATION PASS ESTABLISHED, not with what we would like it to
            # have established. `mark_absent` reads exactly these two fields and refuses without
            # them, so passing `isolated=True` on a batch that was never isolated is the one lie
            # that would let an outage negative-cache the universe.
            att.close(
                _outcome(verdict),
                rows_written=len(verdict.rows),
                error=verdict.error,
                duration_ms=int((time.monotonic() - started) * 1000),
                isolated=verdict.isolated,
                control_answered=verdict.control_answered,
            )
            tasks = [
                ledger.Task(
                    facet=prices.FACET,
                    subject=subject.security_id,
                    security_id=subject.security_id,
                    asked_with=symbol,
                    watermark=None,
                    version=0,
                )
                for symbol, subject in by_symbol.items()
            ]
            # `verdict.dead` holds SYMBOLS; the ledger's subjects are security ids. Translating here
            # rather than widening the verdict keeps the isolation layer ignorant of our keying.
            dead_ids = {by_symbol[s].security_id for s in verdict.dead if s in by_symbol} | {
                subject.security_id
                for symbol, subject in by_symbol.items()
                if symbol.upper() in {d.upper() for d in verdict.dead}
            }
            ledger.record(
                conn,
                prices.FACET,
                tasks,
                BatchVerdict(
                    rows=verdict.rows,
                    dead=sorted(dead_ids),
                    error=verdict.error,
                    isolated=verdict.isolated,
                    control_answered=verdict.control_answered,
                    throttled_out=verdict.throttled_out,
                ),
                att,
                rows_per_subject=_rows_per_subject(verdict, by_symbol),
            )
        conn.commit()
    return verdict


def _outcome(verdict: BatchVerdict) -> Outcome:
    """What the ATTEMPT established, which is a different question from what each subject did."""
    if verdict.throttled_out:
        return Outcome.THROTTLED
    if verdict.error is not None:
        return Outcome.TRANSPORT
    return Outcome.ANSWERED if verdict.rows else Outcome.EMPTY


def _rows_per_subject(
    verdict: BatchVerdict, by_symbol: dict[str, prices.Subject]
) -> dict[str, int]:
    """How many rows each SUBJECT got, so `record` can tell answered from empty per security."""
    upper = {s.upper(): subject for s, subject in by_symbol.items()}
    counts: dict[str, int] = {}
    for row in verdict.rows:
        symbol = str(row.get("symbol") or "").upper()
        subject = upper.get(symbol) or (
            next(iter(by_symbol.values())) if len(by_symbol) == 1 else None
        )
        if subject is not None:
            counts[subject.security_id] = counts.get(subject.security_id, 0) + 1
    return counts


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
    day — the vendor is asked once per SYMBOL whatever we batch. With it, each batch opens an
    `ingest.attempt` before the call and offers its verdict to `ingest.mark_absent`, which refuses
    unless the attempt says the subject was asked ALONE and a control answered. The rule lives in
    SQL so an over-eager caller is stopped by the database rather than by review.
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
            # reported `unasked=0`, so 5,437 of 12,017 securities were never asked, the partition
            # materialised as complete and `every_askable_security_was_asked` passed. A refused
            # ask is not an answer, so the refused batch counts too.
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
    partitions_def=trading_day,
    # ONE RUN FOR A RANGE. Filling a week-long gap costs one run rather than seven, because the
    # asset reads the whole window and asks for it in a single pass.
    backfill_policy=dg.BackfillPolicy.single_run(),
    pool="yfinance",
    io_manager_key="parquet_io",
    group_name="prices",
    kinds={"yfinance", "parquet"},
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="What yfinance said about the whole universe for this window, unchanged.",
)
def raw_price_bars(context: AssetExecutionContext, config: PriceRun, postgres: Postgres) -> Any:
    # THE WINDOW'S OWN EXCLUSIVE END, NOT A DAY SUBTRACTED FROM IT — and the subtraction was
    # costing a multiple of the data. Measured against the real hub:
    #
    #     start=2026-09-01 end=2026-09-01  ->  7 rows, 2026-09-01..2026-09-10
    #     start=2026-09-01 end=2026-09-02  ->  2 rows, 2026-09-01..2026-09-02
    #     start=2026-09-01 end=2026-09-03  ->  3 rows, exactly
    #
    # A DEGENERATE RANGE IS IGNORED: `start == end` returns everything from `start` to TODAY, so
    # asking for one old day dragged back every session since it — 653 bars discarded for 96
    # securities on the first parity run, which is what made it visible. A range of at least a day
    # is honoured exactly, so handing the provider the half-open window Dagster already gives us
    # asks for two days instead of two weeks. `price_bar` still publishes only the partition's
    # own day, so correctness never depended on this.
    window: tuple[datetime, datetime] = context.partition_time_window
    start, end = window[0].date(), window[1].date()

    with postgres.connect() as conn:
        # ENQUEUE WHAT THE FACET OWES BEFORE ASKING, so a security promoted since the last run has a
        # ledger row to record its health against. `mark_absent` updates `ingest.task`; with no row
        # there is nothing to update and the mark is silently lost — every count would still look
        # right, because the marks would be written and never read.
        #
        # It is a SET operation, not an append: measured, the first call enqueues 12,016 equities
        # and the second enqueues 0.
        enqueued = ledger.sync_population(conn, prices.FACET)
        conn.commit()
        subjects = prices.askable_subjects(conn, provider=PROVIDER.code, limit=config.limit)

    context.log.info(
        "asking %s subjects for %s..%s (%s newly enqueued)", len(subjects), start, end, enqueued
    )
    rows, stats = _collect(
        context,
        subjects,
        start=start,
        end=end,
        batch_size=PROVIDER.batch_size,
        budget_seconds=config.budget_seconds,
        postgres=postgres,
    )
    # `rows: 0` IS A LEGITIMATE VALUE and is reported rather than filtered — a chart that cannot
    # draw a zero cannot show a collection that stopped, which is the only thing it is for.
    context.add_output_metadata(
        {"subjects": len(subjects), "rows": len(rows), "enqueued": enqueued, **stats}
    )
    # KEYED BY THE PROVIDER'S OWN DATE, PARSED FOR PLACEMENT ONLY. `raw_rows` writes no
    # `trade_date` — deriving one into the row would be a stage-1 interpretation, and a key is
    # needed to place a file, not to store. A row whose date will not parse keys to "", which
    # `by_partition` files in the run's last partition rather than dropping.
    return _by_partition(context, rows, key=_partition_day)


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
    for subject in subjects:
        stored = raw_store.stored_rows_for(context.asset_key, subject.security_id)
        dates = [d for d in (prices.row_date(row) for row in stored) if d is not None]
        if dates:
            watermarks[subject.security_id] = max(dates)

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
            "rows": len(rows),
            "loading_full_history": len(loading),
            "extending_from_watermark": len(extending),
            **stats,
        }
    )
    return _by_partition(context, rows, key=lambda r: str(r["security_id"]))


def _earliest(watermarks: dict[str, date], end: date) -> date:
    """The oldest watermark in the cohort, re-asking its newest stored day.

    THE STORED DAY IS RE-ASKED RATHER THAN SKIPPED, because a provider restates: a bar can be
    corrected, split-adjusted or filled in after the fact, and the merge supersedes it per key. A
    watermark of `end` would also make a degenerate range — `start == end` returns everything from
    `start` to today on this provider, measured — so the window is held at least a day wide.
    """
    oldest = min(watermarks.values())
    return min(oldest, end - timedelta(days=1))
