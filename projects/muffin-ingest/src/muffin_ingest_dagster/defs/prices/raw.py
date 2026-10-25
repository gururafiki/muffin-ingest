"""Stage 1 of the prices family: what the provider sent, kept whole."""

import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import price_chart, prices
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document
from muffin_ingest.providers.isolation import BatchVerdict
from muffin_ingest.providers.vocab import throttled
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices.partitions import (
    HISTORY_PARTITIONS_PER_RUN,
    PROVIDER,
    security_partitions,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.pacing import Pacer
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

    `quote_only` IS A LISTING YAHOO ANSWERS WITH ITS LIVE QUOTE AND NO BAR. Picton (PCTN.L), JTC,
    Orla, Kelun-Biotech and a TMBThanachart NVDR were answered this way on every visit, a full
    history included (measured 2026-10-25: three documents of one point each, stamped at
    `regularMarketTime`). Stage 2 never publishes a live quote, so they were counted as answered
    while nothing changed, and kept their openbb bars' guessed labels. Decided by the user
    2026-10-25: it is an absence, so it becomes a miss on the same terms, leaves the rotation for
    `DEAD_FOR_DAYS`, and the symbology repair looks for a live line.
    """
    try:
        series = yahoo_chart.parse(document.body)
    except yahoo_chart.YahooRefused:
        return None, "unreadable"
    if series.error is not None:
        return series, "absent"
    if series.granularity not in (None, price_chart.INTERVAL):
        return series, "not_daily"
    if series.points and all(point.is_live for point in series.points):
        return series, "quote_only"
    return series, ("answered" if series.points else "empty")


def _pacer() -> Pacer:
    """The chart lane's pace, read from the module at every wait so a test can set it."""
    return Pacer(lambda: prices_partitions.CHART_SECONDS_BETWEEN_REQUESTS)


def _ask_chart(
    pacer: Pacer, symbol: str, start: date | None, end: date
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
        # THE WINDOW STARTS AT THIS SECURITY'S OWN WATERMARK, so this URL is never asked again and
        # http-cache is told not to keep the body:
        # docs/deferred/2026-10-10-http-cache-keeps-every-yahoo-chart-for-a-year.md (umbrella).
        store=False,
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

    THE OUTCOMES PARTITION THE SUBJECTS: answered, empty, absent, quote_only, not_daily,
    unreadable, transport and unasked sum to the subjects asked about, and the asset adds
    `not_askable` to reach `requested`. A gap in that sum is a branch that forgot to count, which
    is how half a night once read as complete.
    """
    deadline = time.monotonic() + budget_seconds
    pacer = _pacer()
    rows: list[dict[str, Any]] = []
    complete: set[str] = set()
    answered: list[prices.Subject] = []
    absent: list[prices.Subject] = []
    stats = {
        "calls": 0,
        "answered": 0,
        "empty": 0,
        "absent": 0,
        "quote_only": 0,
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
        elif outcome in ("absent", "quote_only"):
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
    decision 2): after one rotation (~5 nights) every security was on chart documents. The openbb
    lane, `raw_price_history`, stayed as the rollback until 2026-10-24 and was then deleted. Later
    visits extend from the newest stored point, less a week (`price_chart.plan`).
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
    return partitioned.by_partition(
        context, rows, key=lambda r: str(r["security_id"]), complete=complete
    )
