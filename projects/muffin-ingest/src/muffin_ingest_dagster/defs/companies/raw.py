"""Stage 1 of the companies family: what Yahoo said about each company, kept whole."""

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import company, prices, statements
from muffin_ingest.providers import yahoo_chart, yahoo_session, yahoo_summary, yahoo_timeseries
from muffin_ingest.providers.documents import Document
from muffin_ingest.providers.vocab import throttled

from muffin_ingest_dagster.defs.companies.partitions import COMPANY_PARTITIONS_PER_RUN
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices.partitions import PROVIDER, security_partitions
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.pacing import Pacer
from muffin_ingest_dagster.lib.resources import Postgres

#: The two raw assets, spelled once: the asset reads what each already holds before it asks.
SUMMARY = dg.AssetKey("raw_yahoo_summary")
STATEMENTS = dg.AssetKey("raw_yahoo_statements")

#: A company's facts change weekly at most and its statements quarterly, so a day without a
#: materialisation is a stopped lane, not a quiet one: the rotation materialises a slice every day.
FRESH = dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36))


class CompanyRun(dg.Config):
    """What bounds a run, rather than what it fetches."""

    #: Cap the companies asked about, heaviest first. For a first look, never for steady state.
    limit: int | None = None
    #: Ask for the summary and the statements whatever raw already holds: the stock page's refresh
    #: button (spec decision 8) and a re-run after a parser fix.
    force: bool = False
    #: Wall-clock budget. A company costs at most three requests at the price lane's 1.4 s, so a
    #: full run of 100 is ~7 minutes; twice that leaves room for a slow provider.
    budget_seconds: int = 900


#: Every outcome a company's summary can have in a run. They partition the companies asked about.
SUMMARY_OUTCOMES = (
    "answered",
    "empty",
    "absent",
    "unreadable",
    "transport",
    "not_due",
    "unasked",
)

#: Every outcome a company's statements can have in a run. They partition the companies too.
STATEMENT_OUTCOMES = (
    "fetched",
    "partial",
    "failed",
    "not_due",
    #: The summary did not answer, so nothing says whether this company has statements.
    "skipped",
    "unasked",
)


@dataclass
class _Visit:
    """What one run collected, and how each company came out."""

    summaries: list[dict[str, Any]] = field(default_factory=list)
    statements: list[dict[str, Any]] = field(default_factory=list)
    #: Companies whose fresh summary answered: their file is replaced (`partitioned.Complete`).
    answered: set[str] = field(default_factory=set)
    stats: dict[str, int] = field(default_factory=dict)
    last_error: str | None = None

    def count(self, name: str, n: int = 1) -> None:
        self.stats[name] = self.stats.get(name, 0) + n


def read_summary(body: bytes) -> tuple[yahoo_summary.Summary | None, str]:
    """What one summary response was, as the outcome it is counted under.

    `absent` is Yahoo naming the symbol as one it does not carry (`Quote not found for symbol`),
    `empty` an answer with no modules. Neither becomes a miss: a `(symbol, yfinance)` miss would
    hide the security from the price sweep on the strength of one endpoint.
    """
    try:
        summary = yahoo_summary.parse(body)
    except yahoo_chart.YahooRefused:
        return None, "unreadable"
    if summary.error is not None:
        return summary, "absent"
    if not summary.modules:
        return summary, "empty"
    return summary, "answered"


def _stored_state(stored: Sequence[Mapping[str, Any]]) -> tuple[str | None, date | None]:
    """The newest stored summary's outcome, and the period the newest ANSWERED one states."""
    outcome: str | None = None
    stated: date | None = None
    for row in sorted(stored, key=lambda r: str(r.get("fetched_at") or ""), reverse=True):
        body = row.get("body")
        if body is None:
            continue
        summary, read = read_summary(bytes(body))
        if outcome is None:
            outcome = read
        if read == "answered" and summary is not None:
            stated = statements.stated_period(summary)
            break
    return outcome, stated


class _Stop(Exception):
    """The run cannot go on: Yahoo refused us, or no session could be had."""


class _StoppedAfterPartial(_Stop):
    """A stop after one frequency was filed: the company is already counted as `partial`."""


class _Asker:
    """One run's requests: paced, through one lazily opened session, stopping on a refusal."""

    def __init__(self, visit: _Visit) -> None:
        self._visit = visit
        self._pacer = Pacer(lambda: prices_partitions.CHART_SECONDS_BETWEEN_REQUESTS)
        self._session: yahoo_session.Session | None = None
        self._reopened = False
        self.consecutive_transport = 0
        self.anything_answered = False

    def _open(self) -> yahoo_session.Session:
        self._pacer.wait()
        try:
            session = yahoo_session.open_session()
        except yahoo_session.SessionRefused as exc:
            self._visit.last_error = str(exc)
            if throttled(str(exc)):
                self._visit.count("throttled")
            raise _Stop(f"no Yahoo session: {exc}") from exc
        self._visit.count("sessions")
        return session

    def summary(self, symbol: str) -> Document:
        """The summary document, or `yahoo_chart.YahooRefused` for a request that did not answer.

        A REFUSED SESSION IS RENEWED ONCE PER RUN. A crumb lives for hours, so a second refusal
        straight after a new one is Yahoo refusing us, not the session expiring.
        """
        if self._session is None:
            self._session = self._open()
        self._pacer.wait()
        self._visit.count("calls")
        try:
            return yahoo_summary.fetch(symbol, self._session)
        except yahoo_summary.CrumbRejected as exc:
            self._visit.count("crumb_rejected")
            if self._reopened:
                self._visit.last_error = str(exc)
                raise _Stop("Yahoo refused a fresh session too") from exc
            self._reopened = True
            self._session = self._open()
            self._pacer.wait()
            self._visit.count("calls")
            try:
                return yahoo_summary.fetch(symbol, self._session)
            except yahoo_summary.CrumbRejected as again:
                self._visit.last_error = str(again)
                raise _Stop("Yahoo refused a fresh session too") from again

    def timeseries(self, symbol: str, frequency: str, today: date) -> Document:
        self._pacer.wait()
        self._visit.count("calls")
        return yahoo_timeseries.fetch(symbol, frequency, today=today)

    def refused(self, exc: yahoo_chart.YahooRefused) -> None:
        """A request that did not answer: a throttle stops the run, three transport faults in a
        row before anything has answered stop it too, anything else is counted and passed."""
        self._visit.last_error = str(exc)
        if throttled(str(exc)):
            self._visit.count("throttled")
            raise _Stop(f"Yahoo is refusing us: {exc}") from exc
        self.consecutive_transport += 1
        if self.consecutive_transport >= 3 and not self.anything_answered:
            raise _Stop(f"three transport failures and nothing answered: {exc}") from exc

    def answered(self) -> None:
        self.consecutive_transport = 0
        self.anything_answered = True


def collect(
    context: AssetExecutionContext,
    subjects: Sequence[prices.Subject],
    raw_store: RawStore,
    *,
    today: date,
    force: bool,
    budget_seconds: int,
) -> _Visit:
    """Ask about each company in weight order: its summary if due, then its statements if due.

    THE OUTCOMES PARTITION THE COMPANIES, twice: every company has exactly one summary outcome and
    one statements outcome per run, so each set sums to the companies asked about. A gap in either
    sum is a branch that forgot to count, which is how half a price night once read as complete.

    A STOP IS NEVER AN ABSENCE. Whatever was not asked when Yahoo refused us, or the budget ran
    out, is `unasked`, never `absent` or `not_due`.
    """
    visit = _Visit(stats={**{f"summary_{o}": 0 for o in SUMMARY_OUTCOMES}})
    visit.stats.update({f"statements_{o}": 0 for o in STATEMENT_OUTCOMES})
    asker = _Asker(visit)
    deadline = time.monotonic() + budget_seconds

    for index, subject in enumerate(subjects):
        remaining = len(subjects) - index
        if time.monotonic() >= deadline:
            visit.count("summary_unasked", remaining)
            visit.count("statements_unasked", remaining)
            context.log.warning(
                "budget of %ss spent; %s companies unasked", budget_seconds, remaining
            )
            break
        sid = subject.security_id
        stored_summary = raw_store.stored_rows_for(
            SUMMARY, sid, columns=("fetched_at", "url", "body")
        )
        stored_statements = raw_store.stored_rows_for(
            STATEMENTS, sid, columns=("fetched_at", "frequency", "url", "body")
        )

        # THE SUMMARY. A fresh answer decides the statements; when the summary is not due, the
        # newest stored document does.
        outcome: str | None
        stated: date | None
        try:
            if company.summary_due(stored_summary, today, force=force):
                outcome, stated = _ask_summary(asker, visit, subject, context.run.run_id)
            else:
                visit.count("summary_not_due")
                outcome, stated = _stored_state(stored_summary)
        except _Stop as stop:
            visit.count("summary_unasked", remaining)
            visit.count("statements_unasked", remaining)
            context.log.warning("%s; stopping with %s companies unasked", stop, remaining)
            break

        # THE STATEMENTS, only for a company whose summary answered: a symbol Yahoo says it does not
        # carry has no statements either, and two requests would only prove it again.
        if outcome not in ("answered", "empty"):
            visit.count("statements_skipped")
            continue
        reason = statements.due(stored_statements, stated=stated, today=today, force=force)
        if reason is None:
            visit.count("statements_not_due")
            continue
        visit.count(f"statements_reason_{reason}")
        try:
            stored_now = _ask_statements(asker, visit, subject, context.run.run_id, today)
        except _Stop as stop:
            # THIS COMPANY'S SUMMARY WAS ASKED; ITS STATEMENTS WERE NOT, unless one frequency was
            # filed before the stop, in which case it is already counted `partial`.
            counted = isinstance(stop, _StoppedAfterPartial)
            visit.count("statements_unasked", remaining - (1 if counted else 0))
            visit.count("summary_unasked", remaining - 1)
            context.log.warning("%s; stopping with %s companies unasked", stop, remaining - 1)
            break
        visit.count(
            "statements_fetched"
            if stored_now == len(yahoo_timeseries.FREQUENCIES)
            else ("statements_partial" if stored_now else "statements_failed")
        )

    if visit.last_error:
        context.log.warning("last error: %s", visit.last_error)
    return visit


def _ask_summary(
    asker: _Asker, visit: _Visit, subject: prices.Subject, run_id: str
) -> tuple[str, date | None]:
    """Ask for one company's summary and file what came back. Raises `_Stop` to end the run."""
    try:
        document = asker.summary(subject.symbol)
    except yahoo_chart.YahooRefused as exc:
        asker.refused(exc)
        visit.count("summary_transport")
        return "transport", None
    asker.answered()
    summary, outcome = read_summary(document.body)
    visit.count(f"summary_{outcome}")
    visit.count("bytes", len(document.body))
    visit.summaries += company.raw_rows(subject, document, run_id=run_id)
    if outcome == "answered" and summary is not None:
        # ONLY AN ANSWER REPLACES THE STORED SUMMARY. An absence or a body we cannot read is filed
        # beside it, so one bad answer cannot delete the last good one.
        visit.answered.add(subject.security_id)
        return outcome, statements.stated_period(summary)
    return outcome, None


def _ask_statements(
    asker: _Asker, visit: _Visit, subject: prices.Subject, run_id: str, today: date
) -> int:
    """Ask for both frequencies and file each answer. Returns how many were stored."""
    stored = 0
    for frequency in yahoo_timeseries.FREQUENCIES:
        try:
            document = asker.timeseries(subject.symbol, frequency, today)
        except yahoo_chart.YahooRefused as exc:
            try:
                asker.refused(exc)
            except _Stop:
                if stored:
                    # The annual answer is filed; the run still stops, and the company counts as
                    # partial rather than unasked, because half of it was asked and kept.
                    visit.count("statements_partial")
                    raise _StoppedAfterPartial from None
                raise
            continue
        asker.answered()
        visit.count("bytes", len(document.body))
        visit.statements += statements.raw_rows(
            subject, document, run_id=run_id, frequency=frequency
        )
        stored += 1
    return stored


@dg.multi_asset(
    name="raw_yahoo_company",
    outs={
        "raw_yahoo_summary": dg.AssetOut(
            io_manager_key="parquet_io",
            group_name="companies",
            kinds={"yahoo", "parquet"},
            # THE LATEST ANSWER, NOT A HISTORY (spec decision 4): an answered summary replaces the
            # partition's file (`partitioned.Complete`), anything else is filed beside it.
            metadata={"merge_on": ["security_id", "fetched_at"]},
            freshness_policy=FRESH,
            description="Each company's newest Yahoo quoteSummary document, kept whole.",
        ),
        "raw_yahoo_statements": dg.AssetOut(
            io_manager_key="parquet_io",
            group_name="companies",
            kinds={"yahoo", "parquet"},
            # EVERY DOCUMENT IS KEPT (spec decision 4). Yahoo's window rolls per line, so an older
            # document is the only place a period that has left the window still exists.
            metadata={"merge_on": ["security_id", "frequency", "fetched_at"]},
            freshness_policy=FRESH,
            description="Every Yahoo fundamentals-timeseries document asked for each company.",
        ),
    },
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(COMPANY_PARTITIONS_PER_RUN),
    pool="yfinance",
)
def raw_yahoo_company(
    context: AssetExecutionContext, config: CompanyRun, postgres: Postgres, raw_store: RawStore
) -> tuple[Any, Any]:
    """Each company's quoteSummary, weekly, and its statements when Yahoo names a new period.

    ONE STEP FOR BOTH DOCUMENTS, because whether the statements are due is read off the summary
    fetched a moment earlier (`statements.due`): `defaultKeyStatistics.mostRecentQuarter` names
    the newest period the company has reported, so the visit after an earnings report asks for the
    statements and the other twelve weeks of a quarter do not.

    WHAT A VISIT COSTS: one request for the summary if a week has passed, two more for the
    statements when due, plus two for a session per run. Never more than the price lane's pace.
    """
    wanted = set(context.partition_keys)
    with postgres.connect() as conn:
        subjects = [
            s
            for s in prices.askable_subjects(conn, provider=PROVIDER.code)
            if s.security_id in wanted
        ]
    askable = len(subjects)
    if config.limit is not None:
        subjects = subjects[: config.limit]

    visit = collect(
        context,
        subjects,
        raw_store,
        today=date.today(),
        force=config.force,
        budget_seconds=config.budget_seconds,
    )
    context.add_output_metadata(
        {
            "requested": len(wanted),
            # Left out by `askable_subjects`: no symbol, not an equity, or a symbol Yahoo rejected
            # alone within 30 days. Counted so each set of outcomes sums to `requested`.
            "not_askable": len(wanted) - askable,
            "limited_out": askable - len(subjects),
            "summary_documents": len(visit.summaries),
            **{k: v for k, v in sorted(visit.stats.items()) if k.startswith("summary_")},
            "sessions": visit.stats.get("sessions", 0),
            "crumb_rejected": visit.stats.get("crumb_rejected", 0),
            "calls": visit.stats.get("calls", 0),
            "bytes": visit.stats.get("bytes", 0),
            "throttled": visit.stats.get("throttled", 0),
        },
        output_name="raw_yahoo_summary",
    )
    context.add_output_metadata(
        {
            "requested": len(wanted),
            "not_askable": len(wanted) - askable,
            "limited_out": askable - len(subjects),
            "statement_documents": len(visit.statements),
            **{k: v for k, v in sorted(visit.stats.items()) if k.startswith("statements_")},
        },
        output_name="raw_yahoo_statements",
    )
    key = _security_of
    return (
        partitioned.by_partition(context, visit.summaries, key=key, complete=visit.answered),
        partitioned.by_partition(context, visit.statements, key=key),
    )


def _security_of(row: Mapping[str, Any]) -> str:
    return str(row["security_id"])
