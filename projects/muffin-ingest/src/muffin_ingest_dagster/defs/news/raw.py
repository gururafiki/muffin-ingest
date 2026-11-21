"""Stage 1 of the news family: each company's RSS document, kept whole."""

import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import news, prices
from muffin_ingest.providers import yahoo_rss
from muffin_ingest.providers.documents import DocumentRefused

from muffin_ingest_dagster.defs.companies.partitions import COMPANY_PARTITIONS_PER_RUN
from muffin_ingest_dagster.defs.prices.partitions import PROVIDER, security_partitions
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.pacing import Pacer
from muffin_ingest_dagster.lib.resources import Postgres

#: SPACING BETWEEN TWO REQUESTS, measured 2026-11-21: 100 companies at one second each, no refusal.
#: Its own number rather than the chart lane's, because this is another host with its own allowance.
SECONDS_BETWEEN_REQUESTS = 1.0

#: A company's news is a week old at most once the asset joins the rotation, so a day without a
#: materialisation is a stopped lane, as for the company facts.
FRESH = dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36))

#: Every outcome a company can have in a run. They partition the companies asked about.
OUTCOMES = (*news.OUTCOMES, "refused", "transport", "unasked")

_STATUS = re.compile(r"HTTP (\d{3})$")


class NewsRun(dg.Config):
    """What bounds a run."""

    #: Cap the companies asked about, heaviest first. For a first look, never for steady state.
    limit: int | None = None
    #: Wall-clock budget: 100 companies at a second each is under two minutes.
    budget_seconds: int = 600


def status_of(exc: DocumentRefused) -> int | None:
    """The HTTP status a refusal names, or None for a request that never answered.

    READ OFF THE TAIL, NEVER SEARCHED FOR: the message starts with the URL, which carries the
    symbol, and `4290.T` contains `429` (umbrella CLAUDE.md, "A refusal raised inside
    `metrics.request` is counted as transport").
    """
    match = _STATUS.search(str(exc))
    return int(match.group(1)) if match else None


@dataclass
class _Visit:
    rows: list[dict[str, Any]] = field(default_factory=list)
    #: Companies whose document Yahoo answered and we could read: their file is replaced.
    replaced: set[str] = field(default_factory=set)
    stats: dict[str, int] = field(default_factory=dict)
    last_error: str | None = None

    def count(self, name: str, n: int = 1) -> None:
        self.stats[name] = self.stats.get(name, 0) + n


def collect(
    context: AssetExecutionContext, subjects: Sequence[prices.Subject], *, budget_seconds: int
) -> _Visit:
    """Ask for each company's feed in weight order, one request each.

    A STOP IS NEVER AN ABSENCE: whatever was not asked when Yahoo refused us with a 429, or three
    requests in a row failed before anything answered, or the budget ran out, is `unasked`.
    """
    visit = _Visit(stats={o: 0 for o in OUTCOMES})
    pacer = Pacer(lambda: SECONDS_BETWEEN_REQUESTS)
    deadline = time.monotonic() + budget_seconds
    failures_in_a_row = 0
    anything_answered = False
    for index, subject in enumerate(subjects):
        remaining = len(subjects) - index
        if time.monotonic() >= deadline:
            visit.count("unasked", remaining)
            context.log.warning(
                "budget of %ss spent; %s companies unasked", budget_seconds, remaining
            )
            break
        pacer.wait()
        visit.count("calls")
        try:
            document = yahoo_rss.fetch(subject.symbol)
        except DocumentRefused as exc:
            visit.last_error = str(exc)
            status = status_of(exc)
            if status == 429:
                visit.count("throttled")
                visit.count("unasked", remaining)
                context.log.warning(
                    "Yahoo is refusing us (%s); %s companies unasked", exc, remaining
                )
                break
            visit.count("transport" if status is None else "refused")
            failures_in_a_row += 1
            if failures_in_a_row >= 3 and not anything_answered:
                visit.count("unasked", remaining - 1)
                context.log.warning(
                    "three failures and nothing answered (%s); %s companies unasked",
                    exc,
                    remaining - 1,
                )
                break
            continue
        failures_in_a_row = 0
        anything_answered = True
        outcome = news.read(document.body).outcome
        visit.count(outcome)
        visit.count("bytes", len(document.body))
        row = document.as_row(context.run.run_id)
        row.update(
            {
                "security_id": subject.security_id,
                "asked_symbol": subject.symbol,
                "provider": yahoo_rss.PROVIDER,
            }
        )
        visit.rows.append(row)
        if outcome != "unreadable":
            # THE NEWEST ANSWER REPLACES THE LAST: the feed is the newest twenty items, and Postgres
            # holds what earlier answers said. A body we cannot read is filed beside the last one.
            visit.replaced.add(subject.security_id)
    if visit.last_error:
        context.log.warning("last error: %s", visit.last_error)
    return visit


@dg.asset(
    io_manager_key="parquet_io",
    group_name="news",
    kinds={"yahoo", "parquet"},
    metadata={"merge_on": ["security_id", "fetched_at"]},
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(COMPANY_PARTITIONS_PER_RUN),
    pool="yfinance",
    freshness_policy=FRESH,
    description="Each company's newest Yahoo RSS headline document, kept whole.",
)
def raw_yahoo_news(context: AssetExecutionContext, config: NewsRun, postgres: Postgres) -> Any:
    """One request per company for its RSS headlines.

    THE `yfinance` POOL, although the host is another: it keeps a launched run from asking Yahoo
    beside the price night or the company lane. Once in `yahoo_companies`, the run holds it anyway.
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
    visit = collect(context, subjects, budget_seconds=config.budget_seconds)
    context.add_output_metadata(
        {
            "requested": len(wanted),
            # Left out by `askable_subjects`, as in the company lane, so the outcomes sum to
            # `requested`: no symbol, not an equity, or a symbol Yahoo's chart rejected alone.
            "not_askable": len(wanted) - askable,
            "limited_out": askable - len(subjects),
            **{o: visit.stats[o] for o in OUTCOMES},
            "calls": visit.stats.get("calls", 0),
            "throttled": visit.stats.get("throttled", 0),
            "bytes": visit.stats.get("bytes", 0),
        }
    )
    return partitioned.by_partition(context, visit.rows, key=_security_of, complete=visit.replaced)


def _security_of(row: Mapping[str, Any]) -> str:
    return str(row["security_id"])
