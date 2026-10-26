"""Stage 1 of the promotion family: what Yahoo's chart said about each candidate line, whole."""

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import price_chart, promotion
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.vocab import throttled

from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices.automation import SWEEP_SLICE
from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION
from muffin_ingest_dagster.defs.promotion.partitions import ASK_PER_PLACE, wave_weeks
from muffin_ingest_dagster.lib.pacing import Pacer
from muffin_ingest_dagster.lib.resources import Postgres

#: The next candidates, in the wave's order. A line with neither a ticker nor a provider symbol
#: cannot be asked about.
CANDIDATES = """
select figi, ticker, provider_symbol, country_iso2
  from market.promotion_candidate
 where coalesce(ticker, provider_symbol) is not null
 order by promotion_tier, preference, listings desc, name, share_class_figi
 limit %s
"""

#: The suffixes Yahoo spells each country's listings with, in the order asked (muffin-deployment
#: migration 20261026130000): Korea `.KS` then `.KQ`, Canada `.TO`, `.V`, `.CN`, `.NE`.
SPELLINGS = (
    "select country_iso2, suffix from market.yahoo_spelling order by country_iso2, preference"
)


@dataclass(frozen=True)
class Policy:
    """`market.promotion_policy`, one row: a control table the worker reads and cannot write."""

    paused: bool
    wave_size: int
    max_pass_nights: int


def read_policy(cur: Any) -> Policy | None:
    cur.execute("select paused, wave_size, max_pass_nights from market.promotion_policy where id")
    row = cur.fetchone()
    return Policy(bool(row[0]), int(row[1]), int(row[2])) if row else None


def capacity(policy: Policy, securities_in_sweep: int) -> int:
    """How many securities the price sweep can still take on.

    The grid is a Dagster partition set, so this half of the cap is Dagster's: a full pass takes
    `len(grid) / SWEEP_SLICE` nights, and the policy says how many it may take. Never negative: the
    wave function reads a negative limit as none at all.
    """
    return max(0, SWEEP_SLICE * policy.max_pass_nights - securities_in_sweep)


def places(policy: Policy | None, securities_in_sweep: int) -> int:
    """How many share classes this week's wave may promote."""
    if policy is None or policy.paused:
        return 0
    return min(policy.wave_size, capacity(policy, securities_in_sweep))


class ProbeRun(dg.Config):
    """What bounds a run, rather than what it asks."""

    #: Ask about at most this many candidates: a live tiny subset, never steady state.
    limit: int | None = None
    #: Wall-clock budget. ~1,000 requests at 1.4 s is ~24 minutes.
    budget_seconds: int = 2700


def _pacer() -> Pacer:
    """The chart lane's pace, read from the module at every wait so a test can set it."""
    return Pacer(lambda: prices_partitions.CHART_SECONDS_BETWEEN_REQUESTS)


@dg.asset(
    partitions_def=wave_weeks,
    # THE CHART LANE'S POOL: the probe spends the same Yahoo allowance as the price night and the
    # company lane, so it runs only when neither is.
    pool="yfinance",
    # A WEEK AND A DAY: a paused or full week still materialises, empty and with its reason, so a
    # stale probe means the schedule stopped.
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=8)),
    io_manager_key="parquet_io",
    group_name="universe",
    kinds={"yahoo", "parquet"},
    description=(
        "What Yahoo's chart said about the next candidate lines for this week's promotion wave, "
        "and about one control symbol, kept whole."
    ),
)
def raw_promotion_probe(
    context: AssetExecutionContext, config: ProbeRun, postgres: Postgres
) -> list[dict[str, Any]]:
    """Asks Yahoo's chart for thirty days of daily bars for each candidate line; stores the bodies.

    EACH LINE UNDER EACH SPELLING ITS COUNTRY USES, until one names the line
    (`muffin_ingest.facets.promotion.spellings`): a KOSDAQ line is `.KQ` under a `.KS` directory,
    and a Hong Kong ticker has four digits (decided by the user 2026-10-26). A wrong spelling is
    answered `absent` or with a non-equity shadow of the right line, so both move the walk on
    (`asks_the_next_spelling`).

    THE CONTROL IS ASKED FIRST, so stage 2 can tell a line Yahoo does not price from a run Yahoo was
    not answering: an absence is recorded only when the control priced in the same run.

    OUTCOMES HERE ARE ABOUT THE SUBJECT, and partition them: `answered` is a line whose spellings
    were walked to an answer, `transport` one a request failed for, `throttled` the provider
    refusing us (the run stops), `unasked` the rest. `calls` counts requests and `respelled` the
    lines that needed more than their first spelling. What a body SAYS is stage 2's.
    """
    keys = len(context.instance.get_dynamic_partitions(SECURITY_PARTITION))
    with postgres.connect() as conn, conn.cursor() as cur:
        policy = read_policy(cur)
        wave = places(policy, keys)
        asked: list[tuple[str, list[str]]] = []
        if wave:
            cur.execute(CANDIDATES, (ASK_PER_PLACE * wave,))
            lines = cur.fetchall()
            cur.execute(SPELLINGS)
            suffixes: dict[str, list[str]] = {}
            for country, suffix in cur.fetchall():
                suffixes.setdefault(str(country), []).append(str(suffix))
            for figi, ticker, provider_symbol, country in lines:
                spelt = promotion.spellings(
                    ticker, provider_symbol, country, suffixes.get(str(country), [])
                )
                if spelt:
                    asked.append((str(figi), spelt))
    if config.limit is not None:
        asked = asked[: max(0, config.limit)]

    reason = (
        "no promotion policy"
        if policy is None
        else "paused"
        if policy.paused
        else "no capacity"
        if not wave
        else "no candidates"
        if not asked
        else None
    )
    if reason:
        context.log.info("promotion probe: nothing asked (%s)", reason)
        context.add_output_metadata(
            {"places": wave, "securities_in_sweep": keys, "asked": 0, "reason": reason}
        )
        return []

    end = datetime.now(UTC).date() + timedelta(days=1)
    period1 = yahoo_chart.day_start(end - promotion.WINDOW)
    period2 = yahoo_chart.day_start(end)
    pacer = _pacer()
    deadline = time.monotonic() + config.budget_seconds
    rows: list[dict[str, Any]] = []
    stats = {
        "calls": 0,
        "answered": 0,
        "transport": 0,
        "throttled": 0,
        "unasked": 0,
        "respelled": 0,
        "bytes": 0,
    }
    last_error: str | None = None
    consecutive_transport = 0
    stopped = False

    subjects: list[tuple[str, str | None, list[str]]] = [
        ("control", None, [promotion.CONTROL_SYMBOL])
    ] + [("candidate", figi, spelt) for figi, spelt in asked]
    for index, (role, figi, spelt) in enumerate(subjects):
        if time.monotonic() >= deadline:
            stats["unasked"] += len(subjects) - index
            context.log.warning(
                "budget of %ss spent; %s unasked", config.budget_seconds, stats["unasked"]
            )
            break
        # EACH SPELLING IN TURN, UNTIL ONE NAMES THE LINE. Stage 1 reads only whether the answer
        # is about another name (unknown, or a non-equity shadow), which is what decides the next
        # request; what the walk says about the line is stage 2's, from the same stored bodies.
        finished = False
        for rank, symbol in enumerate(spelt):
            stats["calls"] += 1
            pacer.wait()
            try:
                # A WINDOW OF DATES, AND NOT STORED: the URL changes every week, so http-cache
                # could only ever serve it a stale answer, and a stale answer is what this probe
                # exists to rule out.
                document = yahoo_chart.fetch(
                    symbol,
                    interval=price_chart.INTERVAL,
                    period1=period1,
                    period2=period2,
                    store=False,
                )
            except yahoo_chart.YahooRefused as exc:
                last_error = str(exc)
                if throttled(last_error):
                    stats["throttled"] += 1
                    stats["unasked"] += len(subjects) - index - 1
                    context.log.warning(
                        "Yahoo is refusing us (%s); stopping with %s unasked",
                        last_error,
                        stats["unasked"],
                    )
                    stopped = True
                else:
                    # A LINE WHOSE SPELLINGS WERE NOT ALL ASKED is not answered, whatever its
                    # earlier spellings said: stage 2 records an absence only for a finished walk.
                    stats["transport"] += 1
                    consecutive_transport += 1
                break
            consecutive_transport = 0
            stats["bytes"] += len(document.body)
            rows.append(
                {
                    **document.as_row(context.run.run_id),
                    # KEYS THAT MAP THE ANSWER TO OUR MODEL and cannot be derived from the body:
                    # Yahoo's answer names the symbol, never the directory line it was asked for,
                    # nor how many spellings that line has.
                    "figi": figi,
                    "asked_symbol": symbol,
                    "role": role,
                    "spelling_rank": rank,
                    "spellings": len(spelt),
                }
            )
            if rank + 1 < len(spelt) and promotion.asks_the_next_spelling(
                promotion.verdict(document.body, document.fetched_at)
            ):
                continue
            finished = True
            stats["answered"] += 1
            stats["respelled"] += int(rank > 0)
            break
        if stopped:
            break
        if not finished and consecutive_transport >= 3 and stats["answered"] == 0:
            stats["unasked"] += len(subjects) - index - 1
            context.log.error("three consecutive transport failures, stopping: %s", last_error)
            break

    if last_error:
        context.log.warning("last error: %s", last_error)
    context.log.info("promotion probe: %s places, %s candidates, %s", wave, len(asked), stats)
    context.add_output_metadata(
        {
            **stats,
            "places": wave,
            "candidates_asked": len(asked),
            "securities_in_sweep": keys,
            "reason": "none",
        }
    )
    return rows
