"""Stage 1 of the SEC family's insider lanes: what SEC published, kept whole."""

import time
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import insider, sec_index
from muffin_ingest.providers import sec_edgar

from muffin_ingest_dagster.defs.sec.partitions import (
    insider_quarters,
    quarter_key,
    quarter_of,
    sec_days,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: The raw assets, spelled once: later stages read what each already holds.
INDEX = dg.AssetKey("raw_sec_daily_index")
FORM4 = dg.AssetKey("raw_sec_form4")
DATASET = dg.AssetKey("raw_sec_insider_dataset")

#: SEC publishes a day's index at about 02:01 UTC the next day (measured 2026-10-26), and the lane
#: collects it at 11:00, so a day older than two is a stopped lane rather than a quiet one.
FRESH_DAY = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=2))

#: A quarter's data set came ~7 days after the quarter until 2026q2, which SEC had not published by
#: 2026-10-26. Four months is the most a quarter may wait before the lane is the suspect.
FRESH_QUARTER = dg.FreshnessPolicy.time_window(fail_window=timedelta(days=120))

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
