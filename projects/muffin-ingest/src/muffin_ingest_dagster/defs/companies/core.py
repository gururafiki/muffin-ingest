"""Stage 2 of the companies family: Yahoo's documents read into the tables that serve them."""

from collections import Counter
from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import company, fiscal, fx, statements
from muffin_ingest.writers import WriteResult, update_existing, upsert

from muffin_ingest_dagster.defs.companies.partitions import COMPANY_PARTITIONS_PER_RUN
from muffin_ingest_dagster.defs.companies.raw import FRESH, STATEMENTS, SUMMARY
from muffin_ingest_dagster.defs.prices.partitions import security_partitions
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.fiscal_tables import replace_calendars
from muffin_ingest_dagster.lib.resources import Postgres

#: A RE-PARSE MAY COVER A COMPANY STAGE 1 NEVER REACHED, whose partition has no file yet. With this
#: flag the I/O manager leaves it out rather than failing the run on the first one.
#:
#: `company_facts` CANNOT CARRY IT: a multi-asset built from specs may give an input nothing but its
#: key (Dagster 1.13, `validate_can_coexist`). In the job stage 1 writes a file for every partition
#: of the run, so only a hand-launched re-parse can meet a missing one, and it must then name
#: companies the rotation has visited.
_TOLERATE_MISSING = {"allow_missing_partitions": True}


def _spec(name: str, table: str, description: str) -> dg.AssetSpec:
    return dg.AssetSpec(
        name,
        deps=[SUMMARY],
        group_name="companies",
        kinds={"postgres"},
        freshness_policy=FRESH,
        metadata={"table": table},
        description=description,
    )


#: ONE ASSET PER TABLE, so each has its own lineage and freshness; ONE STEP AND ONE TRANSACTION,
#: so a company's facts never land half from one document and half from the last.
#:
#: A TUPLE, NOT A LIST: the defs loader collects every module-level LIST of specs as assets of their
#: own (`find_objects_in_module_of_types`), which defined each of these twice.
FACT_SPECS = (
    _spec(
        "security_profile",
        "market.security_profile",
        "Description, employees, website, headquarters and beta; also the operating country.",
    ),
    _spec(
        "security_officer",
        "market.security_officer",
        "The officers each company's newest profile lists; departed ones are removed.",
    ),
    _spec(
        "security_fundamentals",
        "market.security_fundamentals",
        "The latest valuation, margins and market cap with its currency; also the reporting "
        "currency.",
    ),
    _spec(
        "security_share_stats",
        "market.security_share_stats",
        "Share counts, short interest and ownership, one row per short-interest date.",
    ),
    _spec(
        "security_estimate",
        "market.security_estimate",
        "Analysts' price targets and recommendation, one row per fetch day.",
    ),
    _spec(
        "yahoo_classification",
        "market.security_taxonomy",
        "Yahoo's sector and industry for each company, under Yahoo's own sector.",
    ),
)


def _changed(result: WriteResult) -> int:
    """Rows the database changed, or -1 when the driver could not say (as the Postgres manager)."""
    return -1 if result.changed is None else result.changed


def _written(result: WriteResult) -> dict[str, Any]:
    return {"rows": result.written, "changed": _changed(result), "collapsed": result.collapsed}


@dg.multi_asset(
    name="company_facts",
    specs=FACT_SPECS,
    ins={"raw_yahoo_summary": dg.AssetIn(key=SUMMARY)},
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(COMPANY_PARTITIONS_PER_RUN),
    pool="sql",
)
def company_facts(
    context: AssetExecutionContext, postgres: Postgres, raw_yahoo_summary: Any
) -> Iterator[dg.MaterializeResult[None]]:
    """Each company's newest summary document, read into seven tables. No provider call.

    A SNAPSHOT TABLE TAKES THE NEWEST DOCUMENT WHOLE, nulls included: a P/E Yahoo withdraws when
    earnings turn negative must not survive from last week. Everything else erases only on a
    statement. A `security` column is set only where the document states one, and the two
    retractions act only where the document says so: a profile listing its officers, a profile
    naming a sector we map, or naming none.
    """
    documents = partitioned.loaded_rows(raw_yahoo_summary)
    with postgres.connect() as conn:
        known = fx.known_currencies(conn)
        parsed = company.normalise(documents, known_currencies=known)
        facts = parsed.facts
        with conn.cursor() as cur:
            profiles = upsert(
                cur,
                "market.security_profile",
                [f.profile for f in facts if f.profile],
                conflict=["security_id"],
                update=[
                    "description",
                    "employees",
                    "website",
                    "hq_city",
                    "hq_state",
                    "hq_country",
                    "beta",
                    "source_code",
                    "as_of",
                ],
            )
            iso_by_name = company.country_codes(cur)
            homes, unresolved = company.country_rows(facts, iso_by_name)
            countries = update_existing(
                cur,
                "market.security",
                homes,
                key="security_id",
                types={"security_id": "uuid", "provider_country_iso2": "text"},
            )

            officers_retracted = company.retract_officers(cur, facts)
            officers = upsert(
                cur,
                "market.security_officer",
                company.officer_rows(facts),
                conflict=["security_id", "name"],
                update=["title", "pay", "year_born", "age", "fiscal_year", "source_code", "as_of"],
                # THE KEY LEAVES THE SOURCE OUT, so the guard keeps another source's officer of the
                # same name. Only Yahoo writes the table today; this is what keeps that true.
                only_replacing={"source_code": company.OFFICER_SOURCE},
            )

            fundamentals = upsert(
                cur,
                "market.security_fundamentals",
                [f.fundamentals for f in facts if f.fundamentals],
                conflict=["security_id"],
                update=[
                    "source_code",
                    "as_of",
                    "pe_ratio",
                    "forward_pe",
                    "peg_ratio",
                    "price_to_book",
                    "profit_margin",
                    "gross_margin",
                    "operating_margin",
                    "return_on_equity",
                    "revenue_growth",
                    "debt_to_equity",
                    "dividend_yield",
                    "beta",
                    "enterprise_value",
                    "market_cap",
                    "market_cap_currency",
                    "raw",
                ],
            )
            reporting = update_existing(
                cur,
                "market.security",
                company.reporting_currency_rows(facts),
                key="security_id",
                types={"security_id": "uuid", "reporting_currency": "text"},
            )

            share_stats = upsert(
                cur,
                "market.security_share_stats",
                [f.share_stats for f in facts if f.share_stats],
                conflict=["security_id", "as_of"],
                update=[
                    "float_shares",
                    "outstanding_shares",
                    "short_interest",
                    "short_percent_of_float",
                    "days_to_cover",
                    "insider_ownership",
                    "institution_ownership",
                    "institutions_count",
                    "source_code",
                    "fetched_at",
                ],
            )
            estimates = upsert(
                cur,
                "market.security_estimate",
                [f.estimate for f in facts if f.estimate],
                conflict=["security_id", "as_of"],
                update=[
                    "target_high",
                    "target_low",
                    "target_consensus",
                    "target_median",
                    "recommendation",
                    "recommendation_mean",
                    "number_of_analysts",
                    "currency_code",
                    "source_code",
                    "fetched_at",
                ],
            )

            classification = company.classify(cur, facts)
            classification_retracted = company.retract_classification(cur, classification)
            classified = upsert(
                cur,
                "market.security_taxonomy",
                classification.rows,
                conflict=["security_id", "node_id", "source_code"],
                update=["as_of"],
            )
        conn.commit()

    if unresolved:
        context.log.warning(
            "headquarters countries with no code of ours (add a country_alias row): %s",
            ", ".join(f"{name} x{n}" for name, n in sorted(unresolved.items())),
        )
    if classification.unmapped:
        context.log.warning(
            "Yahoo sector labels with no muffin sector, so those companies keep their stored rows: "
            "%s",
            ", ".join(f"{label} x{n}" for label, n in sorted(classification.unmapped.items())),
        )

    # THE PARSE'S COUNTERS GO ON THE FIRST ASSET ONLY: they describe the documents, not a table.
    yield dg.MaterializeResult(
        asset_key="security_profile",
        metadata={
            **parsed.stats,
            **_written(profiles),
            "countries_set": _changed(countries),
            "countries_unresolved": sum(unresolved.values()),
            "unresolved_names": ", ".join(sorted(unresolved)) or "none",
        },
    )
    yield dg.MaterializeResult(
        asset_key="security_officer",
        metadata={**_written(officers), "retracted": officers_retracted},
    )
    yield dg.MaterializeResult(
        asset_key="security_fundamentals",
        metadata={**_written(fundamentals), "reporting_currencies_set": _changed(reporting)},
    )
    yield dg.MaterializeResult(asset_key="security_share_stats", metadata=_written(share_stats))
    yield dg.MaterializeResult(asset_key="security_estimate", metadata=_written(estimates))
    yield dg.MaterializeResult(
        asset_key="yahoo_classification",
        metadata={
            **_written(classified),
            "companies_replaced": len(classification.replaced),
            "nodes_created": classification.nodes_created,
            "retracted": classification_retracted,
            "sector_unmapped": sum(classification.unmapped.values()),
            "unmapped_labels": ", ".join(sorted(classification.unmapped)) or "none",
        },
    )


@dg.asset(
    ins={"raw_yahoo_statements": dg.AssetIn(key=STATEMENTS, metadata=_TOLERATE_MISSING)},
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(COMPANY_PARTITIONS_PER_RUN),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="companies",
    kinds={"postgres"},
    metadata={
        "table": "market.security_statement",
        # THE PROVIDER IS IN THE KEY (Phase 5 decision 6, muffin-deployment migration
        # 20261025140000). Yahoo's row for a period SEC also reports is kept beside SEC's, rather
        # than refused by a guard on whose row it may replace, and the serving view and the
        # derivation choose by source priority.
        "conflict": ["security_id", "source_code", "statement", "period_ending", "period_type"],
        # YAHOO'S WINDOW ROLLS PER LINE, so a narrower answer must not erase a stored line.
        "merge_json": ["data"],
    },
    freshness_policy=FRESH,
    description="Yahoo's annual, quarterly and half-yearly statements, with their currency.",
)
def yahoo_statement(
    context: AssetExecutionContext, postgres: Postgres, raw_yahoo_statements: Any
) -> list[dict[str, Any]]:
    """Every stored timeseries document, merged per line, as `market.security_statement` rows.

    The writer re-queues a row for the metric derivation when its values change (a trigger on the
    table), so `derived_security_metrics` picks up exactly what moved.
    """
    # A CURRENCY THE METRICS CANNOT REFERENCE IS WITHHELD HERE: `TRL` stalled the derivation for
    # every company from 2026-10-21 to 10-24.
    with postgres.connect() as conn:
        known = fx.known_currencies(conn)
    parsed = statements.normalise(
        partitioned.loaded_rows(raw_yahoo_statements), known_currencies=known
    )
    if parsed.unknown_keys:
        context.log.warning(
            "timeseries types in none of the three statements (raw keeps them): %s",
            ", ".join(f"{k} x{n}" for k, n in sorted(parsed.unknown_keys.items())[:20]),
        )
    context.add_output_metadata(
        {**parsed.stats, "unknown_keys": len(parsed.unknown_keys)},
    )
    return parsed.rows


#: The period ends Yahoo published for each security, by period type.
YAHOO_PERIOD_ENDS = """
select security_id::text, period_type, array_agg(distinct period_ending order by period_ending)
  from market.security_statement
 where security_id = any(%s::uuid[]) and source_code = 'yfinance'
 group by 1, 2
"""

#: The securities a SEC calendar already covers. SEC states a filer's years in its own reports, so
#: its calendar replaces Yahoo's and never the other way round.
SEC_CALENDARS = """
select distinct security_id::text from market.fiscal_calendar
 where security_id = any(%s::uuid[]) and source_code <> 'yfinance'
"""


@dg.asset(
    deps=[yahoo_statement],
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(COMPANY_PARTITIONS_PER_RUN),
    pool="sql",
    group_name="companies",
    kinds={"postgres"},
    metadata={"table": "market.fiscal_calendar"},
    freshness_policy=FRESH,
    description=(
        "A fiscal calendar observed from Yahoo's period ends, for companies SEC has none for; then "
        "each company's statements are labelled with their fiscal period."
    ),
)
def yahoo_fiscal_calendar(
    context: AssetExecutionContext, postgres: Postgres
) -> dg.MaterializeResult[None]:
    """Each company's calendar from the periods Yahoo published (`fiscal.calendar_from_periods`).

    OBSERVED, NOT STATED: no filing names a year here, so each is labelled by the calendar year of
    its end. That is why SEC's calendar wins wherever it exists, and this never touches one.

    THEN EVERY COMPANY IN THE RUN IS LABELLED, SEC's included: a Yahoo row for a period SEC also
    reports takes its label from SEC's calendar, which is what lets the serving view show the period
    once.

    NOT IN THE `yahoo_companies` JOB YET. A label re-queues the statement for the metric
    derivation, so joining the running rotation would re-derive ~180,000 statements a day before
    the lane has run on a tiny subset. It is materialised by hand until then, and joins the job at
    Phase 5 step 7.
    """
    sids = list(context.partition_keys)
    stats: Counter[str] = Counter()
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SEC_CALENDARS, (sids,))
            by_sec = {str(r[0]) for r in cur.fetchall()}
            cur.execute(YAHOO_PERIOD_ENDS, (sids,))
            ends: dict[str, dict[str, list[date]]] = {}
            for sid, period_type, days in cur.fetchall():
                ends.setdefault(str(sid), {})[str(period_type)] = list(days)
            as_of = datetime.now(UTC).isoformat()
            wanted: dict[str, tuple[list[dict[str, object]], list[dict[str, object]]]] = {}
            for sid in sids:
                if sid in by_sec:
                    stats["calendar_from_sec"] += 1
                    continue
                held = ends.get(sid, {})
                if not held.get("annual"):
                    stats["no_annual_periods"] += 1
                    continue
                calendar = fiscal.calendar_from_periods(
                    held["annual"], held.get("quarter", ()), held.get("half", ())
                )
                stats.update({f"fiscal_{k}": v for k, v in calendar.stats.items()})
                if not calendar.regimes:
                    stats["no_calendar"] += 1
                    continue
                wanted[sid] = fiscal.rows(
                    calendar, security_ids=[sid], source="yfinance", as_of=as_of
                )
            stats.update(replace_calendars(cur, wanted))
            cur.execute("select market.label_fiscal_periods(%s::uuid[])", (sids,))
            labelled = cur.fetchone()
            stats["labelled"] = int(labelled[0]) if labelled and labelled[0] is not None else 0
        conn.commit()
    return dg.MaterializeResult(metadata={"securities": len(sids), **dict(sorted(stats.items()))})
