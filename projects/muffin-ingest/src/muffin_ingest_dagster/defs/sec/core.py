"""Stage 2 of the SEC family: stored documents read into the market tables. No provider call."""

from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from datetime import date, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import fiscal, fx, insider, sec_facts, sec_filings
from muffin_ingest.writers import (
    WriteResult,
    replace_scope,
    retract_unproduced,
    update_existing,
    upsert,
)

from muffin_ingest_dagster.defs.sec.partitions import (
    FILERS_PER_RUN,
    insider_quarters,
    quarter_key,
    quarter_of,
    sec_days,
    sec_filers,
)
from muffin_ingest_dagster.defs.sec.raw import (
    COMPANYFACTS,
    DATASET,
    FORM4,
    FRESH_DAY,
    FRESH_FILER,
    FRESH_QUARTER,
    SUBMISSIONS,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.io_managers import RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: `insider_trade`'s columns a data set restates. The key (`trade_key`) is not among them.
TRADE_COLUMNS = (
    "security_id",
    "accession_number",
    "line_key",
    "filing_date",
    "transaction_date",
    "owner_name",
    "owner_title",
    "owner_cik",
    "issuer_cik",
    "form_type",
    "transaction_code",
    "transaction_type",
    "direction",
    "security_type",
    "shares",
    "price",
    "shares_owned_after",
    "is_derivative",
    "source_code",
    "as_of",
)

#: A QUARTER'S DATA SET REPLACES BOTH NEW LANES' ROWS for its filings: those filed in the quarter,
#: and any accession the set names (a filing is in the set of the quarter it was filed in, measured
#: on 2006q1, 2016q1, 2025q4 and 2026q1). Legacy rows carry no accession and are left alone until
#: the contract (step 11).
RETRACT_QUARTER = """
delete from market.insider_trade
 where accession_number is not null
   and source_code = any(%s)
   and (filing_date between %s and %s or accession_number = any(%s))
"""


def _as_of(row: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(str(row["fetched_at"]))


@dg.asset(
    ins={"raw_sec_form4": dg.AssetIn(key=FORM4)},
    partitions_def=sec_days,
    backfill_policy=dg.BackfillPolicy.multi_run(1),
    pool="sql",
    group_name="sec",
    kinds={"postgres"},
    metadata={"table": "market.insider_trade"},
    freshness_policy=FRESH_DAY,
    description="Each stored Form 4's lines as insider trades (`sec-form4`), insert-only.",
)
def sec_form4_trade(
    context: AssetExecutionContext,
    postgres: Postgres,
    raw_store: RawStore,
    raw_sec_form4: Any,
) -> dg.MaterializeResult[None]:
    """The day's Form 4s, read by our own XML reader (decision 9, amended 2026-10-27).

    INSERT-ONLY. A data set's row and this lane's row for one line share their `trade_key`
    (security, accession, line), so a Form 4 re-read after its quarter's data set landed would
    otherwise overwrite the data set's row. Skipped outright once that data set is held.
    """
    day = date.fromisoformat(context.partition_key)
    covered = raw_store.stored_rows_for(
        DATASET, quarter_key(day.year, (day.month - 1) // 3 + 1), ("fetched_at",)
    )
    if covered:
        return dg.MaterializeResult(metadata={"covered_by_dataset": True, "rows": 0})

    stats: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    with postgres.connect() as conn:
        issuers = insider.tracked_issuers(conn)
        codes = insider.transaction_codes(conn)
        for stored in partitioned.loaded_rows(raw_sec_form4):
            try:
                trades = insider.form4_rows(
                    bytes(stored["body"]), issuers, codes, as_of=_as_of(stored)
                )
            except insider.Form4Unreadable as exc:
                stats["unreadable"] += 1
                context.log.warning("%s: %s", stored.get("accession"), exc)
                continue
            stats.update(trades.stats)
            rows += trades.rows
        with conn.cursor() as cur:
            result = upsert(cur, "market.insider_trade", rows, conflict=["trade_key"], update=None)
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            "covered_by_dataset": False,
            **dict(sorted(stats.items())),
            "rows": result.written,
            "inserted": -1 if result.changed is None else result.changed,
            "collapsed": result.collapsed,
        }
    )


@dg.asset(
    ins={"raw_sec_insider_dataset": dg.AssetIn(key=DATASET)},
    partitions_def=insider_quarters,
    backfill_policy=dg.BackfillPolicy.multi_run(1),
    pool="sql",
    group_name="sec",
    kinds={"postgres"},
    metadata={"table": "market.insider_trade"},
    freshness_policy=FRESH_QUARTER,
    description=(
        "A quarter's insider data set as insider trades (`sec-form345`), replacing both lanes' "
        "rows for its filings."
    ),
)
def sec_insider_trade(
    context: AssetExecutionContext, postgres: Postgres, raw_sec_insider_dataset: Any
) -> dg.MaterializeResult[None]:
    """The quarter's data set read into `insider_trade`: its filings' rows are retracted, from
    either new lane, and the set's lines written, in one transaction."""
    year, quarter = quarter_of(context.partition_key)
    [stored] = partitioned.loaded_rows(raw_sec_insider_dataset)
    first, last = insider.quarter_bounds(year, quarter)
    with postgres.connect() as conn:
        issuers = insider.tracked_issuers(conn)
        codes = insider.transaction_codes(conn)
        trades = insider.dataset_rows(
            bytes(stored["body"]), issuers, codes, as_of=_as_of(stored), quarter=(year, quarter)
        )
        with conn.cursor() as cur:
            cur.execute(
                RETRACT_QUARTER,
                (
                    [insider.SOURCE_DATASET, insider.SOURCE_FORM4],
                    first,
                    last,
                    sorted(trades.accessions),
                ),
            )
            retracted = cur.rowcount
            result = upsert(
                cur,
                "market.insider_trade",
                trades.rows,
                conflict=["trade_key"],
                update=list(TRADE_COLUMNS),
            )
        conn.commit()
    if trades.stats.get("filed_outside_quarter"):
        context.log.warning(
            "%s filings dated outside %sq%s: the retraction assumes a set holds its quarter's",
            trades.stats["filed_outside_quarter"],
            year,
            quarter,
        )
    return dg.MaterializeResult(
        metadata={
            **dict(sorted(trades.stats.items())),
            "filings": len(trades.accessions),
            "retracted": retracted,
            "rows": result.written,
            "changed": -1 if result.changed is None else result.changed,
            "collapsed": result.collapsed,
        }
    )


# --- the filer lane ------------------------------------------------------------------------------


class SecFactsRun(dg.Config):
    """What a filer re-parse may do."""

    #: Parse every filer the run covers, not only those whose documents this run fetched: after a
    #: parser or vocabulary change, over files already on disk. No provider call.
    reparse: bool = False


def _spec(name: str, deps: Sequence[dg.AssetKey], table: str, description: str) -> dg.AssetSpec:
    return dg.AssetSpec(
        name,
        deps=deps,
        group_name="sec",
        kinds={"postgres"},
        freshness_policy=FRESH_FILER,
        metadata={"table": table},
        description=description,
    )


#: ONE ASSET PER TABLE, ONE STEP AND ONE TRANSACTION PER FILER, so a filer's statements are never
#: labelled from a calendar another transaction has half written. A TUPLE: the defs loader collects
#: a module-level LIST of specs as assets of their own.
FILER_SPECS = (
    _spec(
        "sec_filing",
        [SUBMISSIONS],
        "market.security_filing",
        "Every kept form each filer filed, from its submissions history, with SEC as regulator.",
    ),
    _spec(
        "filer_profile",
        [SUBMISSIONS],
        "market.filer_profile",
        "The registrant's identity from its newest submissions; also `security.sic`.",
    ),
    _spec(
        "security_former_name",
        [SUBMISSIONS],
        "market.security_former_name",
        "The registrant's earlier names, one row per name.",
    ),
    _spec(
        "sec_fiscal_calendar",
        [SUBMISSIONS, COMPANYFACTS],
        "market.fiscal_calendar",
        "Each filer's fiscal calendar and periods, from its own reports and company facts.",
    ),
    _spec(
        "sec_statement",
        [COMPANYFACTS],
        "market.security_statement",
        "SEC statements (`sec-xbrl`), quarterly and annual, labelled with their fiscal period.",
    ),
)

#: `security_filing`'s columns this lane writes, and the ones whose change is a change. NEVER the
#: segment markers: the edge's segments handler owns them until Phase 6.
FILING_UPDATE = (
    "filing_date",
    "report_date",
    "report_type",
    "report_url",
    "filing_detail_url",
    "is_xbrl",
    "source_code",
    "disclosure_source_code",
    "as_of",
)
STATEMENT_KEY = ["security_id", "source_code", "statement", "period_ending", "period_type"]

FILER_PROFILE = """
select security_id::text from market.filer_profile where security_id = any(%s::uuid[])
"""


def _newest(rows: Sequence[Mapping[str, Any]], **match: Any) -> Mapping[str, Any] | None:
    """The newest stored row whose columns equal `match`, by `fetched_at`."""
    found = [r for r in rows if all(r.get(k) == v for k, v in match.items())]
    return max(found, key=lambda r: str(r["fetched_at"])) if found else None


def _tally(stats: Counter[str], result: WriteResult, name: str) -> None:
    stats[f"{name}_rows"] += result.written
    stats[f"{name}_changed"] += 0 if result.changed is None else result.changed
    stats[f"{name}_collapsed"] += result.collapsed


@dg.multi_asset(
    name="sec_filer_facts",
    specs=FILER_SPECS,
    partitions_def=sec_filers,
    backfill_policy=dg.BackfillPolicy.multi_run(FILERS_PER_RUN),
    pool="sql",
)
def sec_filer_facts(
    context: AssetExecutionContext, config: SecFactsRun, postgres: Postgres, raw_store: RawStore
) -> Iterator[dg.MaterializeResult[None]]:
    """Each filer's stored submissions and company facts, read into five tables.

    A FILER IS PARSED when this run fetched one of its documents, when `reparse` is set, or when an
    equity mapped to its CIK has no profile yet (a share class newly mapped). Otherwise its tables
    already say what its files say, and reading megabytes to write nothing is skipped.

    A CIK FANS OUT to every equity holding it at parse time, so GOOG and GOOGL each get Alphabet's
    filings, profile, calendar and statements.
    """
    keys = list(context.partition_keys)
    run_id = context.run.run_id
    stats: Counter[str] = Counter()
    with postgres.connect() as conn:
        filers = insider.tracked_issuers(conn)
        forms = sec_filings.forms(conn)
        concepts = sec_facts.concepts(conn)
        lines = sec_facts.lines(conn)
        known = fx.known_currencies(conn)
        mapped = sorted({sid for k in keys for sid in filers.get(int(k), [])})
        with conn.cursor() as cur:
            cur.execute(FILER_PROFILE, (mapped,))
            profiled = {str(r[0]) for r in cur.fetchall()}
    kept = {code for code, form in forms.items() if form.kept}

    for key in keys:
        sids = filers.get(int(key), [])
        if not sids:
            stats["filers_not_mapped"] += 1
            continue
        runs = raw_store.stored_rows_for(SUBMISSIONS, key, ("run_id",))
        runs = [*runs, *raw_store.stored_rows_for(COMPANYFACTS, key, ("run_id",))]
        fresh = any(r.get("run_id") == run_id for r in runs)
        if not (fresh or config.reparse or any(s not in profiled for s in sids)):
            stats["filers_unchanged"] += 1
            continue
        stats["filers_parsed"] += 1
        with postgres.connect() as conn:
            _parse_filer(
                conn, key, sids, raw_store, stats,
                kept=kept, forms=forms, concepts=concepts, lines=lines, known=known,
                log=context.log,
            )  # fmt: skip
            conn.commit()

    common = {
        "filers_requested": len(keys),
        "filers_parsed": stats["filers_parsed"],
        "filers_unchanged": stats["filers_unchanged"],
        "filers_not_mapped": stats["filers_not_mapped"],
    }

    def picked(*prefixes: str) -> dict[str, int]:
        return {k: v for k, v in sorted(stats.items()) if k.startswith(prefixes)}

    yield dg.MaterializeResult(
        asset_key="sec_filing", metadata={**common, **picked("filing", "submissions_")}
    )
    yield dg.MaterializeResult(
        asset_key="filer_profile", metadata={**common, **picked("profile", "sic_")}
    )
    yield dg.MaterializeResult(
        asset_key="security_former_name", metadata={**common, **picked("former_name")}
    )
    yield dg.MaterializeResult(
        asset_key="sec_fiscal_calendar", metadata={**common, **picked("calendar", "fiscal_")}
    )
    yield dg.MaterializeResult(
        asset_key="sec_statement",
        metadata={**common, **picked("statement", "facts_", "labelled")},
    )


def _parse_filer(
    conn: Any,
    key: str,
    sids: Sequence[str],
    raw_store: RawStore,
    stats: Counter[str],
    *,
    kept: set[str],
    forms: Mapping[str, sec_filings.Form],
    concepts: Sequence[sec_facts.Concept],
    lines: Mapping[str, str],
    known: frozenset[str],
    log: Any,
) -> None:
    """One filer's five tables, in the caller's transaction.

    NOTHING IS RETRACTED ON A DOCUMENT THAT WAS NOT READ: an absence, a body that is not a
    submissions or facts document, or a filer with no answered document leaves its rows as they
    are. Only an answered, parsed company facts document retracts statements it no longer produces.
    """
    cik = int(key)
    submissions = raw_store.stored_rows_for(
        SUBMISSIONS, key, ("document", "page", "http_status", "fetched_at", "body")
    )
    main = _newest(submissions, document="submissions", http_status=200)
    facts = _newest(
        raw_store.stored_rows_for(COMPANYFACTS, key, ("http_status", "fetched_at", "body")),
        http_status=200,
    )

    found: list[sec_filings.Filing] = []
    with conn.cursor() as cur:
        if main is None:
            stats["submissions_none"] += 1
        else:
            try:
                body = bytes(main["body"])
                found = sec_filings.filings(body, kept)
                for page in submissions:
                    if (
                        page.get("document") == "submissions_page"
                        and page.get("http_status") == 200
                    ):
                        found += sec_filings.filings(bytes(page["body"]), kept)
                profile = sec_filings.profile(body)
                former = sec_filings.former_names(body)
            except sec_filings.SubmissionsUnreadable as exc:
                # COUNTED AND SKIPPED, never read as a filer with no filings: its rows stay as they
                # are, and so does everything this filer's calendar would decide.
                stats["submissions_unreadable"] += 1
                log.warning("filer %s: stored submissions unreadable: %s", key, exc)
                return
            as_of = str(main["fetched_at"])
            rows = sec_filings.filing_rows(found, cik=cik, security_ids=sids, as_of=as_of)
            _tally(
                stats,
                upsert(
                    cur, "market.security_filing", rows,
                    conflict=["security_id", "accession_number"],
                    update=FILING_UPDATE, compare=FILING_UPDATE[:-1],
                ),
                "filing",
            )  # fmt: skip
            columns = profile.filer_profile_columns()
            profiles = [
                {"security_id": s, **columns, "source_code": sec_filings.SOURCE, "as_of": as_of}
                for s in sids
            ]
            updated = [c for c in profiles[0] if c != "security_id"]
            _tally(
                stats,
                upsert(
                    cur, "market.filer_profile", profiles, conflict=["security_id"],
                    update=updated, compare=[c for c in updated if c != "as_of"],
                ),
                "profile",
            )  # fmt: skip
            sic = update_existing(
                cur,
                "market.security",
                [
                    {
                        "security_id": s,
                        "sic": profile.sic,
                        "sic_description": profile.sic_description,
                    }
                    for s in sids
                ],
                key="security_id",
                types={"security_id": "uuid", "sic": "text", "sic_description": "text"},
            )
            stats["sic_changed"] += 0 if sic.changed is None else sic.changed
            names = [
                {
                    "security_id": s,
                    "name": f.name,
                    "from_date": f.from_date.isoformat() if f.from_date else None,
                    "to_date": f.to_date.isoformat() if f.to_date else None,
                    "source_code": sec_filings.SOURCE,
                }
                for s in sids
                for f in former
            ]
            _tally(
                stats,
                upsert(
                    cur, "market.security_former_name", names, conflict=["security_id", "name"],
                    update=["from_date", "to_date", "source_code"],
                ),
                "former_name",
            )  # fmt: skip

        labels: dict[str, fiscal.FilingLabel] = {}
        spans: list[fiscal.Span] = []
        facts_body: bytes | None = None
        if facts is None:
            stats["facts_none"] += 1
        else:
            facts_body = bytes(facts["body"])
            try:
                labels, spans = fiscal.summarise_facts(facts_body)
            except fiscal.FactsUnreadable as exc:
                stats["facts_unreadable"] += 1
                log.warning("filer %s: stored company facts unreadable: %s", key, exc)
                return

        calendar = fiscal.calendar_from_sec(sec_filings.reports(found, forms), labels, spans)
        stats.update({f"fiscal_{k}": v for k, v in calendar.stats.items()})
        if calendar.regimes:
            calendars, periods = fiscal.rows(
                calendar,
                security_ids=sids,
                source=sec_facts.SOURCE,
                as_of=str((facts or main or {}).get("fetched_at")),
            )
            for sid in sids:
                replace_scope(
                    cur,
                    "market.fiscal_calendar",
                    [c for c in calendars if c["security_id"] == sid],
                    scope={"security_id": sid},
                    conflict=["security_id", "regime"],
                )
            _tally(
                stats,
                upsert(
                    cur, "market.fiscal_period", periods,
                    conflict=["security_id", "regime", "fiscal_year", "fiscal_period"],
                    update=[c for c in periods[0] if c not in ("security_id", "regime")],
                ),
                "calendar_period",
            )  # fmt: skip
            stats["calendar_filers"] += 1
            stats["calendar_regimes"] += len(calendar.regimes)
            stats["calendar_52_53"] += any(r.is_52_53_week for r in calendar.regimes)
            stats["calendar_year_end_changes"] += sum(
                r.change_kind == "year_end_change" for r in calendar.regimes
            )
        else:
            # NO CALENDAR, NO STATEMENTS: every fact would be refused as off the calendar, and the
            # retraction would then delete every statement this filer has.
            stats["calendar_none"] += 1
            return

        if facts is not None and facts_body is not None:
            read = sec_facts.read(facts_body, concepts)
            stats["facts_read"] += len(read.facts)
            out = sec_facts.statement_rows(
                read.facts, calendar, concepts, lines,
                security_ids=sids, known_currencies=known, as_of=str(facts["fetched_at"]),
            )  # fmt: skip
            stats.update({f"statement_{k}": v for k, v in out.stats.items()})
            _tally(
                stats,
                upsert(
                    cur, "market.security_statement", out.rows, conflict=STATEMENT_KEY,
                    update=["currency", "data", "as_of"], compare=["currency", "data"],
                ),
                "statement",
            )  # fmt: skip
            for sid in sids:
                retracted = retract_unproduced(
                    cur,
                    "market.security_statement",
                    [r for r in out.rows if r["security_id"] == sid],
                    scope={"security_id": sid, "source_code": sec_facts.SOURCE},
                    key=["statement", "period_ending", "period_type"],
                    types={"statement": "text", "period_ending": "date", "period_type": "text"},
                )
                stats["statement_retracted"] += retracted.retracted
        cur.execute("select market.label_fiscal_periods(%s::uuid[])", (list(sids),))
        labelled = cur.fetchone()
        stats["labelled"] += int(labelled[0]) if labelled and labelled[0] is not None else 0
