"""Stage 2 of the SEC family's insider lanes: stored filings read into `market.insider_trade`."""

from collections import Counter
from datetime import date, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import insider
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.sec.partitions import (
    insider_quarters,
    quarter_key,
    quarter_of,
    sec_days,
)
from muffin_ingest_dagster.defs.sec.raw import DATASET, FORM4, FRESH_DAY, FRESH_QUARTER
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
