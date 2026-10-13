"""Stage 2 of the prices family: raw parsed and normalised into core rows."""

from datetime import date, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import actions, fx, price_chart, prices

from muffin_ingest_dagster.defs.prices.partitions import (
    HISTORY_PARTITIONS_PER_RUN,
    HISTORY_START,
    PROVIDER,
    security_partitions,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

_loaded_rows = partitioned.loaded_rows


@dg.asset(
    partitions_def=security_partitions,
    # THE STAGE THAT WAS OOM-KILLED, and the reason the width above is a budget rather than a
    # preference: this one holds the raw documents AND their parsed bars simultaneously.
    backfill_policy=dg.BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="prices",
    kinds={"postgres"},
    metadata={"table": "market.price_bar", "conflict": ["security_id", "trade_date"]},
    description="Each security's chart documents as daily bars, labelled with the quote currency.",
)
def price_bar_history(
    context: AssetExecutionContext, postgres: Postgres, raw_price_chart: Any
) -> list[dict[str, Any]]:
    """`market.price_bar`, keyed `(security_id, trade_date)` — the only writer of that table.

    READS YAHOO'S CHART DOCUMENTS SINCE 2026-10-10, and the reason is the label. Until then this
    read openbb's rows and labelled every bar with the listing's currency, falling back to
    `security.currency_code`: a guess, wrong for 7 of 13 sampled listings and out by 100x for every
    London line. The documents state the quote currency, so the label is now observed
    (`facets.price_chart`, which also holds the unit-change rule).

    NOT `replace_scope`. A security's bars are rewritten from its documents on every visit, and the
    writer skips a row whose values did not change, so a visit costs the bars that moved.

    BUT WITHIN ITS OWN RANGE, A SECURITY'S DOCUMENTS ARE THE ANSWER, so a bar on a date they no
    longer hold is retracted (`prices.retract_bars_absent_from_raw`). Without that, a history
    reloaded under a new symbol leaves the old listing's bars on every day the new one did not
    trade: 5,097 of them across 95 securities on 2026-10-04.
    """
    documents = _loaded_rows(raw_price_chart)
    with postgres.connect() as conn:
        known = fx.known_currencies(conn)
        # UP TO BUT NOT INCLUDING TODAY. Somewhere a market is open and its bar for today is a
        # session in progress; raw keeps it because it is what the provider said, and this is where
        # it is refused.
        parsed = price_chart.normalise(
            documents,
            known=known,
            source_code=PROVIDER.code,
            window=(HISTORY_START, date.today()),
        )
        before = price_chart.newest_labels(conn, sorted(parsed.newest_label))
        # BEFORE THE WRITE, IN ITS OWN TRANSACTION. It deletes only dates the documents no longer
        # hold, which the write below never produces, so neither order can undo the other.
        with conn.cursor() as cur:
            retracted = prices.retract_bars_absent_from_raw(cur, parsed.held)
        conn.commit()

    relabelled = [
        sid for sid, label in parsed.newest_label.items() if sid in before and before[sid] != label
    ]
    for sid in relabelled[:20]:
        context.log.info(
            "%s: newest bar relabelled %s -> %s", sid, before[sid], parsed.newest_label[sid]
        )
    if parsed.unknown_currencies:
        context.log.warning(
            "quote currencies with no code of ours, so their bars are unlabelled: %s",
            ", ".join(f"{code} x{n}" for code, n in sorted(parsed.unknown_currencies.items())),
        )
    context.add_output_metadata(
        {
            **parsed.stats,
            "rows": len(parsed.rows),
            # SECURITIES WHOSE NEWEST BAR CHANGED LABEL: a stored guess replaced by what the
            # provider states. Large on a security's first visit, then ~0.
            "labels_changed": len(relabelled),
            # Bars removed because their date sits inside the documents' range and the documents no
            # longer have it: another listing's leftovers, or a bar Yahoo withdrew.
            "retracted": retracted,
            "unknown_currencies": ", ".join(sorted(parsed.unknown_currencies)) or "none",
        }
    )
    return parsed.rows


@dg.asset(
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(HISTORY_PARTITIONS_PER_RUN),
    pool="sql",
    io_manager_key="postgres_io",
    group_name="prices",
    kinds={"postgres"},
    metadata={
        "table": "market.security_corporate_action",
        "conflict": ["security_id", "ex_date", "kind"],
        # THE KEY LEAVES THE SOURCE OUT, and Tiingo wrote 6,799 rows under it. A day Tiingo holds
        # keeps Tiingo's row until its parity check retires it (Phase 4 decision 9).
        "only_replacing": {"source_code": actions.SOURCE},
    },
    # IN THE NIGHTLY JOB, so it runs whenever the sweep does: a night without it is a stopped lane.
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36)),
    description="Each security's dividends and splits, from the documents its bars come from.",
)
def yahoo_corporate_action(
    context: AssetExecutionContext, postgres: Postgres, raw_price_chart: Any
) -> list[dict[str, Any]]:
    """`market.security_corporate_action`, Yahoo's rows, from documents the sweep already stores.

    NO REQUEST OF ITS OWN: every chart document is asked with `events=div,split` (Stage 0), so the
    edge's `security-dividends` asked openbb, monthly, for data this lane holds.

    WITHIN A SECURITY'S COVERED SPAN, ITS DOCUMENTS ARE THE ANSWER, as for bars: a Yahoo row on a
    day they no longer state is retracted (`actions.retract_absent`), and only Yahoo's.
    """
    stats: dict[str, int] = {}
    merged = {
        security_id: actions.merge(rows, stats)
        for security_id, rows in partitioned.rows_per_partition(context, raw_price_chart).items()
    }
    # BEFORE THE WRITE, IN ITS OWN TRANSACTION, as `price_bar_history` retracts: it deletes only
    # days the documents no longer state, which the write never produces.
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            retracted = actions.retract_absent(cur, merged)
        conn.commit()
    rows = [row for sid, held in sorted(merged.items()) for row in actions.rows_for(sid, held)]
    context.add_output_metadata(
        {
            **stats,
            "securities": len(merged),
            "covered": sum(1 for held in merged.values() if held.first is not None),
            "dividends": sum(1 for r in rows if r["kind"] == actions.DIVIDEND),
            "splits": sum(1 for r in rows if r["kind"] == actions.SPLIT),
            "retracted": retracted,
        }
    )
    return rows
