"""Stage 1 of the macro family: what openbb's providers, and BIS, sent, kept whole."""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import macro
from muffin_ingest.providers import bis, openbb
from muffin_ingest.providers.documents import DocumentRefused
from muffin_ingest.providers.outcome import Outcome

from muffin_ingest_dagster.defs.macro.partitions import FRESH, SERIES_PER_RUN, macro_series
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

OUTCOMES = (
    "answered",
    "empty",
    "throttled",
    "failed",
    "unroutable",
    "disabled",
    "uncatalogued",
    "limited_out",
)


class MacroRun(dg.Config):
    limit: int | None = None


@dg.asset(
    partitions_def=macro_series,
    backfill_policy=dg.BackfillPolicy.multi_run(SERIES_PER_RUN),
    # THE `yfinance` POOL: four series (the S&P 500, gold, oil, bitcoin) ask Yahoo through openbb,
    # and the pool keeps them from asking beside the price night or the company lane. The other 34
    # are FRED, OECD and the Fed, a few seconds in all.
    pool="yfinance",
    io_manager_key="parquet_io",
    # MERGING, SO AN EMPTY OR FAILED ANSWER REPLACES NOTHING; an answered one is `Complete` and
    # replaces the file whole (`partitioned.Complete`).
    metadata={"merge_on": ["indicator_code", "fetched_at"]},
    group_name="macro",
    kinds={"openbb", "parquet"},
    freshness_policy=FRESH,
    description=(
        "Each catalogued macro series' last 400 days, as openbb's provider (or BIS) answered."
    ),
)
def raw_macro_series(context: AssetExecutionContext, config: MacroRun, postgres: Postgres) -> Any:
    """One request per enabled series, in the catalogue's order.

    PER SERIES, so one dead route cannot take the others with it (the edge's rule). A disabled row,
    or a key the catalogue no longer holds, is counted and not asked. A run in which nothing
    answered fails: that is a provider or network event, not 38 dead series.
    """
    wanted = list(context.partition_keys)
    with postgres.connect() as conn:
        catalogue = {s.code: s for s in macro.catalogue(conn)}
    stats = {o: 0 for o in OUTCOMES}
    asked = [code for code in wanted if code in catalogue and catalogue[code].enabled]
    stats["uncatalogued"] = sum(1 for code in wanted if code not in catalogue)
    stats["disabled"] = sum(
        1 for code in wanted if code in catalogue and not catalogue[code].enabled
    )
    if config.limit is not None:
        stats["limited_out"] = max(0, len(asked) - config.limit)
        asked = asked[: config.limit]

    start = datetime.now(UTC).date() - macro.WINDOW
    rows: list[dict[str, Any]] = []
    answered: set[str] = set()
    errors: dict[str, str] = {}
    for code in asked:
        series = catalogue[code]
        if series.provider == bis.PROVIDER:
            # BIS, ASKED DIRECTLY: Japan's CPI, which OECD stopped publishing (`providers.bis`).
            # The answer is stored as one document row, its CSV whole; stage 2 reads it.
            try:
                flow, key = bis.series_key(series.route, series.params)
                document = bis.fetch(flow, key, start)
            except bis.UnknownSeries as exc:
                stats["unroutable"] += 1
                errors[code] = str(exc)
                continue
            except DocumentRefused as exc:
                stats["throttled" if "HTTP 429" in str(exc) else "failed"] += 1
                errors[code] = str(exc)[:300]
                continue
            if document is None:
                stats["empty"] += 1
                continue
            row = document.as_row(context.run.run_id)
            row.update(
                {
                    "indicator_code": code,
                    "route": series.route,
                    "provider": series.provider,
                    "asked_from": start.isoformat(),
                }
            )
            rows.append(row)
            answered.add(code)
            stats["answered"] += 1
            continue
        try:
            answer = openbb.macro_series(
                series.route,
                provider=series.provider,
                start=start,
                country=series.provider_country,
                params=series.params,
            )
        except openbb.UnknownMacroRoute as exc:
            stats["unroutable"] += 1
            errors[code] = openbb.redacted(str(exc))
            continue
        except openbb.ProviderRefused as exc:
            stats["throttled"] += 1
            errors[code] = openbb.redacted(str(exc))
            continue
        except Exception as exc:  # one series' failure is recorded, never fatal
            outcome = openbb.classify(str(exc))
            stats["empty" if outcome is Outcome.DEAD_SUBJECT else "failed"] += 1
            errors[code] = openbb.redacted(f"{type(exc).__name__}: {exc}")[:300]
            continue
        if not answer.rows:
            stats["empty"] += 1
            continue
        fetched_at = datetime.now(UTC).isoformat()
        for row in answer.rows:
            row.update(
                {
                    "indicator_code": code,
                    "route": series.route,
                    "provider": series.provider,
                    "asked_from": start.isoformat(),
                    "fetched_at": fetched_at,
                    "run_id": context.run.run_id,
                }
            )
            rows.append(row)
        answered.add(code)
        stats["answered"] += 1

    for code, message in sorted(errors.items()):
        context.log.warning("%s: %s", code, message)
    context.add_output_metadata(
        {
            "requested": len(wanted),
            **stats,
            "rows": len(rows),
            "errors": ", ".join(sorted(errors)) or "none",
        }
    )
    if asked and not answered:
        raise dg.Failure(
            f"none of {len(asked)} series answered; a provider or network event, not dead series",
            metadata={**stats, "errors": "; ".join(f"{k}: {v}" for k, v in sorted(errors.items()))},
        )
    return partitioned.by_partition(
        context, rows, key=lambda row: str(row["indicator_code"]), complete=answered
    )
