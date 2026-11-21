"""Stage 2 of the macro family: each series' newest answer into `market.macro_observation`."""

from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import macro
from muffin_ingest.writers import retract_unproduced, upsert

from muffin_ingest_dagster.defs.macro.partitions import FRESH, SERIES_PER_RUN, macro_series
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

RAW = dg.AssetKey("raw_macro_series")

KEY = ["indicator_code", "as_of", "dimension"]

#: A RE-PARSE MAY NAME A SERIES STAGE 1 NEVER ANSWERED (`jp-cpi` has never had a point), whose file
#: does not exist.
_TOLERATE_MISSING = {"allow_missing_partitions": True}


@dg.asset(
    ins={"raw_macro_series": dg.AssetIn(key=RAW, metadata=_TOLERATE_MISSING)},
    partitions_def=macro_series,
    backfill_policy=dg.BackfillPolicy.multi_run(SERIES_PER_RUN),
    pool="sql",
    group_name="macro",
    kinds={"postgres"},
    freshness_policy=FRESH,
    metadata={"table": "market.macro_observation"},
    description="Each series' points, retracting what its newest answer no longer states.",
)
def macro_observation(
    context: AssetExecutionContext, postgres: Postgres, raw_macro_series: Any
) -> dg.MaterializeResult[None]:
    """No provider call. One transaction.

    `fetched_at` never decides an update (`compare`), so re-reading an unchanged series writes
    nothing. RETRACTION IS WITHIN THE ANSWER'S WINDOW and only after an answer with points: a series
    with no file, or a file with no point, retracts nothing.
    """
    rows = partitioned.loaded_rows(raw_macro_series)
    parsed = macro.normalise(rows, codes=list(context.partition_keys))
    written = changed = retracted = 0
    no_window: list[str] = []
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            for answered in parsed.series:
                produced = macro.observation_rows(answered)
                result = upsert(
                    cur,
                    "market.macro_observation",
                    produced,
                    conflict=KEY,
                    update=["value", "fetched_at"],
                    compare=["value"],
                )
                written += result.written
                changed += 0 if result.changed is None else result.changed
                if answered.window is None:
                    no_window.append(answered.code)
                    continue
                retracted += retract_unproduced(
                    cur,
                    "market.macro_observation",
                    produced,
                    scope={"indicator_code": answered.code},
                    key=["as_of", "dimension"],
                    types={"as_of": "date", "dimension": "text"},
                    within={"as_of": answered.window},
                ).retracted
        conn.commit()
    return dg.MaterializeResult(
        metadata={
            "series": len(context.partition_keys),
            "answered": len(parsed.series),
            "no_rows": len(parsed.no_rows),
            "no_points": len(parsed.no_points),
            "no_window": len(no_window),
            "points": written,
            "changed": changed,
            "retracted": retracted,
            "series_without_rows": ", ".join(parsed.no_rows) or "none",
            "series_without_points": ", ".join(parsed.no_points) or "none",
        }
    )
