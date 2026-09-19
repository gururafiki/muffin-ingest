"""Jobs, schedules and sensors of the symbology family."""

from datetime import timedelta
from typing import Any

import dagster as dg

from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION, security_partitions
from muffin_ingest_dagster.defs.symbology.raw import _security_attributes, raw_figi_ticker
from muffin_ingest_dagster.lib.resources import Postgres

#: When a security whose probe said `miss` gets asked again. Not never — a pair not quoted today may
#: be quoted next quarter (the FX history rule, same shape).
REASK_AFTER = timedelta(days=30)


def _probe_rows_readable(conn: Any, *, older_than: timedelta) -> set[str]:
    """security_ids carrying a probe MISS older than `REASK_AFTER` — the candidates for re-ask."""
    with conn.cursor() as cur:
        cur.execute(
            "select distinct security_id from market.identifier_probe "
            "where outcome = 'miss' and observed_at < now() - %s",
            (older_than,),
        )
        return {row[0] for row in cur.fetchall()}


# --- sensor -------------------------------------------------------------------------------------


@dg.sensor(
    target=raw_figi_ticker,
    minimum_interval_seconds=6 * 3600,
    description="Securities needing a symbol become rung partitions; stale misses are re-asked.",
)
def new_symbols_needed(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """Keep the ladder's shared grid in step with the universe.

    ADDS the securities that need this evidence (and are not already partitions), and RE-ASKS a
    security whose probe is a stale MISS by deleting its partition so the next tick queues it
    again — day 31 coming back is the grid-is-the-queue proof, without a custom condition.
    """
    with postgres.connect() as conn:
        attributes = _security_attributes(conn)
        stale = _probe_rows_readable(conn, older_than=REASK_AFTER)

    existing = set(context.instance.get_dynamic_partitions(SECURITY_PARTITION))
    add = [sid for sid in attributes if sid not in existing]
    context.log.info(
        "%s securities need symbols, %s new, %s stale-miss re-asks",
        len(attributes),
        len(add),
        len(stale),
    )
    requests: list[Any] = []
    if stale:
        requests.append(security_partitions.build_delete_request(sorted(stale)))
    if add or stale:
        requests.append(security_partitions.build_add_request(sorted(set(add) | stale)))
    return dg.SensorResult(run_requests=[], dynamic_partitions_requests=requests)
