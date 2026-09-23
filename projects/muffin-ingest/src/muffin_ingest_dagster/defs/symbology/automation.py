"""Jobs, schedules and sensors of the symbology family."""

from typing import Any

import dagster as dg
from muffin_ingest.facets import symbology as sym

from muffin_ingest_dagster.defs.symbology.partitions import (
    SYMBOLOGY_PARTITIONS,
    symbology_subjects,
)
from muffin_ingest_dagster.defs.symbology.raw import raw_figi_ticker
from muffin_ingest_dagster.lib.resources import Postgres


#: WHY THIS ONE IS A SPEND DECISION AND THE DISCOVERY SENSORS WERE NOT.
#:
#: `new_nport_filings` and `new_exchange_sweeps` return `run_requests=[]` and their assets carry no
#: automation condition, so starting them makes work VISIBLE and asks no provider for anything.
#: This sensor seeds a grid whose rungs DO carry a condition, behind a RUNNING default automation
#: sensor — so the partitions it adds are asked about within the hour.
#:
#: What that costs, measured 2026-09-21: 5,697 equities missing a ticker and 1,618 missing a
#: provider symbol. Keyed, OpenFIGI takes 100 mapping jobs a request, so the two mapping rungs are
#: ~57 and ~17 requests — minutes, against a budget nothing else here competes for.
#:
#: `raw_yahoo_symbol` is deliberately left WITHOUT a condition: it is one request per subject on
#: the budget the nightly price sweep lives on, and 1,618 of those is more than a whole night's
#: measured allowance. It stays an operator's backfill until that trade is measured.
@dg.sensor(
    target=raw_figi_ticker,
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="A security missing any of the ladder's evidence becomes a partition.",
)
def new_symbols_needed(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """ADD ONLY. Seeding is the sensor's whole job; asking is the automation condition's.

    THE EARLIER VERSION DELETED PARTITIONS TO SCHEDULE WORK, and the grid it deleted from was the
    PRICE lane's: `security_partitions` served both families, so re-asking a security whose symbol
    probe was a stale miss also threw away the record that its bars had been collected. Nothing had
    noticed because this sensor has never been started. The grid is now this family's own
    (`partitions.py` says why), and a re-ask is `ReAskAfter`, which requests a partition without
    touching what the grid records.

    A GRID IS NOT A QUEUE YOU POP FROM. Deleting a key to make a subject look new is the same
    mistake as a backlog view that asks "does the output look right?" — it destroys the one piece
    of state that distinguishes "never asked" from "asked, and the provider had nothing".

    THE POPULATION IS THE UNION of what the rungs need, because they share one grid and
    `security_symbology` consumes all three. Each rung then narrows to its own subset inside the
    run, so a subject seeded for its missing local symbol does not cost a Yahoo request it has no
    use for.
    """
    with postgres.connect() as conn:
        needed = sym.subjects_needing(conn, sym.NEEDS_TICKER) | sym.subjects_needing(
            conn, sym.NEEDS_SYMBOL
        )

    existing = set(context.instance.get_dynamic_partitions(SYMBOLOGY_PARTITIONS))
    add = sorted(needed - existing)
    context.log.info(
        "%s securities need symbology evidence, %s not yet in the grid, %s already there",
        len(needed),
        len(add),
        len(existing),
    )
    requests: list[Any] = [symbology_subjects.build_add_request(add)] if add else []
    return dg.SensorResult(run_requests=[], dynamic_partitions_requests=requests)
