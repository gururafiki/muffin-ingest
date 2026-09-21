"""Jobs, schedules and sensors of the discovery family."""

from datetime import timedelta

import dagster as dg
from muffin_ingest import settings
from muffin_ingest.facets import nport
from muffin_ingest.providers import sec_nport

from muffin_ingest_dagster.defs.discovery.core import _exchanges, _tracked_funds
from muffin_ingest_dagster.defs.discovery.partitions import (
    NPORT_PARTITIONS,
    SWEEP_PARTITIONS,
    exchange_sweeps,
    nport_filings,
)
from muffin_ingest_dagster.defs.discovery.raw import (
    raw_exchange_sweep,
    raw_fund_directory,
    raw_nport_filing,
)
from muffin_ingest_dagster.lib.resources import Postgres

# --- sensors ------------------------------------------------------------------------------------


def _directory_map() -> dict[str, tuple[str, str]]:
    """The fund directory's `{symbol: (cik, series_id)`, read from its raw artefact.

    The fallback for a fund the operator has added to `tracked_fund` but the pipeline has never
    ingested — its row carries no cik/series_id yet, and the directory is the only keyless map that
    can supply them. Read off disk because a sensor has no I/O manager; the daily asset guarantees
    the file is fresh.
    """
    from pathlib import Path

    import pyarrow.parquet as pq

    path = Path(settings.raw_root()) / "raw_fund_directory.parquet"
    if not path.exists():
        return {}
    table = pq.read_table(path)
    if table.column_names == ["collected_nothing"]:
        return {}
    out: dict[str, tuple[str, str]] = {}
    for row in table.to_pylist():
        body = row.get("body")
        if not body:
            continue
        try:
            directory = nport.fund_directory(bytes(body))
        except nport.NportUnreadable:
            continue
        out.update(directory)
    return out


#: WHY BOTH DISCOVERY SENSORS SHIP RUNNING, AND WHY THAT IS NOT A SPEND DECISION.
#:
#: Each of them returns `run_requests=[]` — they ADD PARTITION KEYS AND MATERIALISE NOTHING. So
#: starting them makes outstanding work VISIBLE (a new filing, a newly enabled venue appears as an
#: unmaterialised partition) and asks no provider for anything; the fetch stays an operator's call,
#: which is what `new_exchange_sweeps`' own note below has always said.
#:
#: That distinction is the whole reason these two go on while `new_symbols_needed` does not. The
#: symbology sensor seeds a grid whose rungs carry `AutomationCondition.missing()`, so the daemon
#: would begin asking the provider the moment the keys existed. These cannot: there is no condition
#: on the far side.
#:
#: Measured 2026-09-21 before the change: `dynamic_partitions` held ONE `exchange_sweep` key — the
#: one added by hand to prove the lane live — so a backfill had nothing to select, and the lane had
#: sat at zero materialisations since it was built. A sensor that ships STOPPED is a lane that does
#: not exist.


@dg.sensor(
    target=raw_nport_filing,
    minimum_interval_seconds=6 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="A tracked fund's newest NPORT-P filing the grid has not queued becomes a "
    "partition.",
)
def new_nport_filings(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    """FTS per enabled series for filings we have not queued yet.

    ADDS KEYS AND REQUESTS NOTHING — a new filing becomes VISIBLE as an unmaterialised partition
    rather than launching a run (the `new_securities_need_history` shape). The accession is fetched
    so the key can carry the directory CIK, which the Archives path needs and the accession alone
    cannot supply.
    """
    import datetime as _dt

    with postgres.connect() as conn:
        funds = _tracked_funds(conn)

    directory = _directory_map()
    existing = set(context.instance.get_dynamic_partitions(NPORT_PARTITIONS))
    add: list[str] = []
    today = _dt.date.today().isoformat()
    start = (_dt.date.today() - timedelta(days=310)).isoformat()
    for fund in funds:
        series = fund["series_id"]
        cik = fund["cik"]
        if not series or not cik:
            # A NEWLY ADDED FUND: `tracked_fund` has no cik/series_id until its first filing is
            # ingested, so the daily directory supplies them.
            resolved = directory.get(fund["symbol"])
            if not resolved:
                continue
            cik, series = resolved
        doc = sec_nport.search_index(series, start=start, end=today)
        refs = nport.filing_refs(doc.body, cik)
        if not refs:
            # A HITLESS SEARCH IS AN ABSENCE, NOT A FAILURE — a fund whose last filing predates the
            # window, or one the FTS has not indexed yet, must not take the rest of the sensor down.
            continue
        newest = refs[-1]
        if newest.key not in existing:
            add.append(newest.key)
    context.log.info("%s funds, %s new filings queued", len(funds), len(add))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[nport_filings.build_add_request(add)] if add else [],
    )


@dg.sensor(
    target=raw_exchange_sweep,
    minimum_interval_seconds=7 * 24 * 3600,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="The enabled venues become sweep partitions.",
)
def new_exchange_sweeps(context: dg.SensorEvaluationContext, postgres: Postgres) -> dg.SensorResult:
    with postgres.connect() as conn:
        venues = _exchanges(conn)
    existing = set(context.instance.get_dynamic_partitions(SWEEP_PARTITIONS))
    add = [exch for exch in venues if exch not in existing]
    context.log.info("%s venues, %s not yet swept", len(venues), len(add))
    return dg.SensorResult(
        run_requests=[],
        dynamic_partitions_requests=[exchange_sweeps.build_add_request(add)] if add else [],
    )


#: The discovery directory is the one daily schedule in the lane; the filings and sweeps are
#: seeded by their sensors, and re-materialising them is an operator's call.
fund_directory = dg.ScheduleDefinition(
    name="fund_directory_schedule",
    target=[raw_fund_directory],
    cron_schedule="0 5 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
