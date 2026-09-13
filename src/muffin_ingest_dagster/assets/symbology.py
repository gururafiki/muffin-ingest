"""The identity ladder: OpenFIGI and Yahoo evidence per security → adopted symbols.

LANE SHAPES, FROM THE PARTITION TABLE:
  the ladder is a BATCH OF SUBJECTS WE CHOOSE → one dynamic partition per `security_id`, each
  materialisable whether the provider answered or had nothing. THE GRID IS THE QUEUE:
  unmaterialised = never asked; materialised with a probe hit/miss = asked; and a THROTTLED
  subject is NOT materialised at all — recording a throttle as a miss is the 8,300-security
  incident, one security at a time.

THREE EVIDENCE RUNGS — `raw_figi_ticker` (OpenFIGI's US lookup, the SEC-usable ticker),
`raw_figi_local_symbol` (OpenFIGI unfiltered, for the local line), `raw_yahoo_symbol` (Yahoo's ISIN
search). Each stores the provider's response whole, keyed to its subject by a `position` column —
the mapping response is POSITIONAL, so a body that covers ten ISINs still answers "what did the
provider say about MY isin" per partition.

`security_symbology` resolves one security's materialised rung files onto `security_identifier`,
`security_provider_symbol` and `identifier_probe` in one transaction — the registries exception,
repeated for the same reason: a hit and a miss are the same run's observations and must land
together, and neither is a permission boundary.

RE-ASK IS THE SENSOR'S JOB: a materialised MISS partition is deleted once its probe is
`REASK_AFTER` days, so the next sensor tick re-seeds it. This is the grid-is-the-queue behaviour
proven the design way — day 31 comes back — without a custom `AutomationCondition`.
"""

# No `from __future__ import annotations` — Dagster resolves `context` by comparing classes.

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext

from muffin_ingest.facets import symbology as sym
from muffin_ingest.providers import openfigi as figi
from muffin_ingest.providers import yahoo_search
from muffin_ingest.writers import upsert
from muffin_ingest_dagster import partitioned
from muffin_ingest_dagster.assets.prices import SECURITY_PARTITION, security_partitions
from muffin_ingest_dagster.resources import Postgres

#: When a security whose probe said `miss` gets asked again. Not never — a pair not quoted today may
#: be quoted next quarter (the FX history rule, same shape).
REASK_AFTER = timedelta(days=30)

#: How many securities one run may cover. Each subject is one or two cheap provider calls; the width
#: is bounded by provider politeness more than memory, and each rung has its own pool.
SYMBOLOGY_PER_RUN = 200


def _security_attributes(conn: Any) -> dict[str, dict[str, Any]]:
    """security_id → {isin, country_iso2}, for the subjects asked this run."""
    with conn.cursor() as cur:
        cur.execute(
            "select i.security_id, i.value, s.country_iso2 "
            "from market.security_identifier i "
            "join market.security s on s.security_id = i.security_id "
            "where i.kind_code = 'isin' and s.is_tradeable = false"
        )
        return {
            sid: {"isin": isin, "country_iso2": country} for sid, isin, country in cur.fetchall()
        }


def _venues(conn: Any) -> dict[str, list[tuple[str, str]]]:
    """country_iso2 → [(exch_code, suffix)], best first — the shape `market.exchange` seeds."""
    with conn.cursor() as cur:
        cur.execute(
            "select country_iso2, exch_code, suffix from market.exchange "
            "where enabled order by country_iso2, preference"
        )
        out: dict[str, list[tuple[str, str]]] = {}
        for country, exch, suffix in cur.fetchall():
            if country:
                out.setdefault(country, []).append((exch, suffix))
        return out


def _probe_rows_readable(conn: Any, *, older_than: timedelta) -> set[str]:
    """security_ids carrying a probe MISS older than `REASK_AFTER` — the candidates for re-ask."""
    with conn.cursor() as cur:
        cur.execute(
            "select distinct security_id from market.identifier_probe "
            "where outcome = 'miss' and observed_at < now() - %s",
            (older_than,),
        )
        return {row[0] for row in cur.fetchall()}


def _rung_output(
    context: AssetExecutionContext, key_to_rows: dict[str, list[dict[str, Any]]]
) -> Any:
    """The single-partition/list vs multi-partition/mapping seam, shared by the three rungs."""
    keys: list[str] = list(key_to_rows)
    return key_to_rows if len(keys) > 1 else key_to_rows[keys[0]]


@dg.asset(
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="openfigi_mapping",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"openfigi", "parquet"},
    description="OpenFIGI's US lookup for each security, response whole.",
)
def raw_figi_ticker(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """Per security, the `/v3/mapping` body restricted to `exchCode: US`. ONE RUN, ONE BATCH.

    The US ticker is the identifier SEC and every non-provider consumer join on; OpenFIGI spells
    it `BRK/B` where SEC and the market spell `BRK-B`. The response body is stored WHOLE for the
    batch that covered the security, with `position` saying which entry answers it — the mapping
    response is positional and a reorder would attach one company's listing to another's.
    """
    with postgres.connect() as conn:
        attributes = _security_attributes(conn)

    wanted = [sid for sid in context.partition_keys if sid in attributes]
    out: dict[str, list[dict[str, Any]]] = {}
    # 10 jobs per request is OpenFIGI's anonymous batch; the loop keeps the provider honest.
    for i in range(0, len(wanted), 10):
        chunk = wanted[i : i + 10]
        jobs = [
            {"idType": "ID_ISIN", "idValue": attributes[s]["isin"], "exchCode": "US"} for s in chunk
        ]
        doc = figi.mapping(jobs)
        for position, sid in enumerate(chunk):
            row = doc.as_row(context.run.run_id)
            row["position"] = position
            row["asked_with"] = attributes[sid]["isin"]
            row["scheme"] = "ticker"
            out[sid] = [row]
    context.add_output_metadata({"asked": len(wanted), "filings": len(out)})
    return _rung_output(context, out)


@dg.asset(
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="openfigi_mapping",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"openfigi", "parquet"},
    description="OpenFIGI's every-venue lookup for each security, response whole.",
)
def raw_figi_local_symbol(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """The same mapping UNFILTERED, so every venue's match is on record for the local line."""
    with postgres.connect() as conn:
        attributes = _security_attributes(conn)

    wanted = [sid for sid in context.partition_keys if sid in attributes]
    out: dict[str, list[dict[str, Any]]] = {}
    for i in range(0, len(wanted), 10):
        chunk = wanted[i : i + 10]
        jobs = [{"idType": "ID_ISIN", "idValue": attributes[s]["isin"]} for s in chunk]
        doc = figi.mapping(jobs)
        for position, sid in enumerate(chunk):
            row = doc.as_row(context.run.run_id)
            row["position"] = position
            row["asked_with"] = attributes[sid]["isin"]
            row["scheme"] = "symbol"
            out[sid] = [row]
    context.add_output_metadata({"asked": len(wanted), "filings": len(out)})
    return _rung_output(context, out)


@dg.asset(
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="yahoo",
    io_manager_key="parquet_io",
    group_name="symbology",
    kinds={"yahoo", "parquet"},
    description="Yahoo's ISIN search answer for each security, response whole.",
)
def raw_yahoo_symbol(context: AssetExecutionContext, postgres: Postgres) -> Any:
    """Per security, Yahoo's `/v1/finance/search?q=<ISIN>` body, response whole."""
    with postgres.connect() as conn:
        attributes = _security_attributes(conn)

    wanted = [sid for sid in context.partition_keys if sid in attributes]
    out: dict[str, list[dict[str, Any]]] = {}
    for sid in wanted:
        doc = yahoo_search.search(attributes[sid]["isin"])
        row = doc.as_row(context.run.run_id)
        row["position"] = 0
        row["asked_with"] = attributes[sid]["isin"]
        row["scheme"] = "symbol"
        out[sid] = [row]
    context.add_output_metadata({"asked": len(wanted), "filings": len(out)})
    return _rung_output(context, out)


@dg.asset(
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="sql",
    group_name="symbology",
    kinds={"postgres"},
    description="The securities a rung resolved, adopted into the identity tables.",
)
def security_symbology(
    context: AssetExecutionContext,
    postgres: Postgres,
    raw_figi_ticker: Any,
    raw_figi_local_symbol: Any,
    raw_yahoo_symbol: Any,
) -> "dg.MaterializeResult[None]":
    """One security's ladder evidence → `security_identifier`, `security_provider_symbol`,
    `identifier_probe`, in one transaction.

    THE PLACEHOLDER-AND-POSITION GUARD LIVES HERE, applied to bytes already on disk: each rung file
    carries the whole batch body plus the `position` that answers THIS security, and the ladder
    picks by that position — never by reordering. A hit and a miss are both recorded as
    observations; a security neither rung could answer still earns its `miss` rows, which is what
    the re-ask sensor reads.
    """
    from muffin_ingest.facets.openfigi import OpenFigiUnreadable

    identifier_rows: list[dict[str, Any]] = []
    symbol_rows: list[dict[str, Any]] = []
    probe_rows: list[dict[str, Any]] = []

    ticker = partitioned.rows_per_partition(context, raw_figi_ticker)
    local = partitioned.rows_per_partition(context, raw_figi_local_symbol)
    yahoo = partitioned.rows_per_partition(context, raw_yahoo_symbol)

    with postgres.connect() as conn:
        attributes = _security_attributes(conn)
        venues = _venues(conn)

    for sid in context.partition_keys:
        attr = attributes.get(sid)
        if not attr:
            continue
        isin = attr["isin"]
        country = attr["country_iso2"]

        mapping_entry = _entry_from_evidence(ticker.get(sid), isin)
        entry_local = _entry_from_evidence(local.get(sid), isin)
        yahoo_hits: list[dict[str, Any]] = []
        for row in yahoo.get(sid, ()):
            try:
                yahoo_hits = sym.parse_yahoo_search(bytes(row["body"]))
            except OpenFigiUnreadable:
                yahoo_hits = []

        # THE LADDER: US ticker first, then the LOCAL line, then Yahoo's home-market hit. The local
        # picker sees the UNFILTERED mapping (entry_local) so it can find the KS/`.T` line; Yahoo
        # is the fallback only where OpenFIGI could not name a local line.
        identifiers, symbols, probes = sym.plan_symbols(
            sid,
            isin=isin,
            country_iso2=country,
            mapping_entry=mapping_entry,
            yahoo_hits=yahoo_hits,
            venues=venues,
            source="openfigi",
        )
        # The local line may resolve from the UNFILTERED rung alone, so merge its pick in.
        local_pick = sym.pick_local_symbol(country, entry_local.hits if entry_local else (), venues)
        if local_pick:
            symbol_rows.append(
                {
                    "security_id": sid,
                    "provider_code": sym.SYMBOL_PROVIDER,
                    "symbol": local_pick["symbol"],
                }
            )
            probe_rows.append(
                {
                    "security_id": sid,
                    "scheme": "symbol",
                    "provider": "openfigi",
                    "asked_with": isin,
                    "value": local_pick["symbol"],
                    "outcome": "hit",
                    "observed_at": _now(),
                }
            )
        identifier_rows += identifiers
        symbol_rows += symbols
        probe_rows += probes

    with postgres.connect() as conn:
        with conn.cursor() as cur:
            if identifier_rows:
                upsert(
                    cur,
                    "market.security_identifier",
                    identifier_rows,
                    conflict=["kind_code", "value"],
                    update=None,
                )
            if symbol_rows:
                upsert(
                    cur,
                    "market.security_provider_symbol",
                    symbol_rows,
                    conflict=["security_id", "provider_code"],
                )
            if probe_rows:
                upsert(
                    cur,
                    "market.identifier_probe",
                    probe_rows,
                    conflict=["security_id", "scheme", "provider"],
                    # THE LATEST OBSERVATION WINS — a re-ask REPLACES the previous answer, which is
                    # exactly what turns a 30-day-old miss into a fresh probe.
                    update=["asked_with", "value", "outcome", "observed_at"],
                )
        conn.commit()

    return dg.MaterializeResult(
        metadata={
            "securities": len(context.partition_keys),
            "identifiers": len(identifier_rows),
            "symbols": len(symbol_rows),
            "probes": len(probe_rows),
        }
    )


def _entry_from_evidence(
    rows: Sequence[dict[str, Any]] | None, isin: str
) -> sym.MappingEntry | None:
    """Reconstruct one security's mapping entry from its evidence row (body + position).

    The row carries the WHOLE batch body and the index that answers this security; parsing the
    body and picking body[position] keeps the positional guarantee on the way back in.
    """
    from muffin_ingest.facets.openfigi import OpenFigiUnreadable

    if not rows:
        return None
    body = bytes(rows[0]["body"])
    position = int(rows[0].get("position") or 0)
    try:
        entries = sym.parse_mapping(body, asked=("",) * (position + 1))
    except OpenFigiUnreadable:
        return None
    if position < len(entries):
        return entries[position]
    return sym.MappingEntry(asked=isin)


def _now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


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
