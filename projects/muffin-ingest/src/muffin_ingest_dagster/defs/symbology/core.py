"""Stage 2 of the symbology family: raw parsed and normalised into core rows."""

from collections.abc import Sequence
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import symbology as sym
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.symbology.partitions import (
    SYMBOLOGY_PER_RUN,
    symbology_subjects,
)
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres


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


@dg.asset(
    partitions_def=symbology_subjects,
    backfill_policy=dg.BackfillPolicy.multi_run(SYMBOLOGY_PER_RUN),
    pool="sql",
    group_name="symbology",
    kinds={"postgres"},
    # EAGER, SO THE LADDER CLOSES ITSELF. The three rungs are requested independently (each
    # holds a different pool), so this waits until all three of a subject's partitions have
    # landed — which is what `eager()`'s no-upstream-partition-missing clause means here, and
    # a skipped rung still materializes its partition, so a skip does not block it.
    automation_condition=dg.AutomationCondition.eager(),
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
        # EVERY SUBJECT IN THE RUN, not just the ones a rung still needed: this stage reads the
        # evidence already on disk, so a security whose ticker landed last week is exactly the
        # one whose local symbol is being adopted now.
        attributes = sym.attributes_for(conn, list(context.partition_keys))
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
        # WHETHER THE SYMBOL RUNGS ASKED, read off the raw files rather than assumed. Both append
        # a row for every subject they ask about, so an ABSENT entry is "skipped — the evidence was
        # already held", not "asked and got nothing". Recording the second for the first writes an
        # observation nobody made, and `stale_misses` then re-asks it in 30 days. The ticker rung
        # needs no flag: `mapping_entry` is reconstructed from its row, so None already means it.
        identifiers, symbols, probes = sym.plan_symbols(
            sid,
            isin=isin,
            country_iso2=country,
            mapping_entry=mapping_entry,
            yahoo_hits=yahoo_hits,
            venues=venues,
            source="openfigi",
            asked_symbol=bool(local.get(sid)) or bool(yahoo.get(sid)),
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

    written: dict[str, int] = {}
    collapsed = 0
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            if identifier_rows:
                result = upsert(
                    cur,
                    "market.security_identifier",
                    identifier_rows,
                    conflict=["kind_code", "value"],
                    update=None,
                )
                written["identifiers"], collapsed = result.written, collapsed + result.collapsed
            if symbol_rows:
                result = upsert(
                    cur,
                    "market.security_provider_symbol",
                    symbol_rows,
                    conflict=["security_id", "provider_code"],
                )
                written["symbols"], collapsed = result.written, collapsed + result.collapsed
            if probe_rows:
                result = upsert(
                    cur,
                    "market.identifier_probe",
                    probe_rows,
                    conflict=["security_id", "scheme", "provider"],
                    # THE LATEST OBSERVATION WINS — a re-ask REPLACES the previous answer, which is
                    # exactly what turns a 30-day-old miss into a fresh probe.
                    update=["asked_with", "value", "outcome", "observed_at"],
                )
                written["probes"], collapsed = result.written, collapsed + result.collapsed
        conn.commit()

    # WHAT LANDED, NOT WHAT WAS BUILT. These read `len(...)` of the lists, and the first real run
    # reported `symbols: 4, probes: 6` against 2 and 4 rows in the database — because the local
    # pick is merged in beside `plan_symbols`' own row and the upsert collapses the pair on its
    # conflict key. Nothing was wrong; the numbers simply described a different thing from the one
    # a reader checking the run would assume, which is how a counter stops being evidence.
    #
    # `collapsed` is the difference, and it is reported rather than hidden: a non-zero value is a
    # statement about the SOURCE. Two here is the local pick agreeing with itself; a number that
    # grows means this asset is building the same row twice for a reason nobody intended.
    return dg.MaterializeResult(
        metadata={
            "securities": len(context.partition_keys),
            "identifiers": written.get("identifiers", 0),
            "symbols": written.get("symbols", 0),
            "probes": written.get("probes", 0),
            "collapsed": collapsed,
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
