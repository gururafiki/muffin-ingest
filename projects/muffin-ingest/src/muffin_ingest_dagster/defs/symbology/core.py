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
from muffin_ingest_dagster.defs.symbology.raw import raw_yahoo_symbol
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
    # EAGER, MINUS THE WAIT ON THE ONE RUNG NOTHING WILL EVER MATERIALISE.
    #
    # `eager()` will not fire while ANY upstream partition is missing, and `raw_yahoo_symbol`
    # deliberately carries no automation condition — it spends the budget the nightly price sweep
    # lives on, so it is an operator's backfill. Plain `eager()` therefore requests ZERO: driven
    # on 1.13.22 with both mapping rungs landed and the Yahoo one absent, `security_symbology` was
    # requested for 0 of 1 subjects. The two automated rungs would have collected OpenFIGI's
    # answers into Parquet for ever and adopted none of them, with every run green — the same
    # shape that kept `security_return` from ever materialising through a whole history load.
    #
    # THE GATE IS KEPT FOR THE TWO AUTOMATED RUNGS and dropped only for the Yahoo one, which is
    # narrower than the `.without(~any_deps_missing())` its sibling in `prices/derived.py` needs:
    # those two DO land on their own, so waiting for them is correct and costs nothing.
    # `any_deps_updated` still watches all three, so an operator's later Yahoo backfill re-fires
    # this step and the new evidence is adopted; `any_deps_in_progress` still covers all three, so
    # a backfill in flight is waited for rather than raced.
    #
    # AND IT IS EVALUATED BY THE RUNGS' SENSOR, `symbology_rungs`, not the default one. Its trigger
    # includes `will_be_requested()`, which only sees what the SAME sensor requests: on the default
    # sensor it saw the rungs' partitions finish one at a time and was requested in fragments —
    # ~420 runs queued for 5,512 subjects on 2026-09-26. With its rungs, it is requested in their
    # tick and runs beside them. An operator's backfill of a rung should include this step for the
    # same reason (`platform/automation.py`).
    automation_condition=dg.AutomationCondition.eager().replace(
        "any_deps_missing",
        dg.AutomationCondition.any_deps_missing().ignore(
            dg.AssetSelection.assets(raw_yahoo_symbol)
        ),
    ),
    # AND THE INPUT HAS TO TOLERATE WHAT THE CONDITION NOW PERMITS. Firing without the Yahoo rung
    # means loading a partition file that does not exist — measured on 2026-09-22, six runs died
    # with `FileNotFoundError: .../raw_figi_local_symbol/<uuid>.parquet` for exactly that reason.
    # `UPathIOManager` has the built-in: the input arrives as `None`, which `rows_per_partition`
    # reads as "no rows", and `plan_symbols` then records no observation because nothing asked.
    ins={"raw_yahoo_symbol": dg.AssetIn(metadata={"allow_missing_partitions": True})},
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
    conflicting: dict[str, tuple[str, ...]] = {}

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
        #
        # AND SINCE 2026-09-26 THE LOCAL RUNG ASKS FOR TWO THINGS, so a row existing no longer
        # means it asked for the symbol: `asked_for` says which. A file written before then has no
        # such column and was asked for the symbol alone.
        local_asked = _asked_for(local.get(sid), default=sym.NEEDS_SYMBOL)
        identifiers, symbols, probes = sym.plan_symbols(
            sid,
            isin=isin,
            country_iso2=country,
            mapping_entry=mapping_entry,
            yahoo_hits=yahoo_hits,
            venues=venues,
            source="openfigi",
            asked_local=sym.NEEDS_SYMBOL in local_asked,
            asked_yahoo=bool(yahoo.get(sid)),
            # THE UNFILTERED RUNG IS ON THE LADDER, not merged in beside it. Its pick used to be
            # appended here with its own `hit` probe, and `plan_symbols` — which could not see it —
            # appended a `miss` under the same key after it; the writer keeps the last row per key,
            # so every local line was adopted AND recorded as a miss (759 on 2026-09-24).
            local_hits=entry_local.hits if entry_local else (),
        )
        identifier_rows += identifiers
        symbol_rows += symbols
        probe_rows += probes

        # THE SHARE CLASS, FROM EVERY OPENFIGI ANSWER ON DISK FOR THIS SUBJECT. Both rungs look up
        # the same ISIN, so their lines name the same class; reading both lets a subject that
        # only the ticker rung asked about still be keyed.
        share = sym.plan_share_class(
            sid,
            isin=isin,
            hits=[
                *(mapping_entry.hits if mapping_entry else ()),
                *(entry_local.hits if entry_local else ()),
            ],
            source="openfigi",
            asked=sym.NEEDS_SHARE_CLASS in local_asked,
        )
        identifier_rows += share.identifiers
        probe_rows += share.probes
        if share.conflicting:
            conflicting[sid] = share.conflicting

    for sid, named in list(conflicting.items())[:20]:
        context.log.warning(f"{sid}: one ISIN answer named {len(named)} share classes {named}")

    written: dict[str, int] = {}
    collapsed = 0
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            # ONE SHARE CLASS, ONE SECURITY. The key already refuses a second holder, silently; this
            # decides it before the write so the refusal is COUNTED (see `adoptable_identifiers`).
            share_rows = [r for r in identifier_rows if r["kind_code"] == sym.SHARE_CLASS_KIND]
            share_adoption = sym.adoptable_identifiers(
                share_rows, sym.identifier_holders(conn, sym.SHARE_CLASS_KIND, share_rows)
            )
            for sid, value, holder in share_adoption.held_elsewhere[:20]:
                context.log.warning(
                    f"share class {value} resolved for {sid} is already held by {holder}; "
                    "kept the holder"
                )
            for value, sids in list(share_adoption.ambiguous.items())[:20]:
                context.log.warning(
                    f"share class {value} claimed by {len(sids)} securities: {sids}"
                )
            adoptable = [
                r for r in identifier_rows if r["kind_code"] != sym.SHARE_CLASS_KIND
            ] + share_adoption.kept
            if adoptable:
                result = upsert(
                    cur,
                    "market.security_identifier",
                    adoptable,
                    conflict=["kind_code", "value"],
                    update=None,
                )
                written["identifiers"], collapsed = result.written, collapsed + result.collapsed
            # A SYMBOL ALREADY HELD IS NEVER REPLACED HERE — see `unreplaced_symbols`. Filtered
            # first, so a refused pick cannot also be counted as a listing held elsewhere.
            symbol_rows, replacing = sym.unreplaced_symbols(
                symbol_rows,
                sym.current_symbols(conn, [str(r["security_id"]) for r in symbol_rows]),
            )
            for sid, held, proposed in replacing[:20]:
                context.log.info(f"{sid} holds {held}; kept it over the pick {proposed}")
            # ONE LISTING, ONE SECURITY — the second unique key this upsert does not name. Decided
            # before the write, because a violation fails the whole batch (see `adoptable_symbols`).
            adoption = sym.adoptable_symbols(symbol_rows, sym.symbol_holders(conn, symbol_rows))
            for sid, symbol, holder in adoption.held_elsewhere[:20]:
                context.log.warning(
                    f"symbol {symbol} resolved for {sid} is already held by {holder}; "
                    "kept the holder"
                )
            for symbol, sids in list(adoption.ambiguous.items())[:20]:
                context.log.warning(f"symbol {symbol} claimed by {len(sids)} securities: {sids}")
            if adoption.kept:
                result = upsert(
                    cur,
                    "market.security_provider_symbol",
                    adoption.kept,
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
    # statement about the SOURCE. It used to be the local pick colliding with `plan_symbols`' own
    # row — and that collision is exactly how a resolved symbol was recorded as a miss, because the
    # writer keeps the last row per key. With one ladder deciding each scheme once, a non-zero value
    # now means this asset built the same row twice for a reason nobody intended.
    return dg.MaterializeResult(
        metadata={
            "securities": len(context.partition_keys),
            "identifiers": written.get("identifiers", 0),
            "symbols": written.get("symbols", 0),
            "probes": written.get("probes", 0),
            "collapsed": collapsed,
            # A REFUSAL IS COUNTED, NOT HIDDEN. Non-zero is a statement about the UNIVERSE: two
            # securities resolving to one listing is a duplicate for identity consolidation to find.
            "symbols_held_elsewhere": len(adoption.held_elsewhere),
            "symbols_ambiguous": len(adoption.ambiguous),
            # A pick that disagreed with a symbol the security already holds. Not a defect: the
            # held one may have been verified against the provider, and replacing it is Stage 3's
            # rule with its own evidence. A large number says the pick is often worse.
            "symbols_already_held": len(replacing),
            "share_classes_offered": len(share_rows),
            "share_classes_held_elsewhere": len(share_adoption.held_elsewhere),
            "share_classes_ambiguous": len(share_adoption.ambiguous),
            # One ISIN's answer naming two classes. Measured at 0 of 1,518 before shipping; a
            # non-zero value is a new shape of answer, and nothing was adopted for it.
            "share_classes_conflicting": len(conflicting),
        }
    )


def _asked_for(rows: Sequence[dict[str, Any]] | None, *, default: str) -> set[str]:
    """What a rung asked this subject for, read off its raw row. No row, no question."""
    if not rows:
        return set()
    value = rows[0].get("asked_for")
    return set(str(value).split(",")) if value else {default}


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
