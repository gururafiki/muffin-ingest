"""Resolving a security's symbols from provider evidence. No network here.

THE SAME COMPANY HAS SEVERAL CORRECT NAMES, and which is right depends on who is asking — this is
the whole subject of `providers/base.py`'s header. Two pickers and one positional parse:

* `parse_mapping` — OpenFIGI `/v3/mapping` answers POSITIONALLY: entry j answers job j, and a
  reordering here would attach one company's listing to another's, which is worse than resolving
  nothing.
* `pick_local_symbol` — the yfinance-addressable local line, chosen by the security's own country
  (Samsung returns 49 matches across every venue it lists; picking arbitrarily prices a Korean bank
  off its Frankfurt line — the port of the edge's `pickLocalSymbol`).
* `pick_home_listing` — Yahoo's ISIN search results filtered to the security's home market. Yahoo's
  index is inconsistent — measured, Walmex resolves to a Frankfurt line ONLY (`4GNB.F`) — so the
  first hit is never taken.

`plan_symbols` turns the chosen matches into the observations this ladder exists to record: one
`identifier_probe` per (scHEME, provider) the run asked, plus the security_identifier rows for the
candidates that matched. A hit and a miss are both recorded; a THROTTLE is recorded by NOT
materialising the partition, never as a miss.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from muffin_ingest.facets.openfigi import OpenFigiUnreadable


@dataclass(frozen=True)
class MappingEntry:
    """One security's answer inside a positional mapping response."""

    asked: str
    hits: tuple[dict[str, Any], ...] = ()
    error: str | None = None

    @property
    def matched(self) -> bool:
        return not self.error and bool(self.hits)


def parse_mapping(body: bytes, asked: Sequence[str]) -> list[MappingEntry]:
    """The `/v3/mapping` response → one entry per requested subject, POSITIONAL.

    An error entry (an invalid id, a refused idValue) is a REFUSAL about that subject, never a
    match — recorded as `error`, which is distinct from "no data" (an empty `hits` tuple is the
    provider genuinely having nothing to say).
    """
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise OpenFigiUnreadable(f"openfigi mapping body is not JSON: {exc}") from exc
    if not isinstance(parsed, list):
        raise OpenFigiUnreadable(
            f"openfigi mapping body is a {type(parsed).__name__}, expected the positional array"
        )

    out: list[MappingEntry] = []
    for i, target in enumerate(asked):
        entry = parsed[i] if i < len(parsed) and isinstance(parsed[i], dict) else {}
        error = entry.get("error")
        data = entry.get("data") or []
        hits: list[dict[str, Any]] = []
        for r in data:
            if not isinstance(r, dict) or not r.get("ticker"):
                continue
            hits.append(
                {
                    "ticker": str(r["ticker"]),
                    "exch_code": str(r["exchCode"]) if r.get("exchCode") else None,
                    "name": str(r["name"]) if r.get("name") else None,
                    "composite_figi": str(r["compositeFIGI"]) if r.get("compositeFIGI") else None,
                    "security_type": str(r["securityType2"]) if r.get("securityType2") else None,
                }
            )
        out.append(
            MappingEntry(asked=target, hits=tuple(hits), error=str(error) if error else None)
        )
    return out


def parse_yahoo_search(body: bytes) -> list[dict[str, Any]]:
    """Yahoo's `/v1/finance/search` response → the `quotes` list.

    An empty `quotes` list is the provider genuinely having nothing for the ISIN — a real answer.
    Non-JSON is OUR problem and refuses.
    """
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise OpenFigiUnreadable(f"yahoo search body is not JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise OpenFigiUnreadable(
            f"yahoo search body is a {type(parsed).__name__}, expected an object with `quotes`"
        )
    quotes = parsed.get("quotes") or []
    if not isinstance(quotes, list):
        raise OpenFigiUnreadable("yahoo search body has a non-list `quotes`")
    out: list[dict[str, Any]] = []
    for q in quotes:
        if not isinstance(q, dict):
            continue
        symbol = str(q.get("symbol") or "").strip()
        if not symbol:
            continue
        out.append(
            {
                "symbol": symbol,
                "exchange": str(q["exchange"]) if q.get("exchange") else None,
                "quote_type": str(q["quoteType"]) if q.get("quoteType") else None,
                "name": str(q.get("shortname") or q.get("longname") or "") or None,
            }
        )
    return out


def _suffixes(country_iso2: str | None, venues: dict[str, list[tuple[str, str]]]) -> list[str]:
    if not country_iso2:
        return []
    return [s for _, s in venues.get(country_iso2.upper(), ())]


def pick_local_symbol(
    country_iso2: str | None,
    matches: Sequence[dict[str, Any]],
    venues: dict[str, list[tuple[str, str]]],
) -> dict[str, str | None] | None:
    """The best LOCAL listing OpenFIGI returned for a security in `country_iso2`.

    OpenFIGI returns `exchCode` INCONSISTENTLY — Samsung as `KS`, TSMC as `TT (Taiwan Stock
    Exchange)` — so the match is exact on the code STRIPPED of its label (`split(' (')`), never a
    `startswith`. Among the venues a country knows (best first), the first venue whose listing
    matched wins; the composite FIGI comes back with the match and is the ONLY key that joins a
    security to the venue directory, so it is carried even though the ladder itself needs only the
    symbol.
    """
    upper = (country_iso2 or "").upper()
    for exch_code, suffix in venues.get(upper, ()):
        for m in matches:
            code = str(m.get("exch_code") or "").split(" (")[0].strip().upper()
            if code == exch_code and m.get("ticker"):
                return {
                    "symbol": f"{m['ticker']}{suffix}",
                    "composite_figi": m.get("composite_figi"),
                }
    return None


def pick_home_listing(
    country_iso2: str | None,
    hits: Sequence[dict[str, Any]],
    venues: dict[str, list[tuple[str, str]]],
) -> str | None:
    """The Yahoo hit that belongs to THIS security's home market, or None.

    MATCHED ON THE SUFFIX, never on Yahoo's exchange code (`NYQ`/`MEX`/`FRA`…) — that would be a
    second venue table authored from memory. Requires the home market: Walmex's ISIN resolves to a
    Frankfurt line ONLY, and taking it prices a Mexican retailer off a thin, differently-
    denominated German line. An OFFSHORE incorporation (KY, BM, VG — no local venue) falls back to
    ANY known suffix, because Alibaba's `KY` incorporation would otherwise refuse even the correct
    `9988.HK`.
    """
    suffixes = _suffixes(country_iso2, venues)
    if not suffixes:
        suffixes = sorted({s for vs in venues.values() for _, s in vs if s != ""})
    if not suffixes:
        return None
    for h in hits:
        if h.get("quote_type") and h["quote_type"] != "EQUITY":
            continue  # an ISIN search readily returns ETFs and warrants on the name
        symbol = str(h.get("symbol") or "").strip()
        if not symbol:
            continue
        for suffix in suffixes:
            if suffix == "":
                if "." not in symbol:
                    return symbol  # the US case: `BRK-B` qualifies, `BRK-B.MX` does not
            elif symbol.upper().endswith(suffix.upper()):
                return symbol
    return None


#: The identifier kinds the ladder adopts into `security_identifier`.
TICKER_KIND = "ticker"
#: The provider code the local symbol is addressable under — matches `market.data_source`.
SYMBOL_PROVIDER = "yfinance"


@dataclass(frozen=True)
class SymbolEvidence:
    """What the ladder is allowed to have concluded about ONE security."""

    security_id: str
    scheme: str
    asked_with: str
    outcome: str  # 'hit' or 'miss'
    value: str | None = None
    source: str | None = None


def plan_symbols(
    security_id: str,
    *,
    isin: str,
    country_iso2: str | None,
    mapping_entry: MappingEntry | None,
    yahoo_hits: Sequence[dict[str, Any]],
    venues: dict[str, list[tuple[str, str]]],
    source: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """One security's ladder evidence → the rows to write.

    THE LADDER: OpenFIGI's US ticker first (it is the identifier every non-provider consumer joins
    on), then the local symbol from OpenFIGI's own matches, then Yahoo's home-market hit. Each
    yields an `identifier_probe` observation; the adopted values become `security_identifier` /
    `security_provider_symbol` rows. A security with NO usable evidence still yields its probe rows
    (outcome 'miss') — that is what a materialised, empty run records.
    """

    probes: list[SymbolEvidence] = []
    identifier_rows: list[dict[str, Any]] = []
    symbol_rows: list[dict[str, Any]] = []

    if mapping_entry is not None:
        if mapping_entry.matched:
            probes.append(
                SymbolEvidence(
                    security_id=security_id,
                    scheme=TICKER_KIND,
                    asked_with=isin,
                    outcome="hit",
                    value=mapping_entry.hits[0]["ticker"],
                    source=source,
                )
            )
            identifier_rows.append(
                {
                    "kind_code": TICKER_KIND,
                    "value": str(mapping_entry.hits[0]["ticker"]).upper(),
                    "security_id": security_id,
                    "source_code": source,
                }
            )
        else:
            probes.append(
                SymbolEvidence(
                    security_id=security_id,
                    scheme=TICKER_KIND,
                    asked_with=isin,
                    outcome="miss",
                    source=source,
                )
            )

    local = pick_local_symbol(country_iso2, mapping_entry.hits if mapping_entry else (), venues)
    home = pick_home_listing(country_iso2, yahoo_hits, venues)
    value = (local or {}).get("symbol") or home
    if value:
        probes.append(
            SymbolEvidence(
                security_id=security_id,
                scheme="symbol",
                asked_with=isin,
                outcome="hit",
                value=value,
                source=source,
            )
        )
        symbol_rows.append(
            {
                "security_id": security_id,
                "provider_code": SYMBOL_PROVIDER,
                "symbol": value,
            }
        )
    else:
        probes.append(
            SymbolEvidence(
                security_id=security_id,
                scheme="symbol",
                asked_with=isin,
                outcome="miss",
                source=source,
            )
        )

    probe_rows: list[dict[str, Any]] = [
        {
            "security_id": p.security_id,
            "scheme": p.scheme,
            "provider": p.source,
            "asked_with": p.asked_with,
            "value": p.value,
            "outcome": p.outcome,
            "observed_at": _now(),
        }
        for p in probes
    ]
    return identifier_rows, symbol_rows, probe_rows


def _now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


# --- who the ladder is for ----------------------------------------------------------------------
#
# WHAT A RUNG ASKS ABOUT IS A QUESTION FOR THE DATABASE, NOT FOR THE ASSET, and it is asked per
# rung because the rungs cost different amounts. OpenFIGI's `/v3/mapping` takes ten jobs a request,
# so a subject that does not need it costs a tenth of a request; Yahoo's search is ONE REQUEST PER
# SUBJECT, so asking it about a security whose symbol we already hold is a whole request spent on a
# known answer.
#
# MEASURED 2026-09-20, which is why this exists. The population these rungs shipped with was
# `security.is_tradeable = false` — 23,341 securities, **15,159 of them bonds**, with no clause
# excluding a security that already holds the identifier the rung supplies. `is_tradeable` is not a
# symbol-resolution marker: it is false by default and set true by promotion, so the population was
# "almost everything", ordered by nothing, aimed at a provider whose US equity lookup cannot serve a
# bond at all. Against the same database the honest populations are **5,697** and **1,618**.
#
# ANTI-JOIN OVER THE ENTITY, never a `where … is null` over rows: a security's OWN ISIN row would
# survive a join-and-filter against the identifier table and the backlog would never drain. That
# defect cost this schema months on `pending_industry`.

_EQUITY_WITH_ISIN = """
  from market.security_identifier i
  join market.security s on s.security_id = i.security_id
 where i.kind_code = 'isin'
   and s.security_type_code = 'equity'
"""

SUBJECTS_NEEDING_TICKER = f"""
select distinct i.security_id::text
{_EQUITY_WITH_ISIN}
   and not exists (select 1 from market.security_identifier t
                    where t.security_id = s.security_id and t.kind_code = %s)
"""

SUBJECTS_NEEDING_SYMBOL = f"""
select distinct i.security_id::text
{_EQUITY_WITH_ISIN}
   and not exists (select 1 from market.security_provider_symbol p
                    where p.security_id = s.security_id and p.provider_code = %s)
"""

#: What each rung is for, so a caller names the EVIDENCE rather than repeating a query.
NEEDS_TICKER = "ticker"
NEEDS_SYMBOL = "symbol"


def subjects_needing(conn: Any, evidence: str) -> set[str]:
    """The security_ids a rung supplying `evidence` still has something to say about."""
    if evidence == NEEDS_TICKER:
        sql, params = SUBJECTS_NEEDING_TICKER, (TICKER_KIND,)
    elif evidence == NEEDS_SYMBOL:
        sql, params = SUBJECTS_NEEDING_SYMBOL, (SYMBOL_PROVIDER,)
    else:
        raise ValueError(f"unknown evidence {evidence!r}; expected one of ticker, symbol")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return {row[0] for row in cur.fetchall()}


ATTRIBUTES_FOR = """
select i.security_id::text, min(i.value), min(s.country_iso2)
  from market.security_identifier i
  join market.security s on s.security_id = i.security_id
 where i.kind_code = 'isin' and i.security_id::text = any(%s)
 group by i.security_id
"""


def attributes_for(conn: Any, security_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    """security_id → {isin, country_iso2}, for the subjects named.

    NARROWED TO THE RUN rather than reading the whole identifier table and filtering in Python —
    27,652 securities carry an ISIN and a run covers 200.

    `min(i.value)` because `security_identifier` is keyed `(kind_code, value)`, so a security MAY
    carry two ISINs; picking by row order made which one the ladder asked with depend on the
    planner. Deterministic is not the same as right, but it is the difference between a stable
    answer and one that changes under an index.
    """
    if not security_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(ATTRIBUTES_FOR, (list(security_ids),))
        return {
            sid: {"isin": isin, "country_iso2": country} for sid, isin, country in cur.fetchall()
        }


STALE_MISSES = """
select distinct security_id::text from market.identifier_probe
 where outcome = 'miss' and observed_at < now() - make_interval(days => %s)
"""


def stale_misses(conn: Any, *, older_than_days: int) -> set[str]:
    """Securities whose recorded answer was "the provider has nothing" and is old enough to re-ask.

    NOT NEVER, AND NOT SOON. A security can gain a US listing, and a pair Yahoo does not index
    today may be indexed next quarter — but re-asking a known absence on every run is how a
    rate-limited provider gets spent on answers already written down.
    """
    with conn.cursor() as cur:
        cur.execute(STALE_MISSES, (older_than_days,))
        return {row[0] for row in cur.fetchall()}
