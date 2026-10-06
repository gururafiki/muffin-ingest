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

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from muffin_ingest.facets.openfigi import OpenFigiUnreadable
from muffin_ingest.providers import yahoo_search


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
                    "share_class_figi": (
                        str(r["shareClassFIGI"]) if r.get("shareClassFIGI") else None
                    ),
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
#: An equity's identity: the OpenFIGI share class, the one key that is the same on every venue a
#: class trades on. `security_identifier`'s `(kind_code, value)` key is what makes it "one security
#: per share class" — decided 2026-09-26 (umbrella docs/specs/2026-09-26-finishing-the-universe-
#: family.md).
SHARE_CLASS_KIND = "share_class_figi"
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
    asked_local: bool,
    asked_yahoo: bool,
    dead_symbol: str | None,
    local_hits: Sequence[dict[str, Any]] = (),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """One security's ladder evidence → the rows to write.

    THE LADDER: OpenFIGI's US ticker first (it is the identifier every non-provider consumer joins
    on), then the local symbol from OpenFIGI's own matches, then Yahoo's home-market hit. Each
    yields an `identifier_probe` observation; the adopted values become `security_identifier` /
    `security_provider_symbol` rows. A security the rung ASKED about and got nothing for still
    yields its probe row (outcome 'miss') — that is what a materialised, empty run records.

    `asked_local` / `asked_yahoo` ARE NOT OPTIONAL INFORMATION, AND OMITTING THEM WROTE EVIDENCE
    FOR A QUESTION NOBODY ASKED. A rung skips a subject whose evidence is already held — that is the
    whole reason `_subjects` filters by `NEEDS_*`, since asking costs a provider request to be told
    what is written down. This function used to emit a `symbol` probe unconditionally, so those
    deliberately-skipped subjects were recorded as MISSES. Measured live 2026-09-22 on the first
    three subjects ever run: all three needed a ticker and already had a symbol, their local and
    Yahoo raw files were correctly EMPTY, and `identifier_probe` still gained three
    `scheme=symbol, outcome=miss` rows.

    That is this codebase's most-repeated rule — a request that was never made and a request that
    answered nothing are different facts — and here it also costs money: `stale_misses` re-asks a
    30-day-old miss, so a false one sends the lane back to the provider for an answer it holds.

    THE TICKER RUNG NEEDS NO SUCH FLAG, because `mapping_entry` already carries the question: the
    caller reconstructs it from that rung's own raw row, and `_map_in_batches` appends a row for
    every subject it asks about — so `None` IS "not asked" and the branch cannot be reached
    without one. A second flag beside it could never be false where the entry is present, and a
    guard that cannot fire reads as protection without being it.

    The symbol side is different, and that is why the flags are here: the value can come from the
    TICKER rung's own hits naming a local line, so a subject whose symbol rungs were both skipped
    still reaches this branch holding a real value. It is written — it is a finding — but nothing
    observed it. Both flags are REQUIRED: a default is a claim about what was asked, made by
    whoever did not say.

    ONE OBSERVATION PER PROVIDER ASKED, NAMED FOR THAT PROVIDER. The two symbol rungs are two
    providers — OpenFIGI's local rung and Yahoo's search — and `identifier_probe` is keyed
    `(security_id, scheme, provider)`. This used to record ONE symbol probe labelled with `source`
    (`openfigi`) whichever rung supplied the value, so a Yahoo hit would have been stored as
    OpenFIGI's answer, and a Yahoo miss would have overwritten OpenFIGI's own miss under the same
    key. Neither was live only because the Yahoo rung had never run. Now each rung that asked earns
    its own row: an OpenFIGI miss beside a Yahoo hit is two true observations, not one false one.
    The adopted symbol still prefers OpenFIGI's local line, then Yahoo's home-market line.

    `dead_symbol` IS NEVER A CANDIDATE. It is the symbol the price lane last rejected alone, with a
    healthy control in the same attempt (`dead_symbols`), so proposing it again proposes a known
    failure — and because OpenFIGI's pick comes first, it would also stop Yahoo's answer from ever
    being adopted. The observations are untouched: OpenFIGI did name it, and that stays recorded.
    REQUIRED for the same reason as the asked flags: `None` is a claim that nothing is known dead.

    `local_hits` ARE THE LOCAL RUNG'S OWN MATCHES, AND THEY BELONG ON THIS LADDER, NOT BESIDE IT.
    The local line used to be picked here only from the TICKER rung's hits — restricted to
    `exchCode: US`, so they name a local line for a US listing and for nothing else — while the
    caller picked it again from the unfiltered local rung and appended that pick's row and `hit`
    probe beside this function's output. Both probes carry one key, `(security_id, scheme,
    provider)`, the writer keeps the LAST row per key, and this function's `miss` came last. So
    every symbol the local rung resolved was ADOPTED and recorded as a MISS: measured 2026-09-24,
    `identifier_probe` held **0** `symbol/hit` rows beside **759** `symbol/miss` rows for
    securities that did hold a yfinance symbol — National Healthcare Properties among them, `NHP`
    adopted from the local rung and its probe reading `miss`. One ladder, one decision per scheme.
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

    if yahoo_hits and not asked_yahoo:
        # Hits come out of the Yahoo rung's own raw file, which exists only for a subject it asked.
        # Hits with no question is a caller wiring the flags wrongly, and guessing which half is
        # true would record an observation on a coin toss.
        raise ValueError(f"{security_id}: Yahoo hits were passed for a subject Yahoo was not asked")

    local = pick_local_symbol(
        country_iso2, (*local_hits, *(mapping_entry.hits if mapping_entry else ())), venues
    )
    local_value = (local or {}).get("symbol")
    home = pick_home_listing(country_iso2, yahoo_hits, venues)
    for asked, provider, found in (
        (asked_local, source, local_value),
        (asked_yahoo, yahoo_search.PROVIDER, home),
    ):
        if asked:
            probes.append(
                SymbolEvidence(
                    security_id=security_id,
                    scheme="symbol",
                    asked_with=isin,
                    outcome="hit" if found else "miss",
                    value=found,
                    source=provider,
                )
            )

    # A VALUE WITHOUT A QUESTION IS STILL NOT AN OBSERVATION, and it is reachable: the ticker
    # rung's own hits can name a local line while both symbol rungs were skipped. The adopted
    # symbol is written either way — it is a finding — and only the rungs that asked are observed.
    dead = (dead_symbol or "").upper()
    value = next((v for v in (local_value, home) if v and v.upper() != dead), None)
    if value:
        symbol_rows.append(
            {"security_id": security_id, "provider_code": SYMBOL_PROVIDER, "symbol": value}
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


@dataclass(frozen=True)
class ShareClassPlan:
    """What one security's evidence says about its share class."""

    identifiers: list[dict[str, Any]]
    probes: list[dict[str, Any]]
    #: Every share class the evidence named, when it named more than one. Nothing is adopted then.
    conflicting: tuple[str, ...] = ()


def plan_share_class(
    security_id: str,
    *,
    isin: str,
    hits: Sequence[dict[str, Any]],
    source: str,
    asked: bool,
) -> ShareClassPlan:
    """One security's mapping hits → its share class, if they name exactly one.

    AN ISIN NAMES ONE SHARE CLASS, AND THE ANSWER SAYS SO ON EVERY LINE. OpenFIGI's mapping by ISIN
    returns the class's lines on every venue, each carrying the same `shareClassFIGI` — measured
    2026-09-26 over the 1,518 answers stored by the local rung: 1,393 name exactly one, 125 name
    none, and not one names two. Lines WITHOUT one are ordinary (2,340 of them, nearly all common
    stock) and say nothing, so they are skipped rather than counted as disagreement.

    TWO IS A REFUSAL, NEVER A CHOICE. If an answer ever names two classes, taking either attaches a
    company's identity by coin toss, so nothing is adopted and the caller reports it.

    THE VALUE IS A FINDING WHETHER OR NOT THE QUESTION WAS ASKED, and only an asked question is an
    observation — the rule `plan_symbols` keeps for the local line. A class named in an answer to a
    symbol question is written; the probe is recorded only when this rung was asked for the class,
    so a skipped question never reads as the provider having nothing.
    """
    named = sorted({str(h["share_class_figi"]) for h in hits if h.get("share_class_figi")})
    if len(named) > 1:
        return ShareClassPlan(identifiers=[], probes=[], conflicting=tuple(named))

    identifiers = [
        {
            "kind_code": SHARE_CLASS_KIND,
            "value": named[0],
            "security_id": security_id,
            "source_code": source,
        }
        for _ in named[:1]
    ]
    probes = (
        [
            {
                "security_id": security_id,
                "scheme": SHARE_CLASS_KIND,
                "provider": source,
                "asked_with": isin,
                "value": named[0] if named else None,
                "outcome": "hit" if named else "miss",
                "observed_at": _now(),
            }
        ]
        if asked
        else []
    )
    return ShareClassPlan(identifiers=identifiers, probes=probes)


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

#: THE SYMBOL A SECURITY HOLDS, WHEN THE PRICE LANE HAS SINCE REJECTED IT ALONE. The price lane
#: records its verdict under the provider the symbol is for (`yfinance`) with `asked_with` the
#: symbol it asked, and `identifier_probe` keeps one row per (security, scheme, provider), so the
#: row is the latest verdict: a symbol that answers again replaces its own miss with a hit. Matching
#: `asked_with` to the HELD symbol is what makes a corrected symbol leave by construction — the
#: old miss names the old spelling. Measured 2026-10-04: 262 equities, every one holding an ISIN.
_DEAD_HELD_SYMBOL = """
select 1 from market.security_provider_symbol p
  join market.identifier_probe d
    on d.security_id = p.security_id and d.scheme = 'symbol' and d.provider = p.provider_code
   and d.outcome = 'miss' and d.asked_with = p.symbol
 where p.security_id = s.security_id and p.provider_code = %s
"""

#: A SECURITY NEEDS A SYMBOL WHEN IT HAS NONE, OR WHEN THE ONE IT HAS IS DEAD. The second arm is
#: Stage 3b (umbrella docs/specs/2026-09-26-finishing-the-universe-family.md): the price lane's
#: verdict is the evidence that the held symbol is wrong, and the ladder is what can name another.
SUBJECTS_NEEDING_SYMBOL = f"""
select distinct i.security_id::text
{_EQUITY_WITH_ISIN}
   and (not exists (select 1 from market.security_provider_symbol p
                     where p.security_id = s.security_id and p.provider_code = %s)
        or exists ({_DEAD_HELD_SYMBOL}))
"""

#: The ticker's anti-join, parameterised by the identifier kind, so the two cannot drift apart.
#: Every equity holding an ISIN starts here, because no security carried a share class before
#: 2026-09-26 — and the local rung's ISIN lookup answers it in the same request that answers the
#: local line, so widening that rung costs one job per subject, a hundred to a request keyed.
SUBJECTS_NEEDING_SHARE_CLASS = SUBJECTS_NEEDING_TICKER


#: What each rung is for, so a caller names the EVIDENCE rather than repeating a query.
NEEDS_TICKER = "ticker"
NEEDS_SYMBOL = "symbol"
NEEDS_SHARE_CLASS = SHARE_CLASS_KIND


def subjects_needing(conn: Any, evidence: str) -> set[str]:
    """The security_ids a rung supplying `evidence` still has something to say about."""
    params: tuple[str, ...]
    if evidence == NEEDS_TICKER:
        sql, params = SUBJECTS_NEEDING_TICKER, (TICKER_KIND,)
    elif evidence == NEEDS_SYMBOL:
        sql, params = SUBJECTS_NEEDING_SYMBOL, (SYMBOL_PROVIDER, SYMBOL_PROVIDER)
    elif evidence == NEEDS_SHARE_CLASS:
        sql, params = SUBJECTS_NEEDING_SHARE_CLASS, (SHARE_CLASS_KIND,)
    else:
        raise ValueError(
            f"unknown evidence {evidence!r}; expected one of ticker, symbol, share_class_figi"
        )
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


# --- one listing, one security -------------------------------------------------------------------
#
# `market.security_provider_symbol` carries TWO unique keys: the primary key `(security_id,
# provider_code)` — one symbol per security — and `(provider_code, symbol)` — one security per
# listing. The adopting step upserts on the first, and an `on conflict` covers only the key it
# names, so a symbol already held by ANOTHER security raised `UniqueViolation` and took the whole
# batch of up to 200 subjects with it. Measured 2026-09-24 on the first day the ladder ran:
# `(yfinance, WLN.PA) already exists` — Worldline holds two securities, ISINs FR0011981968 and
# FR00140182K6, and OpenFIGI correctly names the same Paris line for both.
#
# That is an IDENTITY fact — one of the two is a stale or duplicate security — and deciding which is
# identity consolidation, not symbol resolution. So this step decides nothing: the existing holder
# keeps the listing, a batch claiming one listing twice adopts neither (an ambiguous match is
# refused, never broken with `min()`), and both are REPORTED. The probe is still written: the
# provider did say this, and that observation is true whoever ends up holding the symbol.

SYMBOL_HOLDERS = """
select provider_code, symbol, security_id::text
  from market.security_provider_symbol
 where provider_code = any(%s) and symbol = any(%s)
"""


def symbol_holders(conn: Any, rows: Sequence[dict[str, Any]]) -> dict[tuple[str, str], str]:
    """(provider_code, symbol) → the security already holding it, for the symbols in `rows`."""
    if not rows:
        return {}
    providers = sorted({str(r["provider_code"]) for r in rows})
    symbols = sorted({str(r["symbol"]) for r in rows})
    with conn.cursor() as cur:
        cur.execute(SYMBOL_HOLDERS, (providers, symbols))
        return {(provider, symbol): sid for provider, symbol, sid in cur.fetchall()}


@dataclass(frozen=True)
class SymbolAdoption:
    """Which symbol rows may be written, and why the others may not."""

    kept: list[dict[str, Any]]
    #: (security_id, symbol, the security already holding it)
    held_elsewhere: list[tuple[str, str, str]]
    #: symbol → the securities in this batch that all claimed it
    ambiguous: dict[str, list[str]]


def adoptable_symbols(
    rows: Sequence[dict[str, Any]], holders: dict[tuple[str, str], str]
) -> SymbolAdoption:
    """Withhold every symbol row that would give one listing to two securities."""
    claimants: dict[tuple[str, str], set[str]] = {}
    for row in rows:
        claimants.setdefault((row["provider_code"], row["symbol"]), set()).add(row["security_id"])
    ambiguous = {key: sorted(sids) for key, sids in claimants.items() if len(sids) > 1}

    kept: list[dict[str, Any]] = []
    held: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (row["provider_code"], row["symbol"])
        if key in ambiguous:
            continue
        holder = holders.get(key)
        if holder is not None and holder != row["security_id"]:
            held.add((row["security_id"], row["symbol"], holder))
            continue
        kept.append(row)
    return SymbolAdoption(
        kept=kept,
        held_elsewhere=sorted(held),
        ambiguous={symbol: sids for (_, symbol), sids in sorted(ambiguous.items())},
    )


# --- one share class, one security ------------------------------------------------------------
#
# THE SAME RULE AS ONE LISTING, ONE SECURITY, ONE LEVEL UP. `security_identifier` is keyed
# `(kind_code, value)`, so a share class already held by another security is silently not written
# by a DO NOTHING upsert — correct, and invisible. Two securities naming one class is an IDENTITY
# fact (Worldline holds two ISINs, FR0011981968 and FR00140182K6, for one company), and deciding
# which survives is consolidation, not symbology. So this decides nothing either: the holder keeps
# the class, a batch naming one class for two securities adopts neither, and both are reported.

IDENTIFIER_HOLDERS = """
select value, security_id::text
  from market.security_identifier
 where kind_code = %s and value = any(%s)
"""


def identifier_holders(conn: Any, kind: str, rows: Sequence[dict[str, Any]]) -> dict[str, str]:
    """value → the security already holding it, for the `kind` values in `rows`."""
    values = sorted({str(r["value"]) for r in rows if r["kind_code"] == kind})
    if not values:
        return {}
    with conn.cursor() as cur:
        cur.execute(IDENTIFIER_HOLDERS, (kind, values))
        return {value: sid for value, sid in cur.fetchall()}


@dataclass(frozen=True)
class IdentifierAdoption:
    """Which identifier rows may be written, and why the others may not."""

    kept: list[dict[str, Any]]
    #: (security_id, value, the security already holding it)
    held_elsewhere: list[tuple[str, str, str]]
    #: value → the securities in this batch that all claimed it
    ambiguous: dict[str, list[str]]


def adoptable_identifiers(
    rows: Sequence[dict[str, Any]], holders: dict[str, str]
) -> IdentifierAdoption:
    """Withhold every identifier row that would give one value to two securities."""
    claimants: dict[str, set[str]] = {}
    for row in rows:
        claimants.setdefault(str(row["value"]), set()).add(str(row["security_id"]))
    ambiguous = {value: sorted(sids) for value, sids in claimants.items() if len(sids) > 1}

    kept: list[dict[str, Any]] = []
    held: set[tuple[str, str, str]] = set()
    for row in rows:
        value, sid = str(row["value"]), str(row["security_id"])
        if value in ambiguous:
            continue
        holder = holders.get(value)
        if holder is not None and holder != sid:
            held.add((sid, value, holder))
            continue
        kept.append(row)
    return IdentifierAdoption(
        kept=kept, held_elsewhere=sorted(held), ambiguous=dict(sorted(ambiguous.items()))
    )


# --- a live symbol is never replaced; a dead one may be -------------------------------------------
#
# ADOPTION FILLS A GAP; IT DOES NOT OVERRULE A SYMBOL THAT WORKS. A yfinance symbol this ladder did
# not write may have been verified against the provider — the edge's `security-symbol-repair`
# adopted `BRK-B`, `ESSITY-B.ST` and `0006.HK` only after the provider answered for them — while the
# ladder's local pick is OpenFIGI's spelling plus a suffix, which is exactly the `BRK/B` and
# `ESSITYB.ST` shape that repair existed to undo. The insert is DO NOTHING on `(security_id,
# provider_code)`, so it can only ever fill a gap; a replacement is a separate, guarded UPDATE.
#
# A DEAD SYMBOL IS THE EXCEPTION, AND THE PRICE LANE IS THE EVIDENCE. A held symbol the provider
# rejected ALONE, with a control proving it healthy in the same attempt (`prices.symbol_probes`),
# is not a verified symbol any more, so replacing it can break nothing that works. Replacing a LIVE
# one stays refused and counted. The write re-checks the death in SQL, in the same statement, so a
# symbol that answered between the read and the write is not replaced.
#
# A REPLACEMENT IS NOT VERIFIED HERE. If it is dead too, the price lane finds out the same way, and
# the ladder is asked once more; the cycle is bounded by `dead_unasked` (one re-ask per death).

CURRENT_SYMBOLS = """
select security_id::text, symbol
  from market.security_provider_symbol
 where provider_code = %s and security_id::text = any(%s)
"""


def current_symbols(conn: Any, security_ids: Sequence[str]) -> dict[str, str]:
    """security_id → its current yfinance symbol, for the securities named."""
    if not security_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(CURRENT_SYMBOLS, (SYMBOL_PROVIDER, sorted(set(security_ids))))
        return {sid: symbol for sid, symbol in cur.fetchall()}


DEAD_SYMBOLS = """
select security_id::text, asked_with
  from market.identifier_probe
 where scheme = 'symbol' and provider = %s and outcome = 'miss'
   and security_id::text = any(%s)
"""


def dead_symbols(conn: Any, security_ids: Sequence[str]) -> dict[str, str]:
    """security_id → the symbol the price lane last rejected alone, for the securities named.

    One row per security at most: `identifier_probe` is keyed (security, scheme, provider), and the
    price lane's latest verdict replaces its earlier one, so a symbol that answers again leaves.
    """
    if not security_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(DEAD_SYMBOLS, (SYMBOL_PROVIDER, sorted(set(security_ids))))
        return {sid: symbol for sid, symbol in cur.fetchall()}


@dataclass(frozen=True)
class SymbolRepair:
    """Which symbol rows fill a gap, which replace a dead symbol, and which were refused."""

    #: No held symbol, or the held one restated.
    kept: list[dict[str, Any]]
    #: A different symbol for a security whose held one the provider rejected alone.
    repairs: list[dict[str, Any]]
    #: (security_id, held, proposed) — the held symbol is not known dead, so it stays.
    refused: list[tuple[str, str, str]]


def repairable_symbols(
    rows: Sequence[dict[str, Any]], current: dict[str, str], dead: dict[str, str]
) -> SymbolRepair:
    """Split symbol rows by what writing them would do to the symbol each security holds."""
    kept: list[dict[str, Any]] = []
    repairs: list[dict[str, Any]] = []
    refused: list[tuple[str, str, str]] = []
    for row in rows:
        sid, proposed = str(row["security_id"]), str(row["symbol"])
        held = current.get(sid)
        if held is None or held == proposed:
            kept.append(row)
        elif dead.get(sid) == held:
            repairs.append(row)
        else:
            refused.append((sid, held, proposed))
    return SymbolRepair(kept=kept, repairs=repairs, refused=refused)


#: THE GUARD IS THE STATEMENT'S, NOT THE CALLER'S. `p.symbol` in the WHERE is the row BEFORE the
#: update, so the death is checked against the symbol actually being replaced, at the moment it is
#: replaced. The trigger on `UPDATE OF symbol` then clears every symbol-keyed negative cache.
REPAIR_DEAD_SYMBOLS = """
update market.security_provider_symbol p
   set symbol = r.symbol
  from unnest(%s::uuid[], %s::text[]) as r(security_id, symbol)
 where p.security_id = r.security_id
   and p.provider_code = %s
   and exists (select 1 from market.identifier_probe d
                where d.security_id = p.security_id and d.scheme = 'symbol'
                  and d.provider = p.provider_code and d.outcome = 'miss'
                  and d.asked_with = p.symbol)
returning p.security_id::text
"""


def repair_dead_symbols(cur: Any, rows: Sequence[dict[str, Any]]) -> list[str]:
    """Replace each security's dead yfinance symbol with the row's; the ids actually replaced."""
    if not rows:
        return []
    ids = [str(r["security_id"]) for r in rows]
    symbols = [str(r["symbol"]) for r in rows]
    cur.execute(REPAIR_DEAD_SYMBOLS, (ids, symbols, SYMBOL_PROVIDER))
    return [row[0] for row in cur.fetchall()]


STALE_MISSES = """
select distinct p.security_id::text from market.identifier_probe p
 where p.outcome = 'miss' and p.observed_at < now() - make_interval(days => %s)
   and (   (p.scheme = %s and not exists (select 1 from market.security_identifier t
                                          where t.security_id = p.security_id and t.kind_code = %s))
        or (p.scheme = 'symbol' and not exists (select 1 from market.security_provider_symbol s
                                          where s.security_id = p.security_id
                                            and s.provider_code = %s))
        or (p.scheme = %s and not exists (select 1 from market.security_identifier c
                                          where c.security_id = p.security_id
                                            and c.kind_code = %s)))
"""


def stale_misses(conn: Any, *, older_than_days: int) -> set[str]:
    """Securities whose recorded answer was "the provider has nothing" and is old enough to re-ask.

    NOT NEVER, AND NOT SOON. A security can gain a US listing, and a pair Yahoo does not index
    today may be indexed next quarter — but re-asking a known absence on every run is how a
    rate-limited provider gets spent on answers already written down.

    ONLY WHILE THE EVIDENCE IS STILL MISSING, or the re-ask never ends. A rung asks only about a
    subject that still needs what it supplies (`subjects_needing`), so a subject whose miss is old
    but whose symbol has since arrived — from any source: another rung, a repair, the old
    handlers — is re-requested, SKIPPED by the rung, earns no new observation, and is therefore
    still a stale miss on the next tick. Every day, for ever: a backlog defined as "wants X and
    does not have X" with no way to leave, the shape this schema has rediscovered four times. The
    anti-join is per scheme because a subject can be missing its ticker and hold its symbol.
    """
    with conn.cursor() as cur:
        cur.execute(
            STALE_MISSES,
            (
                older_than_days,
                TICKER_KIND,
                TICKER_KIND,
                SYMBOL_PROVIDER,
                SHARE_CLASS_KIND,
                SHARE_CLASS_KIND,
            ),
        )
        return {row[0] for row in cur.fetchall()}


def reask_day(security_id: str, *, cycle_days: int) -> int:
    """The day of a `cycle_days`-day cycle on which this subject may be re-asked.

    A HASH OF THE ID, NEVER PYTHON'S `hash()`. `hash()` of a string is salted per process
    (PYTHONHASHSEED), and this runs in the code-location server, which every roll replaces — so a
    subject would land on a different day after each roll, and one whose days kept moving ahead of
    the calendar could wait far longer than a cycle. SHA-256 gives the same day in every process,
    on every machine and in every Python version, which is what lets a test say which day it is.
    """
    digest = hashlib.sha256(security_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % cycle_days


def due_on(subjects: Iterable[str], day: date, *, cycle_days: int) -> set[str]:
    """The subjects whose re-ask day is `day`: each on one day of every `cycle_days`.

    A BULK EVENT MAKES A WAVE 30 DAYS LATER, AND A WAVE IS ONE RUN PER SUBJECT. Every miss recorded
    by one drain turns stale on the same morning — measured 2026-10-06, 5,320 of them on
    2026-10-26 — and Dagster puts only CONTIGUOUS partition keys in one run, while a stale set is
    scattered through the grid: the 255 subjects of the first re-ask became 254 runs and held the
    `sql` pool for 2h13m. Spread by subject, a wave of 5,320 is ~177 a day and the next bulk event
    makes none. Each subject is still re-asked within `older_than_days + cycle_days - 1` days of
    its miss.

    The day is the calendar date's ordinal, never the day of the month: a 31-day month would give
    two days the same position and skip another.
    """
    today = day.toordinal() % cycle_days
    return {s for s in subjects if reask_day(s, cycle_days=cycle_days) == today}


#: THE RUNG THAT IS RE-ASKED ABOUT A DEAD SYMBOL, recorded under this provider by `plan_symbols`.
#: OpenFIGI's mapping is the automated symbol rung; Yahoo's is an operator's backfill.
REASKED_BY = "openfigi"

DEAD_UNASKED = """
select distinct p.security_id::text
  from market.security_provider_symbol p
  join market.identifier_probe d
    on d.security_id = p.security_id and d.scheme = 'symbol' and d.provider = p.provider_code
   and d.outcome = 'miss' and d.asked_with = p.symbol
  join market.security s on s.security_id = p.security_id
 where p.provider_code = %s
   and s.security_type_code = 'equity'
   and exists (select 1 from market.security_identifier i
                where i.security_id = p.security_id and i.kind_code = 'isin')
   and not exists (select 1 from market.identifier_probe q
                    where q.security_id = p.security_id and q.scheme = 'symbol'
                      and q.provider = %s and q.observed_at > d.observed_at)
"""


def dead_unasked(conn: Any) -> set[str]:
    """Securities whose held symbol the price lane rejected alone after OpenFIGI last answered.

    ONCE PER DEATH, OR IT NEVER ENDS. A dead symbol OpenFIGI names again (Taiwan's TPEx lines are
    filed under `TT`, the TWSE code, so the pick is `.TW` however often it is asked) earns a fresh
    OpenFIGI observation and leaves; it comes back only when the price lane rejects the symbol
    again, after its own 30-day window. Measured 2026-10-04: 262 such securities, about three keyed
    requests a month.

    The population is the one `SUBJECTS_NEEDING_SYMBOL` adds for a dead symbol (an equity with an
    ISIN), so a re-asked subject is one the rung will actually ask about.
    """
    with conn.cursor() as cur:
        cur.execute(DEAD_UNASKED, (SYMBOL_PROVIDER, REASKED_BY))
        return {row[0] for row in cur.fetchall()}
