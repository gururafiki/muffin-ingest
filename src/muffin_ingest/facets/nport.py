"""SEC N-PORT filings → fund holdings and newly discovered securities. No network here.

The whole parse is IDENTITY. A filing names a position by ISIN/CUSIP/LEI and almost never by
ticker, so every holding has to resolve to a `security` before it can be stored. Resolution is
ISIN → CUSIP → FIGI/`<other>`; LEI is deliberately excluded from that ladder because it names the
ISSUER (GOOG and GOOGL share one) and populates `market.issuer` instead.

A REGEX PARSER, NOT A DOM, ported deliberately from the edge function that ran this for a year:
the document is machine-generated and regular, and building a DOM for ~900 KB inside a worker is
the kind of thing that made the price refresh die without answering.

THIS IS STAGE 2. Every rule below runs against bytes already on disk, so a correction costs a
re-parse and never a re-fetch — which is why `parse_holdings` takes `bytes`, not a URL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

#: N-PORT `units` for a share position; anything else is a bond, contract or money-market line.
SHARE_UNITS = frozenset({"NS"})

#: N-PORT `assetCat` → our security type. THIS IS READING THE FILING, NOT GUESSING: `assetCat` is
#: a REQUIRED field with a closed vocabulary, so it is the filer's own statement of what the
#: instrument is. Without it every bond, future, repo and money-market position collapsed into
#: `other` and 15,205 instruments were indistinguishable.
ASSET_CATEGORY_TYPE = {
    "EC": "equity",  # equity-common
    "EP": "equity",  # equity-preferred — still an ownership claim
    "DBT": "bond",
    "ABS-MBS": "bond",  # mortgage-backed: debt
    "ABS-O": "bond",  # other asset-backed: debt
    "LON": "bond",  # a loan is debt
    "DE": "derivative",
    "DFE": "derivative",
    "STIV": "cash",  # short-term investment vehicle (money-market)
    "RA": "cash",  # repurchase agreement — a cash equivalent
}

#: SEC's PLACEHOLDER identifiers — `<cusip>000000000</cusip>` means "no CUSIP", not a value.
#: Measured across five funds: 221 of 308 holdings (72%) carry it. Accepting it makes
#: `security_identifier`'s `(kind, value)` key collapse every holder of the placeholder into ONE
#: security. The sentinels are the same class of junk under a spellable name.
_SENTINELS = frozenset({"N/A", "NONE", "UNKNOWN"})

_IDENTIFIER_FORMAT = {
    "isin": re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$"),
    "cusip": re.compile(r"^[A-Z0-9]{9}$"),
    "figi": re.compile(r"^BBG[A-Z0-9]{9}$"),
    "lei": re.compile(r"^[A-Z0-9]{20}$"),
}


class NportUnreadable(RuntimeError):
    """The body is not the N-PORT shape this parser knows.

    DELIBERATELY FATAL. A silently-empty parse looks exactly like a filing that has no holdings,
    and the two demand opposite responses.
    """


def is_usable_identifier(kind: str, raw: str | None) -> bool:
    """THE PLACEHOLDER GUARD, and the single most important rule in this family.

    Four rules, in order: falsy; a spellable sentinel ("N/A", "NONE", "UNKNOWN"); all-same-
    character (which covers `000000000` and `ZZZZZZZZZ`); and a format regex where the scheme has
    one. A holding whose only identifier fails all four is UNIDENTIFIABLE, and it is skipped
    rather than invented — an unidentifiable position cannot be joined to anything later, and a
    synthetic key would quietly become a duplicate of a real security.

    IT IS A GUARD, NOT A TIDINESS: measured over five funds, treating the placeholder as real
    collapsed Accenture, Seagate, TE Connectivity and NXP into whichever was seen first.
    Nothing errorred; the fund simply reported the wrong companies, and it showed up only as
    XLK's weights summing to 97.1% instead of 100%.
    """
    if not raw:
        return False
    v = raw.strip().upper()
    if not v or v in _SENTINELS:
        return False
    if re.fullmatch(r"(.)\1*", v):
        return False
    fmt = _IDENTIFIER_FORMAT.get(kind)
    return bool(fmt.fullmatch(v)) if fmt else True


def identifiers_of(holding: dict[str, Any]) -> list[tuple[str, str]]:
    """The identifiers that identify THE SECURITY, most authoritative first.

    LEI IS DELIBERATELY ABSENT for the reason in the module docstring. A `Bloomberg Identifier`
    is a FIGI/BBGID and is the only key some derivative rows carry; anything else under
    `<other>` is `other`.
    """
    out: list[tuple[str, str]] = []
    isin = holding.get("isin")
    if is_usable_identifier("isin", isin):
        out.append(("isin", isin.strip().upper()))  # type: ignore[union-attr]
    cusip = holding.get("cusip")
    if is_usable_identifier("cusip", cusip):
        out.append(("cusip", cusip.strip().upper()))  # type: ignore[union-attr]
    for desc, value in holding.get("other") or []:
        kind = "figi" if re.search(r"bloomberg", desc, re.IGNORECASE) else "other"
        if is_usable_identifier(kind, value):
            out.append((kind, value.strip().upper()))
    return out


def _tag(block: str, name: str) -> str | None:
    """One element's TEXT. Note `N/A` collapses to None the way the edge did — `N/A` means the
    filer did not state it, and a `N/A` string is the sort of value that later fails a numeric
    cast or joins as a real thing."""
    m = re.search(rf"<{name}>([^<]*)</{name}>", block)
    v = m.group(1).strip() if m else ""
    return v if v and v != "N/A" else None


def _num(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        n = float(value)
    except ValueError:
        return None
    return n if _isfinite(n) else None


def _isfinite(n: float) -> bool:
    import math

    return math.isfinite(n)


def _debt_terms(block: str) -> dict[str, Any]:
    """The debt terms of one holding, SCOPED TO ITS `<debtSec>` BLOCK.

    The scoping is not decoration: a derivative's `<fwdDeriv>`/`<optionSwaption>` blocks carry
    their own maturities, so reading `maturityDt` from the whole holding would attribute a swap's
    expiry to the security. Measured on AGG: 8,867 `maturityDt` inside `<debtSec>` and 0 outside.

    TWO VALUES THAT LOOK MISSING AND ARE NOT:
      * `couponKind` is literally the string `"None"` on 5 of AGG's holdings — a REPORTED KIND,
        not an absence.
      * `annualizedRt` of **0.0 is a real zero-coupon bond**. Treating 0 as absent would drop
        every one of them — the OPPOSITE of the dividend case, where a 0 means "no dividend on
        this bar". Same-looking value, opposite meaning.
    """
    debt = re.search(r"<debtSec>[\s\S]*?</debtSec>", block)
    if not debt:
        return {}
    d = debt.group(0)
    rate = _num(_tag(d, "annualizedRt"))
    default = _tag(d, "isDefault")
    return {
        "maturity_date": _tag(d, "maturityDt"),
        "coupon_kind": _tag(d, "couponKind"),
        # `is not None`, NOT a truthiness test — 0.0 is a zero-coupon bond, not a missing rate.
        "coupon_rate": rate if rate is not None else None,
        "in_default": True if default == "Y" else False if default == "N" else None,
    }


def parse_holdings(body: bytes) -> list[dict[str, Any]]:
    """The filings' `<invstOrSec>` blocks → one dict per holding.

    NOTE IDENTIFIERS ARE ATTRIBUTES, not element text: `<identifiers><isin value="…"/></…>`.
    Reading them as text yields empty strings and silently loses every ISIN. `other` entries are
    `(desc, value)` pairs, because the single `desc` that means FIGI travels with the value it
    names. An empty parse is fatal by construction (see `NportUnreadable`).
    """
    text = body.decode("utf-8", errors="replace")
    blocks = re.findall(r"<invstOrSec>[\s\S]*?</invstOrSec>", text)
    out: list[dict[str, Any]] = []
    for b in blocks:
        name = _tag(b, "name")
        if not name:
            continue
        isin = re.search(r'<isin[^>]*value="([^"]+)"', b)
        other = re.findall(r'<other[^>]*otherDesc="([^"]*)"[^>]*value="([^"]+)"', b)
        row: dict[str, Any] = {
            "name": name,
            "title": _tag(b, "title"),
            "lei": _tag(b, "lei"),
            "cusip": _tag(b, "cusip"),
            "isin": isin.group(1) if isin else None,
            "other": [(d, v) for d, v in other] or None,
            "balance": _num(_tag(b, "balance")),
            "units": _tag(b, "units"),
            "currency": _tag(b, "curCd"),
            "value_usd": _num(_tag(b, "valUSD")),
            "weight": _num(_tag(b, "pctVal")),
            "asset_category": _tag(b, "assetCat"),
            "issuer_category": _tag(b, "issuerCat"),
            "country": _tag(b, "invCountry"),
        }
        row.update(_debt_terms(b))
        out.append(row)
    if not out:
        raise NportUnreadable(
            "the filing parsed to zero holdings — a real N-PORT has dozens of them, so an empty "
            "result is a shape change rather than a fund that holds nothing"
        )
    return out


def series_id(body: bytes) -> str | None:
    """The `<seriesId>` in the filing header — read by the discovery probe, which matches it
    against the FTS result rather than opening every trust's submissions."""
    m = re.search(r"<seriesId>([^<]+)</seriesId>", body.decode("utf-8", errors="replace"))
    return m.group(1).strip() if m else None


def fund_directory(body: bytes) -> dict[str, tuple[str, str]]:
    """`company_tickers_mf.json` → `{symbol: (cik, series_id)}`.

    THE SHAPE IS `{"fields": [...], "data": [[...], ...]}` — an OBJECT with parallel columns, not
    an array of objects. The CIK is stored padded because the filing index path does not care but
    the identity tables join on the integer form elsewhere. A symbol maps to one share class of
    one series; first entry wins.
    """
    import json

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise NportUnreadable(f"company_tickers_mf.json is not JSON: {exc}") from exc
    fields = parsed.get("fields") or []
    try:
        i_cik, i_series, i_symbol = (
            fields.index("cik"),
            fields.index("seriesId"),
            fields.index("symbol"),
        )
    except ValueError as exc:
        raise NportUnreadable(f"fund directory header changed: {fields!r}") from exc

    out: dict[str, tuple[str, str]] = {}
    for row in parsed.get("data") or []:
        if len(row) <= max(i_cik, i_series, i_symbol):
            continue
        symbol = str(row[i_symbol] or "").strip().upper()
        if not symbol or symbol in out:
            continue
        out[symbol] = (str(row[i_cik]), str(row[i_series]))
    if not out:
        raise NportUnreadable(
            "fund directory parsed to zero funds — the published file lists ~28,000"
        )
    return out


@dataclass
class FilingRef:
    """One NPORT-P filing the trust filed: enough to fetch its primary document."""

    accession: str  # no dashes, as the Archives path wants
    cik: str  # THE FILER, served under the DIRECTORY cik — the FTS accession embeds a DIFFERENT one
    report_date: str
    filing_date: str

    @property
    def key(self) -> str:
        """The document's unique identity for the partition grid. The accession alone cannot
        name the Archives path — measured, the doc 404s under the CIK the accession embeds and
        is served under the fielder's — so the key carries both."""
        return f"{self.cik}:{self.accession}"


def filing_refs(body: bytes, cik: str) -> list[FilingRef]:
    """EDGAR full-text search-index JSON → the filings for one series, newest report first.

    THE RESULT IS RELEVANCE-ORDERED BY THE SEARCH ENGINE, NOT BY DATE — the explicit sort is what
    makes "the newest" well-defined. Among filings for the same period, the latest FILING date
    wins, which is how an NPORT-P/A amendment supersedes the original it corrects.
    """
    import json

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise NportUnreadable(f"nport search-index is not JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not isinstance(parsed.get("hits"), dict):
        shape = sorted(parsed.keys()) if isinstance(parsed, dict) else type(parsed).__name__
        raise NportUnreadable(f"nport search-index has no `hits` object — shape is {shape}")

    raw = (parsed.get("hits") or {}).get("hits") or []
    out: list[FilingRef] = []
    for h in raw:
        src = h.get("_source") or {}
        accession = str(h.get("_id") or "").split(":")[0].replace("-", "")
        report_date = str(src.get("period_ending") or "")
        if not accession or not report_date:
            continue
        out.append(
            FilingRef(
                accession=accession,
                cik=cik,
                report_date=report_date,
                filing_date=str(src.get("file_date") or ""),
            )
        )
    out.sort(key=lambda f: (f.report_date, f.filing_date))
    # A HITLESS SEARCH IS AN ABSENCE, NOT A SHAPE CHANGE — a fund whose last filing predates the
    # search window returns zero hits and that is a real answer. Non-JSON or a response with no
    # `hits` key is OUR problem and raises above.
    return out


# --- discovery planning: holdings → securities, identifiers, issuers, fund_holding rows ----------


def country_of(holding: dict[str, Any], known: set[str]) -> str | None:
    """The ISO-2 codes the `market.countries` table actually knows.

    N-PORT uses `XX` for "country unknown" — a real value that is not a country, and an FK
    violation if stored. Anything unknown becomes NULL: unknown provenance is not worth losing a
    500-holding filing over, and inventing a country would be worse.
    """
    c = holding.get("country")
    return c.upper() if c and known and c.upper() in known else None


def issuer_rows(holdings: list[dict[str, Any]], countries: set[str]) -> list[dict[str, Any]]:
    """One issuer per LEI, name/country from the first holding carrying it.

    `market.issuer.lei` is UNIQUE, so two share classes of one company converge on one issuer
    row instead of being merged into one security — which is where the LEI belongs and the reason
    it is not a security identifier. The issuer_id is STABLE for a given filing (derived from the
    LEI), so a re-run upserts the same row rather than minting a fresh uuid.
    """
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for h in holdings:
        lei = h.get("lei")
        if not is_usable_identifier("lei", lei):
            continue
        lei = lei.strip().upper()  # type: ignore[union-attr]
        if lei in seen:
            continue
        seen.add(lei)
        out.append(
            {
                "issuer_id": _stable_issuer_id(lei),
                "name": h.get("name"),
                "lei": lei,
                "country_iso2": country_of(h, countries),
            }
        )
    return out


def _stable_issuer_id(lei: str) -> str:
    """A deterministic but unguessable issuer_id from the LEI.

    The edge function minted `crypto.randomUUID()` per new issuer, which made a re-run of a
    filing mint a SECOND issuer for the same LEI (the old one left orphaned). Deriving from the
    LEI makes resolution idempotent: the same filing re-parsed maps the same LEI to the same
    issuer_id, and the upsert on `lei` (UNIQUE) converges.
    """
    import hashlib
    import uuid

    return str(uuid.UUID(bytes=hashlib.sha256(lei.encode()).digest()[:16], version=4))


def plan_holdings(
    holdings: list[dict[str, Any]],
    *,
    known_identifiers: dict[str, str],
    countries: set[str],
    source: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str | None]]:
    """Resolve every holding to a security_id, planning the rows that create new ones.

    THE FOUR RETURNS: `security_rows` (new securities), `identifier_rows` (new identifiers),
    `issuer_rows` (one per LEI), and `ids` — the security_id for every holding's index, positional
    so the fund_holding asset can pair rows with ids. A holding already known maps to its existing
    id and adds no row; a holding with NO usable identifier is skipped (id None) rather than
    invented.

    `known_identifiers` is `"kind:value" → security_id`, read from `market.security_identifier`
    — the SAME relation the resolution writes, which is what makes a re-run a no-op: the second
    pass finds everything the first created.

    THE CUSIP PLACEHOLDER IS WHY `(kind, value)` IS NOT A SAFE KEY. The guard above is what keeps
    `000000000` out of `identifiers_of`, so two placeholder-only holdings never collapse here —
    and the mutation that removes the guard is the test that pins this.
    """
    wanted: list[list[tuple[str, str]]] = [identifiers_of(h) for h in holdings]
    known = dict(known_identifiers)
    security_rows: list[dict[str, Any]] = []
    identifier_rows: list[dict[str, Any]] = []
    ids: list[str | None] = []
    issuers = issuer_rows(holdings, countries)
    issuer_by_lei = {r["lei"]: r["issuer_id"] for r in issuers}

    for i, h in enumerate(holdings):
        idents = wanted[i]
        if not idents:
            ids.append(None)
            continue
        hit = next((known[f"{k}:{v}"] for k, v in idents if f"{k}:{v}" in known), None)
        if hit:
            ids.append(hit)
            continue
        security_id = new_security_id()
        ids.append(security_id)
        lei = h.get("lei")
        lei = (
            lei.strip().upper()
            if isinstance(lei, str) and is_usable_identifier("lei", lei)
            else None
        )
        asset_cat = str(h.get("asset_category") or "").upper()
        security_rows.append(
            {
                "security_id": security_id,
                "issuer_id": issuer_by_lei.get(lei) if lei else None,
                "name": h.get("name"),
                # The filing's own asset category first; `units` only as a fallback for a filer
                # that omits it. An unmapped category stays `other` rather than being forced.
                "security_type_code": ASSET_CATEGORY_TYPE.get(asset_cat)
                or ("equity" if (h.get("units") or "").upper() in SHARE_UNITS else "other"),
                "currency_code": h.get("currency"),
                "country_iso2": country_of(h, countries),
                "is_tradeable": False,  # until a ticker is resolved by the symbology rungs
            }
        )
        for kind, value in idents:
            key = f"{kind}:{value}"
            if key in known:
                continue
            known[key] = security_id
            identifier_rows.append(
                {
                    "kind_code": kind,
                    "value": value,
                    "security_id": security_id,
                    "source_code": source,
                }
            )

    return security_rows, identifier_rows, issuers, ids


def new_security_id() -> str:
    import uuid

    return str(uuid.uuid4())


def fund_holding_rows(
    holdings: list[dict[str, Any]],
    ids: list[str | None],
    *,
    fund_id: str,
    report_date: str,
    source: str,
) -> list[dict[str, Any]]:
    """The holdings snapshot for `market.fund_holding`, keyed `(fund_id, security_id, as_of)`.

    A fund can hold the same security in two lots, which the PK cannot store twice. COMBINE them
    rather than keeping the first: a fund holding a security in two lots holds the SUM, so
    dropping one silently understates the position (XLK reported State Street twice, and keeping
    only the first lost 0.07 of its 0.10%). A holding that resolved to nothing is omitted — an
    unidentifiable position has no security_id to key on.
    """
    by_id: dict[str, dict[str, Any]] = {}
    for i, h in enumerate(holdings):
        sid = ids[i]
        if not sid:
            continue
        prev = by_id.get(sid)
        row = {
            "fund_id": fund_id,
            "security_id": sid,
            "as_of": report_date,
            "weight": h.get("weight"),
            "balance": h.get("balance"),
            "market_value": h.get("value_usd"),
            "currency_code": h.get("currency"),
            "asset_category_code": h.get("asset_category"),
            "issuer_category_code": h.get("issuer_category"),
            "source_code": source,
        }
        if prev is None:
            by_id[sid] = row
            continue
        prev["weight"] = _add(prev.get("weight"), row["weight"])
        prev["balance"] = _add(prev.get("balance"), row["balance"])
        prev["market_value"] = _add(prev.get("market_value"), row["market_value"])
    return list(by_id.values())


def _add(a: Any, b: Any) -> Any:
    if a is None and b is None:
        return None
    return (a or 0) + (b or 0)


def filing_date_of(body: bytes) -> date:
    """The filing's own report-period end — THE DATE THAT TRAVELS WITH THE DATA.

    N-PORT's header carries `repPdDate` (the period the filing reports on) and `repPdEnd` (a
    different as-of; EWJ files `repPdDate` 2026-05-31 beside `repPdEnd` 2026-08-31). The former is
    what the EDGAR search index calls `period_ending`, and it is the date the holdings snapshot
    is keyed on. Read from the bytes, so a re-parse of the same file yields the same day without
    any call — and if the document and the search index ever disagree, the document wins because
    it is what we hold.
    """
    m = re.search(
        r"<repPdDate>(\d{4}-\d{2}-\d{2})</repPdDate>", body.decode("utf-8", errors="replace")
    )
    if not m:
        raise NportUnreadable("the filing carries no <repPdDate> — the snapshot needs an as_of")
    return date.fromisoformat(m.group(1))
