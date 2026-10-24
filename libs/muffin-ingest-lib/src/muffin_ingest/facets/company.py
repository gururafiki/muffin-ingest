"""A company's facts from one Yahoo quoteSummary document, for the seven places they are served.

WHAT THIS REPLACES. Nine edge resources asked openbb for a company's profile, profile detail,
industry, fundamentals, share statistics, price targets and officers, and each openbb route called
yfinance's `get_info()`, which is three Yahoo URLs. One quoteSummary document carries every module
those routes read (`providers.yahoo_summary`), and stage 2 reads it into:

  * `security_profile` (description, employees, website, headquarters, beta);
  * `security_officer` (replaced per company, so a departed officer leaves);
  * `security_fundamentals` (the latest snapshot, now with a market cap and ITS currency);
  * `security_share_stats` and `security_estimate` (time series);
  * the `yfinance` rows of `security_taxonomy` (sector and industry, replaced per company);
  * `security.reporting_currency` and `security.provider_country_iso2`.

The quote currency is not here: the price lane reads it from every chart response (`meta.currency`)
and labels each bar with it, and `security_currency` serves it from the bars.

UNITS, MEASURED 2026-10-10/11 ON 18 LISTINGS IN 13 CURRENCIES, including every subunit Yahoo quotes
in (GBp, ZAc, ILA, KWF). Yahoo converts most per-share figures to the QUOTE currency's major unit,
and the defects are where it then compares them with a price still in a subunit:

  * `bookValue` is converted to the quote's major unit: BHP.AX 14.40 against 9.73 USD of equity per
    share (AUD), Lenovo 4.21 against 0.55 USD (HKD), VOD.L 1.88 against 2.19 EUR (GBP). So
    `priceToBook` is RIGHT for BHP.AX (4.23), 0992.HK (7.79) and CSU.TO (11.0), and wrong by exactly
    the subunit factor where the price is in pence, cents, agorot or fils: VOD.L 62.9 for 0.63,
    NPN.JO 144.4 for 1.44, NBK.KW 1,664 for 1.66. See `price_to_book`.
  * `summaryDetail.trailingPE` and `forwardPE` divide the major-unit price by EPS converted the same
    way, and are right everywhere measured (SHEL.L's trailing EPS of 3.38 is 4.53 USD in pounds).
    `defaultKeyStatistics.forwardPE` is NOT: it divides the price as quoted by EPS in the reporting
    currency, so it is 1,001 for VOD.L and 23.5 for BHP.AX where the right figures are 11.8 and
    16.3. It is used only where the quote and reporting currencies are spelled identically.
  * `marketCap` is in the major unit of the quote currency: cap / (price x shares) is exactly 0.01
    for GBp, ZAc and ILA and 0.001 for KWF, and 1.0 for every other quote.
  * `dividendYield` is a FRACTION here (SAP 0.0133) and the column holds a PERCENT, as every app
    reader and `check_fundamental_units.py` expect, so it is multiplied by 100. Margins, ROE and
    revenue growth are fractions on both sides; `debtToEquity` is percent-like on both (SAP 21.966).
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from muffin_ingest.facets import fx, price_chart
from muffin_ingest.facets.prices import Subject
from muffin_ingest.providers import yahoo_chart, yahoo_summary
from muffin_ingest.providers.documents import Document

#: The source these rows are written under: the same as the edge's, so a row it wrote is the row
#: this lane rewrites rather than a second opinion beside it.
SOURCE = "yfinance"
#: Officers carry their own source, as the edge's `security-management` wrote them.
OFFICER_SOURCE = "yfinance-profile"

#: Yahoo's sector labels to muffin's level-1 taxonomy codes, copied from the edge
#: (`resources.ts`, `FINVIZ_SECTOR_IDS`): Yahoo's eleven, plus finviz's `Financial`.
SECTOR_CODES: dict[str, str] = {
    "Financial Services": "financials",
    "Energy": "energy",
    "Utilities": "utilities",
    "Real Estate": "real-estate",
    "Consumer Defensive": "consumer-staples",
    "Financial": "financials",
    "Communication Services": "communication-services",
    "Healthcare": "health-care",
    "Consumer Cyclical": "consumer-discretionary",
    "Industrials": "industrials",
    "Technology": "information-technology",
    "Basic Materials": "materials",
}

#: The taxonomy the sector and industry nodes belong to.
TAXONOMY = "muffin"


def slug(label: str) -> str:
    """The edge's slug, exactly: lower-case, runs of anything else to one hyphen, 60 characters."""
    return re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")[:60]


def industry_code(sector_code: str, label: str) -> str:
    """An industry node's code: one node per (sector, Yahoo industry name), as the edge minted them.

    THE PARENT IS YAHOO'S OWN SECTOR. The edge hung an industry under whatever sector the security
    was already served under, which minted `industrials--software-application` for Uber: an industry
    from one provider under another source's sector. Measured 2026-10-11, the two disagree for 23
    of 11,939 securities with a Yahoo sector, and those now show no industry rather than one their
    served sector does not contain (`security_current` requires the parent to match).
    """
    return f"{sector_code}--{slug(label)}"


# --- stage 1 -------------------------------------------------------------------------------------

#: EVERY column stage 1 adds to a document row. A test holds `raw_rows` to exactly this set.
CONTEXT_COLUMNS = frozenset({"security_id", "asked_symbol", "provider"})


def raw_rows(subject: Subject, document: Document, *, run_id: str) -> list[dict[str, Any]]:
    """One row per response: the body byte for byte, plus who it was asked for."""
    row = document.as_row(run_id)
    row.update(
        {"security_id": subject.security_id, "asked_symbol": subject.symbol, "provider": "yahoo"}
    )
    return [row]


# --- stage 2 -------------------------------------------------------------------------------------


def _num(value: Any) -> float | None:
    """A finite number, or None. Yahoo sends `{"raw": "Infinity"}` for some ratios, and a bool is
    an int to Python."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value) if math.isfinite(value) else None


def _count(value: Any, *, floor: int = 0) -> int | None:
    """A whole number above `floor`, or None: a count or a year, never a measurement."""
    number = _num(value)
    if number is None or number <= floor:
        return None
    return round(number)


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def unwrap(value: Any) -> Any:
    """Yahoo's `{"raw": ..., "fmt": ...}` wrappers to their raw values, all through a structure."""
    if isinstance(value, dict):
        if "raw" in value and ("fmt" in value or "longFmt" in value):
            return value["raw"]
        return {k: unwrap(v) for k, v in value.items()}
    if isinstance(value, list):
        return [unwrap(v) for v in value]
    return value


def quote_unit(spelling: Any) -> tuple[str, float] | None:
    """The quote currency's MAJOR code and how many quoted units make one, or None if unknown.

    `GBp` is (GBP, 100) and `KWF` (KWD, 1000), from `fx.SUBUNITS` through the price lane's explicit
    spelling map; a plain code is itself at 1. A spelling neither knows gives None, and every
    figure that needs the unit is withheld rather than guessed.
    """
    if not isinstance(spelling, str) or not spelling:
        return None
    ours = price_chart.YAHOO_CURRENCY.get(spelling, spelling)
    if ours in fx.SUBUNITS:
        parent, per = fx.SUBUNITS[ours]
        return parent, per
    if len(ours) == 3 and ours.isalpha() and ours.isupper():
        return ours, 1.0
    return None


def price_to_book(stated: float | None, quote: tuple[str, float] | None) -> float | None:
    """Yahoo's P/B with the price brought to the unit its book value is in.

    `bookValue` is in the quote's MAJOR unit, so a subunit price divided by it is out by the
    subunit's factor (VOD.L 62.87 for 0.6287) and every other quote is right as sent (BHP.AX 4.23,
    whose book is AUD although it reports in USD). An unknown quote unit withholds it.
    """
    if stated is None or quote is None:
        return None
    return stated / quote[1]


def _forward_pe(
    summary_detail: Mapping[str, Any], key_stats: Mapping[str, Any], quote: Any, reporting: Any
) -> float | None:
    """`summaryDetail.forwardPE`, else `defaultKeyStatistics.forwardPE` only when the quote and the
    reporting currency are spelled identically (see the module docstring: the second is the price
    as quoted over EPS in the reporting currency, so `GBp` against `GBP` is out by 100)."""
    stated = _num(yahoo_summary.value(dict(summary_detail), "forwardPE"))
    if stated is not None:
        return stated
    if isinstance(quote, str) and quote and quote == reporting:
        return _num(yahoo_summary.value(dict(key_stats), "forwardPE"))
    return None


@dataclass
class CompanyFacts:
    """Everything one document says about one company, shaped for the tables that serve it.

    A field is None (or empty) when the document does not state it; the writers decide what an
    absence means for each table, and none of them erases on one.
    """

    security_id: str
    fetched_at: str
    profile: dict[str, Any] | None = None
    #: None when the document has no profile module, which is not the same as a profile listing no
    #: officers: only the second retracts the officers stored.
    officers: list[dict[str, Any]] | None = None
    fundamentals: dict[str, Any] | None = None
    share_stats: dict[str, Any] | None = None
    estimate: dict[str, Any] | None = None
    sector_label: str | None = None
    industry_label: str | None = None
    country_name: str | None = None
    reporting_currency: str | None = None


@dataclass
class Normalised:
    """Each company's facts, and the counters stage 2 reports."""

    facts: list[CompanyFacts] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)


def _fetched_at(row: Mapping[str, Any]) -> str | None:
    value = row.get("fetched_at")
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value else None


def _fetch_order(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row.get("fetched_at") or ""), str(row.get("url") or ""))


def _day(stamp: str) -> date:
    return date.fromisoformat(stamp[:10])


def normalise(
    rows: Sequence[Mapping[str, Any]], *, known_currencies: Collection[str]
) -> Normalised:
    """Stored summary documents to company facts, the newest document per company. No network.

    A DOCUMENT IN WHICH YAHOO SAYS IT DOES NOT CARRY THE SYMBOL is counted as `absent` and yields
    nothing. It is never a negative cache: a `(symbol, yfinance)` miss would hide the security from
    the price sweep for thirty days and send it to symbol repair, on the strength of one endpoint.
    """
    stats = {
        "documents": 0,
        "legacy_rows": 0,
        "unreadable": 0,
        "absent": 0,
        "empty": 0,
        "companies": 0,
        "profiles": 0,
        "officers": 0,
        "fundamentals": 0,
        "share_stats": 0,
        "estimates": 0,
        "no_quote_unit": 0,
        "cap_withheld": 0,
        "estimate_currency_withheld": 0,
        "reporting_currency_mapped": 0,
        "reporting_currency_withheld": 0,
    }
    newest: dict[str, Mapping[str, Any]] = {}
    for row in sorted(rows, key=_fetch_order):
        sid = row.get("security_id")
        if sid is None:
            continue
        if row.get("body") is None:
            stats["legacy_rows"] += 1
            continue
        newest[str(sid)] = row

    out = Normalised(stats=stats)
    for sid, row in sorted(newest.items()):
        stats["documents"] += 1
        try:
            summary = yahoo_summary.parse(bytes(row["body"]))
        except yahoo_chart.YahooRefused:
            stats["unreadable"] += 1
            continue
        if summary.error is not None:
            stats["absent"] += 1
            continue
        if not summary.modules:
            stats["empty"] += 1
            continue
        fetched = _fetched_at(row) or datetime.now(UTC).isoformat()
        out.facts.append(_facts(sid, fetched, summary, known_currencies, stats))
        stats["companies"] += 1
    return out


def _facts(
    sid: str,
    fetched: str,
    summary: yahoo_summary.Summary,
    known: Collection[str],
    stats: dict[str, int],
) -> CompanyFacts:
    v = yahoo_summary.value
    profile = summary.module("assetProfile")
    price = summary.module("price")
    detail = summary.module("summaryDetail")
    key_stats = summary.module("defaultKeyStatistics")
    financial = summary.module("financialData")
    holders = summary.module("majorHoldersBreakdown")

    quote_spelling = price.get("currency") or detail.get("currency")
    reporting = financial.get("financialCurrency")
    quote = quote_unit(quote_spelling)
    if quote is None and (detail or key_stats):
        stats["no_quote_unit"] += 1

    facts = CompanyFacts(security_id=sid, fetched_at=fetched)
    reporting_code, how = price_chart.reporting_currency(_text(reporting), known)
    if how in ("mapped", "withheld"):
        stats[f"reporting_currency_{how}"] += 1
    if reporting_code is not None:
        facts.reporting_currency = reporting_code

    beta = _num(v(detail, "beta"))
    if beta is None:
        beta = _num(v(key_stats, "beta"))

    if "assetProfile" in summary.modules:
        employees = _count(v(profile, "fullTimeEmployees"))
        facts.profile = {
            "security_id": sid,
            "description": _text(v(profile, "longBusinessSummary")),
            "employees": employees,
            "website": _text(v(profile, "website")),
            "hq_city": _text(v(profile, "city")),
            "hq_state": _text(v(profile, "state")),
            "hq_country": _text(v(profile, "country")),
            "beta": beta,
            "source_code": SOURCE,
            "as_of": fetched,
        }
        stats["profiles"] += 1
        facts.officers = _officers(sid, fetched, profile.get("companyOfficers"))
        stats["officers"] += len(facts.officers)
        facts.sector_label = _text(v(profile, "sector"))
        facts.industry_label = _text(v(profile, "industry"))
        facts.country_name = _text(v(profile, "country"))

    if detail or key_stats or financial:
        cap = _num(v(detail, "marketCap"))
        if cap is None:
            cap = _num(v(price, "marketCap"))
        if cap is not None and cap <= 0:
            cap = None
        cap_currency = quote[0] if quote is not None else None
        if cap is not None and (cap_currency is None or cap_currency not in known):
            # STORED ONLY WITH ITS CURRENCY (a check constraint holds the pair together): a cap
            # whose unit is not known is withheld rather than labelled with a guess.
            stats["cap_withheld"] += 1
            cap = None
        dividend_yield = _num(v(detail, "dividendYield"))
        facts.fundamentals = {
            "security_id": sid,
            "source_code": SOURCE,
            "as_of": fetched,
            "pe_ratio": _num(v(detail, "trailingPE")),
            "forward_pe": _forward_pe(detail, key_stats, quote_spelling, reporting),
            "peg_ratio": _num(v(key_stats, "pegRatio")),
            "price_to_book": price_to_book(_num(v(key_stats, "priceToBook")), quote),
            "profit_margin": _num(v(financial, "profitMargins")),
            "gross_margin": _num(v(financial, "grossMargins")),
            "operating_margin": _num(v(financial, "operatingMargins")),
            "return_on_equity": _num(v(financial, "returnOnEquity")),
            "revenue_growth": _num(v(financial, "revenueGrowth")),
            "debt_to_equity": _num(v(financial, "debtToEquity")),
            "dividend_yield": None if dividend_yield is None else dividend_yield * 100,
            "beta": beta,
            "enterprise_value": _num(v(key_stats, "enterpriseValue")),
            "market_cap": cap,
            "market_cap_currency": cap_currency if cap is not None else None,
            "raw": json.dumps(
                {
                    "summaryDetail": unwrap(detail),
                    "defaultKeyStatistics": unwrap(key_stats),
                    "financialData": unwrap(financial),
                },
                sort_keys=True,
            ),
        }
        stats["fundamentals"] += 1

    # OPENBB'S RULE, KEPT: no share statistics without a share count, which is what made a row.
    outstanding = _num(v(key_stats, "sharesOutstanding"))
    if outstanding is not None:
        short_date = _num(v(key_stats, "dateShortInterest"))
        # THE PROVIDER'S DATE when it states one (US listings, twice a month), so a weekly visit
        # inside one reporting period rewrites one row rather than minting a history; the fetch day
        # otherwise, as the edge did.
        as_of = (
            datetime.fromtimestamp(short_date, UTC).date()
            if short_date is not None and short_date > 0
            else _day(fetched)
        )
        facts.share_stats = {
            "security_id": sid,
            "as_of": as_of.isoformat(),
            "float_shares": _num(v(key_stats, "floatShares")),
            "outstanding_shares": outstanding,
            "short_interest": _num(v(key_stats, "sharesShort")),
            "short_percent_of_float": _num(v(key_stats, "shortPercentOfFloat")),
            "days_to_cover": _num(v(key_stats, "shortRatio")),
            "insider_ownership": _num(v(key_stats, "heldPercentInsiders")),
            "institution_ownership": _num(v(key_stats, "heldPercentInstitutions")),
            "institutions_count": _count(v(holders, "institutionsCount"), floor=-1),
            "source_code": SOURCE,
            "fetched_at": fetched,
        }
        stats["share_stats"] += 1

    # OPENBB'S RULE, KEPT: an estimate needs a stated number of analysts.
    analysts = _num(v(financial, "numberOfAnalystOpinions"))
    if analysts is not None:
        # TARGETS ARE IN THE QUOTE CURRENCY AS QUOTED: VOD.L's mean target is 121.88 against a price
        # of 118.35, both pence. So the label is the quote's own code (`GBX`, not `GBP`) — which the
        # edge got wrong by upper-casing `GBp` — and it is withheld until `market.currency` has it.
        currency = price_chart.quote_currency(quote_spelling, known)
        if currency is None and quote_spelling:
            stats["estimate_currency_withheld"] += 1
        facts.estimate = {
            "security_id": sid,
            "as_of": _day(fetched).isoformat(),
            "target_high": _num(v(financial, "targetHighPrice")),
            "target_low": _num(v(financial, "targetLowPrice")),
            "target_consensus": _num(v(financial, "targetMeanPrice")),
            "target_median": _num(v(financial, "targetMedianPrice")),
            "recommendation": _text(v(financial, "recommendationKey")),
            "recommendation_mean": _num(v(financial, "recommendationMean")),
            "number_of_analysts": round(analysts),
            "currency_code": currency,
            "source_code": SOURCE,
            "fetched_at": fetched,
        }
        stats["estimates"] += 1
    return facts


def _officers(sid: str, fetched: str, listed: Any) -> list[dict[str, Any]]:
    """The officers Yahoo lists, with the edge's rules for figures that are not facts.

    A PAY OF ZERO IS NOT A SALARY: Yahoo sends 0 for officers whose pay it does not carry, and "$0"
    would say something false about a person. Years before 1900 and non-positive ages are the same
    kind of placeholder. Names keep Yahoo's words with its doubled spaces collapsed
    (`Mr. Kevan  Parekh`).
    """
    out: list[dict[str, Any]] = []
    for officer in listed if isinstance(listed, list) else ():
        if not isinstance(officer, dict):
            continue
        name = " ".join(str(officer.get("name") or "").split())
        if not name:
            continue
        pay = _num(yahoo_summary.value(officer, "totalPay"))
        out.append(
            {
                "security_id": sid,
                "name": name,
                "title": _text(officer.get("title")),
                "pay": pay if pay is not None and pay > 0 else None,
                "year_born": _count(officer.get("yearBorn"), floor=1900),
                "age": _count(officer.get("age")),
                "fiscal_year": _count(officer.get("fiscalYear"), floor=1900),
                "source_code": OFFICER_SOURCE,
                "as_of": fetched,
            }
        )
    return out


# --- what a visit asks for -----------------------------------------------------------------------

#: How long a company's summary may go unasked.
SUMMARY_REFRESH_DAYS = 7


def summary_due(stored: Sequence[Mapping[str, Any]], today: date, *, force: bool = False) -> bool:
    """Is this company's summary due? Never fetched, older than a week, or forced."""
    if force:
        return True
    days = [_day(stamp) for stamp in (_fetched_at(row) for row in stored) if stamp]
    return not days or (today - max(days)).days >= SUMMARY_REFRESH_DAYS


# --- stage 2: the writes that need SQL -----------------------------------------------------------

#: Every spelling of a country a provider might use, lower-cased: `countries.name` first, then the
#: aliases, which win. The globe reads the same table, so the two cannot disagree about a country.
COUNTRY_NAMES = """
select lower(trim(name)), iso2, 0 as rank from market.countries
union all
select lower(trim(provider_name)), iso2, 1 as rank from market.country_alias
order by rank
"""


def country_codes(cur: Any) -> dict[str, str]:
    """Lower-cased country name to ISO-2, the aliases laid over the formal names."""
    cur.execute(COUNTRY_NAMES)
    return {str(name): str(iso2) for name, iso2, _ in cur.fetchall()}


def country_rows(
    facts: Sequence[CompanyFacts], iso_by_name: Mapping[str, str]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """`security.provider_country_iso2` for each company whose headquarters we can place, and the
    names we could not, with a count each.

    THE OPERATING COUNTRY, NOT THE FILED ONE: N-PORT reports where a company is incorporated
    (Alibaba in the Cayman Islands), the profile where it is. An unresolved name is NAMED, never
    dropped silently: `country_alias` is filled from what the provider actually says.
    """
    rows: list[dict[str, Any]] = []
    unresolved: dict[str, int] = {}
    for fact in facts:
        name = (fact.country_name or "").strip()
        if not name:
            continue
        iso = iso_by_name.get(name.lower())
        if iso is None:
            unresolved[name] = unresolved.get(name, 0) + 1
            continue
        rows.append({"security_id": fact.security_id, "provider_country_iso2": iso})
    return rows, unresolved


def reporting_currency_rows(facts: Sequence[CompanyFacts]) -> list[dict[str, Any]]:
    """`security.reporting_currency`, where the document states one."""
    return [
        {"security_id": f.security_id, "reporting_currency": f.reporting_currency}
        for f in facts
        if f.reporting_currency
    ]


#: Officers a company's newest profile no longer lists, ONLY THIS LANE'S: the edge never retracted,
#: so a departed chief executive stayed on the page for as long as the company was tracked.
RETRACT_OFFICERS_NOT_LISTED = """
delete from market.security_officer o
 using unnest(%s::uuid[]) as r(security_id)
 where o.security_id = r.security_id
   and o.source_code = %s
   and not exists (
         select 1 from unnest(%s::uuid[], %s::text[]) as k(security_id, name)
          where k.security_id = o.security_id and k.name = o.name)
returning o.security_id::text
"""


def officer_rows(facts: Sequence[CompanyFacts]) -> list[dict[str, Any]]:
    return [officer for f in facts if f.officers for officer in f.officers]


def retract_officers(cur: Any, facts: Sequence[CompanyFacts]) -> int:
    """Delete the officers each listed company's newest profile no longer names.

    A DOCUMENT WITH NO PROFILE MODULE RETRACTS NOTHING (`officers is None`): the absence of a module
    is not a statement that the company has no officers. A profile listing none does retract.
    """
    listed = [f for f in facts if f.officers is not None]
    if not listed:
        return 0
    keep = [(o["security_id"], o["name"]) for f in listed for o in (f.officers or ())]
    cur.execute(
        RETRACT_OFFICERS_NOT_LISTED,
        (
            [f.security_id for f in listed],
            OFFICER_SOURCE,
            [sid for sid, _ in keep],
            [name for _, name in keep],
        ),
    )
    return len(cur.fetchall())


SECTOR_NODES = """
select code, node_id::text from market.taxonomy_node where taxonomy_id = %s and level = 1
"""

#: One node per (sector, Yahoo industry name), created on discovery: the vocabulary is Yahoo's and
#: open. DO NOTHING on conflict, then read back, because a concurrent writer may have won the insert
#: and an id that was never written would fail the foreign key.
INSERT_INDUSTRY_NODES = """
insert into market.taxonomy_node (taxonomy_id, code, name, parent_id, level)
select %s, n.code, n.name, n.parent_id, 2
  from unnest(%s::text[], %s::text[], %s::uuid[]) as n(code, name, parent_id)
on conflict (taxonomy_id, code) do nothing
"""

NODE_IDS = """
select code, node_id::text
  from market.taxonomy_node
 where taxonomy_id = %s and code = any(%s::text[])
"""

#: Yahoo's classification rows a company's newest profile no longer states. Only Yahoo's, and only
#: in muffin's taxonomy: N-PORT, the segment derivation, SIC and Wikidata each state their own.
RETRACT_CLASSIFICATION = """
delete from market.security_taxonomy t
 using unnest(%s::uuid[]) as r(security_id)
 where t.security_id = r.security_id
   and t.source_code = %s
   and t.node_id in (select node_id from market.taxonomy_node where taxonomy_id = %s)
   and not exists (
         select 1 from unnest(%s::uuid[], %s::uuid[]) as k(security_id, node_id)
          where k.security_id = t.security_id and k.node_id = t.node_id)
returning t.security_id::text
"""


@dataclass
class Classification:
    """Yahoo's sector and industry rows for the companies a run read, and what it could not
    place."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    #: Companies whose Yahoo rows these REPLACE: their profile named a sector we map, or none.
    replaced: list[str] = field(default_factory=list)
    nodes_created: int = 0
    #: Sector labels with no muffin code, with a count. Such a company keeps its stored rows: a
    #: vocabulary change upstream must not read as Yahoo withdrawing every sector.
    unmapped: dict[str, int] = field(default_factory=dict)


def classify(cur: Any, facts: Sequence[CompanyFacts]) -> Classification:
    """Sector and industry rows for every company whose document carries a profile.

    A PROFILE WITH NO SECTOR IS A STATEMENT (a fund, a shell), so that company's Yahoo rows are
    replaced by none. A sector we cannot map is not: the stored rows stay and the label is counted.
    """
    out = Classification()
    cur.execute(SECTOR_NODES, (TAXONOMY,))
    sector_node = {str(code): str(node) for code, node in cur.fetchall()}

    wanted: dict[str, tuple[str, str]] = {}
    pairs: list[tuple[CompanyFacts, str, str | None]] = []
    for fact in facts:
        if fact.profile is None:
            continue
        label = fact.sector_label
        if label is None:
            out.replaced.append(fact.security_id)
            continue
        code = SECTOR_CODES.get(label)
        node = sector_node.get(code) if code else None
        if code is None or node is None:
            out.unmapped[label] = out.unmapped.get(label, 0) + 1
            continue
        out.replaced.append(fact.security_id)
        industry = None
        if fact.industry_label:
            industry = industry_code(code, fact.industry_label)
            wanted.setdefault(industry, (fact.industry_label, node))
        pairs.append((fact, node, industry))

    industry_node: dict[str, str] = {}
    if wanted:
        codes = sorted(wanted)
        cur.execute(
            INSERT_INDUSTRY_NODES,
            (
                TAXONOMY,
                codes,
                [wanted[c][0] for c in codes],
                [wanted[c][1] for c in codes],
            ),
        )
        count = getattr(cur, "rowcount", None)
        out.nodes_created = count if isinstance(count, int) and count > 0 else 0
        cur.execute(NODE_IDS, (TAXONOMY, codes))
        industry_node = {str(code): str(node) for code, node in cur.fetchall()}

    for fact, sector, industry in pairs:
        nodes = [sector]
        if industry is not None and industry in industry_node:
            nodes.append(industry_node[industry])
        out.rows += [
            {
                "security_id": fact.security_id,
                "node_id": node,
                "source_code": SOURCE,
                "as_of": fact.fetched_at,
            }
            for node in nodes
        ]
    return out


def retract_classification(cur: Any, classification: Classification) -> int:
    """Delete the Yahoo rows each replaced company's newest profile no longer states."""
    if not classification.replaced:
        return 0
    cur.execute(
        RETRACT_CLASSIFICATION,
        (
            sorted(classification.replaced),
            SOURCE,
            TAXONOMY,
            [r["security_id"] for r in classification.rows],
            [r["node_id"] for r in classification.rows],
        ),
    )
    return len(cur.fetchall())
