"""FX rates: what a unit of each currency is worth in USD, today and for the last ten years.

WHY HISTORY AT ALL. `fx_rate` once held three days, which is enough to reprice a market cap TODAY
and useless for a ratio SERIES — converting a 2021 bar with today's rate is wrong by every
intervening move and looks entirely ordinary. A P/E chart for a company that reports in one currency
and trades in another either has a rate per bar or it has nothing.

WEEKLY, NOT DAILY, and deliberately: FX moves slowly relative to the quantity being converted, and
524 weekly points per currency against ~2,600 daily keeps the table small enough to join cheaply.
The consumer carries each rate forward to the following bars, so a daily price between two weekly
rates uses the most recent one rather than interpolating — A RATE WE DID NOT OBSERVE IS NOT A RATE.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document

#: Subunits are NOT currencies, and pretending otherwise is what made Tel Aviv look like a 100x
#: crash. Yahoo has no pair for agorot, cents or fils; each is a fixed fraction of its parent.
#: Data rather than a branch at the call site, so nothing converting a figure has to know which of
#: the 43 codes are subunits.
SUBUNITS: dict[str, tuple[str, float]] = {
    "ILA": ("ILS", 100.0),
    "ZAC": ("ZAR", 100.0),
    "KWF": ("KWD", 1000.0),
}

#: How long a currency the provider has nothing for stays unasked. THIRTY DAYS, NOT NEVER — a pair
#: not quoted today may be quoted next quarter, and Yahoo carries exactly ONE bar for `GELUSD=X`,
#: so a history fetch for the lari SUCCEEDS, writes a single recent row, and leaves "has a rate
#: older than 90 days" false for ever. It was re-fetched eight times a day with nothing to show and
#: no count anywhere could report it.
ABSENT_FOR_DAYS = 30


@dataclass(frozen=True)
class Rate:
    """One currency's value in USD on one date."""

    currency_code: str
    as_of: date
    usd_per_unit: float
    #: `None` for an observed rate; the parent's code when arithmetic produced it from a subunit.
    derived_from: str | None = None


def is_plausible(usd_per_unit: float) -> bool:
    """A SANITY BAND, because a wrong rate is silent and enormous.

    The failure it exists to catch is an INVERTED PAIR: `USDTWD` and `TWDUSD` are both valid Yahoo
    symbols and exact reciprocals, returning 31.9 and 0.0314 — both entirely plausible-looking.

    THE CEILING IS 10, NOT 100, and the first version of this rule got that wrong. Every real
    currency sits below it: the Kuwaiti dinar is the highest at ~3.26 USD, then Bahrain ~2.65 and
    Oman ~2.60. A ceiling of 100 accepted an inverted TWD at 31.9 — the guard's own headline case —
    while looking generous. 10 keeps 3x headroom over the highest real currency and refuses it. The
    floor is 1e-7: the Vietnamese dong, the lowest held here, is ~0.0000382, and an inverted dong
    would be ~26,200 and fail the ceiling.

    WHAT IT CANNOT CATCH, stated rather than implied: an inversion where both directions land inside
    the band — a currency near 0.5 inverts to 2.0 and neither is refused. Claiming otherwise would
    make it trusted beyond its reach. What protects those is `yahoo_chart.pair()` being the one
    place the symbol is assembled.

    Rejecting is right rather than correcting: a rate we are unsure of must not silently reprice the
    8,169 non-USD market caps that depend on it.
    """
    import math

    return math.isfinite(usd_per_unit) and 1e-7 < usd_per_unit < 10


#: Currencies to ask about. SUBUNITS ARE EXCLUDED because no provider quotes them — they are derived
#: from their parent below — and USD is excluded because it is 1 by definition, not by measurement.
ASKABLE_CURRENCIES = """
select c.code
  from market.currency c
 where c.code <> 'USD'
   and c.code <> all(%s)
   and (c.history_missing_at is null or c.history_missing_at < current_date - %s)
 order by c.code
"""


def askable_currencies(conn: Any, *, include_absent: bool = False) -> list[str]:
    """Which currencies are worth a request right now."""
    with conn.cursor() as cur:
        cur.execute(
            ASKABLE_CURRENCIES,
            (list(SUBUNITS), 0 if include_absent else ABSENT_FOR_DAYS),
        )
        return [row[0] for row in cur.fetchall()]


def raw_rows(
    currency: str,
    document: Document,
    *,
    interval: str,
    range_: str,
    run_id: str,
) -> list[dict[str, Any]]:
    """THE PROVIDER'S ANSWER, UNOPENED, plus who asked and how — the artifact, not the answer.

    ONE ROW PER CALL, CARRYING THE WHOLE BODY. Until 2026-09-12 this took the pivoted `Point`
    objects `yahoo_chart.chart()` had already built, so what reached disk was our reshaping of
    Yahoo's nested parallel arrays rather than Yahoo's bytes. Every field we knew to look for
    survived; a field we did not — a second quote block, an array whose length does not match
    `timestamp`, anything Yahoo adds beside it — did not, and could only be recovered by re-asking
    for ten years of history per currency. The two-stage split exists precisely so that adopting a
    field costs a re-parse of files already on disk.

    THE ASKED CURRENCY AND THE REQUEST PARAMETERS ARE RECORDED, for the same reason the price lane
    records `asked_symbol` beside `observed_symbol`: when a value turns out wrong the first
    question is always what was actually requested, and `range`/`interval` are the two parameters
    that decide what a chart body even contains. `currency_code` is ours and is written anyway —
    without it stage 2 would re-derive the pair from a symbol, so a mapping changed between fetch
    and transform would silently re-attribute a whole series.
    """
    row = document.as_row(run_id)
    row.update(
        {
            "currency_code": currency,
            "asked_symbol": yahoo_chart.pair(currency),
            "interval": interval,
            "range": range_,
            "provider": "yahoo",
        }
    )
    return [row]


#: THE SOURCE IS `yfinance`, NOT `yahoo`, AND THAT IS DELIBERATE DESPITE THE CALL BEING DIRECT.
#:
#: `market.data_source` names the VENDOR, and this schema has always called Yahoo `yfinance` — all
#: 22,236 existing `fx_rate` rows say so, written by a resource that also called
#: `query2.finance.yahoo.com/v8/finance/chart` directly. Whether our side reaches it through a
#: Python library or over HTTP is a fact about us, not about who published the number.
#:
#: Seeding a second code for one vendor would split the table and make "which source said this"
#: ambiguous — the same-fact-in-two-places drift this codebase keeps paying for. It would also cost
#: a migration and therefore a deploy, to record a distinction nobody reading the data wants.
#:
#: The first run found this the way the rule says it will: `source_code` is a foreign key, the code
#: was not seeded, and the whole write failed with `fx_rate_source_code_fkey` AFTER the provider had
#: answered all 38 currencies. "A resource that writes a new source_code must seed it in the same
#: migration, and nothing downstream can catch the omission."
SOURCE_CODE = "yfinance"


@dataclass(frozen=True)
class Normalised:
    """Typed rates, and what reading the bodies established about them.

    THE COUNTS MOVED HERE WITH THE RULES. They used to be collected at fetch — `live_points`,
    `null_closes`, `outside_window` — which is what made stage 1 the place that decided what a bar
    is. They are observations about a PARSE now, so every one of them can be recomputed from files
    on disk, and `refused_by_band` in particular is a statement about the provider rather than a
    repair to be pleased about.
    """

    rates: list[Rate]
    stats: dict[str, int]


def normalise(
    rows: Sequence[Mapping[str, Any]],
    *,
    source_code: str = SOURCE_CODE,
    window: tuple[date, date] | None = None,
) -> Normalised:
    """Raw bodies to typed rates. No provider call, so a fix here is free to re-run.

    EVERY RULE THAT NARROWS IS HERE, and that is the whole reason this function grew a parser.
    A live quote is not a close; a null close is a padded session; an implausible rate is most
    likely an inverted pair; a point outside the partition's window belongs to another partition.
    Each of those was applied before anything was stored until 2026-09-12, so correcting any one
    of them meant re-fetching ten years of history per currency.

    DROPPED, NOT CORRECTED. Inverting an implausible rate back would repair a value whose
    provenance is already in doubt, and the band cannot tell an inversion from a genuinely odd
    quote.
    """
    out: list[Rate] = []
    stats = {
        "documents": 0,
        "points": 0,
        "live_points": 0,
        "null_closes": 0,
        "undatable": 0,
        "outside_window": 0,
        "refused_by_band": 0,
        # A BODY ON DISK THAT WILL NOT PARSE IS OUR PROBLEM, and it must not blank the other
        # forty-two currencies. Counted so the asset can report it; non-zero is never normal.
        "unreadable": 0,
        # A ROW WITH NO BODY WAS WRITTEN BEFORE 2026-09-12, when raw held a per-point pivot rather
        # than the response. Nothing in it can be re-read under today's rules — the pivot IS the
        # loss this format replaced — so it yields nothing, and it is COUNTED rather than skipped:
        # a stage-2 re-run over an old partition would otherwise publish no rates and read exactly
        # like a day the provider had nothing for. The remedy is re-materialising that raw
        # partition, which is one call per currency.
        "legacy_rows": 0,
    }
    for row in rows:
        body = row.get("body")
        if body is None:
            stats["legacy_rows"] += 1
            continue
        stats["documents"] += 1
        try:
            series = yahoo_chart.parse(bytes(body))
        except yahoo_chart.YahooRefused:
            stats["unreadable"] += 1
            continue
        stats["points"] += len(series.points)
        stats["live_points"] += series.live_points
        stats["null_closes"] += series.null_closes
        stats["undatable"] += series.undatable
        currency = str(row.get("currency_code") or "")
        for point in series.points:
            # A LIVE QUOTE IS NOT A CLOSE. Raw keeps the mid-session point because it is what the
            # provider said; publishing it is the intraday-capture defect this pipeline replaces
            # a resource for, so it is refused here — where changing the rule costs a re-parse.
            if point.is_live:
                continue
            # A NULL CLOSE IS A PADDED SESSION, also kept in the body and refused here.
            if point.close is None or point.close <= 0:
                continue
            # THE PARTITION'S OWN WINDOW, APPLIED TO A FILE. The spot lane asks a five-day range
            # so a weekend or holiday still yields a close; it does not ask for five days in
            # order to store them. Filtering at fetch threw those points away — and a point
            # deleted at fetch is one no re-parse can recover.
            if window is not None and not (window[0] <= point.as_of < window[1]):
                stats["outside_window"] += 1
                continue
            if not is_plausible(point.close):
                stats["refused_by_band"] += 1
                continue
            out.append(
                Rate(
                    currency_code=currency,
                    as_of=point.as_of,
                    usd_per_unit=point.close,
                    derived_from=None,
                )
            )
    return Normalised(rates=out, stats=stats)


def with_subunits(rates: Sequence[Rate]) -> list[Rate]:
    """Every parent rate, plus the subunit rates that follow from it BY ARITHMETIC.

    IN THE SAME PASS AS THE PARENT, which is the whole point. A subunit derived separately — or
    later, or only for spot — leaves ILA with three days of history against ILS's ten years, and any
    consumer joining "the most recent rate at or before this bar" then silently falls back to a
    recent rate for every historical bar. That is the Tel Aviv shape again: a conversion that is
    wrong by every intervening move and looks ordinary.

    `derived_from` is a COLUMN rather than an inference, because "observed" and "computed from an
    observation" are different facts and a later reader must not have to guess which it holds.
    """
    out = list(rates)
    by_parent: dict[str, list[Rate]] = {}
    for rate in rates:
        by_parent.setdefault(rate.currency_code, []).append(rate)

    for code, (parent, per) in SUBUNITS.items():
        for rate in by_parent.get(parent, ()):
            out.append(
                Rate(
                    currency_code=code,
                    as_of=rate.as_of,
                    usd_per_unit=rate.usd_per_unit / per,
                    derived_from=parent,
                )
            )
    return out


def core_rows(rates: Sequence[Rate], *, source_code: str = SOURCE_CODE) -> list[dict[str, Any]]:
    """Typed rates as `market.fx_rate` rows."""
    return [
        {
            "currency_code": rate.currency_code,
            "as_of": rate.as_of.isoformat(),
            "usd_per_unit": rate.usd_per_unit,
            "source_code": source_code,
            "derived_from": rate.derived_from,
        }
        for rate in rates
    ]
