"""Yahoo's chart endpoint, called DIRECTLY — the one provider that does not go through the hub.

WHY NOT openbb. openbb carries no keyless FX pair endpoint, and the ECB's free daily reference
rates — the obvious alternative — cover **27 of the 43 currencies this schema holds**, missing TWD
(535 Taiwanese securities), VND, AED, SAR, QAR, KWD, PEN, CLP, COP, ARS and GEL. Yahoo returns all
of them, is keyless, and is the same endpoint `security-yahoo-symbols` already used, so a second
provider would add failure modes while covering a strict subset.

IT GOES THROUGH `http-cache`, which is what a direct call costs here: `provider_base` returns the
cache location when one is configured and the real origin otherwise, so the hop stays removable.
That is the opposite of the hub, whose own egress the cache cannot see at all.

THE DEFAULT USER-AGENT IS REFUSED often enough to matter, and the failure reads as an outage rather
than a header problem — so one is always sent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import httpx

from muffin_ingest import settings

#: Yahoo's own host for the chart API. The cache location overrides it when configured.
REAL_ORIGIN = "https://query2.finance.yahoo.com"

#: Yahoo refuses a bare python-httpx UA on this endpoint often enough that it reads as an outage.
BROWSER_UA = "Mozilla/5.0 (compatible; muffin-market-data)"


class YahooRefused(Exception):
    """The request did not produce an answer — a transport fault, never an absence."""


@dataclass(frozen=True)
class Point:
    """One aligned row of Yahoo's parallel arrays: the date, the close, and EVERYTHING ELSE.

    `close` is named because every rule in this pipeline turns on it — the plausibility band, the
    live-quote drop, the null drop. `quote` carries the provider's whole row for that timestamp
    (`open`, `high`, `low`, `volume`, and whatever else it sends) so stage 1 can store it.

    IT USED TO BE `as_of` AND `close` ALONE, which silently made the FX lane's raw layer the
    narrowest in the pipeline: everything Yahoo sends but the close was discarded at the moment of
    fetching and unrecoverable without re-asking for ten years of history per currency.
    """

    as_of: date
    #: The close AS THE PROVIDER SENT IT — `None` where it sent a null, which it does for a padded
    #: session with no data yet. Not coerced and not dropped: stage 2 decides what a usable close
    #: is, so a rule change there costs a re-parse rather than ten years of refetching.
    close: float | None
    #: The provider's own row for this timestamp, unaltered. Empty only if Yahoo sent no quote
    #: block at all, which it does for a symbol it does not carry.
    quote: dict[str, Any] = field(default_factory=dict)
    #: The raw epoch stamp, kept so stage 2 can re-derive the date under a corrected rule.
    timestamp: float | None = None
    #: TRUE FOR A MID-SESSION QUOTE, identified by its stamp equalling `regularMarketTime`.
    #: RECORDED RATHER THAN DROPPED. Publishing one is the intraday-capture defect this pipeline
    #: replaces a resource for — but that is a decision for stage 2, and dropping it here made
    #: "Yahoo had only a live quote for the lari" a counter rather than something in the data.
    is_live: bool = False


@dataclass
class Series:
    """EVERY point the provider returned, and what was observed about them.

    THE COUNTS ARE NOT DECORATION. `live_points` non-zero is the normal case during a session and
    zero after one closes; `null_closes` says the provider is padding. Both were invisible until a
    partition claimed a day it had not collected.

    THEY ARE NAMED FOR WHAT THEY COUNT, NOT FOR WHAT USED TO HAPPEN TO THEM. Until 2026-09-12
    they were `live_dropped` / `nulls_dropped` and the points really were discarded here, which
    made stage 1 the place that decided what a bar is — so correcting either rule meant
    re-fetching ten years of history per currency. The points are kept; `facets/fx.py` applies
    the rules; a number that is not what its name says is the failure mode this repo is mostly
    about.
    """

    points: list[Point] = field(default_factory=list)
    #: Points that are a LIVE quote rather than a completed bar. Present in `points`.
    live_points: int = 0
    #: Points whose close was null — a padded row for a session with no data yet. Present too.
    null_closes: int = 0
    #: The exchange's timezone name, as the provider states it. Recorded so a future date-shift
    #: argument is settled by what was received rather than by what someone remembers.
    timezone_name: str | None = None
    #: THE WHOLE `meta` BLOCK, UNREAD. 27 keys on a real response — `currency`, `longName`,
    #: `firstTradeDate`, `instrumentType`, `fiftyTwoWeekHigh` — of which this module interprets
    #: three and the pipeline stored none. Carried so stage 1 can write it: a field nobody wants
    #: today is exactly the field whose absence costs a refetch tomorrow.
    meta: dict[str, Any] = field(default_factory=dict)


def pair(currency: str) -> str:
    """The Yahoo symbol for one currency against USD — BUILT IN EXACTLY ONE PLACE.

    `USDTWD=X` and `TWDUSD=X` are both valid and exact reciprocals, returning 31.9 and 0.0314 —
    both entirely plausible, so nothing about a returned value says which was asked. The
    plausibility band catches the inversion for most currencies and provably cannot for one near
    0.5; what protects those is the pair being assembled here rather than at each call site.
    """
    return f"{currency}USD=X"


def chart(symbol: str, *, range_: str, interval: str, timeout_s: float = 20.0) -> Series:
    """The raw close series, with nulls dropped and nothing else interpreted.

    A NON-2xx AND AN EMPTY SERIES ARE DIFFERENT FACTS and this keeps them so: the first raises, the
    second returns `[]`. Collapsing them is the defect this whole rework exists to remove — over the
    REST hop a throttled provider and a symbol nobody carries are both an empty 204, and that
    confusion once recorded ~8,300 securities as permanently unanswerable in an afternoon.
    """
    base = settings.provider_base("yahoo", REAL_ORIGIN)
    url = f"{base}/v8/finance/chart/{symbol}"
    try:
        response = httpx.get(
            url,
            params={"range": range_, "interval": interval},
            headers={"User-Agent": BROWSER_UA},
            timeout=timeout_s,
        )
    except httpx.HTTPError as exc:
        raise YahooRefused(f"{type(exc).__name__}: {exc}") from exc

    try:
        body: Any = response.json()
    except ValueError:
        body = None

    chart_body = ((body or {}).get("chart") or {}) if isinstance(body, dict) else {}
    error = chart_body.get("error")

    # A 404 THAT NAMES THE ABSENCE IS AN ANSWER, NOT A FAULT — measured, not assumed. `ZZZUSD=X`
    # returns **HTTP 404** carrying
    #
    #     {"code": "Not Found", "description": "No data found, symbol may be delisted"}
    #
    # The first version of this function raised on every non-200, which would have reported each
    # unquoted currency as a transport failure — and because a transport failure must never mark a
    # subject absent, the negative cache could never fill and those pairs would be re-asked for
    # ever. That is the same shape as a SEC 400 naming a company with no Form 4, recorded here
    # twice already. Read the message, not the status.
    if response.status_code == 404 and error:
        return Series()
    if response.status_code != 200:
        raise YahooRefused(f"HTTP {response.status_code} for {symbol}")
    if body is None:
        raise YahooRefused(f"unparseable body for {symbol}")

    # Yahoo can also state an unknown symbol INSIDE a 200. Same fact, same treatment.
    if error:
        return Series()

    results = chart_body.get("result") or []
    if not results:
        return Series()
    first = results[0] or {}
    stamps = first.get("timestamp") or []
    quotes = (first.get("indicators") or {}).get("quote") or [{}]
    quote = quotes[0] or {}
    closes = quote.get("close") or []
    # EVERY PARALLEL ARRAY YAHOO SENT, not just the close — `open`, `high`, `low`, `volume`, and
    # anything it adds later. Read generically so a new array needs no code change, which is the
    # point of keeping raw whole.
    series_by_field = {
        name: values
        for name, values in quote.items()
        if isinstance(values, list) and len(values) == len(stamps)
    }
    adj = (first.get("indicators") or {}).get("adjclose") or [{}]
    adjclose = (adj[0] or {}).get("adjclose") if adj else None
    if isinstance(adjclose, list) and len(adjclose) == len(stamps):
        series_by_field["adjclose"] = adjclose
    meta = first.get("meta") or {}

    # A BAR'S DATE IS ITS EXCHANGE'S DATE, NOT UTC's, AND FOR FX THAT IS A WHOLE DAY.
    #
    # Measured: an FX daily bar is stamped at the session's OPEN in `exchangeTimezoneName`, which
    # is `Europe/London` — the 2026-09-10 session arrives as `1788994800`, **2026-09-09T23:00Z**.
    # Reading `.date()` in UTC dates every FX bar a day early, and a partition filtering to its own
    # window then discards the lot: a real run reported `outside_window=190` — 38 currencies x 5
    # points, all of them — and wrote nothing while reporting success.
    #
    # `gmtoffset` is the provider's own statement of the offset, so this is reading the response
    # rather than assuming a venue.
    #
    # THE PRICE LANE DOES NOT HAVE THIS DEFECT, AND I ASSERTED THAT IT DID BEFORE CHECKING.
    # Measured against the old table on exactly the timezone-exposed venues: HK 540/540, TW
    # 542/542, SG 554/554, TH 272/272, ID 532/532, KR 532/534, AU 558/560 — same-date closes agree,
    # which a one-day shift makes impossible. (902 new bars also match the NEXT day's close, and
    # 899 of those match the same day too: an unchanged close across two sessions, not a shift.)
    # openbb's yfinance adapter hands back a normalised date; this raw endpoint does not.
    offset = meta.get("gmtoffset")
    tz = timezone(timedelta(seconds=int(offset))) if isinstance(offset, int | float) else UTC

    # THE LAST POINT IS OFTEN A LIVE QUOTE, NOT A BAR, and it is exactly identifiable: its timestamp
    # EQUALS `regularMarketTime`. Measured to the second on three separate series. Publishing it is
    # the intraday-capture defect this pipeline is replacing a resource for — a mid-session price
    # wearing a close's clothes. `GELUSD=X` is the extreme case: its ONLY point is the live quote,
    # so Yahoo has no completed weekly bar for the lari at all, which is a far more precise
    # statement than "it returned one row".
    live_at = meta.get("regularMarketTime")

    out = Series(timezone_name=meta.get("exchangeTimezoneName"), meta=dict(meta))
    for index, (stamp, close) in enumerate(zip(stamps, closes, strict=False)):
        if not isinstance(stamp, int | float):
            # The ONLY row dropped here, and it is not a row: without a timestamp there is no
            # date to file it under and nothing downstream could ever interpret it.
            continue
        is_live = live_at is not None and stamp == live_at
        usable = isinstance(close, int | float) and not isinstance(close, bool) and close > 0
        # COUNTED, NOT DROPPED. Both were removed here until 2026-09-12, which made stage 1 the
        # place that decided what a bar is — so correcting either rule meant re-fetching. They
        # are now observations about the response, and `facets/fx.py` applies them.
        out.live_points += int(is_live)
        out.null_closes += int(not usable)
        out.points.append(
            Point(
                as_of=datetime.fromtimestamp(float(stamp), tz=tz).date(),
                close=float(close) if usable else None,
                timestamp=float(stamp),
                is_live=is_live,
                quote={
                    name: values[index]
                    for name, values in series_by_field.items()
                    if values[index] is not None
                },
            )
        )
    return out
