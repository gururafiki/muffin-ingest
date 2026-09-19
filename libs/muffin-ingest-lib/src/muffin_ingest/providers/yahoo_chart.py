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

TWO FUNCTIONS, AND THE SPLIT IS THE WHOLE POINT OF THIS MODULE.

  `fetch`  the only thing here that touches the network. It returns THE RESPONSE BODY, byte for
           byte, as a `Document` — the same shape the registry lane stores.
  `parse`  bytes to points. Pure, stage-2 code, living beside `fetch` so Yahoo's payload shape is
           described in exactly one place.

IT USED TO BE ONE FUNCTION AND THAT MADE THIS THE NARROWEST RAW LAYER IN THE PIPELINE. Yahoo
answers with NESTED PARALLEL ARRAYS —

    {"chart": {"result": [{"meta": {...29 keys...},
                           "timestamp": [...],
                           "indicators": {"quote":    [{"open": [...], "high": [...], ...}],
                                          "adjclose": [{"adjclose": [...]}]}}],
               "error": null}}

— and `chart()` pivoted that into one object per timestamp before anything was stored. Even after
the arrays were read generically, the pivot still decided what counts as a field: anything that is
not a length-matching array under `indicators.quote[0]`, a second quote block, or a key Yahoo adds
beside `timestamp` was dropped at the moment of fetching and could only be recovered by re-asking
for ten years of history per currency. A reshaping is an interpretation however faithful, and an
interpretation made before the bytes are on disk is one no re-parse can revise. Stage 1 now stores
the body; every rule below runs against a file.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import httpx

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document

#: Yahoo's own host for the chart API. The cache location overrides it when configured.
REAL_ORIGIN = "https://query2.finance.yahoo.com"

#: Yahoo refuses a bare python-httpx UA on this endpoint often enough that it reads as an outage.
BROWSER_UA = "Mozilla/5.0 (compatible; muffin-market-data)"


class YahooRefused(Exception):
    """The request did not produce an answer — a transport fault, never an absence."""


@dataclass(frozen=True)
class Point:
    """One aligned row of Yahoo's parallel arrays: the date, the close, and EVERYTHING ELSE.

    A STAGE-2 TYPE. Nothing here is stored; it is what `parse` hands the FX rules so they have
    something to decide on. `close` is named because every rule turns on it — the plausibility
    band, the live-quote refusal, the null refusal.
    """

    as_of: date
    #: The close AS THE PROVIDER SENT IT — `None` where it sent a null, which it does for a padded
    #: session with no data yet. Not coerced away: the rules decide what a usable close is.
    close: float | None
    #: The provider's own row for this timestamp. Empty only if Yahoo sent no quote block at all,
    #: which it does for a symbol it does not carry.
    quote: dict[str, Any] = field(default_factory=dict)
    #: The raw epoch stamp, so a corrected date rule can be applied without re-reading the body.
    timestamp: float | None = None
    #: TRUE FOR A MID-SESSION QUOTE, identified by its stamp equalling `regularMarketTime`.
    is_live: bool = False


@dataclass
class Series:
    """Every point the body contained, and what was observed about them.

    THE COUNTS ARE NOT DECORATION. `live_points` non-zero is the normal case during a session and
    zero after one closes; `null_closes` says the provider is padding. Both were invisible until a
    partition claimed a day it had not collected.
    """

    points: list[Point] = field(default_factory=list)
    #: Points that are a LIVE quote rather than a completed bar. Present in `points`.
    live_points: int = 0
    #: Points whose close was null — a padded row for a session with no data yet. Present too.
    null_closes: int = 0
    #: Rows with no usable timestamp. The one thing `parse` cannot place, so it cannot emit a
    #: `Point` for it — counted here, and the BODY still holds it either way.
    undatable: int = 0
    #: The exchange's timezone name, as the provider states it.
    timezone_name: str | None = None
    #: The whole `meta` block. 29 keys on the EURUSD=X body captured 2026-09-12 — a count that is
    #: not stable across symbols or days, which is the case for storing the body rather than a
    #: list of the keys we knew about. This module interprets three of them.
    meta: dict[str, Any] = field(default_factory=dict)


def pair(currency: str) -> str:
    """The Yahoo symbol for one currency against USD — BUILT IN EXACTLY ONE PLACE.

    `USDTWD=X` and `TWDUSD=X` are both valid and exact reciprocals, returning 31.9 and 0.0314 —
    both entirely plausible, so nothing about a returned value says which was asked. The
    plausibility band catches the inversion for most currencies and provably cannot for one near
    0.5; what protects those is the pair being assembled here rather than at each call site.
    """
    return f"{currency}USD=X"


def _names_an_absence(body: bytes) -> bool:
    """Does this body say, in Yahoo's own words, that it does not carry the symbol?

    A 404 THAT NAMES THE ABSENCE IS AN ANSWER, NOT A FAULT — measured, not assumed. `ZZZUSD=X`
    returns **HTTP 404** whose whole body, captured 2026-09-12, is

        {"chart":{"result":null,"error":{"code":"Not Found",
                  "description":"No data found, symbol may be delisted"}}}

    The first version of this provider raised on every non-200, which reported each unquoted
    currency as a transport failure — and because a transport failure must never mark a subject
    absent, the negative cache could never fill and those pairs would be re-asked for ever. Same
    shape as a SEC 400 naming a company with no Form 4, recorded twice already. Read the message,
    not the status.

    PEEKING IS NOT PARSING. This reads one key to decide whether we were answered at all; the
    bytes it looked at are stored unchanged, so nothing here narrows what stage 2 can see.
    """
    try:
        parsed = json.loads(body)
    except ValueError:
        return False
    return bool(isinstance(parsed, dict) and (parsed.get("chart") or {}).get("error"))


def fetch(symbol: str, *, range_: str, interval: str, timeout_s: float = 20.0) -> Document:
    """The response body, EXACTLY as Yahoo sent it. No parse, no reshape, no interpretation.

    A NON-2xx AND AN EMPTY SERIES ARE DIFFERENT FACTS and this keeps them so: a refusal raises, an
    answer is returned for `parse` to read. Collapsing them is the defect this whole rework exists
    to remove — over the REST hop a throttled provider and a symbol nobody carries are both an
    empty 204, and that confusion once recorded ~8,300 securities as permanently unanswerable in
    an afternoon.

    `fetched_at` IS RECORDED HERE because the date travels with the data: a chart body carries
    epoch stamps for its points and nothing at all about when it was served.
    """
    base = settings.provider_base("yahoo", REAL_ORIGIN)
    url = f"{base}/v8/finance/chart/{symbol}"
    params = {"range": range_, "interval": interval}
    with metrics.request("yahoo") as outcome:
        try:
            response = httpx.get(
                url,
                params=params,
                headers={"User-Agent": BROWSER_UA},
                timeout=timeout_s,
            )
        except httpx.HTTPError as exc:
            raise YahooRefused(f"{type(exc).__name__}: {exc}") from exc
        if response.status_code != 200 and not _names_an_absence(response.content):
            outcome["outcome"] = "refused"
            raise YahooRefused(f"HTTP {response.status_code} for {symbol}")
    return Document(
        # THE REQUEST, NOT JUST THE ENDPOINT. `range` and `interval` decide what a chart body even
        # contains, so they are provenance — built from what we sent rather than read back off the
        # response, which carries a URL only when a request object is attached to it.
        url=str(httpx.URL(url, params=params)),
        body=response.content,
        content_type=response.headers.get("content-type", "application/json"),
        fetched_at=datetime.now(UTC),
    )


def parse(body: bytes) -> Series:
    """Bytes to points. NO NETWORK, so every rule below costs a re-parse and never a re-fetch.

    An unreadable body raises; a body that says Yahoo has nothing returns an empty `Series`. The
    two are kept apart here for the same reason `fetch` keeps them apart one layer up.
    """
    try:
        parsed: Any = json.loads(body) if body else None
    except ValueError as exc:
        raise YahooRefused(f"unparseable chart body: {exc}") from exc

    chart_body = ((parsed or {}).get("chart") or {}) if isinstance(parsed, dict) else {}
    # Yahoo states an unknown symbol both as a 404 and INSIDE a 200. Same fact, same treatment.
    if chart_body.get("error"):
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
    # anything it adds later. Read generically so a new array needs no code change here; and
    # because the BODY is what was stored, a shape this loop cannot see is still on disk.
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
    # rather than assuming a venue. IT IS NOW A STAGE-2 RULE: until 2026-09-12 this ran before
    # anything was stored, so getting the timezone wrong cost ten years of refetching per currency
    # rather than one re-parse of files already on disk.
    #
    # THE PRICE LANE DOES NOT HAVE THIS DEFECT, AND I ASSERTED THAT IT DID BEFORE CHECKING.
    # Measured against the old table on exactly the timezone-exposed venues: HK 540/540, TW
    # 542/542, SG 554/554, TH 272/272, ID 532/532, KR 532/534, AU 558/560 — same-date closes agree,
    # which a one-day shift makes impossible. openbb's yfinance adapter hands back a normalised
    # date; this raw endpoint does not.
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
            # There is no date to file this under, so no `Point` can be built for it. Counted
            # rather than passed over in silence — and the body on disk still holds the row, so
            # a rule that could place it needs no provider call.
            out.undatable += 1
            continue
        is_live = live_at is not None and stamp == live_at
        usable = isinstance(close, int | float) and not isinstance(close, bool) and close > 0
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
