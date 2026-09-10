"""Period returns from a daily series — ported rule for rule, because each rule cost something.

A return looks like one division and is four judgements: is this series still being priced, is the
anchor comparable with the latest bar, did anything actually move, and is the window one we can
denominate at all. Every one of them was learned from a wrong number on a deployed page.

The constants and the arithmetic match `resources.ts` exactly, including the rounding, because the
gate for this port is PARITY with what production currently serves — 200 securities, every period,
compared before the old resource is switched off. A "better" rule here would fail that gate while
being right, which is the worst way to spend a cutover.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date, timedelta

from muffin_ingest.facets.prices import Bar

#: Calendar days back to the anchor bar, per period. `1d` and `ytd` are NOT here: both anchor on
#: something other than a lookback (see below), and giving them a day count is how `1d` becomes a
#: flat 0.00% across a weekend.
#:
#: `10y` is deliberately absent, matching production: `market.return_period` carries the code
#: because the vocabulary allows it, and nothing has ever produced one. Adding it here would change
#: what the cutover is compared against.
PERIOD_DAYS: dict[str, int] = {
    "1w": 7,
    "1m": 30,
    "3m": 91,
    "6m": 182,
    "1y": 365,
    "3y": 1095,
    "5y": 1826,
}

#: A single-bar move this large is a UNIT CHANGE or an unadjusted corporate action, not a market.
#:
#: Measured 2026-08-12 across all 40 securities returning >= +300% over a year, by re-fetching each
#: series and taking its largest one-day ratio. The two populations separate cleanly with NOTHING
#: between them: the largest legitimate move was 2.04x (ASAAF) and the smallest illegitimate one
#: 6.0x (KLTHF). AMRM.TA and ISHO.TA jump ~100x on the SAME day, both quoted in `ILA` — Yahoo
#: switching Tel Aviv quotes from shekels to agorot.
#:
#: NOT a cap on the reported return. SNDK really is up ~2,700%; clipping it would swap a right
#: number for a different wrong one. What is untrustworthy is a return measured ACROSS the break.
DISCONTINUITY_RATIO = 5.0

#: A series whose newest bar is older than this cannot produce a current return.
#:
#: When an instrument stops trading the provider keeps serving its final bars, so every period is
#: measured between two identical closes and comes back as exactly +0.0% — which reads as "the
#: market was flat", not "this fund is dead". Measured 2026-08-13: Egypt, Nigeria and Portugal each
#: showed +0.0% on ALL periods, dated that day, from ETFs whose last filing was 2022-2024.
#:
#: Ten days rather than two: a long weekend plus a public holiday either side is legitimate, and
#: several of these markets close for multi-day festivals.
STALE_DAYS = 10


def _round_like_js(value: float) -> float:
    """Percent to four decimal places, rounding HALF UP as JavaScript does.

    Python's `round` is banker's rounding, so `round(0.5)` is 0 where `Math.round(0.5)` is 1. It
    would differ from production only at an exact half — which is rare enough to never show up in a
    test and to show up eventually in a parity comparison, which is the worst combination.
    """
    return math.floor(value * 1_000_000 + 0.5) / 10_000


def first_comparable_index(series: Sequence[Bar]) -> int:
    """Index of the bar immediately AFTER the most recent discontinuity, or 0 when there is none.

    Walked from the END backwards, so it finds the MOST RECENT break: anything at or after it is
    denominated the same way the latest bar is. Applied PER PERIOD rather than per symbol, because
    after a break the short windows are still perfectly good and only the long ones must go.
    """
    for i in range(len(series) - 1, 0, -1):
        prev = series[i - 1].close
        cur = series[i].close
        if (
            prev > 0
            and cur > 0
            and (cur / prev > DISCONTINUITY_RATIO or prev / cur > DISCONTINUITY_RATIO)
        ):
            return i
    return 0


def index_at_or_before(series: Sequence[Bar], on_or_before: date) -> int | None:
    """The newest bar dated on or before a given day, or None if the series starts after it."""
    for i in range(len(series) - 1, -1, -1):
        if series[i].trade_date <= on_or_before:
            return i
    return None


def _eligible(series: Sequence[Bar], now: date) -> bool:
    if len(series) < 2:
        return False
    if series[-1].trade_date < now - timedelta(days=STALE_DAYS):
        return False
    latest = series[-1].close
    return math.isfinite(latest) and latest > 0


def _anchors(series: Sequence[Bar], now: date) -> dict[str, int | None]:
    """Which bar each period is measured FROM."""
    anchors: dict[str, int | None] = {
        # THE PREVIOUS BAR, not a one-day lookback. A weekend or holiday resolves "yesterday" to the
        # same bar and reports a flat 0.00% for every security at once.
        "1d": len(series) - 2,
    }
    for period, days in PERIOD_DAYS.items():
        anchors[period] = index_at_or_before(series, now - timedelta(days=days))
    # THE LAST CLOSE OF LAST YEAR, so early January is measured from the true year-end rather than
    # from the first bar of the new year (which would report ~0% for the first days of trading).
    anchors["ytd"] = index_at_or_before(series, date(now.year - 1, 12, 31))
    return anchors


def price_returns(series: Sequence[Bar], now: date) -> dict[str, float]:
    """Price return per period, in percent. Dividends excluded — see `total_returns`."""
    if not _eligible(series, now):
        return {}

    latest = series[-1].close
    comparable_from = first_comparable_index(series)

    def at(idx: int | None) -> float | None:
        if idx is None or idx < comparable_from:
            return None
        anchor = series[idx].close
        if anchor == 0:
            return None
        # A WINDOW THAT NEVER MOVED IS NOT A 0.00% RETURN — it is a series that is not being priced.
        #
        # Measured 2026-08-13: `GOTO.JK` had 62 bars across the 3-month window and ONE distinct
        # close (50, every session); `AOT-R.BK` had 65 bars and two. Both are ordinary listings on
        # live exchanges, so the provider is padding rather than quoting. Nothing else can see it:
        # the closes are positive, the latest bar is today, and there is no discontinuity to cut.
        #
        # Per WINDOW, not per series — a security can legitimately be flat over a week and
        # informative over a year, and the long windows of these same symbols do move.
        if not any(bar.close != anchor for bar in series[idx + 1 :]):
            return None
        return _round_like_js(latest / anchor - 1)

    return {
        period: value
        for period, idx in _anchors(series, now).items()
        if (value := at(idx)) is not None
    }


def total_returns(series: Sequence[Bar], now: date) -> dict[str, float]:
    """Daily-reinvested TOTAL return per period, in percent.

    REINVESTED, not summed. The simple form `(P_end - P_start + sum(D)) / P_start` treats a dividend
    paid nine years ago as if it had sat in cash ever since, which understates a long horizon on an
    income-paying market by a large margin.

    THE SAME ELIGIBILITY RULES AS THE PRICE RETURN, and deliberately not a loose copy of them: a
    total return computed across a redenomination is wrong for exactly the same reason, and two
    similar-looking sets of checks are how they drift apart. The one rule NOT shared is the
    never-moved test — reinvestment can move a total return through a flat price, so a flat window
    is not evidence here.
    """
    if not _eligible(series, now):
        return {}

    comparable_from = first_comparable_index(series)

    # cum[i] is the value at bar i of one unit invested at bar 0 with dividends reinvested.
    cum = [1.0] * len(series)
    for i in range(1, len(series)):
        prev = series[i - 1].close
        if not math.isfinite(prev) or prev <= 0:
            # A non-positive previous close cannot denominate a return. Carry the factor forward
            # unchanged rather than poisoning every later period with NaN or infinity.
            cum[i] = cum[i - 1]
            continue
        cum[i] = cum[i - 1] * ((series[i].close + (series[i].dividend or 0.0)) / prev)

    def at(idx: int | None) -> float | None:
        if idx is None or idx < comparable_from:
            return None
        base = cum[idx]
        if not math.isfinite(base) or base <= 0:
            return None
        value = cum[-1] / base - 1
        return _round_like_js(value) if math.isfinite(value) else None

    return {
        period: value
        for period, idx in _anchors(series, now).items()
        if (value := at(idx)) is not None
    }
