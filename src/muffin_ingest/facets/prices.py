"""Daily bars: reading what the provider said, and refusing what it cannot have meant.

Everything in this module is PURE — it takes rows and returns rows. The network call lives in the
Dagster asset, the write lives in an I/O manager, and the per-subject verdict lives in the ledger.
That separation is what makes these rules testable against frozen bytes instead of against a
provider, which is how the transformation half of this pipeline stops costing requests to fix.

THREE SHAPES THE PROVIDER RESPONSE HAS THAT LOOK LIKE ONE:

  1. `symbol` IS PRESENT ONLY WHEN SEVERAL SYMBOLS WERE REQUESTED. A single-symbol batch comes back
     with no symbol column at all, so it has to be TOLD which symbol it asked about. Getting this
     wrong attributes a whole series to the wrong company and nothing downstream can see it.
  2. Dividends and splits ride on the SAME response. openbb's yfinance provider defaults
     `include_actions` to true, so they cost no extra call and were previously discarded.
  3. A split ratio is RECORDED, NEVER APPLIED. The bars are already split-adjusted; feeding a ratio
     back into a price would corrupt it. Its value is coverage — Tiingo is US-only, so a Tokyo or
     Zurich split was invisible.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class Bar:
    """One security's close on one day, plus whatever actions the same response carried."""

    trade_date: date
    close: float
    volume: int | None = None
    #: Cash dividend with THIS bar's date as the ex-date, when the provider reported one.
    dividend: float | None = None
    #: Split ratio on this date. Recorded, never applied — see the module docstring.
    split_ratio: float | None = None


def _as_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and len(value) >= 10:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    to_date = getattr(value, "date", None)
    return to_date() if callable(to_date) else None


def _positive(value: Any) -> float | None:
    """A number strictly greater than zero, or nothing.

    A ZERO CLOSE IS NOT A PRICE, and admitting one is not a small error: it yields -100% on every
    period at once, which was a 1,078-row defect. Booleans are refused explicitly because `bool` is
    an `int` in Python and `True` would otherwise store as 1.0.
    """
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, int | float):
        return None
    number = float(value)
    return number if number > 0 else None


def _optional_number(value: Any) -> float | None:
    """A number that may legitimately be zero or negative — an action, not a price."""
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, int | float):
        return None
    return float(value)


def bar_from(row: Mapping[str, Any], sole_symbol: str) -> tuple[str, Bar] | None:
    """One provider row to `(symbol, Bar)`, or None if it is not a usable bar.

    `sole_symbol` is what to attribute the row to when the response carries no `symbol` column —
    which is the single-symbol case, not an error.
    """
    trade_date = _as_date(row.get("date"))
    close = _positive(row.get("close"))
    if trade_date is None or close is None:
        return None

    volume = row.get("volume")
    return str(row.get("symbol") or sole_symbol).upper(), Bar(
        trade_date=trade_date,
        close=close,
        volume=int(volume)
        if isinstance(volume, int | float) and not isinstance(volume, bool)
        else None,
        dividend=_optional_number(row.get("dividend")),
        split_ratio=_optional_number(row.get("split_ratio")),
    )


def bars_by_symbol(rows: Sequence[Mapping[str, Any]], sole_symbol: str) -> dict[str, list[Bar]]:
    """Group a batched response by symbol, sorted ascending by date.

    SORTED HERE AND NOT AT THE CALLER, because every rule downstream — the previous bar for `1d`,
    the anchor for a lookback, the comparability cut — reads the series positionally, and a provider
    that changes its ordering would otherwise change the numbers rather than fail.
    """
    out: dict[str, list[Bar]] = {}
    for row in rows:
        parsed = bar_from(row, sole_symbol)
        if parsed is None:
            continue
        symbol, bar = parsed
        out.setdefault(symbol, []).append(bar)
    for series in out.values():
        series.sort(key=lambda b: b.trade_date)
    return out
