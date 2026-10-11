"""Dividends and splits, read from the chart documents the price lane already stores.

NO REQUEST OF ITS OWN. The price lane asks every chart with `events=div,split` (Phase 4 Stage 0),
so each stored document carries the company's dividends and splits for the window it covers, and a
first visit's full history carries all of them. The edge's `security-dividends` asked openbb for the
same events (yfinance's `Ticker.dividends` reads this endpoint): one request per company every 30
days, for data the price lane now holds.

UNITS. Both are in the series' own terms, which is what their one reader needs:

  * a dividend's `amount` is SPLIT-ADJUSTED, like Yahoo's closes: AAPL's 1987 quarterly dividend of
    $0.12 arrives as 0.000536, which is 0.12 over the 224 shares four splits made of one. It is in
    the currency the series is quoted in (JPY 50.0 for 7203.T), so `security_return` divides it by
    a close it is comparable with;
  * a split is stored as a RATIO, numerator over denominator: Yahoo's `4:1` is 4.0, as the edge's
    Tiingo resource stored `splitFactor` (2 for two-for-one, 0.1 for one-for-ten).

A DATE IS THE EXCHANGE'S DATE, through the body's own `gmtoffset`, as the price lane dates its bars
(`yahoo_chart.split_days`). 7203.T's dividend is stamped 00:00 UTC, which is 09:00 in Tokyo.

THE NEWEST DOCUMENT STATES THE DAYS IT COVERS, as for bars (`price_chart.merge`): an event an older
document carried inside a newer one's span, and the newer one does not, is withdrawn. Without it a
dividend Yahoo corrected would be held for ever by the document that first had it.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

from muffin_ingest.facets import price_chart
from muffin_ingest.providers import yahoo_chart

#: The source these rows are written under, the same as the edge's `security-dividends`.
SOURCE = "yfinance"

#: The corporate action kinds this reads (`market.corporate_action_kind`).
DIVIDEND = "dividend"
SPLIT = "split"


@dataclass(frozen=True)
class Action:
    """One dividend or split, as the newest document covering its day states it."""

    ex_date: date
    kind: str
    value: float
    #: The symbol the document was asked with: an action that cannot be tied to a series cannot be
    #: checked against the series it is supposed to explain.
    observed_symbol: str
    fetched_at: str


@dataclass
class SecurityActions:
    """One security's actions, and the days its documents speak for."""

    actions: dict[tuple[date, str], Action] = field(default_factory=dict)
    #: The first and last day any usable document covered: the span a retraction may touch.
    first: date | None = None
    last: date | None = None


def _local_day(stamp: Any, offset: Any) -> date | None:
    if not isinstance(stamp, int | float) or isinstance(stamp, bool):
        return None
    tz = timezone(timedelta(seconds=int(offset))) if isinstance(offset, int | float) else UTC
    return datetime.fromtimestamp(float(stamp), tz=tz).date()


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value) if math.isfinite(value) else None


def events_of(series: yahoo_chart.Series) -> list[tuple[date, str, float]]:
    """A parsed body's dividends and splits, dated in the exchange's calendar.

    A DIVIDEND MUST BE POSITIVE AND A SPLIT A RATIO OTHER THAN ONE, the rules the edge applied to
    Tiingo's `divCash` and `splitFactor`: a zero dividend is an ordinary day, and a 1:1 split is no
    split at all.
    """
    offset = series.meta.get("gmtoffset")
    out: list[tuple[date, str, float]] = []
    dividends = series.events.get("dividends")
    for item in dividends.values() if isinstance(dividends, dict) else ():
        if not isinstance(item, dict):
            continue
        day = _local_day(item.get("date"), offset)
        amount = _number(item.get("amount"))
        if day is not None and amount is not None and amount > 0:
            out.append((day, DIVIDEND, amount))
    splits = series.events.get("splits")
    for item in splits.values() if isinstance(splits, dict) else ():
        if not isinstance(item, dict):
            continue
        day = _local_day(item.get("date"), offset)
        numerator = _number(item.get("numerator"))
        denominator = _number(item.get("denominator"))
        if day is None or numerator is None or not denominator or numerator <= 0:
            continue
        ratio = numerator / denominator
        if ratio > 0 and abs(ratio - 1) > 1e-9:
            out.append((day, SPLIT, ratio))
    return sorted(out)


def merge(rows: Sequence[Mapping[str, Any]], stats: dict[str, int]) -> SecurityActions:
    """One security's chart documents, oldest fetch first, into the actions they now state.

    Only documents the price lane would publish bars from speak: an absence, an unreadable body and
    a body that is not daily say nothing about the days they do not cover.
    """
    merged = SecurityActions()
    for row in sorted(rows, key=price_chart.fetch_order):
        body = row.get("body")
        if body is None:
            continue
        try:
            series = yahoo_chart.parse(bytes(body))
        except yahoo_chart.YahooRefused:
            continue
        if series.error is not None or series.granularity not in (None, price_chart.INTERVAL):
            continue
        if not series.points:
            continue
        days = [point.as_of for point in series.points]
        first, last = min(days), max(days)
        merged.first = first if merged.first is None else min(merged.first, first)
        merged.last = last if merged.last is None else max(merged.last, last)
        events = events_of(series)
        in_span = {key for key in merged.actions if first <= key[0] <= last}
        # WITHDRAWN: stated by an older document, not by this newer one that covers the day.
        stats["withdrawn"] = stats.get("withdrawn", 0) + len(
            in_span - {(day, kind) for day, kind, _ in events}
        )
        for key in in_span:
            del merged.actions[key]
        symbol = str(row.get("asked_symbol") or "")
        fetched = row.get("fetched_at")
        fetched_at = fetched.isoformat() if isinstance(fetched, datetime) else str(fetched or "")
        for day, kind, value in events:
            merged.actions[(day, kind)] = Action(day, kind, value, symbol, fetched_at)
    stats["stated"] = stats.get("stated", 0) + len(merged.actions)
    return merged


def rows_for(security_id: str, merged: SecurityActions) -> list[dict[str, Any]]:
    """`market.security_corporate_action` rows for one security."""
    return [
        {
            "security_id": security_id,
            "ex_date": action.ex_date.isoformat(),
            "kind": action.kind,
            "value": action.value,
            "observed_symbol": action.observed_symbol,
            "source_code": SOURCE,
            "as_of": action.fetched_at,
        }
        for action in sorted(merged.actions.values(), key=lambda a: (a.ex_date, a.kind))
    ]


#: Yahoo's rows inside a security's covered span that its documents no longer state. ONLY YAHOO'S:
#: another source's row is that source's statement, and this lane has no standing to withdraw it.
RETRACT_ACTIONS_ABSENT_FROM_RAW = """
delete from market.security_corporate_action a
 using unnest(%s::uuid[], %s::date[], %s::date[]) as r(security_id, first_date, last_date)
 where a.security_id = r.security_id
   and a.source_code = %s
   and a.ex_date between r.first_date and r.last_date
   and not exists (
         select 1
           from unnest(%s::uuid[], %s::date[], %s::text[]) as h(security_id, ex_date, kind)
          where h.security_id = a.security_id and h.ex_date = a.ex_date and h.kind = a.kind)
returning a.security_id::text
"""


def retract_absent(cur: Any, merged: Mapping[str, SecurityActions]) -> int:
    """Delete each security's Yahoo actions inside its covered span that its documents lack.

    A security whose documents cover no day is left alone: an empty history is a dead symbol or a
    refusal, and retracting on it would delete a company's dividends to record a quiet night.
    """
    ids, firsts, lasts, held_ids, held_dates, held_kinds = [], [], [], [], [], []
    for sid, actions in sorted(merged.items()):
        if actions.first is None or actions.last is None:
            continue
        ids.append(sid)
        firsts.append(actions.first)
        lasts.append(actions.last)
        for day, kind in sorted(actions.actions):
            held_ids.append(sid)
            held_dates.append(day)
            held_kinds.append(kind)
    if not ids:
        return 0
    cur.execute(
        RETRACT_ACTIONS_ABSENT_FROM_RAW,
        (ids, firsts, lasts, SOURCE, held_ids, held_dates, held_kinds),
    )
    return len(cur.fetchall())
