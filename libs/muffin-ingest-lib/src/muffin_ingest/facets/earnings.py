"""Earnings dates and surprises, from nasdaq's daily calendar and Yahoo's stored summaries.

Stage 2 of Phase 6b's earnings lanes (umbrella docs/specs/2026-11-21-macro-and-events.md, decision
3: both sources, every row naming its provider, the serving views choosing per company). Rows go to
`market.earnings_calendar` and `market.security_eps_history`, keyed with the provider since the
cutover migration, so the two sources' rows for one company sit side by side.

NASDAQ, measured on the captures of 2026-11-21 (`tests/fixtures/nasdaq`):

  * money is a string: `"$4.23"`, `"$648,310,820,000"`, and a loss in parentheses;
  * `"N/A"` and `""` mean missing, for money, counts and the surprise alike;
  * `surprise` is ALREADY A PERCENT (`"8.46"`), unlike openbb's copy of it, which is a fraction;
  * `fiscalQuarterEnding` reads `"Sep/2026"`, stored as `"2026-09"`, the edge's form, and as the
    month's last day for the history (52/53-week quarters close a few days off it);
  * `time` reads `"time-after-hours"`, stored without its prefix, as the edge stored it;
  * a day with no reports is `data: null` with code 1002, which is an answer, not a failure;
  * a past day carries `eps` and `surprise`, a day to come `lastYearEPS` and `lastYearRptDt`.

YAHOO, from the `raw_yahoo_summary` documents the company lane already stores (no request):

  * `calendarEvents.earnings.earningsDate` is one or two MOMENTS (a range), dated here in the
    exchange's own zone (`quoteType.timeZoneFullName`): an Australian 08:00 report is the previous
    day in UTC;
  * `earningsHistory.history` is the last four quarters, `quarter` a month end at midnight UTC and
    `surprisePercent` a FRACTION (Toyota 0.303), stored as a percent.
"""

from __future__ import annotations

import json
import re
from calendar import monthrange
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from muffin_ingest.providers import yahoo_chart, yahoo_summary

#: The source each provider's rows are written under.
NASDAQ = "nasdaq"
YAHOO = "yfinance"

#: How long a calendar row is kept, the edge's prune: a date older than this serves no page.
CALENDAR_DAYS = 90

#: What one stored nasdaq day can be.
DAY_OUTCOMES = ("answered", "empty", "unreadable")

_MONTHS = {
    m: i
    for i, m in enumerate(
        ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
        start=1,
    )
}
_QUARTER = re.compile(r"^([A-Z][a-z]{2})/(\d{4})$")


def money(text: Any) -> float | None:
    """`"$4.23"` → 4.23, `"($0.12)"` and `"-$0.12"` → -0.12, `"N/A"` and `""` → None."""
    if not isinstance(text, str):
        return None
    s = text.strip()
    negative = (s.startswith("(") and s.endswith(")")) or s.startswith("-")
    s = s.strip("()").lstrip("-").replace("$", "").replace(",", "").strip()
    try:
        number = float(s)
    except ValueError:
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return -number if negative else number


def count(text: Any) -> int | None:
    number = money(text)
    return round(number) if number is not None and number >= 0 else None


def quarter(text: Any) -> tuple[str, date] | None:
    """`"Sep/2026"` → (`"2026-09"`, 2026-09-30); anything else → None."""
    if not isinstance(text, str):
        return None
    found = _QUARTER.match(text.strip())
    if not found or found.group(1) not in _MONTHS:
        return None
    year, month = int(found.group(2)), _MONTHS[found.group(1)]
    return f"{year:04d}-{month:02d}", date(year, month, monthrange(year, month)[1])


@dataclass(frozen=True)
class NasdaqDay:
    outcome: str
    rows: tuple[dict[str, Any], ...] = ()


def read_nasdaq(body: bytes) -> NasdaqDay:
    """One stored day, classified. `data: null` and empty `rows` both mean a day with no reports."""
    try:
        parsed = json.loads(body)
    except ValueError:
        return NasdaqDay("unreadable")
    if not isinstance(parsed, dict) or not isinstance(parsed.get("status"), dict):
        return NasdaqDay("unreadable")
    if parsed["status"].get("rCode") != 200:
        return NasdaqDay("unreadable")
    data = parsed.get("data")
    if data is None:
        return NasdaqDay("empty")
    if not isinstance(data, dict) or not isinstance(data.get("rows") or [], list):
        return NasdaqDay("unreadable")
    rows = tuple(r for r in (data.get("rows") or []) if isinstance(r, dict))
    return NasdaqDay("answered" if rows else "empty", rows)


@dataclass
class Normalised:
    #: Rows for `market.earnings_calendar`.
    calendar: list[dict[str, Any]] = field(default_factory=list)
    #: Rows for `market.security_eps_history`.
    history: list[dict[str, Any]] = field(default_factory=list)
    #: Scopes whose stored calendar rows this answer replaces: days (nasdaq) or companies (Yahoo).
    answered: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)


def _fetch_order(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row.get("fetched_at") or ""), str(row.get("url") or ""))


def normalise_nasdaq(
    rows: Sequence[Mapping[str, Any]], *, ids: Mapping[str, str], today: date
) -> Normalised:
    """Stored days to calendar and history rows, from each day's newest document.

    `ids` maps an upper-case symbol to its security. An unmatched symbol keeps a calendar row with
    no security, as the edge kept it, and gets no history row: the history is per security.
    """
    stats = {f"days_{o}": 0 for o in DAY_OUTCOMES}
    stats.update(
        {
            "days": 0,
            "legacy_rows": 0,
            "listed": 0,
            "matched": 0,
            "calendar_too_old": 0,
            "reported": 0,
            "no_quarter": 0,
        }
    )
    newest: dict[str, Mapping[str, Any]] = {}
    for row in sorted(rows, key=_fetch_order):
        day = row.get("day")
        if day is None:
            continue
        if row.get("body") is None:
            stats["legacy_rows"] += 1
            continue
        newest[str(day)] = row

    out = Normalised(stats=stats)
    keep_from = today - timedelta(days=CALENDAR_DAYS)
    for key, row in sorted(newest.items()):
        day = date.fromisoformat(key)
        stats["days"] += 1
        result = read_nasdaq(bytes(row["body"]))
        stats[f"days_{result.outcome}"] += 1
        if result.outcome == "unreadable":
            continue
        out.answered.append(key)
        fetched = str(row.get("fetched_at") or "") or None
        for item in result.rows:
            symbol = str(item.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            stats["listed"] += 1
            sid = ids.get(symbol)
            if sid:
                stats["matched"] += 1
            consensus = money(item.get("epsForecast"))
            q = quarter(item.get("fiscalQuarterEnding"))
            time = str(item.get("time") or "").strip()
            if day >= keep_from:
                out.calendar.append(
                    {
                        "source_code": NASDAQ,
                        "symbol": symbol,
                        "report_date": day,
                        "security_id": sid,
                        "name": (str(item.get("name") or "").strip() or None),
                        "eps_consensus": consensus,
                        "eps_previous": money(item.get("lastYearEPS")),
                        "num_estimates": count(item.get("noOfEsts")),
                        "period_ending": q[0] if q else None,
                        "reporting_time": time.removeprefix("time-") or None,
                        "as_of": fetched,
                    }
                )
            else:
                stats["calendar_too_old"] += 1
            actual = money(item.get("eps"))
            if actual is None or not sid:
                continue
            if q is None:
                stats["no_quarter"] += 1
                continue
            stats["reported"] += 1
            surprise = money(item.get("surprise"))
            out.history.append(
                {
                    "security_id": sid,
                    "period_ending": q[1],
                    "source_code": NASDAQ,
                    "eps_actual": actual,
                    "eps_estimated": consensus,
                    "surprise": None if consensus is None else round(actual - consensus, 6),
                    # NASDAQ'S `surprise` IS A PERCENT ALREADY: 8.46 is 8.46%.
                    "surprise_pct": surprise,
                    "reported_date": day,
                    "as_of": fetched,
                }
            )
    return out


def _zone(summary: yahoo_summary.Summary) -> ZoneInfo:
    name = summary.module("quoteType").get("timeZoneFullName")
    if isinstance(name, str) and name:
        try:
            return ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError):
            pass
    return ZoneInfo("UTC")


def _stamp(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        return None
    return int(value)


def _number(module: Mapping[str, Any], key: str) -> float | None:
    found = yahoo_summary.value(dict(module), key)
    if isinstance(found, bool) or not isinstance(found, int | float):
        return None
    number = float(found)
    return number if number == number and abs(number) != float("inf") else None


def normalise_yahoo(rows: Sequence[Mapping[str, Any]], *, today: date) -> Normalised:
    """Stored summaries to calendar and history rows, from each company's newest document.

    `answered` lists the companies whose newest document is a summary: their Yahoo calendar rows are
    replaced by what it states, which may be nothing.
    """
    stats = {
        "documents": 0,
        "legacy_rows": 0,
        "unreadable": 0,
        "absent": 0,
        "dated": 0,
        "undated": 0,
        "calendar_too_old": 0,
        "quarters": 0,
        "quarters_without_actual": 0,
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
    keep_from = today - timedelta(days=CALENDAR_DAYS)
    for sid, row in sorted(newest.items()):
        stats["documents"] += 1
        try:
            summary = yahoo_summary.parse(bytes(row["body"]))
        except yahoo_chart.YahooRefused:
            stats["unreadable"] += 1
            continue
        if summary.error is not None or not summary.modules:
            stats["absent"] += 1
            continue
        out.answered.append(sid)
        fetched = str(row.get("fetched_at") or "") or None
        symbol = str(row.get("asked_symbol") or "").strip()
        zone = _zone(summary)

        events = summary.module("calendarEvents").get("earnings")
        events = events if isinstance(events, dict) else {}
        moments = [s for s in (_stamp(v) for v in events.get("earningsDate") or []) if s]
        if moments and symbol:
            days = sorted({datetime.fromtimestamp(s, zone).date() for s in moments})
            if days[0] >= keep_from:
                stats["dated"] += 1
                price = summary.module("price")
                name = price.get("longName") or price.get("shortName")
                estimate = events.get("isEarningsDateEstimate")
                out.calendar.append(
                    {
                        "source_code": YAHOO,
                        "symbol": symbol,
                        "report_date": days[0],
                        "report_date_end": days[-1] if len(days) > 1 else None,
                        "security_id": sid,
                        "name": name.strip() if isinstance(name, str) and name.strip() else None,
                        "eps_consensus": _number(events, "earningsAverage"),
                        "eps_low": _number(events, "earningsLow"),
                        "eps_high": _number(events, "earningsHigh"),
                        "revenue_consensus": _number(events, "revenueAverage"),
                        "is_date_estimate": estimate if isinstance(estimate, bool) else None,
                        "as_of": fetched,
                    }
                )
            else:
                stats["calendar_too_old"] += 1
        else:
            stats["undated"] += 1

        history = summary.module("earningsHistory").get("history")
        for entry in history if isinstance(history, list) else []:
            if not isinstance(entry, dict):
                continue
            stamp = _stamp(yahoo_summary.value(entry, "quarter"))
            actual = _number(entry, "epsActual")
            if stamp is None or actual is None:
                stats["quarters_without_actual"] += 1
                continue
            stats["quarters"] += 1
            estimate_eps = _number(entry, "epsEstimate")
            surprise_pct = _number(entry, "surprisePercent")
            out.history.append(
                {
                    "security_id": sid,
                    # A MONTH END AT MIDNIGHT UTC, so UTC is the right reading.
                    "period_ending": datetime.fromtimestamp(stamp, UTC).date(),
                    "source_code": YAHOO,
                    "eps_actual": actual,
                    "eps_estimated": estimate_eps,
                    "surprise": None if estimate_eps is None else round(actual - estimate_eps, 6),
                    # YAHOO'S `surprisePercent` IS A FRACTION: 0.303 is 30.3%.
                    "surprise_pct": None if surprise_pct is None else round(surprise_pct * 100, 4),
                    "reported_date": None,
                    "as_of": fetched,
                }
            )
    return out
