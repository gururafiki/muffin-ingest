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


#: How each field is read from one nasdaq row, and when its value counts as stated.
_FIELDS: dict[str, Any] = {
    "name": lambda item: str(item.get("name") or "").strip() or None,
    "eps_consensus": lambda item: money(item.get("epsForecast")),
    "eps_previous": lambda item: money(item.get("lastYearEPS")),
    "num_estimates": lambda item: count(item.get("noOfEsts")),
    "fiscal_quarter": lambda item: quarter(item.get("fiscalQuarterEnding")),
    # "time-not-supplied" IS AN ABSENCE WEARING A VALUE (umbrella CLAUDE.md), so it never replaces a
    # time an earlier answer stated.
    "reporting_time": lambda item: (
        str(item.get("time") or "").strip().removeprefix("time-").replace("not-supplied", "")
        or None
    ),
    "eps_actual": lambda item: money(item.get("eps")),
    "surprise_pct": lambda item: money(item.get("surprise")),
}


def _merged(items: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], int]:
    """One symbol's fields on one day, each from the newest answer that states it.

    `items` are that symbol's rows, newest answer first. A DAY'S LATER ANSWER IS NARROWER, NOT A
    CORRECTION: once the day has passed nasdaq drops `lastYearEPS` and says "time-not-supplied" for
    a report its forward answer timed (measured 2026-11-21: 130 and 78 of the edge's rows in one
    week). Returns the fields and how many came from an older answer than the newest.
    """
    merged: dict[str, Any] = {}
    older = 0
    for name, read in _FIELDS.items():
        for depth, item in enumerate(items):
            value = read(item)
            if value is not None:
                merged[name] = value
                older += depth > 0
                break
        else:
            merged[name] = None
    return merged, older


def normalise_nasdaq(
    rows: Sequence[Mapping[str, Any]], *, ids: Mapping[str, str], today: date
) -> Normalised:
    """Stored days to calendar and history rows.

    THE NEWEST READABLE ANSWER FOR A DAY SAYS WHO REPORTS THAT DAY, and each of those companies'
    fields comes from the newest answer that states it (`_merged`). A company the newest answer no
    longer lists has moved its report away from the day, and gets no row.

    `ids` maps an upper-case symbol to its security. An unmatched symbol keeps a calendar row with
    no security, as the edge kept it, and gets no history row: the history is per security.
    """
    stats = {f"days_{o}": 0 for o in DAY_OUTCOMES}
    stats.update(
        {
            "days": 0,
            "documents": 0,
            "legacy_rows": 0,
            "listed": 0,
            "matched": 0,
            "fields_from_older_answers": 0,
            "calendar_too_old": 0,
            "reported": 0,
            "no_quarter": 0,
        }
    )
    by_day: dict[str, list[Mapping[str, Any]]] = {}
    for row in sorted(rows, key=_fetch_order, reverse=True):
        day = row.get("day")
        if day is None:
            continue
        if row.get("body") is None:
            stats["legacy_rows"] += 1
            continue
        stats["documents"] += 1
        by_day.setdefault(str(day), []).append(row)

    out = Normalised(stats=stats)
    keep_from = today - timedelta(days=CALENDAR_DAYS)
    for key, documents in sorted(by_day.items()):
        day = date.fromisoformat(key)
        stats["days"] += 1
        answers = [(d, read_nasdaq(bytes(d["body"]))) for d in documents]
        readable = [(d, a) for d, a in answers if a.outcome != "unreadable"]
        if not readable:
            stats["days_unreadable"] += 1
            continue
        newest_doc, newest = readable[0]
        stats[f"days_{newest.outcome}"] += 1
        out.answered.append(key)
        fetched = str(newest_doc.get("fetched_at") or "") or None
        rows_by_symbol: dict[str, list[Mapping[str, Any]]] = {}
        for _, answer in readable:
            for item in answer.rows:
                symbol = str(item.get("symbol") or "").strip().upper()
                if symbol:
                    rows_by_symbol.setdefault(symbol, []).append(item)
        listed = []
        for item in newest.rows:
            symbol = str(item.get("symbol") or "").strip().upper()
            if symbol and symbol not in listed:
                listed.append(symbol)
        for symbol in listed:
            stats["listed"] += 1
            sid = ids.get(symbol)
            if sid:
                stats["matched"] += 1
            fields, older = _merged(rows_by_symbol[symbol])
            stats["fields_from_older_answers"] += older
            q = fields["fiscal_quarter"]
            if day >= keep_from:
                out.calendar.append(
                    {
                        "source_code": NASDAQ,
                        "symbol": symbol,
                        "report_date": day,
                        "security_id": sid,
                        "name": fields["name"],
                        "eps_consensus": fields["eps_consensus"],
                        "eps_previous": fields["eps_previous"],
                        "num_estimates": fields["num_estimates"],
                        "period_ending": q[0] if q else None,
                        "reporting_time": fields["reporting_time"] or "not-supplied",
                        "as_of": fetched,
                    }
                )
            else:
                stats["calendar_too_old"] += 1
            actual = fields["eps_actual"]
            if actual is None or not sid:
                continue
            if q is None:
                stats["no_quarter"] += 1
                continue
            stats["reported"] += 1
            consensus = fields["eps_consensus"]
            out.history.append(
                {
                    "security_id": sid,
                    "period_ending": q[1],
                    "source_code": NASDAQ,
                    "eps_actual": actual,
                    "eps_estimated": consensus,
                    "surprise": None if consensus is None else round(actual - consensus, 6),
                    # NASDAQ'S `surprise` IS A PERCENT ALREADY: 8.46 is 8.46%.
                    "surprise_pct": fields["surprise_pct"],
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


def _answered_on(fetched: str | None, zone: ZoneInfo) -> date | None:
    """The day Yahoo answered, in the exchange's zone, or None when the stamp cannot be read."""
    if not fetched:
        return None
    try:
        moment = datetime.fromisoformat(fetched)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(zone).date()


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

    A DATE ALREADY PAST WHEN YAHOO ANSWERED CARRIES NO ESTIMATE. `calendarEvents` keeps the date
    of the report a company last made until it sets the next one, while `earningsAverage`, `Low`,
    `High` and `revenueAverage` are always the CURRENT quarter's, the one not yet reported.
    Measured on SAP's answer of 2026-11-19: the date was 2026-10-21, its average 1.98 matched
    `earningsTrend`'s `0q` (the quarter ending 2026-12-31), and the October report's own estimate
    was 1.83 (`earningsHistory`). Pairing them put "was expected 1.98" beside the wrong report.
    Judged by the day Yahoo answered, never the day of the parse, so a replay keeps an estimate that
    was right when it was fetched.
    """
    stats = {
        "documents": 0,
        "legacy_rows": 0,
        "unreadable": 0,
        "absent": 0,
        "dated": 0,
        "undated": 0,
        "calendar_too_old": 0,
        "past_without_estimate": 0,
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
                answered_on = _answered_on(fetched, zone)
                past = answered_on is not None and days[-1] < answered_on
                if past:
                    stats["past_without_estimate"] += 1
                out.calendar.append(
                    {
                        "source_code": YAHOO,
                        "symbol": symbol,
                        "report_date": days[0],
                        "report_date_end": days[-1] if len(days) > 1 else None,
                        "security_id": sid,
                        "name": name.strip() if isinstance(name, str) and name.strip() else None,
                        "eps_consensus": None if past else _number(events, "earningsAverage"),
                        "eps_low": None if past else _number(events, "earningsLow"),
                        "eps_high": None if past else _number(events, "earningsHigh"),
                        "revenue_consensus": None if past else _number(events, "revenueAverage"),
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
