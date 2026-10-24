"""A company's statements from Yahoo's fundamentals-timeseries, spelled as openbb spelled them.

THE SPELLING IS THE CONTRACT. `market.metric_source_field` maps each metric to the field name each
provider uses, `derive_security_metrics` reads `data->>'<field>'`, and the app reads the same keys.
The edge wrote openbb's yfinance rows, so the keys are openbb's: yfinance's type name without its
frequency prefix, through openbb's `to_snake_case`, then openbb's per-model aliases
(`pretax_income` becomes `total_pre_tax_income`). Reproduced here exactly, so nothing downstream
changes; `OPENBB_FIELD` is copied from openbb-yfinance 1.6.3, and the image check compares it with
the installed models.

A ROW CARRIES EVERY LINE THE STATEMENT HAS FOR ANY PERIOD, null where this period lacks it. That is
what openbb's table produced (a frame indexed by every line with data, one column per period), and
the 154,629 stored rows have that shape: up to 36 null keys in a row.

THREE THINGS THE EDGE COULD NOT DO, because openbb dropped what they need:

  * THE CURRENCY. Every point states `currencyCode`, the reporting currency, so a row says what its
    figures are in. All 154,629 Yahoo rows the edge wrote have it null.
  * HALF-YEARS. Semi-annual reporters (VOD.L, NESN.SW, BHP.AX) come back in the quarterly series
    labelled `3M`. See `cadence`.
  * EVERY PERIOD YAHOO RETURNS, not openbb's newest five (and the edge's four annual).
"""

from __future__ import annotations

import itertools
import json
import math
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from muffin_ingest.facets import price_chart
from muffin_ingest.facets.prices import Subject
from muffin_ingest.providers import yahoo_chart, yahoo_summary, yahoo_timeseries
from muffin_ingest.providers.documents import Document

#: The source these rows are written under. The edge wrote the same, so a period it wrote is the
#: same row this lane rewrites.
SOURCE = "yfinance"

#: openbb-yfinance 1.6.3's `__alias_dict__` per statement model, INVERTED: the snake-cased yfinance
#: key on the left, openbb's field name on the right. Every other key keeps its snake-cased name.
OPENBB_FIELD: dict[str, dict[str, str]] = {
    "income": {
        "basic_average_shares": "weighted_average_basic_shares_outstanding",
        "basic_eps": "basic_earnings_per_share",
        "diluted_average_shares": "weighted_average_diluted_shares_outstanding",
        "diluted_eps": "diluted_earnings_per_share",
        "net_income_common_stockholders": "net_income_attributable_to_common_shareholders",
        "pretax_income": "total_pre_tax_income",
        "research_and_development": "research_and_development_expense",
        "selling_general_and_administration": "selling_general_and_admin_expense",
    },
    "balance": {
        "current_assets": "total_current_assets",
        "gross_ppe": "plant_property_equipment_gross",
        "inventory": "inventories",
        "net_ppe": "plant_property_equipment_net",
        "other_short_term_investments": "short_term_investments",
        "receivables": "net_receivables",
        "stockholders_equity": "total_common_equity",
        "total_equity_gross_minority_interest": "total_equity_non_controlling_interests",
    },
    "cash": {
        "changes_in_cash": "net_change_in_cash_and_equivalents",
        "common_stock_issuance": "issuance_of_common_equity",
        "common_stock_payments": "repurchase_of_common_equity",
        "payment_of_dividends": "cash_dividends_paid",
        "purchase_of_ppe": "investments_in_property_plant_and_equipment",
    },
}


def to_snake_case(name: str) -> str:
    """openbb's `to_snake_case`, verbatim (openbb_core.provider.utils.helpers)."""
    first = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", first).lower().replace(" ", "_").replace("__", "_")


def field_name(statement: str, key: str) -> str:
    """The key a stored row uses for one of Yahoo's types (without its frequency prefix)."""
    snake = to_snake_case(key)
    return OPENBB_FIELD[statement].get(snake, snake)


# --- stage 1 -------------------------------------------------------------------------------------

#: EVERY column stage 1 adds to a document row. A test holds `raw_rows` to exactly this set.
CONTEXT_COLUMNS = frozenset({"security_id", "asked_symbol", "provider", "frequency"})


def raw_rows(
    subject: Subject, document: Document, *, run_id: str, frequency: str
) -> list[dict[str, Any]]:
    """One row per response, the body byte for byte, plus who it was asked for and which frequency.

    The frequency is recorded rather than read back off the URL: it decides how the body's type
    names are read, and stage 2 must not parse a URL to learn it.
    """
    row = document.as_row(run_id)
    row.update(
        {
            "security_id": subject.security_id,
            "asked_symbol": subject.symbol,
            "provider": "yahoo",
            "frequency": frequency,
        }
    )
    return [row]


# --- stage 2 -------------------------------------------------------------------------------------

#: A gap between two period ends that is a HALF-YEAR, in days. 181-184 measured.
HALF_GAP = (170, 195)


def cadence(period_ends: Sequence[date]) -> str:
    """`half` when a company's quarterly series is a half-yearly one, else `quarter`.

    EVERY GAP MUST BE A HALF-YEAR, AND THAT IS STRICTER THAN THE RULE FIRST WRITTEN. The spec said a
    period 170-195 days after the previous one is a half. Measured on production 2026-10-11, 2,210
    securities have such a gap in their Yahoo quarter series, and 1,451 of them ALSO have 90-day
    gaps: they are quarterly reporters missing a quarter, and the period after the hole carries a
    full three-month income statement (600011.SS 2025-12-31, 184 days after 2025-06-30). The per-gap
    rule would have filed those quarters as half-years. 755 have only half-year gaps, which is
    VOD.L's shape (2025-03-31, 2025-09-30, 2026-03-31).

    A series with a single period has no gap to read and stays `quarter`, which is what it was.
    """
    ends = sorted(set(period_ends))
    if len(ends) < 2:
        return "quarter"
    gaps = [(b - a).days for a, b in itertools.pairwise(ends)]
    return "half" if all(HALF_GAP[0] <= g <= HALF_GAP[1] for g in gaps) else "quarter"


@dataclass
class Normalised:
    """`market.security_statement` rows, and what was decided on the way."""

    rows: list[dict[str, Any]]
    stats: dict[str, int] = field(default_factory=dict)
    #: Type names Yahoo sent that are in none of the three statements' lists, with a count.
    unknown_keys: dict[str, int] = field(default_factory=dict)


def _fetch_order(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row.get("fetched_at") or ""), str(row.get("url") or ""))


def _fetched_at(row: Mapping[str, Any]) -> str | None:
    value = row.get("fetched_at")
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value else None


def normalise(
    rows: Sequence[Mapping[str, Any]], *, known_currencies: Collection[str] | None = None
) -> Normalised:
    """Stored timeseries documents to statement rows. No provider call.

    A VALUE IS THE NEWEST ONE ANY DOCUMENT CARRIES FOR ITS LINE AND PERIOD. Yahoo keeps a rolling
    window PER LINE, not per period: measured 2026-10-11, Samsung's quarterly lines now start at
    2025-06-30, and its 2025-03-31 keeps ten lines of the ~180 the edge stored in August (`ebit`
    9.33T among those gone). So a later document covering a period with fewer lines is a narrower
    window, not a retraction, and a line it omits keeps the value an older document carried. A
    restatement is still taken: the newer document's value for the same line and period wins.

    A ROW CARRIES EVERY LINE THE STATEMENT HAS FOR ANY PERIOD, null where no document has a value,
    which is the shape openbb's table produced and the stored rows have.

    A CURRENCY IS WHAT THE PERIOD'S VALUES SAY. They agree in every document measured; when they do
    not, the most common code wins and the row is counted under `mixed_currency`, because one
    currency column cannot hold two and a guess would be worse than a count. Yahoo's own spellings
    are mapped (`price_chart.YAHOO_REPORTING_CURRENCY`) and a code `known_currencies` lacks is
    withheld, so a row can never carry a code the metrics cannot reference.
    """
    stats = {
        "documents": 0,
        "legacy_rows": 0,
        "unreadable": 0,
        "errors": 0,
        "empty_documents": 0,
        "points": 0,
        "non_finite": 0,
        "securities": 0,
        "statements": 0,
        "half_years": 0,
        "mixed_currency": 0,
        "no_currency": 0,
        "currency_mapped": 0,
        "currency_withheld": 0,
    }
    unknown: dict[str, int] = {}
    by_security: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        sid = row.get("security_id")
        if sid is not None:
            by_security.setdefault(str(sid), []).append(row)

    out: list[dict[str, Any]] = []
    for sid, documents in sorted(by_security.items()):
        # (frequency, statement, period end) -> line -> (value, currency), newest document last
        values: dict[tuple[str, str, date], dict[str, tuple[float | None, str | None]]] = {}
        # The newest fetch that supplied any value to a period: when the row was last stated.
        stamped: dict[tuple[str, str, date], str | None] = {}
        # Every line a statement has for any period, per frequency: a row carries all of them.
        lines: dict[tuple[str, str], set[str]] = {}
        quarter_ends: set[date] = set()
        for document in sorted(documents, key=_fetch_order):
            body = document.get("body")
            frequency = str(document.get("frequency") or "")
            if body is None or frequency not in yahoo_timeseries.FREQUENCIES:
                stats["legacy_rows"] += 1
                continue
            stats["documents"] += 1
            try:
                series = yahoo_timeseries.parse(bytes(body), frequency)
            except yahoo_chart.YahooRefused:
                stats["unreadable"] += 1
                continue
            if series.error is not None:
                stats["errors"] += 1
                continue
            for name in series.unknown_keys:
                unknown[name] = unknown.get(name, 0) + 1
            if not series.points:
                stats["empty_documents"] += 1
                continue
            fetched = _fetched_at(document)
            for point in series.points:
                stats["points"] += 1
                value = point.value
                if value is not None and not math.isfinite(value):
                    stats["non_finite"] += 1
                    value = None
                line = field_name(point.statement, point.key)
                lines.setdefault((frequency, point.statement), set()).add(line)
                if frequency == "quarterly":
                    quarter_ends.add(point.as_of)
                if value is None:
                    # A point with no value is no evidence against one an older document carried.
                    continue
                key = (frequency, point.statement, point.as_of)
                values.setdefault(key, {})[line] = (value, point.currency)
                stamped[key] = fetched

        if not values:
            continue
        stats["securities"] += 1
        quarterly_type = cadence(sorted(quarter_ends))
        for (frequency, statement, end), carried in sorted(values.items()):
            period_type = "annual" if frequency == "annual" else quarterly_type
            stats["half_years"] += int(period_type == "half")
            counts: dict[str, int] = {}
            for _, code in carried.values():
                if code:
                    counts[code] = counts.get(code, 0) + 1
            currency = _currency_of(counts, stats, known_currencies)
            data: dict[str, Any] = {"period_ending": end.isoformat()}
            for line in sorted(lines.get((frequency, statement), ())):
                data[line] = carried[line][0] if line in carried else None
            out.append(
                {
                    "security_id": sid,
                    "statement": statement,
                    "period_ending": end.isoformat(),
                    "period_type": period_type,
                    "currency": currency,
                    "data": json.dumps(data, sort_keys=True),
                    "source_code": SOURCE,
                    "as_of": stamped[(frequency, statement, end)],
                }
            )
            stats["statements"] += 1
    return Normalised(rows=out, stats=stats, unknown_keys=unknown)


def _currency_of(
    currencies: Mapping[str, int], stats: dict[str, int], known: Collection[str] | None
) -> str | None:
    """The period's currency, if its points name a plain three-letter code we can reference."""
    codes: dict[str, int] = {}
    mapped = False
    for code, n in currencies.items():
        ours = price_chart.YAHOO_REPORTING_CURRENCY.get(code, code)
        if len(ours) == 3 and ours.isalpha() and ours.isupper():
            codes[ours] = codes.get(ours, 0) + n
            mapped = mapped or ours != code
    if not codes:
        stats["no_currency"] += 1
        return None
    if len(codes) > 1:
        stats["mixed_currency"] += 1
    chosen = sorted(codes.items(), key=lambda item: (-item[1], item[0]))[0][0]
    if known is not None and chosen not in known:
        stats["currency_withheld"] += 1
        return None
    stats["currency_mapped"] += int(mapped)
    return chosen


# --- what a visit asks for -----------------------------------------------------------------------

#: How long statements may go unasked whatever the summary says: restatements, and lines Yahoo
#: fills in after first publishing a period.
REFRESH_AFTER_DAYS = 90

#: How much later than the newest period we hold Yahoo must state one before it counts as NEW. A
#: new period is a quarter or a half later; 20 days absorbs a 52/53-week calendar, which Yahoo's
#: summary states as it is (AAPL `mostRecentQuarter` 2026-06-27) and its timeseries files under the
#: month end (2026-06-30).
NEW_PERIOD_DAYS = 20

#: How many asks a newly stated period gets before the lane waits for the refresh instead. Yahoo's
#: summary can name a period its timeseries does not carry yet, or ever; without a bound such a
#: company would cost two requests every week for nothing.
TRIES_PER_PERIOD = 3


def stated_period(summary: yahoo_summary.Summary) -> date | None:
    """The newest period end Yahoo says the company has reported, from the summary.

    MEASURED 2026-10-10/11 ON 17 LISTINGS: `defaultKeyStatistics.mostRecentQuarter` equals the
    newest
    period end in the timeseries for every one of them, half-yearly reporters included (VOD.L and
    NPN.JO state 2026-03-31, which is their half-year), and `lastFiscalYearEnd` the newest annual
    one. That makes it a direct statement of a period we may not hold.

    THE EARNINGS DATE IS NOT. `calendarEvents.earnings.earningsDate` names the NEXT report, so the
    moment a company has reported, the date that would have said "fetch" is already gone from the
    newest summary, and the summary keeps only its newest document.
    """
    key_stats = summary.module("defaultKeyStatistics")
    days = []
    for name in ("mostRecentQuarter", "lastFiscalYearEnd"):
        stamp = yahoo_summary.value(key_stats, name)
        if isinstance(stamp, int | float) and not isinstance(stamp, bool) and stamp > 0:
            days.append(datetime.fromtimestamp(stamp, UTC).date())
    return max(days) if days else None


def held_period(stored: Sequence[Mapping[str, Any]]) -> date | None:
    """The newest period end the stored documents carry: the newest document with points of each
    frequency, read newest first, so a company's whole history is not re-parsed on every visit."""
    newest: dict[str, date] = {}
    for row in sorted(stored, key=_fetch_order, reverse=True):
        frequency = str(row.get("frequency") or "")
        body = row.get("body")
        if frequency in newest or body is None or frequency not in yahoo_timeseries.FREQUENCIES:
            continue
        try:
            series = yahoo_timeseries.parse(bytes(body), frequency)
        except yahoo_chart.YahooRefused:
            continue
        if series.points:
            newest[frequency] = max(p.as_of for p in series.points)
            if len(newest) == len(yahoo_timeseries.FREQUENCIES):
                break
    return max(newest.values()) if newest else None


def due(
    stored: Sequence[Mapping[str, Any]],
    *,
    stated: date | None,
    today: date,
    force: bool = False,
) -> str | None:
    """Why this company's statements are asked for on this visit, or None when they are not.

    `stated` is the period the company's FRESH summary names (`stated_period`): the summary is
    fetched first in the same visit, so this reads today's answer rather than last week's.
    """
    if force:
        return "forced"
    fetches = [d for d in (_fetch_day(row) for row in stored) if d is not None]
    if not fetches:
        return "never_fetched"
    if (today - max(fetches)).days >= REFRESH_AFTER_DAYS:
        return "refresh_due"
    if stated is None:
        return None
    held = held_period(stored)
    if held is not None and stated <= held + timedelta(days=NEW_PERIOD_DAYS):
        return None
    # ASKS SINCE THE PERIOD ENDED, by day: each visit stores two documents, one per frequency.
    tries = len({d for d in fetches if d >= stated})
    return "new_period" if tries < TRIES_PER_PERIOD else None


def _fetch_day(row: Mapping[str, Any]) -> date | None:
    stamp = _fetched_at(row)
    if not stamp:
        return None
    try:
        return date.fromisoformat(stamp[:10])
    except ValueError:
        return None
