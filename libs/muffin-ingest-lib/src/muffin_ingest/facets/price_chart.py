"""Daily bars from Yahoo's chart documents, each labelled with the currency it is quoted in.

THE LABEL IS OBSERVED, NOT GUESSED. Until 2026-10-10 a bar's `currency_code` came from the primary
listing's currency, falling back to `security.currency_code` — which holds the currency of a line a
US fund holds, or a metrics response's REPORTING currency. Neither is the currency of the line the
lane prices. Seven of thirteen sampled labels were wrong (VOD.L `EUR` for pence, ALG.KW `KWD` for
fils, 0992.HK `USD` for Hong Kong dollars), and a ratio divided by the wrong one is out by 100x,
1,000x or a plain exchange rate. Yahoo states the quote currency in every chart response, so the
price lane now reads it. Spec: umbrella
`docs/specs/2026-10-06-the-price-lane-reads-the-quote-currency.md`.

EVERYTHING HERE IS PURE: stored documents in, `market.price_bar` rows out. Stage 1 stores each
response whole (`raw_rows`); every rule below runs against those files, so changing one costs a
re-parse and never a re-fetch.

THREE RULES DECIDE A BAR'S LABEL, in this order:

  1. The provider's code maps to ours EXPLICITLY (`YAHOO_CURRENCY`). Yahoo spells pence `GBp` and
     cents `ZAc`; case-folding turns `GBp` into `GBP`, which is a hundred times the price.
  2. The code must be one `market.currency` holds. Every currency column references that table, so
     an unknown code would fail the whole write; it is withheld and counted instead.
  3. A bar before the newest CHANGE OF UNIT gets no label, and nor does a bar inside a BOUNCE.
     Yahoo relabels a whole history when a venue changes unit: AMRM.TA's bars before 2026-05-18 are
     shekels, its later ones agorot, and every one of them says `ILA`. Only the size of the jump
     shows where the unit changed, and a label inferred from a jump would be a guess (the user's
     decision 3a, 2026-10-10), so those bars carry none and the ratio chart shows a gap.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from muffin_ingest.facets.prices import Subject
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document

#: The bar size this lane asks for, and the only one stage 2 will publish as a daily bar.
INTERVAL = "1d"

#: Yahoo's quote-currency codes that are not ours, mapped one by one. NEVER CASE-FOLDED: Yahoo
#: separates pence from pounds by the case of one letter, so `GBp.upper()` is `GBP`, a hundred times
#: the price, and only `ZAc` happens to survive it. `ILA` and `KWF` are listed although they map to
#: themselves, so the four subunit spellings Yahoo uses are visible in one place.
YAHOO_CURRENCY: dict[str, str] = {"GBp": "GBX", "ZAc": "ZAC", "ILA": "ILA", "KWF": "KWF"}


def quote_currency(code: object, known: Collection[str]) -> str | None:
    """Our code for the currency Yahoo says a line is quoted in, or None when there is none to give.

    A three-letter upper-case code passes through only if `market.currency` holds it. Anything else
    — a code we have not seeded, a spelling not in `YAHOO_CURRENCY` — is withheld rather than
    guessed, and the caller counts it.
    """
    if not isinstance(code, str) or not code:
        return None
    ours = YAHOO_CURRENCY.get(code, code)
    if len(ours) == 3 and ours.isalpha() and ours.isupper() and ours in known:
        return ours
    return None


# --- stage 1 -------------------------------------------------------------------------------------

#: EVERY column stage 1 adds to a document row, beside the document's own (`Document.as_row`). A
#: test holds `raw_rows` to exactly this set, so a column derived from the body cannot creep in.
CONTEXT_COLUMNS = frozenset(
    {"security_id", "asked_symbol", "provider", "interval", "period1", "period2"}
)


def raw_rows(
    subject: Subject,
    document: Document,
    *,
    run_id: str,
    period1: int,
    period2: int,
) -> list[dict[str, Any]]:
    """ONE ROW PER RESPONSE, carrying the body byte for byte, plus who it was asked for and how.

    `security_id` is ours and is written anyway, as `asked_symbol` is: stage 2 must not re-resolve
    the symbol with today's mapping, or a symbol repaired between the fetch and the transform would
    re-attribute a whole history. The window is recorded because it decides what the body contains.
    """
    row = document.as_row(run_id)
    row.update(
        {
            "security_id": subject.security_id,
            "asked_symbol": subject.symbol,
            "provider": "yahoo",
            "interval": INTERVAL,
            "period1": period1,
            "period2": period2,
        }
    )
    return [row]


#: HOW FAR BEHIND ITS NEWEST STORED POINT AN EXTENSION RE-READS. The vendor is asked once per
#: ticker whatever the window, so a wider one costs bytes, never a request. It buys a rule where
#: there was an accident: Yahoo closes a US session with a NaN at 00:00 UTC and sometimes leaves a
#: day `null` for days before filling it (2026-09-22 for KO, CZR, EMBC and PRAA), and a week of
#: re-reading asks again for both at the next visit.
REREAD = timedelta(days=7)

#: HOW OLD A FULL HISTORY MAY GET BEFORE A VISIT RELOADS IT INSTEAD OF EXTENDING IT. Still one
#: request. It bounds three things nothing else does: a restatement deeper than `REREAD`, a split
#: whose event the extension missed, and the file itself, which an extension grows by a document
#: per visit and a reload replaces with one.
RELOAD_AFTER = timedelta(days=90)


@dataclass(frozen=True)
class Plan:
    """What one visit asks a security for: everything (`start` None) or the days since `start`."""

    start: date | None
    #: Why — `never_loaded`, `symbol_changed`, `reload_due` or `extend`. Counted per run.
    reason: str
    #: The day the newest stored full history was fetched; a split on or after it makes it stale.
    loaded_on: date | None = None


def plan(stored: Sequence[Mapping[str, Any]], symbol: str, today: date) -> Plan:
    """Decide from what a partition already holds, before asking anything.

    A SYMBOL CHANGE RESTARTS THE HISTORY. A stored document asked with another symbol is another
    listing's, and extending it would append this listing to that one (69 openbb histories held two
    symbols on 2026-10-04). A full load that answers REPLACES the file, so the old one goes with it.

    THE WATERMARK IS THE NEWEST STORED POINT, read from the newest document that has one. Requests
    advance in time, so that is usually the smallest document and the only one parsed; the full
    history, the big one, is reached only when nothing newer has a point.
    """
    if any(row.get("asked_symbol") != symbol for row in stored):
        return Plan(start=None, reason="symbol_changed" if stored else "never_loaded")
    # THE OLDEST FULL LOAD IN THE FILE IS THE ONE THAT ANSWERED. One that answers REPLACES the file,
    # so anything older is gone; a later one still in it did not replace (a refusal, an absence, a
    # history with no points) and says nothing about how fresh the stored history is.
    loads = [d for d in (_fetched_on(row) for row in stored if row.get("period1") == 0) if d]
    if not loads:
        return Plan(start=None, reason="never_loaded")
    loaded_on = min(loads)
    if today - loaded_on >= RELOAD_AFTER:
        return Plan(start=None, reason="reload_due")
    for row in sorted(stored, key=fetch_order, reverse=True):
        body = row.get("body")
        if body is None:
            continue
        try:
            series = yahoo_chart.parse(bytes(body))
        except yahoo_chart.YahooRefused:
            continue
        if series.points and series.granularity in (None, INTERVAL):
            newest = max(point.as_of for point in series.points)
            # AT LEAST A DAY WIDE: `period1 == period2` is a degenerate window.
            start = min(newest - REREAD, today - timedelta(days=1))
            return Plan(start=start, reason="extend", loaded_on=loaded_on)
    return Plan(start=None, reason="never_loaded")


def stale_after_split(series: yahoo_chart.Series, loaded_on: date | None) -> bool:
    """Has a split restated the stored history since it was loaded?

    Yahoo's `close` is split-adjusted, so a split on or after the day the full history was fetched
    leaves every older stored close at the pre-split price. Measured 2026-10-10: the openbb lane's
    stored histories were adjusted only by ACCIDENT — a batch shared one window, so one security
    with an old watermark dragged the whole batch's re-read back months. Per-security windows end
    the accident, so the rule has to be explicit. ON or after, because a split on the fetch day may
    or may not have been applied when the body was served.
    """
    if loaded_on is None:
        return False
    return any(day >= loaded_on for day in yahoo_chart.split_days(series))


def _fetched_on(row: Mapping[str, Any]) -> date | None:
    value = row.get("fetched_at")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and len(value) >= 10:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


# --- stage 2: one security's documents to one daily series ---------------------------------------


@dataclass
class History:
    """One security's documents, merged: the bars to publish and what the provider currently states.

    `held` is EVERY date the documents still carry, usable or not. The retraction reads it: a date a
    document holds with a null close is a session the provider has, so a bar stored for it is not
    deleted on the strength of a padded row.
    """

    bars: dict[date, yahoo_chart.Point] = field(default_factory=dict)
    held: set[date] = field(default_factory=set)
    #: The provider's own spelling of the quote currency, from the newest daily document with one.
    currency: str | None = None


def fetch_order(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row.get("fetched_at") or ""), str(row.get("url") or ""))


def merge(rows: Sequence[Mapping[str, Any]], stats: dict[str, int]) -> History:
    """One security's documents, oldest fetch first, into the series they now state.

    THE NEWEST DOCUMENT IS THE PROVIDER'S ANSWER FOR THE DATES IT COVERS. A point it carries
    replaces an older one for the same date; a date inside its first-to-last span that it no longer
    carries is gone, because the provider has stopped saying it. Without the second half a bar Yahoo
    removed would be held for ever by the older document that once had it — the
    upsert-cannot-retract defect one layer down.

    A NULL CLOSE DOES NOT ERASE A REAL ONE. Yahoo pads a session it has not closed yet, and leaves
    a day `null` for days afterwards before filling it (2026-09-22 for KO, CZR, EMBC and PRAA,
    measured 2026-09-24). Neither is a restatement, so an older usable close for that date stands.
    A LIVE QUOTE is never a close either: its stamp equals `regularMarketTime`. On Saturday
    2026-10-10 BHP.AX's body still ended in one dated the Friday, beside that Friday's completed
    bar, which is the bar published.

    A DOCUMENT THAT IS NOT DAILY IS REFUSED WHOLE. `range=max` came back quarterly for AAPL however
    the interval was spelled (measured 2026-10-10), and a quarterly close stored as a daily bar is
    wrong on every day but one.
    """
    history = History()
    state: dict[date, yahoo_chart.Point | None] = {}
    for row in sorted(rows, key=fetch_order):
        body = row.get("body")
        if body is None:
            stats["legacy_rows"] = stats.get("legacy_rows", 0) + 1
            continue
        stats["documents"] = stats.get("documents", 0) + 1
        try:
            series = yahoo_chart.parse(bytes(body))
        except yahoo_chart.YahooRefused:
            stats["unreadable"] = stats.get("unreadable", 0) + 1
            continue
        if series.error is not None:
            stats["absences"] = stats.get("absences", 0) + 1
            continue
        if series.granularity not in (None, INTERVAL):
            stats["not_daily"] = stats.get("not_daily", 0) + 1
            continue
        if series.currency is not None:
            history.currency = series.currency
        if not series.points:
            continue
        stats["points"] = stats.get("points", 0) + len(series.points)
        stats["live_points"] = stats.get("live_points", 0) + series.live_points
        stats["null_closes"] = stats.get("null_closes", 0) + series.null_closes
        stats["undatable"] = stats.get("undatable", 0) + series.undatable

        carried = {point.as_of for point in series.points}
        first, last = min(carried), max(carried)
        for day in [d for d in state if first <= d <= last and d not in carried]:
            del state[day]
            stats["withdrawn"] = stats.get("withdrawn", 0) + 1
        for point in series.points:
            usable = point.close is not None and not point.is_live
            if usable:
                # RESTATED means a different close for a day already held — a correction, or a
                # split adjustment the reload rule should have caught. Re-reading an unchanged day
                # is what every extension does for a week and is not counted.
                held_before = state.get(point.as_of)
                if isinstance(held_before, yahoo_chart.Point) and held_before.close != point.close:
                    stats["restated"] = stats.get("restated", 0) + 1
                state[point.as_of] = point
            elif point.as_of not in state:
                state[point.as_of] = None

    history.held = set(state)
    history.bars = {day: point for day, point in state.items() if point is not None}
    return history


# --- stage 2: where the current unit starts ------------------------------------------------------

#: The subunit factors in use: pence, cents and agorot are a hundredth of their parent, fils a
#: thousandth (`fx.SUBUNITS`).
SUBUNIT_FACTORS: tuple[float, ...] = (100.0, 1000.0)

#: How far a close-to-close step may sit from a factor and still be read as one, as a ratio either
#: side. MEASURED, 2026-10-10, over two years of `price_bar`: the Tel Aviv changes of 2026-05-18
#: stepped 96.6x to 102.6x and DIA.MC's 1,029.9x; ordinary noise above 5x stays far outside.
FACTOR_TOLERANCE = 1.25

#: How soon a step must be undone to be a BOUNCE rather than a change of unit, in bars. Measured the
#: same day: every bounce found returned within one to six bars (the Johannesburg lines on
#: 2025-01-10 in one, XPML11.SA in three, ROSE.L in five and six), and no change of unit was undone
#: at all. Ten bars is two trading weeks.
BOUNCE_HORIZON = 10


def subunit_step(ratio: float) -> float | None:
    """The factor a close-to-close ratio matches, signed: +f for a rise by f, -f for a fall by f."""
    for factor in SUBUNIT_FACTORS:
        if factor / FACTOR_TOLERANCE <= ratio <= factor * FACTOR_TOLERANCE:
            return factor
        if 1 / (factor * FACTOR_TOLERANCE) <= ratio <= FACTOR_TOLERANCE / factor:
            return -factor
    return None


@dataclass(frozen=True)
class Units:
    """Which bars are quoted in the unit the provider's label now names."""

    #: Index of the first bar after the newest change of unit; 0 when the series never changed.
    since: int
    #: Indices of bars inside a bounce: quoted in the other unit for a day or a week, then back.
    bounced: frozenset[int]
    changes: int
    bounces: int


def units(closes: Sequence[float]) -> Units:
    """Find the newest change of unit and every bounce in a series of usable closes, oldest first.

    A SUBUNIT-SIZED STEP IS ONE OF TWO THINGS, and only time tells them apart. A step undone by the
    opposite step within `BOUNCE_HORIZON` bars is a BOUNCE: one session Yahoo quoted in rand rather
    than cents, measured on six Johannesburg lines at once on 2025-01-10. A step that stands is a
    CHANGE OF UNIT. Treating every jump as a change would have unlabelled whole histories for one
    bad session: of 96 steps above 5x over two years, 35 were subunit-sized and most of those were
    bounces. Treating none as a change would have labelled AMRM.TA's shekels as agorot.
    """
    since = 0
    changes = bounces = 0
    bounced: set[int] = set()
    i = 1
    while i < len(closes):
        step = subunit_step(closes[i] / closes[i - 1])
        if step is None:
            i += 1
            continue
        back = next(
            (
                j
                for j in range(i + 1, min(len(closes), i + 1 + BOUNCE_HORIZON))
                if subunit_step(closes[j] / closes[j - 1]) == -step
            ),
            None,
        )
        if back is None:
            since = i
            changes += 1
            i += 1
        else:
            bounced.update(range(i, back))
            bounces += 1
            i = back + 1
    return Units(since=since, bounced=frozenset(bounced), changes=changes, bounces=bounces)


# --- stage 2: the whole run ----------------------------------------------------------------------


@dataclass
class Normalised:
    """`market.price_bar` rows, what the provider still holds per security, and what was decided."""

    rows: list[dict[str, Any]]
    #: Every date each security's documents still carry, for `prices.retract_bars_absent_from_raw`.
    held: dict[str, set[date]]
    #: The label each security's NEWEST bar was given — None where it was withheld.
    newest_label: dict[str, str | None]
    stats: dict[str, int]
    #: Provider codes that could not be mapped to one of ours, with how many securities sent each.
    unknown_currencies: dict[str, int]


def normalise(
    rows: Sequence[Mapping[str, Any]],
    *,
    known: Collection[str],
    source_code: str,
    window: tuple[date, date],
) -> Normalised:
    """Stored documents to labelled bars. No provider call, so every rule here is free to re-run.

    `window` is `[start, end)`: the end is today, because somewhere a market is open and its bar
    for today is a session in progress whose close is not a close yet.
    """
    stats: dict[str, int] = {
        "documents": 0,
        "legacy_rows": 0,
        "absences": 0,
        "not_daily": 0,
        "unreadable": 0,
        "points": 0,
        "live_points": 0,
        "null_closes": 0,
        "undatable": 0,
        "withdrawn": 0,
        "restated": 0,
        "securities": 0,
        "bars": 0,
        "bars_unlabelled": 0,
        "bars_before_a_unit_change": 0,
        "bars_in_a_bounce": 0,
        "unit_changes_found": 0,
        "unit_bounces_found": 0,
        "currency_unknown": 0,
        "outside_window": 0,
    }
    by_security: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        sid = row.get("security_id")
        if sid is not None:
            by_security.setdefault(str(sid), []).append(row)

    out: list[dict[str, Any]] = []
    held: dict[str, set[date]] = {}
    newest_label: dict[str, str | None] = {}
    unknown: dict[str, int] = {}
    for sid, documents in sorted(by_security.items()):
        history = merge(documents, stats)
        if history.held:
            held[sid] = history.held
        bars = sorted(history.bars.items())
        bars = [(day, point) for day, point in bars if window[0] <= day < window[1]]
        stats["outside_window"] += len(history.bars) - len(bars)
        if not bars:
            continue
        stats["securities"] += 1
        label = quote_currency(history.currency, known)
        if label is None:
            stats["currency_unknown"] += 1
            if history.currency:
                unknown[history.currency] = unknown.get(history.currency, 0) + 1
        found = units([point.close or 0.0 for _, point in bars])
        stats["unit_changes_found"] += found.changes
        stats["unit_bounces_found"] += found.bounces
        for index, (day, point) in enumerate(bars):
            before = index < found.since
            bounced = index in found.bounced
            stats["bars_before_a_unit_change"] += int(before)
            stats["bars_in_a_bounce"] += int(bounced and not before)
            code = None if before or bounced else label
            stats["bars_unlabelled"] += int(code is None)
            volume = point.quote.get("volume")
            out.append(
                {
                    "security_id": sid,
                    "trade_date": day.isoformat(),
                    "close": point.close,
                    "volume": int(volume)
                    if isinstance(volume, int | float) and not isinstance(volume, bool)
                    else None,
                    "currency_code": code,
                    "source_code": source_code,
                }
            )
        newest_label[sid] = out[-1]["currency_code"]
        stats["bars"] += len(bars)
    return Normalised(
        rows=out, held=held, newest_label=newest_label, stats=stats, unknown_currencies=unknown
    )


#: The label each security's newest stored bar carries, before this run writes — one primary-key
#: probe per security, so a run reads 25 rows to say how many labels it changed.
NEWEST_LABELS = """
select distinct on (security_id) security_id::text, currency_code
  from market.price_bar
 where security_id = any(%s::uuid[])
 order by security_id, trade_date desc
"""


def newest_labels(conn: Any, security_ids: Sequence[str]) -> dict[str, str | None]:
    """What each security's newest stored bar is labelled now, for `labels_changed`."""
    if not security_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(NEWEST_LABELS, (list(security_ids),))
        return {str(row[0]): row[1] for row in cur.fetchall()}
