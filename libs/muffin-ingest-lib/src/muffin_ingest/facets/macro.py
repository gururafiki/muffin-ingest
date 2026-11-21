"""Macro series: the catalogue, and the points a series' answer holds. No network here.

A PORT OF THE EDGE'S `macro-indicators` (`resources.ts::extractMacroPoints`), with one change the
edge could not make: an upsert cannot retract, so a point the provider stopped publishing stayed
served for ever. Measured 2026-11-21: `us-unemployment` served 0.039 for 2026-10-01, written 11-12,
while OECD's answer, edge and in-process alike, ends at 09-01. Stage 2 now retracts what an answer
no longer states, within the window that answer covered (`Answered.window`).

The catalogue is `market.macro_indicator`, a control table: a series is a row, and its route is
dispatched to a typed call by `providers.openbb.macro_series`.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from muffin_ingest import writers

#: How far back each series is asked for. These are latest-value panels, not chart history, and an
#: unbounded request on a daily series is the difference between 300 rows and 20,000. The edge's.
WINDOW = timedelta(days=400)

#: The routes that answer a SNAPSHOT rather than a series: the yield curve is the latest date's
#: curve, a point per maturity, and ignores the start date. Its retraction window is the dates it
#: answered for, never the 400 days asked: a window of 400 days would delete every earlier curve.
SNAPSHOT_ROUTES = frozenset({"/api/v1/fixedincome/government/yield_curve"})

CATALOGUE_SQL = """
select code, route, provider, coalesce(params, '{}'::jsonb), provider_country, enabled
  from market.macro_indicator
 order by sort_order, code
"""


@dataclass(frozen=True)
class Series:
    """One row of `market.macro_indicator`."""

    code: str
    route: str
    provider: str
    params: dict[str, Any] = field(default_factory=dict)
    provider_country: str | None = None
    enabled: bool = True


def catalogue(conn: writers.DbConn) -> list[Series]:
    """Every catalogued series, enabled or not, in the catalogue's own order."""
    with conn.cursor() as cur:
        cur.execute(CATALOGUE_SQL)
        return [
            Series(
                code=str(code),
                route=str(route),
                provider=str(provider),
                params=dict(params or {}),
                provider_country=str(country) if country else None,
                enabled=bool(enabled),
            )
            for code, route, provider, params, country, enabled in cur.fetchall()
        ]


@dataclass(frozen=True)
class Point:
    as_of: str
    dimension: str
    value: float


def _day(value: Any) -> str | None:
    """The provider's date as `YYYY-MM-DD`, from a string, a date or a datetime."""
    if isinstance(value, datetime | date):
        return value.isoformat()[:10]
    if isinstance(value, str) and len(value) >= 10:
        return value[:10]
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if math.isfinite(n) else None


def points(rows: Iterable[Mapping[str, Any]]) -> list[Point]:
    """The points an answer's rows hold, ported from the edge (`extractMacroPoints`).

    A yield curve row is (date, maturity) → rate, emitted as several points sharing a date. Every
    other row is a scalar, read from `value`, then `rate`, then `close` (so an OHLC bar is read as
    its close, not its open). FRED's value hides under a key named after the series, so when no
    known field matched, the first other numeric column is the value.

    ONE POINT PER (date, dimension): OECD returns several breakdowns for one date and the first is
    kept. Without this, the upsert would carry a key twice and fail the whole statement.
    """
    out: list[Point] = []
    seen: set[tuple[str, str]] = set()

    def add(as_of: str, dimension: str, value: float | None) -> None:
        if value is None or (as_of, dimension) in seen:
            return
        seen.add((as_of, dimension))
        out.append(Point(as_of=as_of, dimension=dimension, value=value))

    for row in rows:
        as_of = _day(row.get("date"))
        if as_of is None:
            continue
        if row.get("maturity") is not None and "rate" in row:
            add(as_of, str(row["maturity"]), _number(row.get("rate")))
            continue
        raw: Any = None
        for name in ("value", "rate", "close"):
            if row.get(name) is not None:
                raw = row[name]
                break
        if raw is None:
            for name, candidate in row.items():
                if name == "date" or name in CONTEXT_COLUMNS:
                    continue
                if isinstance(candidate, int | float) and not isinstance(candidate, bool):
                    raw = candidate
                    break
        add(as_of, "", _number(raw))
    return out


#: What stage 1 adds to each of openbb's rows: which series it answers, what was asked, and when.
#: `asked_from` is the start of the window asked, which stage 2 needs to know what it may retract.
CONTEXT_COLUMNS = frozenset(
    {"indicator_code", "route", "provider", "asked_from", "fetched_at", "run_id"}
)


@dataclass(frozen=True)
class Answered:
    """One series' newest answer: its points, and the window they state the whole truth for."""

    code: str
    points: list[Point]
    fetched_at: str
    window: tuple[str, str] | None


@dataclass
class Parsed:
    series: list[Answered] = field(default_factory=list)
    #: Series whose stored file holds no row at all (never answered, or a marker file).
    no_rows: list[str] = field(default_factory=list)
    #: Series whose newest answer held rows but no point (every value missing or not a number).
    no_points: list[str] = field(default_factory=list)


def normalise(rows: Sequence[Mapping[str, Any]], codes: Sequence[str]) -> Parsed:
    """The newest answer per series code, as points and a retraction window.

    THE WINDOW IS WHAT THE REQUEST COVERED: from `asked_from` to the day it was fetched, for a
    series; the answer's own first and last dates, for a snapshot. A re-parse of an old file
    therefore retracts within that file's window and never touches points published since.
    """
    by_code: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        code = row.get("indicator_code")
        if isinstance(code, str):
            by_code.setdefault(code, []).append(row)

    parsed = Parsed()
    for code in codes:
        answer = by_code.get(code) or []
        if not answer:
            parsed.no_rows.append(code)
            continue
        newest = max(str(r.get("fetched_at") or "") for r in answer)
        latest = [r for r in answer if str(r.get("fetched_at") or "") == newest]
        found = points(latest)
        if not found:
            parsed.no_points.append(code)
            continue
        route = str(latest[0].get("route") or "")
        if route in SNAPSHOT_ROUTES:
            dates = sorted({p.as_of for p in found})
            window: tuple[str, str] | None = (dates[0], dates[-1])
        else:
            start = _day(latest[0].get("asked_from"))
            end = _day(newest)
            window = (start, end) if start and end else None
        parsed.series.append(Answered(code=code, points=found, fetched_at=newest, window=window))
    return parsed


def observation_rows(answered: Answered) -> list[dict[str, Any]]:
    """`market.macro_observation` rows for one answered series."""
    return [
        {
            "indicator_code": answered.code,
            "as_of": p.as_of,
            "dimension": p.dimension,
            "value": p.value,
            "fetched_at": answered.fetched_at,
        }
        for p in answered.points
    ]
