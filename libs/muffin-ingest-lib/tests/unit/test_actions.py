"""Dividends and splits from the chart documents the price lane already stores.

`yahoo_chart_aapl_max.body` is AAPL's whole history at `range=max` (quarterly bars, so it is the
one document stage 2 refuses), carrying all 92 dividends and five splits; `yahoo_chart_7203_t_ext`
is a Tokyo extension with a dividend inside the window (`capture.md`).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from muffin_ingest.facets import actions
from muffin_ingest.providers import yahoo_chart

FIXTURES = Path(__file__).parents[1] / "fixtures"


def chart(name: str) -> bytes:
    return (FIXTURES / f"yahoo_chart_{name}.body").read_bytes()


def daily(name: str) -> bytes:
    """A captured body relabelled as daily, so its events reach the merge."""
    parsed = json.loads(chart(name))
    parsed["chart"]["result"][0]["meta"]["dataGranularity"] = "1d"
    return json.dumps(parsed).encode()


def stored(content: bytes, *, fetched: str, symbol: str = "X") -> dict[str, Any]:
    return {
        "security_id": "s",
        "asked_symbol": symbol,
        "body": content,
        "fetched_at": fetched,
        "url": fetched,
    }


def window(days: list[str], dividends: dict[str, float]) -> bytes:
    """A daily chart body over `days`, with `dividends` (day -> amount) in its events."""

    def stamp(day: str) -> int:
        return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()) + 14 * 3600

    result = {
        "meta": {"currency": "USD", "gmtoffset": -14400, "dataGranularity": "1d"},
        "timestamp": [stamp(d) for d in days],
        "indicators": {"quote": [{"close": [100.0] * len(days)}]},
        "events": {
            "dividends": {
                str(stamp(d)): {"amount": a, "date": stamp(d)} for d, a in dividends.items()
            }
        },
    }
    return json.dumps({"chart": {"result": [result], "error": None}}).encode()


def test_dividends_are_split_adjusted_and_splits_are_ratios() -> None:
    """AAPL's 1987 quarterly dividend of $0.12 arrives as 0.000536: 0.12 over the 224 shares four
    splits made of one, the same terms as Yahoo's split-adjusted closes."""
    events = actions.events_of(yahoo_chart.parse(chart("aapl_max")))
    dividends = [(d, v) for d, k, v in events if k == actions.DIVIDEND]
    splits = [(d, v) for d, k, v in events if k == actions.SPLIT]
    assert len(dividends) == 92
    assert dividends[0] == (date(1987, 5, 11), 0.000536)
    assert splits == [
        (date(1987, 6, 16), 2.0),
        (date(2000, 6, 21), 2.0),
        (date(2005, 2, 28), 2.0),
        (date(2014, 6, 9), 7.0),
        (date(2020, 8, 31), 4.0),
    ]


def test_a_dividend_is_dated_in_the_exchange_s_calendar() -> None:
    events = actions.events_of(yahoo_chart.parse(chart("7203_t_ext")))
    assert events == [(date(2026, 9, 29), actions.DIVIDEND, 50.0)]


def test_a_body_that_is_not_daily_speaks_for_no_day() -> None:
    """`range=max` came back quarterly: stage 2 publishes no bar from it, so it states nothing."""
    stats: dict[str, int] = {}
    merged = actions.merge([stored(chart("aapl_max"), fetched="2026-10-10T00:00:00+00:00")], stats)
    assert merged.actions == {} and merged.first is None
    merged = actions.merge([stored(daily("aapl_max"), fetched="2026-10-10T00:00:00+00:00")], stats)
    assert len(merged.actions) == 97


def test_the_newest_document_states_the_days_it_covers() -> None:
    """A dividend an older document carried inside a newer one's span, and the newer one does not,
    is withdrawn; one outside the newer span stands."""
    older = window(
        ["2026-09-01", "2026-09-15", "2026-10-01"], {"2026-09-01": 0.2, "2026-10-01": 0.3}
    )
    newer = window(["2026-09-20", "2026-10-02", "2026-10-09"], {"2026-10-09": 0.4})
    stats: dict[str, int] = {}
    merged = actions.merge(
        [
            stored(newer, fetched="2026-10-10T00:00:00+00:00"),
            stored(older, fetched="2026-10-02T00:00:00+00:00"),
        ],
        stats,
    )
    assert sorted((d.isoformat(), a.value) for (d, _), a in merged.actions.items()) == [
        ("2026-09-01", 0.2),
        ("2026-10-09", 0.4),
    ]
    assert stats["withdrawn"] == 1
    assert (merged.first, merged.last) == (date(2026, 9, 1), date(2026, 10, 9))
    rows = actions.rows_for("s", merged)
    assert rows[1] == {
        "security_id": "s",
        "ex_date": "2026-10-09",
        "kind": "dividend",
        "value": 0.4,
        "observed_symbol": "X",
        "source_code": "yfinance",
        "as_of": "2026-10-10T00:00:00+00:00",
    }


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[Any]]] = []

    def execute(self, sql: str, params: Any = ()) -> None:
        self.calls.append((sql, list(params)))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return [("s",)]


def test_a_retraction_touches_only_yahoo_s_rows_and_only_inside_the_span() -> None:
    merged = {
        "s": actions.merge(
            [
                stored(
                    window(["2026-09-01", "2026-10-01"], {"2026-10-01": 0.3}),
                    fetched="2026-10-02T00:00:00+00:00",
                )
            ],
            {},
        ),
        "quiet": actions.SecurityActions(),
    }
    cur = FakeCursor()
    assert actions.retract_absent(cur, merged) == 1
    sql, params = cur.calls[0]
    assert "a.source_code = %s" in sql
    assert params == [
        ["s"],
        [date(2026, 9, 1)],
        [date(2026, 10, 1)],
        "yfinance",
        ["s"],
        [date(2026, 10, 1)],
        ["dividend"],
    ]
    assert actions.retract_absent(FakeCursor(), {"quiet": actions.SecurityActions()}) == 0
