"""The company lane's statement rules, over Yahoo's fundamentals-timeseries bodies.

`yahoo_timeseries_*.json.gz` are response bodies, byte for byte once decompressed: one per symbol
and frequency, each the single request that carries all three statements (`capture.md`).
`yahoo_statement_stored_005930.KS_income_annual.json` is the row the edge wrote for Samsung's 2025
annual income statement through openbb, read from production on 2026-10-10: the spelling contract.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from muffin_ingest.facets import statements
from muffin_ingest.facets.prices import Subject
from muffin_ingest.providers import yahoo_chart, yahoo_summary, yahoo_timeseries
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parents[1] / "fixtures"


def series_body(symbol: str, frequency: str) -> bytes:
    path = FIXTURES / f"yahoo_timeseries_{symbol}_{frequency}.json.gz"
    return gzip.decompress(path.read_bytes())


def stored(
    symbol: str,
    frequency: str,
    *,
    body: bytes | None = None,
    fetched: str = "2026-10-10T12:00:00+00:00",
) -> dict[str, Any]:
    return {
        "security_id": f"sec-{symbol}",
        "asked_symbol": symbol,
        "provider": "yahoo",
        "frequency": frequency,
        "url": f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/{symbol}#{frequency}",
        "body": series_body(symbol, frequency) if body is None else body,
        "fetched_at": fetched,
    }


def rows_by_key(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {(r["statement"], r["period_type"], r["period_ending"]): r for r in rows}


def synthetic(
    frequency: str, lines: dict[str, list[tuple[str, float | None]]], *, currency: str = "KRW"
) -> bytes:
    """A timeseries body in Yahoo's shape: one result per line, a point per period."""
    result = []
    for key, points in lines.items():
        type_name = f"{frequency}{key}"
        result.append(
            {
                "meta": {"symbol": ["X"], "type": [type_name]},
                type_name: [
                    {
                        "asOfDate": day,
                        "periodType": "3M" if frequency == "quarterly" else "12M",
                        "currencyCode": currency,
                        "reportedValue": {"raw": value, "fmt": str(value)},
                    }
                    for day, value in points
                ],
            }
        )
    return json.dumps({"timeseries": {"result": result, "error": None}}).encode()


# --- the request ---------------------------------------------------------------------------------


def test_one_request_carries_all_three_statements_of_one_frequency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """375 types answer and 750 are a 400 (measured 2026-10-10), so a visit is two requests."""
    assert len(yahoo_timeseries.types("annual")) == 375
    assert all(t.startswith("quarterly") for t in yahoo_timeseries.types("quarterly"))
    sent: list[httpx.URL] = []

    def get(url: str, *, params: dict[str, str], **kwargs: Any) -> httpx.Response:
        sent.append(httpx.URL(url, params=params))
        return httpx.Response(200, content=series_body("AAPL", "annual"))

    monkeypatch.setattr(httpx, "get", get)
    yahoo_timeseries.fetch("BRK/B", "annual", today=date(2026, 10, 10))
    # `URL.path` DECODES, so the check reads the raw request target.
    assert sent[0].raw_path.startswith(
        b"/ws/fundamentals-timeseries/v1/finance/timeseries/BRK%2FB?"
    )
    params = dict(sent[0].params)
    assert params["period1"] == str(yahoo_chart.day_start(yahoo_timeseries.HISTORY_FROM))
    assert params["period2"] == str(yahoo_chart.day_start(date(2026, 10, 11)))
    assert len(params["type"].split(",")) == 375


def test_every_line_yahoo_sent_is_in_a_known_statement() -> None:
    """A key in none of the three lists would be dropped from the rows, so it is counted."""
    for path in sorted(FIXTURES.glob("yahoo_timeseries_*.json.gz")):
        frequency = path.name.rsplit("_", 1)[1].split(".")[0]
        parsed = yahoo_timeseries.parse(gzip.decompress(path.read_bytes()), frequency)
        assert not parsed.unknown_keys, (path.name, parsed.unknown_keys)


# --- spelling and shape --------------------------------------------------------------------------


def test_a_row_is_spelled_exactly_as_openbb_spelled_it() -> None:
    """THE SPELLING IS THE CONTRACT: `metric_source_field` maps metrics to these keys, and one
    character apart (`total_pretax_income` against `total_pre_tax_income`) empties a series.

    Measured against the edge's own row for the same period: the same 48 keys and the same values,
    with the currency it could never record.
    """
    expected = json.loads(
        (FIXTURES / "yahoo_statement_stored_005930.KS_income_annual.json").read_text()
    )
    out = statements.normalise([stored("005930.KS", "annual")])
    row = rows_by_key(out.rows)[("income", "annual", expected["period_ending"])]
    assert json.loads(row["data"]) == expected
    assert row["currency"] == "KRW" and row["source_code"] == "yfinance"


def test_a_semi_annual_reporter_is_filed_as_halves() -> None:
    """VOD.L's quarterly series is 2025-03-31, 2025-09-30, 2026-03-31: Yahoo labels them `3M`."""
    out = statements.normalise([stored("VOD.L", "quarterly")])
    types = {r["period_type"] for r in out.rows}
    assert types == {"half"}, types
    assert out.stats["half_years"] == len(out.rows)


def test_a_quarterly_reporter_missing_a_quarter_is_not_a_half_year_reporter() -> None:
    """0992.HK skips its fiscal Q4 (reported in the annual), so one gap is 182 days among 92-day
    ones. The per-gap rule first written would have filed 2026-06-30 as a half-year; 1,451
    production securities have that shape."""
    out = statements.normalise([stored("0992.HK", "quarterly")])
    assert {r["period_type"] for r in out.rows} == {"quarter"}


@pytest.mark.parametrize(
    ("ends", "expected"),
    [
        (["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"], "quarter"),
        (["2025-03-31", "2025-09-30", "2026-03-31"], "half"),
        (["2025-03-31", "2025-06-30", "2025-12-31"], "quarter"),
        (["2025-06-30"], "quarter"),
    ],
)
def test_cadence(ends: list[str], expected: str) -> None:
    assert statements.cadence([date.fromisoformat(d) for d in ends]) == expected


def test_a_narrower_later_window_keeps_what_an_older_document_carried() -> None:
    """YAHOO'S WINDOW ROLLS PER LINE: Samsung's 2025-03-31 kept ten of the ~180 lines the edge
    stored in August. A later document covering the period with fewer lines is narrower, not a
    correction — while a value it does restate wins, and a point with no value erases nothing."""
    older = synthetic(
        "quarterly", {"EBIT": [("2025-03-31", 9.0)], "TotalRevenue": [("2025-03-31", 2.0)]}
    )
    newer = synthetic(
        "quarterly",
        {
            "TotalRevenue": [("2025-03-31", 3.0), ("2025-06-30", 5.0)],
            "EBIT": [("2025-06-30", None)],
        },
    )
    out = statements.normalise(
        [
            stored("X", "quarterly", body=newer, fetched="2026-10-10T00:00:00+00:00"),
            stored("X", "quarterly", body=older, fetched="2026-08-10T00:00:00+00:00"),
        ]
    )
    rows = rows_by_key(out.rows)
    march = json.loads(rows[("income", "quarter", "2025-03-31")]["data"])
    assert (march["ebit"], march["total_revenue"]) == (9.0, 3.0)
    june = json.loads(rows[("income", "quarter", "2025-06-30")]["data"])
    assert june["ebit"] is None and june["total_revenue"] == 5.0
    assert rows[("income", "quarter", "2025-03-31")]["as_of"] == "2026-10-10T00:00:00+00:00"


def test_raw_is_the_body_plus_who_asked_and_which_frequency() -> None:
    document = Document(
        url="u", body=b"{}", content_type="application/json", fetched_at=datetime.now(UTC)
    )
    row = statements.raw_rows(Subject("s", "X", 1.0), document, run_id="r", frequency="annual")[0]
    assert set(row) == set(document.as_row("r")) | statements.CONTEXT_COLUMNS


# --- when statements are due ---------------------------------------------------------------------


def summary(symbol: str) -> yahoo_summary.Summary:
    return yahoo_summary.parse(
        gzip.decompress((FIXTURES / f"yahoo_summary_{symbol}.json.gz").read_bytes())
    )


def test_the_summary_states_the_newest_period_and_the_timeseries_holds_it() -> None:
    """Measured on 17 listings: `mostRecentQuarter` is the newest timeseries period, half-yearly
    reporters included. AAPL's is its 52-week quarter end, 2026-06-27, which the timeseries files
    under 2026-06-30, so the comparison has to tolerate a few days."""
    assert statements.stated_period(summary("AAPL")) == date(2026, 6, 27)
    assert statements.stated_period(summary("VOD.L")) == date(2026, 3, 31)
    held = statements.held_period([stored("AAPL", "annual"), stored("AAPL", "quarterly")])
    assert held == date(2026, 6, 30)
    assert (
        statements.due(
            [stored("AAPL", "annual"), stored("AAPL", "quarterly")],
            stated=date(2026, 6, 27),
            today=date(2026, 10, 10),
        )
        is None
    )


def test_statements_are_due_for_a_period_yahoo_names_and_we_do_not_hold() -> None:
    held = [stored("AAPL", "annual"), stored("AAPL", "quarterly")]
    today = date(2026, 11, 5)
    assert statements.due(held, stated=date(2026, 9, 26), today=today) == "new_period"
    assert statements.due([], stated=None, today=today) == "never_fetched"
    assert statements.due(held, stated=None, today=date(2027, 1, 8)) == "refresh_due"
    assert statements.due(held, stated=None, today=today, force=True) == "forced"


def test_a_period_the_timeseries_never_carries_is_asked_for_three_times_then_left() -> None:
    """Without a bound, a company whose summary is ahead of its timeseries would cost two
    requests every week for nothing."""
    stated = date(2026, 9, 30)
    # Fetched before the stated period ended, so neither is an ask about it.
    before = "2026-09-01T12:00:00+00:00"
    held = [stored("AAPL", "annual", fetched=before), stored("AAPL", "quarterly", fetched=before)]
    tries = [
        stored(
            "AAPL",
            "quarterly",
            fetched=(datetime(2026, 10, 15, tzinfo=UTC) + timedelta(weeks=i)).isoformat(),
        )
        for i in range(3)
    ]
    today = date(2026, 11, 20)
    assert statements.due(held + tries[:2], stated=stated, today=today) == "new_period"
    assert statements.due(held + tries, stated=stated, today=today) is None


# --- the currency ---------------------------------------------------------------------------------


def test_yahoo_s_retired_lira_code_is_read_as_the_lira() -> None:
    """`TRL` is the code the lira lost in 2005; Yahoo still labels some Turkish statements with it
    while the values are TRY (Aksigorta 2022: 7.2 bn beside its TRY 2021 and 2023 figures).
    Written through, it stopped the metric derivation for every company from 2026-10-21 to 10-24."""
    body = synthetic("annual", {"TotalRevenue": [("2022-12-31", 7227436471.0)]}, currency="TRL")
    out = statements.normalise([stored("X", "annual", body=body)], known_currencies={"TRY", "USD"})
    assert [r["currency"] for r in out.rows] == ["TRY"]
    assert out.stats["currency_mapped"] == 1
    assert out.stats["currency_withheld"] == 0


def test_a_currency_the_metrics_cannot_reference_is_withheld() -> None:
    """A plain code `market.currency` does not hold is withheld and counted, never written: the
    derivation copies it into `security_metric`, whose currency is a foreign key."""
    body = synthetic("annual", {"TotalRevenue": [("2023-12-31", 1.0)]}, currency="XYZ")
    withheld = statements.normalise([stored("X", "annual", body=body)], known_currencies={"USD"})
    assert [r["currency"] for r in withheld.rows] == [None]
    assert withheld.stats["currency_withheld"] == 1
    # The control: a known code passes, and with no database there is no membership test at all.
    known = synthetic("annual", {"TotalRevenue": [("2023-12-31", 1.0)]}, currency="USD")
    kept = statements.normalise([stored("X", "annual", body=known)], known_currencies={"USD"})
    assert [r["currency"] for r in kept.rows] == ["USD"]
    unchecked = statements.normalise([stored("X", "annual", body=body)])
    assert [r["currency"] for r in unchecked.rows] == ["XYZ"]
