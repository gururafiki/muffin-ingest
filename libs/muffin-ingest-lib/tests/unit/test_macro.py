"""The macro lane's parser and openbb dispatch, over answers captured on 2026-11-21."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest.facets import macro
from muffin_ingest.providers import openbb

MACRO = Path(__file__).resolve().parents[1] / "fixtures" / "macro"


def captured(code: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((MACRO / f"{code}.json").read_text())
    return data


def answer(code: str, fetched_at: str = "2026-11-21T19:00:00+00:00") -> list[dict[str, Any]]:
    """The rows stage 1 stores for one captured answer: openbb's, plus the context columns."""
    data = captured(code)
    return [
        {
            **row,
            "indicator_code": code,
            "route": data["route"],
            "provider": "test",
            "asked_from": "2025-10-17",
            "fetched_at": fetched_at,
            "run_id": "r1",
        }
        for row in data["rows"]
    ]


# --- points ---------------------------------------------------------------------------------------


def test_fred_s_value_is_read_from_the_column_named_after_the_series() -> None:
    found = macro.points(answer("de-10y-yield-fred"))
    assert len(found) == 13
    assert (found[-1].as_of, found[-1].dimension) == ("2026-10-01", "")
    assert found[-1].value == pytest.approx(3.4159090909)


def test_a_stage_one_context_column_is_never_read_as_the_value() -> None:
    """FRED's fallback takes the first numeric column, and stage 1 adds columns of its own."""
    rows = [{"date": "2026-10-01", "fetched_at": 5, "run_id": 7, "SERIES": 3.4}]
    assert macro.points(rows) == [macro.Point("2026-10-01", "", 3.4)]


def test_a_bar_is_read_as_its_close_not_its_open() -> None:
    found = macro.points(answer("gold"))
    assert found[-1].as_of == "2026-11-20"
    assert found[-1].value == pytest.approx(3515.39990234375)


def test_a_rate_is_read_before_the_percentiles_beside_it() -> None:
    found = macro.points(answer("us-effr"))
    assert found[-1].as_of == "2026-11-19"
    assert found[-1].value == pytest.approx(0.0388)


def test_a_yield_curve_is_one_point_per_maturity() -> None:
    found = macro.points(answer("us-yield-curve"))
    assert len(found) == 11
    assert {p.as_of for p in found} == {"2026-11-19"}
    [thirty] = [p for p in found if p.dimension == "year_30"]
    assert thirty.value == pytest.approx(0.054)


def test_one_point_per_date_and_dimension_the_first_kept() -> None:
    """OECD returns several breakdowns for one date; the upsert must never carry a key twice."""
    rows = [
        {"date": "2026-09-01", "value": 0.03, "expenditure": "total"},
        {"date": "2026-09-01", "value": 0.09, "expenditure": "energy"},
    ]
    assert macro.points(rows) == [macro.Point("2026-09-01", "", 0.03)]


def test_a_missing_or_non_numeric_value_is_no_point() -> None:
    rows: list[dict[str, Any]] = [
        {"date": "2026-09-01", "value": float("nan")},
        {"date": "2026-08-01", "value": None},
        {"date": "2026-07-01", "value": "n/a"},
        {"date": None, "value": 1.0},
        {"date": "2026-06-01", "value": True},
    ]
    assert macro.points(rows) == []


def test_a_date_object_is_read_like_a_string() -> None:
    """Parquet hands stage 2 `datetime.date`; the capture holds strings. Both are the same day."""
    assert macro.points([{"date": date(2026, 9, 1), "value": 1.0}]) == [
        macro.Point("2026-09-01", "", 1.0)
    ]


# --- normalise ------------------------------------------------------------------------------------


def test_a_series_window_runs_from_the_start_asked_to_the_day_fetched() -> None:
    parsed = macro.normalise(answer("us-unemployment"), codes=["us-unemployment"])
    [series] = parsed.series
    assert series.window == ("2025-10-17", "2026-11-21")
    # The point the edge served for 10-01 is not in today's answer, so the window retracts it.
    assert max(p.as_of for p in series.points) == "2026-09-01"


def test_a_snapshot_s_window_is_the_dates_it_answered_never_the_400_days_asked() -> None:
    """A 400-day window on the yield curve would delete every earlier curve."""
    parsed = macro.normalise(answer("us-yield-curve"), codes=["us-yield-curve"])
    assert parsed.series[0].window == ("2026-11-19", "2026-11-19")


def test_only_the_newest_answer_of_a_series_is_read() -> None:
    old = [{**row, "value": 9.9} for row in answer("us-cpi", "2026-11-20T07:40:00+00:00")]
    parsed = macro.normalise(old + answer("us-cpi"), codes=["us-cpi"])
    [series] = parsed.series
    assert series.fetched_at == "2026-11-21T19:00:00+00:00"
    assert all(p.value != 9.9 for p in series.points)


def test_a_series_with_no_rows_or_no_points_is_named_and_never_retracts() -> None:
    rows = [{**r, "value": None} for r in answer("us-cpi")]
    parsed = macro.normalise(rows, codes=["us-cpi", "jp-cpi"])
    assert parsed.series == []
    assert parsed.no_points == ["us-cpi"]
    assert parsed.no_rows == ["jp-cpi"]


def test_observation_rows_carry_the_answer_s_fetch_time() -> None:
    [series] = macro.normalise(answer("us-cpi"), codes=["us-cpi"]).series
    rows = macro.observation_rows(series)
    assert len(rows) == len(series.points)
    assert rows[0]["indicator_code"] == "us-cpi"
    assert {r["fetched_at"] for r in rows} == {"2026-11-21T19:00:00+00:00"}


# --- the dispatch ---------------------------------------------------------------------------------


def test_every_catalogued_route_has_a_typed_call() -> None:
    """The ten routes `market.macro_indicator` names (measured 2026-11-21)."""
    assert set(openbb.MACRO_ROUTES) == {
        "/api/v1/economy/fred_series",
        "/api/v1/economy/cpi",
        "/api/v1/economy/gdp/real",
        "/api/v1/economy/unemployment",
        "/api/v1/fixedincome/government/yield_curve",
        "/api/v1/fixedincome/rate/effr",
        "/api/v1/fixedincome/rate/sofr",
        "/api/v1/index/price/historical",
        "/api/v1/derivatives/futures/historical",
        "/api/v1/crypto/price/historical",
    }
    assert set(openbb.MACRO_ROUTES) >= macro.SNAPSHOT_ROUTES


@pytest.mark.parametrize(
    ("route", "kwargs", "message"),
    [
        ("/api/v1/economy/gdp/nominal", {}, "no typed call"),
        ("/api/v1/economy/fred_series", {"params": {"symbol": "X", "frequency": "q"}}, "swallow"),
        ("/api/v1/economy/fred_series", {"params": {}}, "needs a symbol"),
        ("/api/v1/economy/cpi", {"country": None}, "needs a country"),
    ],
)
def test_the_dispatch_refuses_what_the_hub_would_swallow(
    route: str, kwargs: dict[str, Any], message: str
) -> None:
    """Refused before openbb is imported: every route takes `**kwargs`, so a wrong key binds."""
    with pytest.raises(openbb.UnknownMacroRoute, match=message):
        openbb.macro_series(route, provider="fred", start=date(2025, 10, 17), **kwargs)


def test_a_credential_in_an_error_is_redacted() -> None:
    text = (
        "HTTPError: 400 for https://api.stlouisfed.org/fred/series/observations"
        "?series_id=X&api_key=abc123def&file_type=json; fmp apikey=zzz token=t0k"
    )
    out = openbb.redacted(text)
    assert "abc123def" not in out and "zzz" not in out and "t0k" not in out
    assert "series_id=X" in out and "api_key=REDACTED" in out
