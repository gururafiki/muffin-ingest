"""Earnings from nasdaq's daily calendar and Yahoo's stored summaries (captured 2026-11-21)."""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest import settings
from muffin_ingest.facets import earnings
from muffin_ingest.providers import nasdaq

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
TODAY = date(2026, 11, 21)


def nasdaq_day(day: str, fetched_at: str = "2026-11-21T16:00:00+00:00") -> dict[str, Any]:
    body = (FIXTURES / "nasdaq" / f"nasdaq_earnings_{day}.json").read_bytes()
    return {"day": day, "body": body, "fetched_at": fetched_at, "url": f"n/{day}"}


def summary(symbol: str, sid: str | None = None, body: bytes | None = None) -> dict[str, Any]:
    raw = body or gzip.decompress((FIXTURES / f"yahoo_summary_{symbol}.json.gz").read_bytes())
    return {"security_id": sid or symbol, "asked_symbol": symbol, "body": raw, "fetched_at": "x"}


# --- the provider --------------------------------------------------------------------------------


def test_the_url_names_the_day_and_the_cache_location_serves_nasdaq_s_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HTTP_CACHE_URL", raising=False)
    monkeypatch.delenv("NASDAQ_BASE_URL", raising=False)
    assert nasdaq.url_for(date(2026, 10, 30)) == (
        "https://api.nasdaq.com/api/calendar/earnings?date=2026-10-30"
    )
    assert settings.CACHE_LOCATIONS[nasdaq.PROVIDER] == "api.nasdaq.com"


# --- nasdaq's strings ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "value"),
    [
        ("$4.23", 4.23),
        ("$648,310,820,000", 648_310_820_000.0),
        ("($0.12)", -0.12),
        ("-$0.12", -0.12),
        ("N/A", None),
        ("", None),
        (None, None),
    ],
)
def test_money(text: Any, value: float | None) -> None:
    assert earnings.money(text) == value


def test_a_quarter_is_a_month_and_its_last_day() -> None:
    assert earnings.quarter("Sep/2026") == ("2026-09", date(2026, 9, 30))
    assert earnings.quarter("Feb/2028") == ("2028-02", date(2028, 2, 29))
    assert earnings.quarter("2026-09") is None
    assert earnings.quarter("N/A") is None


# --- a nasdaq day --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("day", "outcome", "rows"),
    [
        ("2026-10-30", "answered", 66),
        ("2026-11-24", "answered", 52),
        ("2015-02-03", "answered", 92),
        ("2016-06-29", "empty", 0),
        ("2026-11-21", "empty", 0),
    ],
)
def test_each_captured_day_reads_as_what_it_is(day: str, outcome: str, rows: int) -> None:
    read = earnings.read_nasdaq(nasdaq_day(day)["body"])
    assert (read.outcome, len(read.rows)) == (outcome, rows)


@pytest.mark.parametrize("body", [b"", b"<html/>", b'{"data": {}}', b'{"status": {"rCode": 400}}'])
def test_a_body_that_is_not_nasdaq_s_answer_is_unreadable(body: bytes) -> None:
    assert earnings.read_nasdaq(body).outcome == "unreadable"


def test_a_past_day_gives_a_calendar_row_and_a_history_row_in_nasdaq_s_units() -> None:
    out = earnings.normalise_nasdaq([nasdaq_day("2026-10-30")], ids={"XOM": "x"}, today=TODAY)
    xom = next(r for r in out.calendar if r["symbol"] == "XOM")
    assert (xom["report_date"], xom["period_ending"], xom["reporting_time"]) == (
        date(2026, 10, 30),
        "2026-09",
        "not-supplied",
    )
    assert (xom["eps_consensus"], xom["num_estimates"], xom["security_id"]) == (3.9, 6, "x")
    (history,) = out.history
    assert history["period_ending"] == date(2026, 9, 30)
    assert (history["eps_actual"], history["eps_estimated"], history["surprise"]) == (
        4.23,
        3.9,
        0.33,
    )
    # NASDAQ'S SURPRISE IS ALREADY A PERCENT: multiplying it again would serve 846%.
    assert history["surprise_pct"] == 8.46
    assert {r["source_code"] for r in out.calendar + out.history} == {"nasdaq"}


def test_a_day_to_come_has_a_forecast_and_last_year_s_eps_but_no_history() -> None:
    out = earnings.normalise_nasdaq([nasdaq_day("2026-11-24")], ids={"DELL": "d"}, today=TODAY)
    dell = next(r for r in out.calendar if r["symbol"] == "DELL")
    assert (dell["eps_consensus"], dell["eps_previous"], dell["reporting_time"]) == (
        6.55,
        2.35,
        "after-hours",
    )
    assert out.history == []


def test_an_unmatched_symbol_keeps_its_calendar_row_but_has_no_history() -> None:
    out = earnings.normalise_nasdaq([nasdaq_day("2026-10-30")], ids={}, today=TODAY)
    assert len(out.calendar) == 66 and all(r["security_id"] is None for r in out.calendar)
    assert out.history == [] and out.stats["matched"] == 0


def test_an_old_day_feeds_the_history_and_not_the_calendar() -> None:
    out = earnings.normalise_nasdaq([nasdaq_day("2015-02-03")], ids={"MUFG": "m"}, today=TODAY)
    assert out.calendar == [] and out.stats["calendar_too_old"] == 92
    assert [(r["security_id"], r["period_ending"]) for r in out.history] == [
        ("m", date(2014, 12, 31))
    ]


def test_a_day_with_no_reports_is_answered_and_replaces_what_was_stored_for_it() -> None:
    out = earnings.normalise_nasdaq([nasdaq_day("2016-06-29")], ids={}, today=TODAY)
    assert out.answered == ["2016-06-29"] and out.stats["days_empty"] == 1


def test_an_unreadable_day_replaces_nothing() -> None:
    row = {**nasdaq_day("2026-10-30"), "body": b"<html>gateway</html>"}
    out = earnings.normalise_nasdaq([row], ids={"XOM": "x"}, today=TODAY)
    assert out.answered == [] and out.calendar == [] and out.stats["days_unreadable"] == 1


def test_only_each_day_s_newest_document_is_read() -> None:
    older = {**nasdaq_day("2026-10-30", "2026-10-29T00:00:00+00:00")}
    newer = {**nasdaq_day("2026-11-21"), "day": "2026-10-30"}
    out = earnings.normalise_nasdaq([newer, older], ids={"XOM": "x"}, today=TODAY)
    assert out.calendar == [] and out.stats["days_empty"] == 1


# --- Yahoo ---------------------------------------------------------------------------------------


def test_a_report_is_dated_in_its_exchange_s_own_zone() -> None:
    # BHP's 2026-08-17 16:12 in Sydney is 06:12 UTC the same day; Toyota's 15:30 in Tokyo likewise.
    # The fixture moves Toyota's moment to 08:00 Tokyo, which is the PREVIOUS day in UTC.
    parsed = json.loads(gzip.decompress((FIXTURES / "yahoo_summary_7203.T.json.gz").read_bytes()))
    events = parsed["quoteSummary"]["result"][0]["calendarEvents"]["earnings"]
    events["earningsDate"] = [1793833200]  # 2026-11-04 23:00 UTC = 2026-11-05 08:00 JST
    out = earnings.normalise_yahoo(
        [summary("7203.T", body=json.dumps(parsed).encode())], today=TODAY
    )
    assert out.calendar[0]["report_date"] == date(2026, 11, 5)


def test_a_yahoo_calendar_row_carries_its_range_and_consensus() -> None:
    out = earnings.normalise_yahoo([summary("AAPL")], today=TODAY)
    (row,) = out.calendar
    assert (row["symbol"], row["report_date"], row["source_code"]) == (
        "AAPL",
        date(2026, 11, 2),
        "yfinance",
    )
    assert (row["eps_consensus"], row["eps_low"], row["eps_high"]) == (1.9822, 1.93, 2.07)
    assert row["revenue_consensus"] == 113617923250 and row["is_date_estimate"] is False


def test_yahoo_s_surprise_is_a_fraction_and_is_stored_as_a_percent() -> None:
    out = earnings.normalise_yahoo([summary("7203.T")], today=TODAY)
    first = min(out.history, key=lambda r: r["period_ending"])
    assert first["period_ending"] == date(2025, 9, 30)
    assert (first["eps_actual"], first["surprise_pct"]) == (71.51, 42.8)
    assert {r["source_code"] for r in out.history} == {"yfinance"}
    assert len(out.history) == 4


def test_a_company_with_no_date_and_no_history_still_answers_and_replaces_its_rows() -> None:
    out = earnings.normalise_yahoo([summary("NESN.SW")], today=TODAY)
    assert out.answered == ["NESN.SW"] and out.calendar == [] and out.history == []


def test_an_absent_or_unreadable_summary_replaces_nothing() -> None:
    absent = json.dumps({"quoteSummary": {"result": None, "error": {"code": "Not Found"}}}).encode()
    out = earnings.normalise_yahoo(
        [summary("X", "a", absent), summary("Y", "b", b"<html/>")], today=TODAY
    )
    assert out.answered == [] and out.stats["absent"] == 1 and out.stats["unreadable"] == 1
