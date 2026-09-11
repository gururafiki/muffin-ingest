"""The FX rules, driven over CAPTURED Yahoo bytes rather than invented ones.

Every shape asserted here came out of `fx_chart.json`, and two of them are shapes nobody would have
written: a 5-day window carrying a null among its six closes, and an unknown pair answering **404**
with a body that names the absence. The second was a live defect in the provider when the capture
was taken — it raised on any non-200, so an unquoted currency read as a transport failure, and a
transport failure must never mark a subject absent.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest

from muffin_ingest.facets import fx
from muffin_ingest.providers import yahoo_chart

CAPTURED: dict[str, Any] = json.loads(
    (Path(__file__).parents[1] / "fixtures" / "fx_chart.json").read_text()
)


def replay(case: str) -> httpx.Response:
    """The captured wire response, rebuilt exactly — status AND body, because the two disagree."""
    payload = CAPTURED[case]
    if payload.get("chart_error"):
        body = {"chart": {"result": None, "error": payload["chart_error"]}}
    else:
        body = {
            "chart": {
                "error": None,
                "result": [
                    {
                        "timestamp": payload["timestamp"],
                        "indicators": {"quote": [{"close": payload["close"]}]},
                    }
                ],
            }
        }
    return httpx.Response(payload["status"], json=body)


@pytest.fixture
def replaying(monkeypatch: pytest.MonkeyPatch) -> Any:
    def install(case: str) -> None:
        monkeypatch.setattr(httpx, "get", lambda *a, **k: replay(case))

    return install


def test_the_pair_is_built_in_one_place_and_is_never_inverted() -> None:
    """`USDTWD=X` and `TWDUSD=X` are both valid and exact reciprocals, so nothing about a returned
    value says which was asked. The band below catches most inversions and provably cannot catch one
    near 0.5 — what protects those is this function being the only place the symbol is assembled."""
    assert yahoo_chart.pair("TWD") == "TWDUSD=X"
    assert yahoo_chart.pair("EUR") == "EURUSD=X"


def test_an_unknown_pair_answers_404_and_that_is_an_ABSENCE_not_a_fault(replaying: Any) -> None:
    """THE CAPTURE'S MOST VALUABLE CASE. `ZZZUSD=X` returns HTTP **404** carrying

        {"code": "Not Found", "description": "No data found, symbol may be delisted"}

    Raising on it — which the provider did when this fixture was taken — reports every unquoted
    currency as a transport failure. And because a transport failure must never mark a subject
    absent, the negative cache could never fill and those pairs would be re-asked for ever, eight
    times a day. Same shape as a SEC 400 that names a company with no Form 4.
    """
    replaying("unknown_pair")
    assert yahoo_chart.chart("ZZZUSD=X", range_="5d", interval="1d") == []


def test_a_transport_failure_is_still_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """The other half, and deleting it makes the test above pass for the wrong reason: an absence
    and a refusal must not collapse into each other in EITHER direction."""

    def boom(*args: object, **kwargs: object) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "get", boom)
    with pytest.raises(yahoo_chart.YahooRefused):
        yahoo_chart.chart("EURUSD=X", range_="5d", interval="1d")

    monkeypatch.setattr(httpx, "get", lambda *a, **k: httpx.Response(500, json={}))
    with pytest.raises(yahoo_chart.YahooRefused):
        yahoo_chart.chart("EURUSD=X", range_="5d", interval="1d")


def test_a_null_among_the_closes_is_dropped_not_carried(replaying: Any) -> None:
    """The captured EUR window has SIX timestamps and a null among the closes. Yahoo returns two
    parallel arrays, so dropping a close without dropping its timestamp shifts every later point
    onto the wrong date — a whole series off by one, with every value individually plausible."""
    replaying("eur_spot")
    points = yahoo_chart.chart("EURUSD=X", range_="5d", interval="1d")

    assert len(points) == 5, "six timestamps, one null close"
    assert all(p.close > 0 for p in points)

    # THE PAIRING IS THE ASSERTION, AND THE FIRST VERSION OF THIS TEST MISSED IT. Checking only
    # that the dates come out ascending and distinct passes under BOTH rules: filtering the nulls
    # out of `close` before zipping still yields five ascending dates, just the wrong five. The
    # mutation went green. What separates them is which DATE the last close lands on — with the
    # null at index 4 of 6, a filter-then-zip puts the final close a day early.
    captured = CAPTURED["eur_spot"]
    last_stamp = date.fromisoformat(captured["last_date"])
    last_close = [c for c in captured["close"] if c is not None][-1]

    assert points[-1].as_of == last_stamp, (
        f"last close landed on {points[-1].as_of} rather than {last_stamp} — the arrays are "
        f"parallel, so dropping a close without its timestamp shifts every later point"
    )
    assert points[-1].close == pytest.approx(last_close)


def test_ten_years_of_weekly_rates_is_what_the_history_lane_gets(replaying: Any) -> None:
    replaying("ils_history")
    points = yahoo_chart.chart("ILSUSD=X", range_="10y", interval="1wk")
    assert len(points) == 524, "ten years of weekly closes"
    assert points[0].as_of < date(2017, 1, 1) < points[-1].as_of


def test_a_currency_the_provider_barely_carries_SUCCEEDS_and_loads_almost_nothing(
    replaying: Any,
) -> None:
    """THE LARI, AND IT IS WHY THE NEGATIVE CACHE EXISTS. Yahoo carries exactly ONE bar for
    `GELUSD=X`, so a ten-year history fetch returns 200 with a single recent point. Nothing errors,
    a row is written, and "has a rate older than 90 days" stays false — so it was re-fetched eight
    times a day for ever with no count anywhere able to report it."""
    replaying("gel_history")
    points = yahoo_chart.chart("GELUSD=X", range_="10y", interval="1wk")
    assert len(points) == 1, "a successful fetch that loads no history is still a successful fetch"


def test_the_band_refuses_the_inverted_pair_it_was_built_for() -> None:
    """`USDTWD=X` really does return **31.60** — captured, not assumed. A ceiling of 100 would look
    generous and accept it; the highest real currency is the Kuwaiti dinar at ~3.26."""
    inverted = CAPTURED["twd_inverted"]["close"]
    observed = [c for c in inverted if c is not None]
    assert observed, "the fixture must carry values or this asserts nothing"
    assert not any(fx.is_plausible(c) for c in observed), f"band admitted an inversion: {observed}"

    # And the right way round is admitted, or the band would simply refuse everything.
    assert fx.is_plausible(1 / observed[0])
    assert fx.is_plausible(3.26), "the Kuwaiti dinar, the highest real rate"
    assert fx.is_plausible(0.0000382), "the Vietnamese dong, the lowest held here"
    assert not fx.is_plausible(26_200), "an inverted dong"


def test_an_implausible_rate_is_DROPPED_rather_than_corrected() -> None:
    """Inverting it back would repair a value whose provenance is already in doubt, and the band
    cannot tell an inversion from a genuinely odd quote."""
    rows = [
        {"currency_code": "TWD", "as_of": "2026-09-10", "close": 31.6},
        {"currency_code": "EUR", "as_of": "2026-09-10", "close": 1.16},
    ]
    rates = fx.normalise(rows)
    assert [r.currency_code for r in rates] == ["EUR"]


def test_a_subunit_gets_its_parent_s_WHOLE_history_in_the_same_pass() -> None:
    """THE RULE THE LANE TURNS ON. A subunit filled separately — or only by the spot lane — ends up
    with three days against its parent's ten years, and a consumer joining "the most recent rate at
    or before this bar" then silently uses a recent rate for every historical bar. That is what made
    Tel Aviv look like a 100x crash: wrong by every intervening move, and ordinary-looking.
    """
    parent = [
        fx.Rate(currency_code="ILS", as_of=date(2020, 1, 6), usd_per_unit=0.29),
        fx.Rate(currency_code="ILS", as_of=date(2026, 9, 7), usd_per_unit=0.33),
    ]
    out = fx.with_subunits(parent)

    agorot = [r for r in out if r.currency_code == "ILA"]
    assert len(agorot) == len(parent), "one agorot rate per shekel rate, not just the newest"
    assert {r.as_of for r in agorot} == {r.as_of for r in parent}
    assert agorot[0].usd_per_unit == pytest.approx(0.0029)
    assert all(r.derived_from == "ILS" for r in agorot), (
        "'observed' and 'computed from an observation' are different facts and a later reader must "
        "not have to guess which this is"
    )


def test_a_subunit_whose_parent_is_absent_produces_nothing_rather_than_a_guess() -> None:
    out = fx.with_subunits(
        [fx.Rate(currency_code="EUR", as_of=date(2026, 9, 7), usd_per_unit=1.16)]
    )
    assert not [r for r in out if r.currency_code in fx.SUBUNITS]


def test_the_divisors_are_the_real_ones() -> None:
    """100 agorot to the shekel, 100 cents to the rand, and **1000** fils to the dinar — the odd one
    out, and getting it wrong is a factor-of-ten error in a currency that is already the highest
    valued on earth."""
    assert fx.SUBUNITS == {"ILA": ("ILS", 100.0), "ZAC": ("ZAR", 100.0), "KWF": ("KWD", 1000.0)}
