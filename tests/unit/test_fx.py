"""The FX rules, driven over CAPTURED Yahoo bytes rather than invented ones.

TWO KINDS OF CAPTURE, FOR TWO DIFFERENT CLAIMS.

  `yahoo_chart_eurusd_5d.body` and `yahoo_chart_zzzusd_404.body` are RESPONSE BODIES, byte for
  byte, taken 2026-09-12. The "raw is exactly what the provider sent" assertions run against them,
  because a body re-assembled from parts cannot prove that nothing was lost.

  `fx_chart.json` is an older capture of selected FIELDS — timestamps, closes and six meta keys per
  case. It still carries the shapes nobody would have written: a five-day window with a null among
  its closes, the lari's single live quote, an inverted pair returning 31.6, and an unknown pair
  answering **404** with a body that names the absence. `rebuilt()` puts it back into Yahoo's wire
  shape for `parse`.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

from muffin_ingest.facets import fx
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parents[1] / "fixtures"
CAPTURED: dict[str, Any] = json.loads((FIXTURES / "fx_chart.json").read_text())
EUR_BODY = (FIXTURES / "yahoo_chart_eurusd_5d.body").read_bytes()
ABSENT_BODY = (FIXTURES / "yahoo_chart_zzzusd_404.body").read_bytes()


def rebuilt(case: str) -> bytes:
    """A field capture put back into Yahoo's wire shape. The meta is PART of that shape, not an
    extra: without `gmtoffset` every bar is dated a day early, and without `regularMarketTime` the
    live quote is stored as a close."""
    payload = CAPTURED[case]
    if payload.get("chart_error"):
        return json.dumps({"chart": {"result": None, "error": payload["chart_error"]}}).encode()
    result = {
        "timestamp": payload["timestamp"],
        "indicators": {"quote": [{"close": payload["close"]}]},
        "meta": payload["meta"],
    }
    return json.dumps({"chart": {"error": None, "result": [result]}}).encode()


def chart_body(points: list[tuple[date, float]]) -> bytes:
    """A minimal body in Yahoo's shape: one daily bar per point, stamped at the London session open
    with `gmtoffset` saying so, and no live quote."""
    tz = timezone(timedelta(hours=1))
    stamps = [int(datetime(d.year, d.month, d.day, tzinfo=tz).timestamp()) for d, _ in points]
    meta = {"gmtoffset": 3600, "exchangeTimezoneName": "Europe/London", "regularMarketTime": 0}
    result = {
        "meta": meta,
        "timestamp": stamps,
        "indicators": {"quote": [{"close": [close for _, close in points]}]},
    }
    return json.dumps({"chart": {"error": None, "result": [result]}}).encode()


def document(body: bytes) -> Document:
    return Document(
        url="https://query2.finance.yahoo.com/v8/finance/chart/X?range=5d&interval=1d",
        body=body,
        content_type="application/json",
        fetched_at=datetime(2026, 9, 12, tzinfo=UTC),
    )


def rates_from(currency: str, body: bytes, **kwargs: Any) -> fx.Normalised:
    """Stage 1 then stage 2, the way the assets run them — through the stored row."""
    rows = fx.raw_rows(currency, document(body), interval="1d", range_="5d", run_id="r")
    return fx.normalise(rows, **kwargs)


@pytest.fixture
def serving(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Yahoo answering with a given status and body — as BYTES, so what `fetch` returns can be
    compared with what was served rather than with a re-serialisation of it."""

    def install(status: int, body: bytes) -> None:
        monkeypatch.setattr(httpx, "get", lambda *a, **k: httpx.Response(status, content=body))

    return install


def test_raw_is_the_response_body_byte_for_byte(serving: Any) -> None:
    """THE RULE THIS LANE BROKE, answered on real bytes.

    Until 2026-09-12 `chart()` pivoted Yahoo's nested parallel arrays into one object per timestamp
    before anything was stored. Every field it knew to look for survived; anything else — a second
    quote block, an array whose length did not match `timestamp`, a key Yahoo adds beside it — was
    gone at the moment of fetching, recoverable only by re-asking for ten years per currency.

    BYTES, NOT A ROUND TRIP. `json.loads(stored) == json.loads(served)` would pass a stage 1 that
    re-serialised the body, and re-serialising is already an interpretation: key order, number
    formatting, whitespace. The sha256 is the identity of the provider's answer.
    """
    serving(200, EUR_BODY)
    doc = yahoo_chart.fetch("EURUSD=X", range_="5d", interval="1d")
    (row,) = fx.raw_rows("EUR", doc, interval="1d", range_="5d", run_id="r")

    assert row["body"] == EUR_BODY
    assert row["sha256"] == hashlib.sha256(EUR_BODY).hexdigest()
    # WHAT IS IN IT, measured rather than assumed: OHLCV, adjclose and a meta block of 29 keys — of
    # which the FX rules read the close, the timestamp and three meta keys.
    result = json.loads(row["body"])["chart"]["result"][0]
    assert set(result["indicators"]["quote"][0]) == {"open", "high", "low", "close", "volume"}
    assert "adjclose" in result["indicators"]
    assert len(result["meta"]) == 29


def test_a_field_yahoo_adds_tomorrow_is_on_disk_without_a_code_change() -> None:
    """The property the rule exists for, stated directly: a key this parser has never heard of is
    stored, and the parser still reads what it does know beside it."""
    served = json.loads(EUR_BODY)
    served["chart"]["result"][0]["events"] = {"splits": {"1789000000": {"numerator": 2}}}
    body = json.dumps(served).encode()

    (row,) = fx.raw_rows("EUR", document(body), interval="1d", range_="5d", run_id="r")
    assert json.loads(row["body"])["chart"]["result"][0]["events"]["splits"]
    assert yahoo_chart.parse(row["body"]).points, "and the known fields still parse beside it"


def test_the_stored_url_is_the_request_including_its_parameters(serving: Any) -> None:
    """`range` and `interval` decide what a chart body even contains, so they are provenance."""
    serving(200, EUR_BODY)
    doc = yahoo_chart.fetch("EURUSD=X", range_="5d", interval="1d")
    assert doc.url.endswith("/v8/finance/chart/EURUSD=X?range=5d&interval=1d"), doc.url


def test_the_real_capture_ends_in_a_live_quote_and_stage_2_refuses_it() -> None:
    """Measured on the captured body: its last timestamp EQUALS `meta.regularMarketTime`, to the
    second — Friday's last tick at 21:29:58Z, a price wearing a close's clothes. Raw keeps it;
    `normalise` refuses it.

    AND IT SHARES ITS DATE WITH A REAL BAR. The body carries 2026-09-11's daily bar, stamped at the
    London session open with a close of 1.16099524, AND the live quote dated the same London day at
    1.16009283. A rule keyed on the DATE — "keep the last point per day" — would publish the live
    price as that day's rate: wrong in the fourth decimal and entirely plausible. The first version
    of this test asserted the live quote's date was absent from the output, which is that same
    mistake made in a test, and the capture refuted it. Only `regularMarketTime` identifies it.
    """
    series = yahoo_chart.parse(EUR_BODY)
    live = series.points[-1]
    assert live.is_live and series.live_points == 1
    bars = [p for p in series.points if not p.is_live]
    assert live.as_of in {p.as_of for p in bars}, "the capture must exhibit the shared date"

    got = rates_from("EUR", EUR_BODY)
    assert got.stats["live_points"] == 1
    assert [(r.as_of, r.usd_per_unit) for r in got.rates] == [(p.as_of, p.close) for p in bars], (
        "every completed bar is published with its own close, and the live quote is not — not "
        "even for the day it shares with a bar"
    )


def test_the_pair_is_built_in_one_place_and_is_never_inverted() -> None:
    """`USDTWD=X` and `TWDUSD=X` are both valid and exact reciprocals, so nothing about a returned
    value says which was asked. The band below catches most inversions and provably cannot catch one
    near 0.5 — what protects those is this function being the only place the symbol is assembled."""
    assert yahoo_chart.pair("TWD") == "TWDUSD=X"
    assert yahoo_chart.pair("EUR") == "EURUSD=X"


def test_an_unknown_pair_answers_404_and_that_is_an_ABSENCE_not_a_fault(serving: Any) -> None:
    """THE CAPTURE'S MOST VALUABLE CASE. `ZZZUSD=X` returns HTTP **404** whose whole body is

        {"chart":{"result":null,"error":{"code":"Not Found",
                  "description":"No data found, symbol may be delisted"}}}

    Raising on it — which the provider did when the field capture was taken — reports every
    unquoted currency as a transport failure. And because a transport failure must never mark a
    subject absent, the negative cache could never fill and those pairs would be re-asked for ever.
    Same shape as a SEC 400 that names a company with no Form 4.

    AND THE ABSENCE IS NOW PROVABLE FROM DISK: the 404's own body is stored like any other answer,
    so "Yahoo said it does not carry this pair" is a file rather than a counter.
    """
    serving(404, ABSENT_BODY)
    doc = yahoo_chart.fetch("ZZZUSD=X", range_="5d", interval="1d")
    assert doc.body == ABSENT_BODY
    assert yahoo_chart.parse(doc.body).points == []
    assert rates_from("ZZZ", doc.body).rates == []


def test_a_transport_failure_is_still_a_failure(
    monkeypatch: pytest.MonkeyPatch, serving: Any
) -> None:
    """The other half, and deleting it makes the test above pass for the wrong reason: an absence
    and a refusal must not collapse into each other in EITHER direction."""

    def boom(*args: object, **kwargs: object) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "get", boom)
    with pytest.raises(yahoo_chart.YahooRefused):
        yahoo_chart.fetch("EURUSD=X", range_="5d", interval="1d")

    serving(500, b"{}")
    with pytest.raises(yahoo_chart.YahooRefused):
        yahoo_chart.fetch("EURUSD=X", range_="5d", interval="1d")


def test_a_404_that_does_not_name_an_absence_is_a_refusal(serving: Any) -> None:
    """THE TWO 404s MUST NOT COLLAPSE. Yahoo's absence names itself in `chart.error`; a 404 from
    anything else — a moved endpoint, a proxy, an HTML error page — says nothing about the pair, and
    storing it as an answer would let the negative cache fill from our own misconfiguration."""
    serving(404, b"<html><body>Not Found</body></html>")
    with pytest.raises(yahoo_chart.YahooRefused):
        yahoo_chart.fetch("EURUSD=X", range_="5d", interval="1d")


def test_a_null_among_the_closes_is_dropped_not_carried() -> None:
    """The captured EUR window has SIX timestamps and a null among the closes. Yahoo returns
    parallel arrays, so dropping a close without dropping its timestamp shifts every later point
    onto the wrong date — a whole series off by one, with every value individually plausible."""
    body = rebuilt("eur_spot")
    series = yahoo_chart.parse(body)
    points = series.points

    assert series.live_points == 1, "the final point is a live quote, not a bar"
    assert series.null_closes == 1, "a padded row for a session with no close yet"
    # EVERY POINT IS READ. The live quote and the null are OBSERVED by the parse and REFUSED by the
    # rules — and since 2026-09-12 both run against the stored body, so correcting either is a
    # re-parse rather than a refetch of every series held.
    assert len(points) == 6
    assert sum(1 for p in points if p.is_live) == 1
    assert sum(1 for p in points if p.close is None) == 1
    assert len(rates_from("EUR", body).rates) == 4, (
        "normalise must refuse the live quote and the null close — a mid-session price wearing "
        "a close's clothes is the defect this pipeline replaces a resource for"
    )

    # THE PAIRING IS THE ASSERTION. Checking only that the dates come out ascending and distinct
    # passes under BOTH rules: filtering the nulls out of `close` before zipping still yields
    # ascending dates, just the wrong ones. With the null at index 4 of 6, a filter-then-zip puts
    # the final close a day early — and every pair is checked, the live quote's own date included.
    captured = CAPTURED["eur_spot"]
    offset = timedelta(seconds=captured["meta"]["gmtoffset"])
    expected = [
        (datetime.fromtimestamp(s, tz=timezone(offset)).date(), c)
        for s, c in zip(captured["timestamp"], captured["close"], strict=True)
    ]
    shifted = (
        "the arrays are parallel, so dropping a close without its timestamp shifts every later "
        "point onto the wrong date — with every value individually plausible"
    )
    assert [p.as_of for p in points] == [d for d, _ in expected], shifted
    assert [p.close for p in points] == [
        c if c is None else pytest.approx(c) for _, c in expected
    ], shifted


def test_ten_years_of_weekly_rates_is_what_the_history_lane_gets() -> None:
    body = rebuilt("ils_history")
    series = yahoo_chart.parse(body)
    assert len(series.points) == 524, "every weekly point, the live quote included"
    assert series.live_points == 1
    rows = fx.raw_rows("ILS", document(body), interval="1wk", range_="10y", run_id="r")
    assert len(fx.normalise(rows).rates) == 523, (
        "stage 2 refuses the live quote, leaving ten years of completed weekly closes"
    )
    assert series.points[0].as_of < date(2017, 1, 1) < series.points[-1].as_of
    assert series.timezone_name == "Europe/London", (
        "recorded from the response, so a future date-shift argument is settled by what was "
        "received rather than by what someone remembers"
    )


def test_a_currency_the_provider_barely_carries_SUCCEEDS_and_loads_almost_nothing() -> None:
    """THE LARI, AND IT IS WHY THE NEGATIVE CACHE EXISTS. Yahoo carries exactly ONE bar for
    `GELUSD=X`, so a ten-year history fetch returns 200 with a single recent point. Nothing errors,
    a row is written, and "has a rate older than 90 days" stays false — so it was re-fetched eight
    times a day for ever with no count anywhere able to report it."""
    body = rebuilt("gel_history")
    series = yahoo_chart.parse(body)
    # "The provider answered, with a live quote and no completed bar" and "the provider returned
    # nothing" are different facts, and only the first is true here — the body on disk says so.
    assert len(series.points) == 1
    assert series.points[0].is_live
    assert series.live_points == 1
    assert rates_from("GEL", body).rates == [], (
        "no COMPLETED weekly bar exists for the lari — which is what the negative cache needs, "
        "and it is derived from the stored body rather than from a counter"
    )


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
    cannot tell an inversion from a genuinely odd quote. COUNTED, because a rising count is a
    statement about the provider rather than a repair to be pleased about."""
    day = date(2026, 9, 10)
    twd = rates_from("TWD", chart_body([(day, 31.6)]))
    eur = rates_from("EUR", chart_body([(day, 1.16)]))
    assert twd.rates == []
    assert twd.stats["refused_by_band"] == 1
    assert [r.usd_per_unit for r in eur.rates] == [1.16]


def test_the_spot_window_is_applied_in_stage_2_against_the_stored_body() -> None:
    """The spot lane asks five days so a weekend still yields a close, and publishes one. The cut
    used to happen before anything was stored — beside a date rule that once dated every FX bar a
    day early — so a fix to either cost ten years of refetching. Now it is a re-parse.

    It also pins the `gmtoffset` rule: the 09-10 bar is stamped 2026-09-09T23:00Z, so reading the
    stamp in UTC would file it under 09-09 and let the 09-11 bar into the 09-10 window instead."""
    days = [(date(2026, 9, 9), 1.15), (date(2026, 9, 10), 1.16), (date(2026, 9, 11), 1.17)]
    got = rates_from("EUR", chart_body(days), window=(date(2026, 9, 10), date(2026, 9, 11)))
    assert [(r.as_of, r.usd_per_unit) for r in got.rates] == [(date(2026, 9, 10), 1.16)]
    assert got.stats["outside_window"] == 2


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


def test_the_source_code_is_one_a_migration_has_actually_seeded() -> None:
    """`source_code` IS A FOREIGN KEY AND NOTHING DOWNSTREAM CAN CATCH A MISSING ROW.

    The first real run proved it: the provider answered all 38 currencies, and the write then failed
    entirely with `fx_rate_source_code_fkey — Key (source_code)=(yahoo) is not present in table
    "data_source"`. CLAUDE.md already carried the rule from migration 88, where
    `security-statements` lost a whole run's yfinance rows the same way.

    `yfinance` rather than `yahoo` is the deliberate answer: `data_source` names the VENDOR, every
    one of the 22,236 existing `fx_rate` rows says `yfinance`, and they were written by a resource
    that also called Yahoo's chart endpoint directly. Whether our side uses a library or plain HTTP
    is a fact about us.
    """
    assert fx.SOURCE_CODE == "yfinance"
    rows = fx.core_rows([fx.Rate("EUR", date(2026, 9, 10), 1.16)])
    assert {r["source_code"] for r in rows} == {"yfinance"}
