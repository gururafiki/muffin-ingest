"""The price lane's chart rules, driven over Yahoo's own bytes.

EVERY `yahoo_chart_*.body` HERE IS A RESPONSE BODY, byte for byte, captured from the node through
http-cache on 2026-10-10 (`capture.md`). Each pins a shape nobody would have written:

  `amrm_ta_break`  Tel Aviv's change from shekels to agorot on 2026-05-18, a 96.6x step, with every
                   bar labelled `ILA` by Yahoo — the reason a label needs a unit rule at all.
  `vod_jo_bounce`  Johannesburg's 2025-01-10, one session quoted in rand among cents, undone the
                   next day — the reason the unit rule needs to tell a bounce from a change.
  `vod_l_ext`      `GBp`: pence, which case-folding turns into pounds.
  `npn_jo_ext`     `ZAc`: cents.
  `alg_kw_week`    `KWF`: fils, a thousandth of a dinar, and Kuwait's Sunday sessions.
  `7203_t_ext`     Tokyo, stamped at 00:00 UTC, with a dividend event in the window.
  `bhp_ax_ext`     a live point dated Friday beside Friday's completed bar, on the Saturday after.
  `aapl_max`       `range=max` with `interval=1d`, which Yahoo answers QUARTERLY.
  `bdms_f_bk_404`  the 404 that names an absence.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

from muffin_ingest.facets import fx, price_chart
from muffin_ingest.facets.prices import Subject
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parents[1] / "fixtures"
KNOWN = frozenset({"USD", "GBP", "GBX", "ZAR", "ZAC", "ILS", "ILA", "KWD", "KWF", "JPY", "AUD"})
TODAY = date(2026, 10, 10)
WINDOW = (date(1970, 1, 1), TODAY)


def body(name: str) -> bytes:
    return (FIXTURES / f"yahoo_chart_{name}.body").read_bytes()


def row(
    content: bytes,
    *,
    sid: str = "s-1",
    symbol: str = "X",
    fetched: datetime = datetime(2026, 10, 10, 0, 5, tzinfo=UTC),
    period1: int = 0,
) -> dict[str, Any]:
    """A stored document row, the way stage 1 writes it."""
    document = Document(
        url=f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?period1={period1}",
        body=content,
        content_type="application/json",
        fetched_at=fetched,
    )
    subject = Subject(security_id=sid, symbol=symbol, weight=1.0)
    return price_chart.raw_rows(subject, document, run_id="r", period1=period1, period2=1)[0]


def made(
    points: list[tuple[date, float | None]], *, currency: str = "USD", live: bool = False
) -> bytes:
    """A small daily body in Yahoo's shape, stamped at a 09:30 open in UTC-4."""
    tz = timezone(timedelta(hours=-4))
    stamps = [
        int(datetime(d.year, d.month, d.day, 9, 30, tzinfo=tz).timestamp()) for d, _ in points
    ]
    meta = {
        "currency": currency,
        "gmtoffset": -14400,
        "dataGranularity": "1d",
        "regularMarketTime": stamps[-1] if live else 0,
    }
    result = {
        "meta": meta,
        "timestamp": stamps,
        "indicators": {"quote": [{"close": [c for _, c in points], "volume": [1] * len(points)}]},
    }
    return json.dumps({"chart": {"result": [result], "error": None}}).encode()


def labels(rows: list[dict[str, Any]]) -> dict[str, str | None]:
    return {r["trade_date"]: r["currency_code"] for r in rows}


# --- the label -----------------------------------------------------------------------------------


def test_pence_is_never_case_folded_into_pounds() -> None:
    """THE HEADLINE CASE. `"GBp".upper()` is `"GBP"`, a hundred times the price, and it reads as
    a perfectly good currency. With pence unseeded the right answer is no label, never pounds."""
    assert price_chart.quote_currency("GBp", KNOWN) == "GBX"
    assert price_chart.quote_currency("GBp", KNOWN - {"GBX"}) is None, "not GBP: no label at all"
    assert price_chart.quote_currency("ZAc", KNOWN) == "ZAC"
    assert price_chart.quote_currency("ILA", KNOWN) == "ILA"
    assert price_chart.quote_currency("KWF", KNOWN) == "KWF"
    assert price_chart.quote_currency("GBP", KNOWN) == "GBP", "a London line in pounds stays pounds"


@pytest.mark.parametrize("code", ["USX", "Gbp", "gbp", "", None, 123, "GBPX"])
def test_a_code_we_do_not_hold_is_withheld_rather_than_guessed(code: object) -> None:
    assert price_chart.quote_currency(code, KNOWN) is None


@pytest.mark.parametrize(
    ("fixture", "code"),
    [("vod_l_ext", "GBX"), ("npn_jo_ext", "ZAC"), ("alg_kw_week", "KWF"), ("aapl_ext", "USD")],
)
def test_each_bar_carries_the_currency_its_listing_is_quoted_in(fixture: str, code: str) -> None:
    """Measured 2026-10-10: VOD.L was stored as `EUR`, NPN.JO as `ZAC`, ALG.KW as `KWD` — the last
    a thousand times out. The provider's own label is what each now carries."""
    out = price_chart.normalise(
        [row(body(fixture))], known=KNOWN, source_code="yfinance", window=WINDOW
    )
    assert out.rows and {r["currency_code"] for r in out.rows} == {code}
    assert out.newest_label == {"s-1": code}


def test_a_currency_nobody_seeded_leaves_the_bars_unlabelled_and_is_named() -> None:
    out = price_chart.normalise(
        [row(body("vod_l_ext"))], known=KNOWN - {"GBX"}, source_code="yfinance", window=WINDOW
    )
    assert {r["currency_code"] for r in out.rows} == {None}
    assert out.unknown_currencies == {"GBp": 1}
    assert out.stats["currency_unknown"] == 1
    assert out.stats["bars_unlabelled"] == len(out.rows)


# --- the unit rule -------------------------------------------------------------------------------


def test_bars_before_a_change_of_unit_carry_no_label() -> None:
    """AMRM.TA's bars before 2026-05-18 are shekels and Yahoo labels them `ILA` anyway. A label
    inferred from the size of the jump would be a guess (decision 3a), so they carry none."""
    out = price_chart.normalise(
        [row(body("amrm_ta_break"))], known=KNOWN, source_code="yfinance", window=WINDOW
    )
    by_day = labels(out.rows)
    before = {d: c for d, c in by_day.items() if d < "2026-05-18"}
    after = {d: c for d, c in by_day.items() if d >= "2026-05-18"}
    assert before and set(before.values()) == {None}
    assert after and set(after.values()) == {"ILA"}
    assert out.stats["unit_changes_found"] == 1
    assert out.stats["bars_before_a_unit_change"] == len(before)


def test_a_bounce_is_not_a_change_of_unit() -> None:
    """JOHANNESBURG, 2025-01-10: one session in rand among cents, undone the next day — measured on
    six lines at once. Read as a change of unit it would have unlabelled every bar before it; read
    as nothing it would label a rand price as cents. Only the bounced bar goes unlabelled."""
    out = price_chart.normalise(
        [row(body("vod_jo_bounce"))], known=KNOWN, source_code="yfinance", window=WINDOW
    )
    by_day = labels(out.rows)
    assert by_day["2025-01-10"] is None
    assert {c for d, c in by_day.items() if d != "2025-01-10"} == {"ZAC"}
    assert (out.stats["unit_changes_found"], out.stats["unit_bounces_found"]) == (0, 1)


@pytest.mark.parametrize(
    ("closes", "since", "bounced"),
    [
        ([1.0, 1.01, 0.99, 1.0], 0, set()),  # no step at all
        ([1.0, 6.0, 1.0, 1.0], 0, set()),  # noise above 5x, far from any factor
        ([1.0, 1.0, 100.0, 101.0, 99.0], 2, set()),  # a change that stands
        ([1.0, 1.0, 1000.0, 1001.0], 2, set()),  # fils: a thousandth
        ([100.0, 100.0, 1.0, 1.0, 1.0, 100.0, 100.0], 0, {2, 3, 4}),  # three days in the parent
        ([100.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 100.0], 12, set()),
    ],
)
def test_the_unit_rule(closes: list[float], since: int, bounced: set[int]) -> None:
    """The last case is a step undone only after the horizon: two changes, the newer one wins."""
    found = price_chart.units(closes)
    assert (found.since, set(found.bounced)) == (since, bounced)


# --- what a document is, and what several of them say ---------------------------------------------


def test_range_max_is_not_daily_and_is_refused_whole() -> None:
    """MEASURED 2026-10-10: `range=max&interval=1d` answered AAPL at `dataGranularity: 3mo`, 169
    points since 1984. Stored as daily bars, every quarter's close would stand for a single day."""
    series = yahoo_chart.parse(body("aapl_max"))
    assert series.granularity == "3mo" and len(series.points) == 169
    out = price_chart.normalise(
        [row(body("aapl_max"))], known=KNOWN, source_code="yfinance", window=WINDOW
    )
    assert out.rows == [] and out.stats["not_daily"] == 1


def test_a_dated_bar_is_in_the_exchange_s_calendar() -> None:
    """Tokyo opens at 00:00 UTC and Kuwait trades on Sundays; both land on their own dates."""
    tokyo = price_chart.normalise(
        [row(body("7203_t_ext"))], known=KNOWN, source_code="yfinance", window=WINDOW
    )
    assert min(labels(tokyo.rows)) == "2026-09-28" and max(labels(tokyo.rows)) == "2026-10-09"
    kuwait = price_chart.normalise(
        [row(body("alg_kw_week"))], known=KNOWN, source_code="yfinance", window=WINDOW
    )
    assert "2026-09-20" in labels(kuwait.rows), "a Sunday session"


def test_a_live_point_beside_its_day_s_bar_does_not_replace_it() -> None:
    """BHP.AX on the Saturday after: Friday's completed bar, then a live point dated Friday."""
    series = yahoo_chart.parse(body("bhp_ax_ext"))
    assert series.live_points == 1
    friday = [p for p in series.points if p.as_of == date(2026, 10, 9)]
    assert len(friday) == 2 and [p.is_live for p in friday] == [False, True]
    out = price_chart.normalise(
        [row(body("bhp_ax_ext"))], known=KNOWN, source_code="yfinance", window=WINDOW
    )
    published = [r for r in out.rows if r["trade_date"] == "2026-10-09"]
    assert [r["close"] for r in published] == [friday[0].close]


def test_a_newer_document_restates_and_withdraws_but_a_null_erases_nothing() -> None:
    d = [date(2026, 9, 21 + i) for i in range(5)]
    older = row(
        made([(d[0], 10.0), (d[1], 11.0), (d[2], 12.0), (d[3], 13.0)]),
        fetched=datetime(2026, 9, 26, tzinfo=UTC),
    )
    # Restates d1, withdraws d2 (inside its span, no longer carried), pads d3 with a null.
    newer = row(
        made([(d[1], 11.5), (d[3], None), (d[4], 14.0)]),
        fetched=datetime(2026, 10, 1, tzinfo=UTC),
        period1=1,
    )
    stats: dict[str, int] = {}
    history = price_chart.merge([newer, older], stats)  # fetch order decides, not list order
    assert {k: v.close for k, v in history.bars.items()} == {
        d[0]: 10.0,
        d[1]: 11.5,
        d[3]: 13.0,
        d[4]: 14.0,
    }
    assert d[2] not in history.held, "withdrawn: the newer statement covers it and no longer has it"
    assert d[3] in history.held, "a null close is a session the provider has"
    assert (stats["restated"], stats["withdrawn"]) == (1, 1)


def test_a_document_naming_an_absence_changes_nothing_stored() -> None:
    stats: dict[str, int] = {}
    history = price_chart.merge(
        [
            row(body("aapl_ext")),
            row(body("bdms_f_bk_404"), fetched=datetime(2026, 10, 11, tzinfo=UTC)),
        ],
        stats,
    )
    assert len(history.bars) == 10 and history.currency == "USD"
    assert stats["absences"] == 1


def test_the_window_refuses_today_and_is_half_open() -> None:
    out = price_chart.normalise(
        [row(made([(date(2026, 10, 9), 1.0), (TODAY, 2.0)]))],
        known=KNOWN,
        source_code="yfinance",
        window=WINDOW,
    )
    assert list(labels(out.rows)) == ["2026-10-09"]
    assert out.stats["outside_window"] == 1


# --- stage 1: what a visit asks for --------------------------------------------------------------


def test_a_first_visit_loads_everything() -> None:
    assert price_chart.plan([], "AAPL", TODAY) == price_chart.Plan(
        start=None, reason="never_loaded"
    )


def test_a_symbol_change_restarts_the_history() -> None:
    stored = [row(body("aapl_ext"), symbol="ASMLF")]
    assert price_chart.plan(stored, "ASML", TODAY).reason == "symbol_changed"


def test_a_visit_extends_from_the_newest_point_less_a_week() -> None:
    loaded = datetime(2026, 9, 1, tzinfo=UTC)
    stored = [
        row(body("aapl_ext"), symbol="AAPL", fetched=loaded, period1=0),
        row(
            made([(date(2026, 9, 1), 1.0)]),
            symbol="AAPL",
            fetched=datetime(2026, 9, 2, tzinfo=UTC),
            period1=5,
        ),
    ]
    chosen = price_chart.plan(stored, "AAPL", TODAY)
    # The newest document is the small one, but its point is older: the newest DOCUMENT with points
    # decides, because requests advance in time.
    assert chosen.reason == "extend" and chosen.start == date(2026, 9, 1) - timedelta(days=7)
    assert chosen.loaded_on == date(2026, 9, 1)


def test_a_full_history_older_than_the_reload_age_is_loaded_again() -> None:
    stored = [row(body("aapl_ext"), symbol="AAPL", fetched=datetime(2026, 7, 1, tzinfo=UTC))]
    assert price_chart.plan(stored, "AAPL", TODAY).reason == "reload_due"


def test_a_later_full_load_that_did_not_answer_does_not_refresh_the_age() -> None:
    """A full history that answers replaces the file, so the OLDEST full load in it is the one that
    did; a later one still in it failed to replace and says nothing about freshness."""
    stored = [
        row(body("aapl_ext"), symbol="AAPL", fetched=datetime(2026, 7, 1, tzinfo=UTC)),
        row(body("bdms_f_bk_404"), symbol="AAPL", fetched=datetime(2026, 10, 1, tzinfo=UTC)),
    ]
    assert price_chart.plan(stored, "AAPL", TODAY).reason == "reload_due"


def test_a_split_on_or_after_the_load_makes_the_history_stale() -> None:
    series = yahoo_chart.Series(
        meta={"gmtoffset": 0},
        events={"splits": {"1": {"date": int(datetime(2026, 9, 3, tzinfo=UTC).timestamp())}}},
    )
    assert price_chart.stale_after_split(series, date(2026, 9, 3))
    assert price_chart.stale_after_split(series, date(2026, 9, 1))
    assert not price_chart.stale_after_split(series, date(2026, 9, 4))
    assert not price_chart.stale_after_split(series, None)
    assert not price_chart.stale_after_split(
        yahoo_chart.parse(body("7203_t_ext")), date(2026, 9, 1)
    ), "a dividend is not a split: Yahoo's close is split-adjusted, not dividend-adjusted"


def test_raw_is_the_body_plus_what_we_asked_and_nothing_derived() -> None:
    content = body("alg_kw_week")
    stored = row(content)
    document_columns = set(
        Document(url="u", body=b"", content_type="c", fetched_at=datetime.now(UTC)).as_row("r")
    )
    assert set(stored) == document_columns | price_chart.CONTEXT_COLUMNS
    assert hashlib.sha256(stored["body"]).hexdigest() == hashlib.sha256(content).hexdigest()


# --- the request ---------------------------------------------------------------------------------


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> list[httpx.URL]:
    urls: list[httpx.URL] = []

    def get(url: str, *, params: dict[str, str], **kwargs: Any) -> httpx.Response:
        urls.append(httpx.URL(url, params=params))
        return httpx.Response(200, content=body("aapl_ext"))

    monkeypatch.setattr(httpx, "get", get)
    return urls


def test_a_history_is_asked_by_dates_with_its_events_and_adjusted_close(
    sent: list[httpx.URL],
) -> None:
    doc = yahoo_chart.fetch(
        "AAPL",
        interval="1d",
        period1=0,
        period2=yahoo_chart.day_start(TODAY),
        events="div,split",
        adjusted=True,
    )
    params = dict(sent[0].params)
    assert params == {
        "period1": "0",
        "period2": str(int(datetime(2026, 10, 10, tzinfo=UTC).timestamp())),
        "interval": "1d",
        "includeAdjustedClose": "true",
        "events": "div,split",
    }
    assert "range" not in params, "range=max is quarterly; a history is asked by dates"
    assert doc.url == str(sent[0])


def test_the_fx_request_is_the_url_it_has_always_been(sent: list[httpx.URL]) -> None:
    """The order of the parameters is the http-cache key, so the FX lane's must not move."""
    yahoo_chart.fetch("EURUSD=X", range_="5d", interval="1d")
    assert str(sent[0]).endswith("/v8/finance/chart/EURUSD=X?range=5d&interval=1d")


@pytest.mark.parametrize(
    "window", [{}, {"range_": "5d", "period1": 0, "period2": 1}, {"period1": 0}]
)
def test_a_window_is_a_preset_or_two_dates_never_both_or_neither(window: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        yahoo_chart.fetch("AAPL", interval="1d", **window)


def test_a_named_absence_is_kept_as_the_provider_said_it() -> None:
    series = yahoo_chart.parse(body("bdms_f_bk_404"))
    assert series.points == [] and series.error == {
        "code": "Not Found",
        "description": "No data found, symbol may be delisted",
    }


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity"])
def test_a_close_that_is_not_finite_is_not_a_close(token: str) -> None:
    """`json.loads` accepts these tokens, `inf > 0` is true, and Postgres sorts NaN above every
    number — so nothing downstream would refuse one."""
    raw = made([(date(2026, 10, 8), 1.0), (date(2026, 10, 9), 2.0)]).replace(b"2.0", token.encode())
    series = yahoo_chart.parse(raw)
    assert [p.close for p in series.points] == [1.0, None]
    assert series.null_closes == 1


# --- FX: the subunit this change adds ------------------------------------------------------------


def test_pence_is_a_hundredth_of_a_pound() -> None:
    assert fx.SUBUNITS["GBX"] == ("GBP", 100.0)


def test_a_subunit_the_currency_table_lacks_is_not_derived() -> None:
    """`fx_rate.currency_code` is a foreign key: one derived `GBX` row before the migration seeding
    it would fail the whole write, every currency with it."""
    pound = [fx.Rate(currency_code="GBP", as_of=date(2026, 10, 9), usd_per_unit=1.34)]
    assert not [
        r for r in fx.with_subunits(pound, known={"GBP", "USD"}) if r.currency_code == "GBX"
    ]
    pence = [r for r in fx.with_subunits(pound, known={"GBP", "GBX"}) if r.currency_code == "GBX"]
    assert [round(r.usd_per_unit, 4) for r in pence] == [0.0134]
