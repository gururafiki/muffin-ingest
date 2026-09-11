"""Bar parsing and the return rules, each test named for the defect it prevents."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from muffin_ingest.derive import returns
from muffin_ingest.derive.returns import (
    PERIOD_DAYS,
    first_comparable_index,
    index_at_or_before,
    price_returns,
    total_returns,
)
from muffin_ingest.facets.prices import Bar, bar_from, bars_by_symbol
from muffin_ingest.providers.base import Provider, SecurityRef
from muffin_ingest.providers.outcome import Outcome
from muffin_ingest.providers.yfinance import Yfinance

NOW = date(2026, 9, 10)


def series(
    closes: list[float], *, end: date = NOW, dividends: dict[int, float] | None = None
) -> list[Bar]:
    """A daily series ending on `end`, oldest first. Index keys in `dividends` are into `closes`."""
    dividends = dividends or {}
    n = len(closes)
    return [
        Bar(end - timedelta(days=n - 1 - i), close, dividend=dividends.get(i))
        for i, close in enumerate(closes)
    ]


# --- the provider ------------------------------------------------------------------------------


def test_yfinance_is_a_provider() -> None:
    assert isinstance(Yfinance(), Provider)


def test_the_provider_symbol_beats_the_us_ticker() -> None:
    """OpenFIGI's US lookup is a thin OTC line for most foreign companies, and pricing off it prices
    a different instrument — 365 of 900 sampled non-US securities were mislabelled that way."""
    ref = SecurityRef("s", provider_symbol="005930.KS", us_ticker="SSNLF")
    assert Yfinance().spell(ref) == "005930.KS"


def test_a_security_with_no_name_this_provider_knows_is_not_an_absence() -> None:
    """`None` means "cannot be asked", which must never reach the ledger as "has no data"."""
    assert Yfinance().spell(SecurityRef("s")) is None


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("YFRateLimitError: Too Many Requests", Outcome.THROTTLED),
        ("'NoneType' object has no attribute 'empty'", Outcome.DEAD_SUBJECT),
        ("connection refused", Outcome.TRANSPORT),
    ],
)
def test_the_provider_shares_the_vocabulary(message: str, expected: Outcome) -> None:
    assert Yfinance().classify(message) is expected


# --- bar parsing -------------------------------------------------------------------------------


def test_a_single_symbol_response_carries_no_symbol_column() -> None:
    """The provider adds `symbol` only when SEVERAL are requested, so one-symbol batches must be
    told what they asked about — otherwise a whole series is attributed to nothing."""
    parsed = bars_by_symbol([{"date": "2026-09-09", "close": 12.5}], "NESN.SW")
    assert list(parsed) == ["NESN.SW"]


def test_a_batched_response_is_attributed_by_its_own_symbol_column() -> None:
    rows = [
        {"symbol": "AAPL", "date": "2026-09-09", "close": 230.0},
        {"symbol": "MSFT", "date": "2026-09-09", "close": 410.0},
    ]
    assert set(bars_by_symbol(rows, "IGNORED")) == {"AAPL", "MSFT"}


@pytest.mark.parametrize("close", [0, -1, None, "12.0", True])
def test_a_close_that_is_not_a_positive_number_is_not_a_bar(close: object) -> None:
    """A zero close yields -100% on EVERY period at once — a 1,078-row defect. `True` is refused
    explicitly because bool is an int in Python and would otherwise store as 1.0."""
    assert bar_from({"date": "2026-09-09", "close": close}, "X") is None


def test_bars_come_back_sorted_however_the_provider_ordered_them() -> None:
    """Every rule downstream reads the series positionally, so a provider that changed its ordering
    would change the numbers rather than fail."""
    rows = [
        {"date": "2026-09-09", "close": 3.0},
        {"date": "2026-09-07", "close": 1.0},
        {"date": "2026-09-08", "close": 2.0},
    ]
    assert [b.close for b in bars_by_symbol(rows, "X")["X"]] == [1.0, 2.0, 3.0]


def test_actions_ride_on_the_same_response() -> None:
    """openbb's yfinance provider defaults `include_actions` to true, so a dividend and a split cost
    no extra call. They used to be discarded."""
    parsed = bar_from(
        {"date": "2026-09-09", "close": 10.0, "dividend": 0.25, "split_ratio": 2.0}, "X"
    )
    assert parsed is not None
    assert (parsed[1].dividend, parsed[1].split_ratio) == (0.25, 2.0)


# --- the return rules --------------------------------------------------------------------------


def test_a_stale_series_produces_nothing() -> None:
    """A delisted instrument keeps being served its final bars, so every period reads +0.00% —
    which looks like a flat market rather than a dead fund. Egypt, Nigeria and Portugal all did."""
    old = series([1.0, 2.0, 3.0], end=NOW - timedelta(days=40))
    assert price_returns(old, NOW) == {}


def test_a_window_that_never_moved_is_not_a_zero_return() -> None:
    """`GOTO.JK` had 62 bars over three months and ONE distinct close. The closes are positive, the
    latest bar is today and there is no discontinuity, so nothing else can see it."""
    assert price_returns(series([50.0] * 40), NOW) == {}


def test_a_flat_window_does_not_suppress_a_longer_one_that_moved() -> None:
    """Per WINDOW, not per series: a security can be flat over a week and informative over a year.

    The series has to reach back PAST the longer window's anchor for this to test anything — a
    shorter one withholds `1m` because there is no anchor at all, which passes for the wrong reason.
    """
    flat_week = series([10.0] * 40 + [20.0] * 8)
    out = price_returns(flat_week, NOW)
    assert "1w" not in out, "the last eight bars are all 20.0, so the week never moved"
    assert out["1m"] == pytest.approx(100.0), "the month contains the step and is informative"


def test_one_day_is_the_previous_bar_and_not_a_date_lookback() -> None:
    """A weekend or holiday resolves "yesterday" to the LATEST bar, not the one before it.

    THE FIXTURE HAS TO MAKE THE TWO RULES DISAGREE, and the first version of this test did not:
    with the newest bar dated today, "the previous bar" and "the newest bar on or before yesterday"
    are the same bar, so a mutation swapping them passed clean. The mutation harness reported it as
    MISSED, which is what that harness is for.

    Here the newest bar is Monday and it is now Tuesday, so a one-day lookback lands on Monday's own
    bar — the security is measured against itself.
    """
    friday_then_monday = [
        Bar(date(2026, 9, 4), 100.0),
        Bar(date(2026, 9, 7), 110.0),
    ]
    assert price_returns(friday_then_monday, date(2026, 9, 8))["1d"] == pytest.approx(10.0)


def test_ytd_anchors_on_last_years_final_close() -> None:
    """Anchoring on the first bar of the new year reports ~0% for the first days of trading."""
    jan = [
        Bar(date(2025, 12, 31), 100.0),
        Bar(date(2026, 1, 2), 110.0),
        Bar(date(2026, 1, 5), 120.0),
    ]
    assert price_returns(jan, date(2026, 1, 5))["ytd"] == pytest.approx(20.0)


def test_a_period_measured_across_a_discontinuity_is_omitted_not_capped() -> None:
    """SNDK really is up ~2,700%; clipping it would swap a right number for a different wrong one.
    What is untrustworthy is a return measured ACROSS the break."""
    broken = series([10.0] * 20 + [1000.0 + i for i in range(15)])
    out = price_returns(broken, NOW)
    assert "1m" not in out, "a window whose anchor predates the break must be dropped"
    assert "1d" in out, "the short windows after the break are still perfectly good"


def test_the_discontinuity_cut_is_the_most_recent_break() -> None:
    two_breaks = series([1.0] * 5 + [100.0] * 5 + [1.0] * 5 + [1.1, 1.2, 1.3])
    assert first_comparable_index(two_breaks) == 10


def test_a_real_move_below_the_ratio_is_not_a_break() -> None:
    """The largest legitimate one-day move measured across the universe was 2.04x, the smallest
    illegitimate one 6.0x — the populations separate with nothing in between."""
    doubled = series([10.0, 20.4, 21.0, 22.0])
    assert first_comparable_index(doubled) == 0


def test_index_at_or_before_returns_nothing_when_the_series_starts_later() -> None:
    assert index_at_or_before(series([1.0, 2.0]), date(2020, 1, 1)) is None


# --- total return ------------------------------------------------------------------------------


def test_a_dividend_moves_the_total_return_through_a_flat_price() -> None:
    """THE REASON THE NEVER-MOVED RULE IS NOT SHARED. The price genuinely did not move, so a price
    return is correctly withheld — while reinvestment did move the total return, so withholding
    that one too would report a paying security as having returned nothing."""
    flat_but_paying = series([100.0] * 30, dividends={25: 1.0})
    assert price_returns(flat_but_paying, NOW) == {}
    assert total_returns(flat_but_paying, NOW)["1w"] == pytest.approx(1.0)


def test_total_return_is_reinvested_rather_than_summed() -> None:
    """The simple form treats a dividend paid years ago as if it had sat in cash ever since.

    A 10% dividend and then a 10% rise COMPOUND to 21%, not the 20% a sum would give.
    """
    rising_and_paying = series([100.0] * 19 + [110.0], dividends={18: 10.0})
    out = total_returns(rising_and_paying, NOW)
    assert out["1d"] == pytest.approx(10.0)
    assert out["1w"] == pytest.approx(21.0), "1.1 * 1.1 - 1, not 0.10 + 0.10"


def test_total_return_obeys_the_same_eligibility_as_the_price_return() -> None:
    """Duplicating the checks loosely is how the two drift apart."""
    old = series([1.0, 2.0, 3.0], end=NOW - timedelta(days=40))
    assert total_returns(old, NOW) == {}


def test_a_non_positive_close_withholds_rather_than_poisons() -> None:
    """A zero close cannot denominate a return, and the failure mode to avoid is CONTAGION.

    `bar_from` rejects a non-positive close, so this can only arrive through a series built another
    way — which is exactly why the guard lives in the computation as well. The first version of this
    test built a series with no zero in it at all and passed while proving nothing.
    """
    closes = [10.0] * 20 + [0.0] + [11.0] * 9
    with_zero = series(closes)
    assert any(b.close == 0.0 for b in with_zero), "the fixture must actually contain the zero"

    out = total_returns(with_zero, NOW)
    assert all(v == v and abs(v) != float("inf") for v in out.values()), "no NaN and no infinity"
    # The windows whose anchor sits before the zero are WITHHELD rather than reported as -100%.
    assert "1m" not in out


# --- the vocabulary the dimension carries -------------------------------------------------------


def test_ten_year_is_deliberately_not_produced() -> None:
    """`market.return_period` carries `10y` because the vocabulary allows it and nothing has ever
    produced one. Adding it here would change what the cutover is compared against."""
    assert "10y" not in PERIOD_DAYS
    long_history = series([100.0 + i for i in range(4000)])
    assert "10y" not in price_returns(long_history, NOW)


def test_what_these_tests_prove_and_what_they_do_not() -> None:
    """WHAT WAS MUTATION-PROVEN HERE, and one thing that was not.

    Each rule below was deleted from `derive/returns.py` and the named test required to go red,
    with the harness asserting the source actually changed first — a pattern that matches nothing
    reports MISSED as loudly as a rule that is genuinely unguarded, which is how the fourth entry
    was found to be decorative:

        the never-moved rule       -> ..._a_window_that_never_moved_is_not_a_zero_return
        the staleness rule         -> ..._a_stale_series_produces_nothing
        the comparability cut      -> ..._measured_across_a_discontinuity_is_omitted_not_capped
        1d = the previous BAR      -> ..._one_day_is_the_previous_bar_and_not_a_date_lookback
        total return NOT sharing   -> ..._a_dividend_moves_the_total_return_through_a_flat_price
          the never-moved rule

    THE FOURTH ONE PASSED CLEAN AT FIRST. With the newest bar dated today, "the previous bar" and
    "the newest bar on or before yesterday" resolve to the SAME bar, so the fixture could not tell
    the two candidate rules apart. It now dates the newest bar to Monday and asks on Tuesday, where
    they disagree.

    WHAT IS NOT PROVEN HERE: that these numbers match what production currently serves. Every rule
    is ported line for line, including the JavaScript rounding, precisely so that comparison is
    meaningful — but it is a PARITY RUN against 200 securities and every period, not a unit test,
    and it is the gate before the old resource is switched off.
    """


def test_the_same_bars_give_the_same_returns_whatever_day_the_job_runs() -> None:
    """AN ASSET RE-RUN OVER UNCHANGED INPUTS MUST NOT PRODUCE A DIFFERENT NUMBER, and until the
    windows were anchored on the last bar this one did.

    `price_returns` measures the VALUE from `series[-1]` and used to measure the WINDOW from
    wall-clock `now`, so a 3-month return over a series ending on a Friday started three months
    before Friday when the job ran on Friday and three months before the following Wednesday when
    it ran on Wednesday — silently including or dropping a bar at the far end. Nothing reported it;
    the number simply moved.

    Measured consequence: the returns parity gate reported 57% disagreement with the old resource,
    and 21% of the sampled disagreements were reproduced EXACTLY by recomputing over our own
    unchanged bars with the last bar as the basis. The old resource had always anchored that way.

    The fixture makes the two rules disagree on purpose — the series ends well before `now`, so a
    clock-anchored window and a data-anchored one cannot pick the same bar.
    """
    last = date(2026, 6, 30)
    series = [
        Bar(trade_date=last - timedelta(days=n), close=100.0 + (n % 37) + n * 0.05)
        for n in range(420, -1, -1)
    ]

    on_the_day = returns.price_returns(series, last)
    a_week_later = returns.price_returns(series, last + timedelta(days=7))

    assert on_the_day, "the fixture must actually produce returns or this asserts nothing"
    assert on_the_day == a_week_later, (
        "the same bars produced different returns a week apart — the window is being measured "
        "from the clock while the value is measured from the last bar"
    )


def test_staleness_still_reads_the_CLOCK_and_not_the_data() -> None:
    """THE OTHER HALF OF THAT SPLIT, and deleting it would be invisible in the test above.

    `now` still has one job: deciding whether a series is being updated at all. Only a clock can
    answer that — a series anchored entirely on its own last bar would call a dead listing perfectly
    current for ever, which is precisely the state `STALE_DAYS` exists to refuse.
    """
    last = date(2026, 6, 30)
    series = [Bar(trade_date=last - timedelta(days=n), close=100.0 + n) for n in range(60, -1, -1)]

    assert returns.price_returns(series, last), "fresh at its own last bar"
    long_after = last + timedelta(days=returns.STALE_DAYS + 5)
    assert returns.price_returns(series, long_after) == {}, (
        "a series whose newest bar is weeks old must yield nothing, however well-formed it is"
    )
