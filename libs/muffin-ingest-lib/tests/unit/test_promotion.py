"""The promotion probe's verdict, over bodies Yahoo actually sent.

Every fixture is a captured chart body. The rule under test is which lines a wave may promote, so
each verdict has a body that reaches it and no other.
"""

from datetime import UTC, datetime, time, timedelta
from pathlib import Path

import pytest

from muffin_ingest.facets import promotion
from muffin_ingest.providers import yahoo_chart

FIXTURES = Path(__file__).parents[1] / "fixtures"


def _body(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _newest_bar(name: str) -> datetime:
    series = yahoo_chart.parse(_body(name))
    newest = max(p.as_of for p in series.points if not p.is_live and p.close)
    return datetime.combine(newest, time(12), tzinfo=UTC)


@pytest.mark.parametrize(
    ("fixture", "want"),
    [
        ("yahoo_chart_bdms_f_bk_404.body", "absent"),
        ("yahoo_chart_aapl_max.body", "not_daily"),
        ("yahoo_chart_eurusd_5d.body", "not_equity"),
        ("yahoo_chart_pctn_l_quote_only.body", "quote_only"),
    ],
    ids=["yahoo-says-absent", "downsampled", "a-currency", "a-live-quote-only"],
)
def test_a_line_yahoo_does_not_price_is_not_priced(fixture: str, want: str) -> None:
    assert promotion.verdict(_body(fixture), datetime(2026, 10, 26, tzinfo=UTC)) == want


def test_a_line_with_a_recent_completed_bar_is_priced() -> None:
    """AAPL's extension body, read the day after its newest bar."""
    fetched = _newest_bar("yahoo_chart_aapl_ext.body") + timedelta(days=1)
    assert promotion.verdict(_body("yahoo_chart_aapl_ext.body"), fetched) == promotion.PRICED


@pytest.mark.parametrize(("days", "want"), [(10, "priced"), (11, "stale")])
def test_a_line_that_stopped_trading_is_stale(days: int, want: str) -> None:
    """The same body, read long enough after its newest bar: the window is from the fetch, so a body
    a provider serves stale cannot pass for a live line."""
    fetched = _newest_bar("yahoo_chart_vod_l_ext.body") + timedelta(days=days)
    assert promotion.verdict(_body("yahoo_chart_vod_l_ext.body"), fetched) == want


def test_a_known_symbol_with_nothing_in_the_window_is_empty() -> None:
    """Yahoo knows the symbol (a result, no error) and nothing traded: not an absence."""
    body = (
        b'{"chart":{"result":[{"meta":{"instrumentType":"EQUITY","dataGranularity":"1d"}}],'
        b'"error":null}}'
    )
    assert promotion.verdict(body, datetime(2026, 10, 26, tzinfo=UTC)) == "empty"


def test_an_unreadable_body_is_not_an_absence() -> None:
    assert (
        promotion.verdict(b"<html>gateway</html>", datetime(2026, 10, 26, tzinfo=UTC))
        == "unreadable"
    )


def test_every_verdict_is_an_outcome_the_table_accepts() -> None:
    """`market.promotion_probe.outcome` is checked against this list (muffin-deployment migration
    20261025170000); a verdict outside it would fail the whole write."""
    assert set(promotion.OUTCOMES) == {
        "priced",
        "absent",
        "empty",
        "stale",
        "quote_only",
        "not_equity",
        "not_daily",
        "unreadable",
    }


@pytest.mark.parametrize(
    ("ticker", "provider_symbol", "country", "suffixes", "want"),
    [
        # KOSDAQ: the KS composite's spelling first, then Yahoo's other Korean suffix.
        ("177900", "177900.KS", "KR", [".KS", ".KQ"], ["177900.KS", "177900.KQ"]),
        # Hong Kong: four digits, never the directory's `883.HK`.
        ("883", "883.HK", "HK", [".HK"], ["0883.HK"]),
        ("8495", "8495.HK", "HK", [".HK"], ["8495.HK"]),
        # Canada: Bloomberg's share-class slash, then every Canadian suffix.
        ("BEK/B", "BEK/B.TO", "CA", [".TO", ".V"], ["BEK-B.TO", "BEK-B.V"]),
        # A country with no suffix listed: the directory's own spelling alone.
        ("ABC", "ABC.ZZ", "ZZ", [], ["ABC.ZZ"]),
        # Padding is Hong Kong's alone: a short Australian number keeps its digits.
        ("360", "360.AX", "AU", [".AX"], ["360.AX"]),
    ],
    ids=["kosdaq", "hk-padded", "hk-four-digits", "ca-slash", "no-suffixes", "au-not-padded"],
)
def test_a_line_is_asked_under_each_spelling_its_country_uses(
    ticker: str, provider_symbol: str, country: str, suffixes: list[str], want: list[str]
) -> None:
    assert promotion.spellings(ticker, provider_symbol, country, suffixes) == want


@pytest.mark.parametrize(
    ("outcome", "next_one"),
    [("absent", True)] + [(o, False) for o in promotion.OUTCOMES if o != "absent"],
)
def test_only_an_unknown_name_sends_the_probe_to_the_next_spelling(
    outcome: str, next_one: bool
) -> None:
    """A stale or quote-only line is the company not trading; another spelling would only find the
    same company, or a different one."""
    assert promotion.asks_the_next_spelling(outcome) is next_one
