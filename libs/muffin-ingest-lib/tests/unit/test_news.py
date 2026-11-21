"""The news lane's provider and parser, over the RSS documents captured on 2026-11-21."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from muffin_ingest import settings
from muffin_ingest.facets import news
from muffin_ingest.providers import yahoo_rss

NEWS = Path(__file__).resolve().parents[1] / "fixtures" / "news"


def body(symbol: str) -> bytes:
    return (NEWS / f"yahoo_rss_{symbol}.xml").read_bytes()


def doc(sid: str, raw: bytes, fetched_at: str = "2026-11-21T16:00:00+00:00") -> dict[str, object]:
    return {"security_id": sid, "body": raw, "fetched_at": fetched_at, "url": f"u/{sid}"}


NOW = datetime(2026, 11, 21, 18, 0, tzinfo=UTC)


# --- the provider --------------------------------------------------------------------------------


def test_a_slash_in_the_symbol_is_encoded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HTTP_CACHE_URL", raising=False)
    monkeypatch.delenv("YAHOO_RSS_BASE_URL", raising=False)
    assert yahoo_rss.url_for("3BBIF/F.BK") == (
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=3BBIF%2FF.BK&region=US&lang=en-US"
    )


def test_the_cache_location_serves_the_feed_s_own_host() -> None:
    assert settings.CACHE_LOCATIONS[yahoo_rss.PROVIDER] == "feeds.finance.yahoo.com"


# --- what a document says ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("symbol", "outcome", "items"),
    [
        ("AAPL", "answered", 18),
        ("SAP.DE", "answered", 18),
        ("7203.T", "answered", 16),
        ("PTSB.IR", "answered", 1),
        ("ZZQQXX", "unknown", 0),
    ],
)
def test_each_capture_reads_as_what_it_is(symbol: str, outcome: str, items: int) -> None:
    result = news.read(body(symbol))
    assert (result.outcome, len(result.items), result.skipped) == (outcome, items, 0)


def test_a_known_symbol_with_no_items_is_empty_not_unknown() -> None:
    # The unknown capture with the symbol put back: the only difference is the name in the channel.
    named = body("ZZQQXX").replace(
        b"Latest Financial News for </description>",
        b"Latest Financial News for PTSB.IR</description>",
    )
    assert named != body("ZZQQXX")
    assert news.read(named).outcome == "empty"


@pytest.mark.parametrize("raw", [b"", b"<html><body>gateway</body></html>", b'{"a": 1}'])
def test_a_body_that_is_not_rss_is_unreadable(raw: bytes) -> None:
    assert news.read(raw).outcome == "unreadable"


def test_an_item_is_dated_in_utc_and_keeps_its_summary() -> None:
    first = news.read(body("AAPL")).items[0]
    assert first.published_at == datetime(2026, 11, 21, 4, 0, tzinfo=UTC)
    assert first.title.startswith("Startup sues Apple in India")
    assert first.summary is not None and first.summary.startswith("A U.S. startup")


def test_an_item_missing_a_link_or_date_is_skipped_and_counted() -> None:
    raw = (
        body("PTSB.IR").replace(b"<pubDate>", b"<pubDateX>").replace(b"</pubDate>", b"</pubDateX>")
    )
    result = news.read(raw)
    assert (result.outcome, len(result.items), result.skipped) == ("answered", 0, 1)


# --- the URL key ---------------------------------------------------------------------------------


def test_only_the_feed_s_tracking_parameter_leaves_the_url() -> None:
    link = "https://www.tikr.com/blog/x?ref=yahoofinance&tlnk=yf-261120-pg825wq7&.tsrc=rss"
    assert (
        news.article_url(link)
        == "https://www.tikr.com/blog/x?ref=yahoofinance&tlnk=yf-261120-pg825wq7"
    )
    assert (
        news.article_url("https://finance.yahoo.com/a.html?.tsrc=rss")
        == "https://finance.yahoo.com/a.html"
    )


def test_every_other_byte_of_the_url_is_kept() -> None:
    # An encoded value and a blank one survive as sent; decoding and re-encoding would change both.
    link = "https://x.com/a?q=a%2Fb+c&flag&.tsrc=rss#frag"
    assert news.article_url(link) == "https://x.com/a?q=a%2Fb+c&flag#frag"
    assert news.article_url("https://x.com/a?b=1") == "https://x.com/a?b=1"
    assert news.article_url("https://x.com/a") == "https://x.com/a"


def test_every_captured_link_loses_the_tracking_parameter() -> None:
    urls = [i.url for s in ("AAPL", "SAP.DE", "7203.T") for i in news.read(body(s)).items]
    assert urls and not any(".tsrc" in u for u in urls)


# --- rows ----------------------------------------------------------------------------------------


def test_an_article_returned_for_two_companies_is_stored_once_and_linked_twice() -> None:
    parsed = news.normalise([doc("a", body("AAPL")), doc("b", body("AAPL"))], now=NOW)
    assert parsed.stats["articles"] == 18
    assert parsed.stats["links"] == 36
    assert {link["security_id"] for link in parsed.links} == {"a", "b"}
    assert {link["source_code"] for link in parsed.links} == {news.SOURCE}
    assert len({a["url"] for a in parsed.articles}) == len(parsed.articles)


def test_only_each_company_s_newest_document_is_read() -> None:
    parsed = news.normalise(
        [doc("a", body("AAPL"), "2026-11-14T00:00:00+00:00"), doc("a", body("PTSB.IR"))], now=NOW
    )
    assert parsed.outcomes == {"a": "answered"}
    assert parsed.stats["articles"] == 1


def test_an_article_past_the_retention_window_is_not_written() -> None:
    # The capture's 18 items run from 2026-11-20 18:13 to 2026-11-21 06:32 UTC. Ninety days before
    # this `now` is 2026-11-21 00:00, the 20th's eleven items are too old, seven are kept.
    later = datetime(2027, 2, 19, 0, 0, tzinfo=UTC)
    parsed = news.normalise([doc("a", body("AAPL"))], now=later)
    assert (parsed.stats["items"], parsed.stats["items_too_old"]) == (18, 11)
    assert parsed.stats["articles"] == 7
    assert all(a["published_at"] >= datetime(2026, 11, 21, tzinfo=UTC) for a in parsed.articles)


def test_documents_are_counted_by_outcome() -> None:
    parsed = news.normalise(
        [doc("a", body("AAPL")), doc("b", body("ZZQQXX")), doc("c", b"<html/>")], now=NOW
    )
    assert {k: parsed.stats[f"documents_{k}"] for k in news.OUTCOMES} == {
        "answered": 1,
        "empty": 0,
        "unknown": 1,
        "unreadable": 1,
    }
    assert parsed.stats["documents"] == 3
