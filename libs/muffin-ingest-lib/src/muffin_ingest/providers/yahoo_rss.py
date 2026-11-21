"""Yahoo's per-company RSS headlines, for the news lane (Phase 6b, decided 2026-11-21).

WHY THIS FEED. The edge's `security-news` asked openbb's yfinance news route, which has answered
**204** since 2026-10-01; the edge stamped `news_fetched_at` whatever came back, so every run read
`remaining: 0` while the newest stored article aged past a month. This feed was measured on
2026-11-21 (umbrella docs/specs/2026-11-21-macro-and-events.md, step 1): 16-18 current headlines
with summaries for AAPL, SAP.DE and 7203.T, no key, and no throttling across 100 companies paced at
one second.

ONE REQUEST PER COMPANY. The feed accepts several symbols, and then merges their items with no
symbol on any of them, so the attribution `news_security` records would be lost.

PAST THE CACHE. The `yahoo-rss` location is `proxy_cache off`: a feed is the newest twenty items, so
a stored answer is last week's news.

THE BODY IS STORED WHOLE and classified in stage 2 (`facets.news`). An unknown symbol is answered
200 with an EMPTY channel whose description reads "Latest Financial News for " with the symbol
blanked, so nothing here can tell it apart from a real answer.
"""

from __future__ import annotations

from urllib.parse import quote

from muffin_ingest import settings
from muffin_ingest.providers.documents import Document, _get

#: Yahoo's own host. `settings.provider_base` points it at http-cache when one is configured.
REAL_ORIGIN = "https://feeds.finance.yahoo.com"

#: The name these requests are counted under, and the `source_code` the links are written under.
PROVIDER = "yahoo-rss"

#: The User-Agent the validation used. The feed has not been measured without one.
BROWSER_UA = "Mozilla/5.0 (compatible; muffin-market-data)"


def url_for(symbol: str) -> str:
    """The feed's URL for one symbol. Every character of the symbol is encoded, `/` included: a
    slash read as a path separator would ask about a different resource (umbrella CLAUDE.md, "A `/`
    in a symbol is a second path segment")."""
    base = settings.provider_base(PROVIDER, REAL_ORIGIN)
    return f"{base}/rss/2.0/headline?s={quote(symbol, safe='')}&region=US&lang=en-US"


def fetch(symbol: str, *, timeout_s: float = 15.0) -> Document:
    """One company's headlines, the response body whole. A non-200 raises `DocumentRefused`."""
    return _get(
        url_for(symbol),
        provider=PROVIDER,
        headers={"User-Agent": BROWSER_UA, "Accept": "application/rss+xml, application/xml"},
        timeout_s=timeout_s,
    )
