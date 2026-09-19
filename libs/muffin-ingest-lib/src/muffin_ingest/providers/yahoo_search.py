"""Yahoo's public symbol search, for the identity ladder: ISIN → the provider symbol.

WHY THIS EXISTS. `security_identifier.kind_code = 'ticker'` comes from OpenFIGI, whose ticker is the
BLOOMBERG spelling, and the price provider does not accept it — `BRK/B`, `RR/.L`, `WALMEX*.MX` all
fail while `BRK-B`, `RR.L` and the local lines work. Asking Yahoo which symbol carries an ISIN is
reading the answer from a source instead of applying rules written from memory, which is how `6.HK`
→ `0006.HK` and `ESSITYB.ST` → `ESSITY-B.ST` (no unusual character at all) are found.

THE ANSWER IS STORED WHOLE — `fetch` returns the response body as a `Document`, and the parse
(`facets/symbology.parse_yahoo_search`) runs in stage 2, so a corrected hit-selection rule costs a
re-parse, never a re-ask. The consumer REQUIRES the hit to belong to the security's home market,
because Yahoo's ISIN index is inconsistent (measured: Walmex resolves to a Frankfurt line only).
"""

from __future__ import annotations

from urllib.parse import quote

from muffin_ingest import settings
from muffin_ingest.providers.documents import Document, _get

#: Yahoo's own host. `settings.provider_base` points it at http-cache when one is configured.
REAL_ORIGIN = "https://query1.finance.yahoo.com"

#: The endpoint answers differently without a browser-like User-Agent.
BROWSER_UA = "Mozilla/5.0 (compatible; muffin-market-data)"


def search(isin: str, *, timeout_s: float = 15.0) -> Document:
    """One ISIN → whatever Yahoo's index says, response body whole.

    `quotesCount=10&newsCount=0` keeps the page to the quotes; the body is stored byte for byte and
    `parse_yahoo_search` reads it in stage 2.
    """
    base = settings.provider_base("yahoo", REAL_ORIGIN)
    url = f"{base}/v1/finance/search?q={quote(isin)}&quotesCount=10&newsCount=0"
    return _get(
        url,
        provider="yahoo",
        headers={"User-Agent": BROWSER_UA, "Accept": "application/json"},
        timeout_s=timeout_s,
    )
