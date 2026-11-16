"""NSE (National Stock Exchange of India): one company's annual results, and a filing's XBRL.

Two requests, measured 2026-11-16 from the node:

  * a company's RESULTS LIST (`api/corporates-financial-results?symbol=&period=Annual`), ~30 KB of
    JSON in 0.2-0.3 s, one row per result with its `xbrl` link. NSE answers an unknown symbol with
    `[]` and HTTP 200;
  * a filing's INSTANCE on the archives host, 46-109 KB, immutable, cached for a year.

THE API MOVED TO `www.nseindia.in`. Measured 2026-11-16: `www.nseindia.com/api/...` answers 301 to
the same path on `www.nseindia.in`, whose API answers 200 behind the same cookie handshake. The
`nse` cache location carries the new host (muffin-deployment); a client that followed the redirect
itself would leave the cache and carry a cookie minted for the other host. The archives host serves
both `nsearchives.nseindia.com` and `.in` with identical bytes, and every one of the 6,892 filings
stored before the move is keyed by its `.com` link, so links are kept in that form (`canonical`).

NSE REFUSES AN UNKNOWN CALLER, AND THE HANDSHAKE THAT FIXES IT ANSWERS 403: the landing page returns
403 while setting the cookies the API then accepts (the edge's `in.ts`, measured 2026-09-06, and
again 2026-11-16 on the new host). So the handshake is never checked for success; the results call
is. A cookie jar is scoped to its domain and the cache host is not NSE's, so the cookies travel as a
header.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import NSE_UA, Document

SITE = "https://www.nseindia.in"
ARCHIVES = "https://nsearchives.nseindia.com"

#: Either archives host, as NSE now writes them: `nsearchives.nseindia.in` or `.com`.
_ARCHIVE_HOST = re.compile(r"^https?://nsearchives\.nseindia\.(?:in|com)(?=/)")


class NseRefused(RuntimeError):
    """NSE answered with a status that refuses us (403, 429, 5xx). Never an absence."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"NSE refused {url}: HTTP {status}")
        self.status = status


class NseTransport(RuntimeError):
    """The request never got a status. Never an absence."""


@dataclass(frozen=True)
class Answer:
    """What NSE served, unchanged, with its status."""

    document: Document
    status: int


def canonical(link: str) -> str:
    """A filing's link on the `.com` archives host, the form every stored accession uses."""
    return _ARCHIVE_HOST.sub(ARCHIVES, link.strip())


def _site() -> str:
    return settings.provider_base("nse", SITE)


def _archives() -> str:
    return settings.provider_base("nse-archives", ARCHIVES)


def _get(url: str, headers: dict[str, str], timeout_s: float, *, provider: str) -> Answer:
    import httpx

    refused: NseRefused | None = None
    with metrics.request(provider) as outcome:
        try:
            response = httpx.get(url, headers=headers, timeout=timeout_s, follow_redirects=False)
        except httpx.HTTPError as exc:
            raise NseTransport(f"{type(exc).__name__}: {exc}") from exc
        if response.status_code == 404:
            outcome["outcome"] = "absent"
        elif response.status_code != 200:
            # A REDIRECT IS A REFUSAL HERE: followed, it would leave the cache (see the docstring).
            outcome["outcome"] = "throttled" if response.status_code == 429 else "refused"
            refused = NseRefused(response.status_code, url)
    # RAISED OUTSIDE THE METRICS BLOCK, which records any exception escaping it as `transport`.
    if refused is not None:
        raise refused
    return Answer(
        document=Document(
            url=str(response.url),
            body=response.content,
            content_type=response.headers.get("content-type", "application/octet-stream"),
            fetched_at=datetime.now(UTC),
        ),
        status=response.status_code,
    )


def _cookies(timeout_s: float) -> str:
    """The landing page's cookies, as one header. Its own status is not checked (it is 403)."""
    import httpx

    try:
        response = httpx.get(
            _site() + "/",
            headers={"User-Agent": NSE_UA, "Accept": "text/html"},
            timeout=timeout_s,
            follow_redirects=False,
        )
    except httpx.HTTPError as exc:
        raise NseTransport(f"handshake: {type(exc).__name__}: {exc}") from exc
    found = (c.split(";")[0].strip() for c in response.headers.get_list("set-cookie"))
    return "; ".join(c for c in found if c)


def filings(symbol: str, timeout_s: float = 30.0) -> Answer:
    """A company's annual results, fresh: the `nse` location is not cached."""
    site = _site()
    cookie = _cookies(min(15.0, timeout_s))
    import httpx

    url = str(
        httpx.URL(
            f"{site}/api/corporates-financial-results",
            params={"index": "equities", "symbol": symbol, "period": "Annual"},
        )
    )
    headers = {
        "User-Agent": NSE_UA,
        "Accept": "application/json",
        "Referer": f"{site}/companies-listing/corporate-filings-financial-results",
        **({"Cookie": cookie} if cookie else {}),
    }
    return _get(url, headers, timeout_s, provider="nse")


def instance(link: str, timeout_s: float = 60.0) -> Answer:
    """A filing's instance, through the `nse-archives` location (cached for a year)."""
    url = _ARCHIVE_HOST.sub(_archives(), canonical(link))
    headers = {"User-Agent": NSE_UA, "Referer": f"{_site()}/"}
    return _get(url, headers, timeout_s, provider="nse-archives")
