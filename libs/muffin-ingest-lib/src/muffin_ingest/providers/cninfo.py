"""CNINFO (巨潮资讯), China's CSRC-designated disclosure platform: its stock lists, the
annual-report announcements of one company, and the report PDFs.

Three requests, three grains, measured 2026-11-15 through http-cache from the node:

  * the STOCK LISTS (`szse_stock.json` 593 KB, 6,265 A-shares from both Shanghai and Shenzhen;
    `bj_stock.json` 58 KB, 607 Beijing stocks): code -> `orgId`, the company id the announcement
    search requires. All 2,173 org ids the edge resolved one company at a time agree with them, so
    two requests a week replace 2,173 lookups;
  * the ANNOUNCEMENT SEARCH (`hisAnnouncement/query`, a form POST), keyed `stock=<code>,<orgId>`:
    a bare code returns a 200 with no announcements, which reads like a company that never filed.
    ~0.6 s each, 24 of 24 answered at 1 s and 0.25 s pacing;
  * the PDF, on a separate STATIC host, immutable once published.

A STATUS IS NOT AN ABSENCE. The edge turned every non-200 into "no report" and stamped the filing
read for good, so a 503 during a CNINFO incident would have hidden that year's report. Here a 404
is an absence, 403/429/5xx a refusal that stops the run, and a request with no status a transport
failure.
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document

WWW_ORIGIN = "https://www.cninfo.com.cn"
STATIC_ORIGIN = "https://static.cninfo.com.cn"

#: The stock lists. `szse_stock.json` and `bj_stock.json` name the A-shares the announcement
#: search is asked about. `hke_stock.json` (2,798 Hong Kong stocks, 5-digit codes) is read for one
#: thing: an H-share of a company that also lists A-shares carries the SAME org id (215 do,
#: measured 2026-11-26), which maps the H-share line onto the A-share filer. Its own reports are
#: never asked.
STOCK_LISTS = ("szse_stock.json", "bj_stock.json", "hke_stock.json")

#: CNINFO's annual-report category. It files the full report, its summary and an English edition
#: under it (`facets.cninfo.classify`).
ANNUAL_CATEGORY = "category_ndbg_szsh"

#: One page, as the edge asked. 19 of 24 companies sampled hold more than 30 announcements in the
#: category (Yangtze 61), so the years before ~2016 are not asked for; widening is a decision of its
#: own (docs/deferred/2026-11-15-cninfo-reads-only-the-newest-search-page.md).
PAGE_SIZE = 30

#: Sent with the announcement search: a re-walk asks the same body, and the `cninfo` location keeps
#: a 200 for a day. The fresh answer is stored over the old one.
PAST_THE_CACHE = {"X-Muffin-Cache-Bypass": "1"}

#: Sent with a report: served from the cache when it is there (an annual report never changes), but
#: never stored. The backlog is ~31,000 reports, ~124 GB, through a cache capped at 40 GB that would
#: evict every other provider's entries to hold them; raw keeps the PDF, and a parser change
#: re-fetches it, paced (Phase 6 decision 4).
NOT_STORED = {"X-Muffin-Cache-No-Store": "1"}

Provider = Literal["cninfo", "cninfo-static"]


class CninfoRefused(RuntimeError):
    """CNINFO answered with a status that refuses us (403, 429, 5xx). Never an absence."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"CNINFO refused {url}: HTTP {status}")
        self.status = status


class CninfoTransport(RuntimeError):
    """The request never got a status. Never an absence."""


@dataclass(frozen=True)
class Answer:
    """What CNINFO served, unchanged, with its status: 200 or 404.

    `too_large` is a 200 whose body was not read because it is over the `max_bytes` asked for.
    """

    document: Document
    status: int
    too_large: bool = False


def _base(provider: Provider) -> str:
    return settings.provider_base(provider, WWW_ORIGIN if provider == "cninfo" else STATIC_ORIGIN)


def _request(
    method: str,
    url: str,
    *,
    provider: Provider,
    data: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout_s: float = 60.0,
    max_bytes: int | None = None,
) -> Answer:
    import httpx

    sent = {"User-Agent": settings.user_agent(), **(headers or {})}
    content = None
    if data is not None:
        # CNINFO answers 411 to a POST with no body: the parameters are form-encoded.
        sent["Content-Type"] = "application/x-www-form-urlencoded"
        content = urllib.parse.urlencode(data).encode()
    refused: CninfoRefused | None = None
    too_large = False
    with metrics.request(provider) as outcome:
        try:
            with (
                httpx.Client(timeout=timeout_s, follow_redirects=True) as client,
                client.stream(method, url, headers=sent, content=content) as response,
            ):
                body, too_large = _read_bounded(response, max_bytes)
        except httpx.HTTPError as exc:
            raise CninfoTransport(f"{url}: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 404:
            outcome["outcome"] = "absent"
        elif response.status_code != 200:
            outcome["outcome"] = "throttled" if response.status_code == 429 else "refused"
            refused = CninfoRefused(response.status_code, url)
        elif too_large:
            outcome["outcome"] = "too_large"
    # RAISED OUTSIDE THE METRICS BLOCK, which records any exception escaping it as `transport`.
    if refused is not None:
        raise refused
    return Answer(
        document=Document(
            url=url,
            body=body,
            content_type=response.headers.get("content-type", "application/octet-stream"),
            fetched_at=datetime.now(UTC),
        ),
        status=response.status_code,
        too_large=too_large,
    )


def _read_bounded(response: Any, max_bytes: int | None) -> tuple[bytes, bool]:
    """A streamed body, or (b"", True) once it is known to be over `max_bytes`."""
    if max_bytes is None or response.status_code != 200:
        return bytes(response.read()), False
    declared = response.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > max_bytes:
        return b"", True
    chunks: list[bytes] = []
    read = 0
    for chunk in response.iter_bytes():
        read += len(chunk)
        if read > max_bytes:
            return b"", True
        chunks.append(chunk)
    return b"".join(chunks), False


def stock_list_url(name: str) -> str:
    return f"{_base('cninfo')}/new/data/{name}"


def stock_list(name: str, timeout_s: float = 60.0) -> Answer:
    """One of CNINFO's stock lists, whole."""
    return _request("GET", stock_list_url(name), provider="cninfo", timeout_s=timeout_s)


def announcements_url() -> str:
    return f"{_base('cninfo')}/new/hisAnnouncement/query"


def announcements_form(code: str, org_id: str, page: int = 1) -> dict[str, Any]:
    """The search's form, exactly as the edge sent it."""
    return {
        "stock": f"{code},{org_id}",
        "tabName": "fulltext",
        "pageSize": PAGE_SIZE,
        "pageNum": page,
        "category": ANNUAL_CATEGORY,
    }


def announcements(code: str, org_id: str, page: int = 1, timeout_s: float = 30.0) -> Answer:
    """One page of a company's annual-report announcements."""
    return _request(
        "POST",
        announcements_url(),
        provider="cninfo",
        data=announcements_form(code, org_id, page),
        headers=PAST_THE_CACHE,
        timeout_s=timeout_s,
    )


def report_fetch_url(public_url: str) -> str:
    """The stored link is the PUBLIC host (a reader's browser opens it); the fetch goes through the
    cache, which serves a report it holds and stores none (`NOT_STORED`)."""
    if not public_url.startswith(STATIC_ORIGIN + "/"):
        raise ValueError(f"not a CNINFO report link: {public_url!r}")
    return f"{_base('cninfo-static')}/{public_url[len(STATIC_ORIGIN) + 1 :]}"


def report(public_url: str, *, max_bytes: int, timeout_s: float = 60.0) -> Answer:
    """One report PDF, refused unread above `max_bytes`. The document keeps the public URL."""
    answer = _request(
        "GET",
        report_fetch_url(public_url),
        provider="cninfo-static",
        headers=NOT_STORED,
        timeout_s=timeout_s,
        max_bytes=max_bytes,
    )
    document = answer.document
    return Answer(
        document=Document(
            url=public_url,
            body=document.body,
            content_type=document.content_type,
            fetched_at=document.fetched_at,
        ),
        status=answer.status,
        too_large=answer.too_large,
    )


def is_pdf(body: bytes) -> bool:
    """CNINFO answers some paths with an HTML error page and HTTP 200."""
    return body.startswith(b"%PDF-")
