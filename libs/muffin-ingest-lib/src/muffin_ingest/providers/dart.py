"""DART (전자공시, Korea's Financial Supervisory Service): its company registry, one company's
periodic filings, and a filing's XBRL archive.

Three requests, three grains, measured 2026-11-16 through http-cache from the node:

  * the REGISTRY (`corpCode.xml`): one ZIP, 3,637,045 bytes, holding `CORPCODE.xml` (30.4 MB) with
    every filer's `corp_code` and, for 4,016 listed companies, its six-digit `stock_code`, the
    digits in front of a `.KS`/`.KQ` symbol. 346 s from the node: DART is slow from outside Korea.
    The edge never fetched it (it timed out twice at 240 s in an earlier spike, through no cache)
    and swept two years of three-month `list.json` windows instead;
  * one company's PERIODIC FILINGS (`list.json?corp_code=`), with no date-range limit once a
    `corp_code` is given;
  * a filing's XBRL ARCHIVE (`fnlttXbrl.xml?rcept_no=&reprt_code=11011`), a ZIP holding the
    instance. Immutable once filed: 802 KB in 73.5 s, measured for Samsung's FY2025 on 2026-09-05.

DART MUST BE ASKED THROUGH THE CACHE HOP. It offers only TLS 1.2 with the static-RSA suite
AES128-GCM-SHA256; OpenResty's OpenSSL completes that handshake, so the `dart` location carries it
(muffin-deployment, `stack/proxy/nginx.conf`).

DART SAYS NO WITH HTTP 200: `{"status":"013"}` (no data) from a `.json` endpoint,
`<result><status>014</status>…` (no file) from an archive endpoint. The status code proves
nothing, so `facets.dart` reads the body.

THE KEY TRAVELS IN THE QUERY STRING (`crtfc_key`), so it is redacted from every URL a document
keeps (`REDACTED`): raw holds what DART sent, never our credential.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document

ORIGIN = "https://opendart.fss.or.kr"

#: The annual report (사업보고서). DART refuses an archive request without a report code.
ANNUAL_REPORT_CODE = "11011"

#: Sent with the registry and the filing lists: the `dart` location keeps a 200 for 90 days, which
#: suits an archive and would replay a stale list. The fresh answer is stored over the old one.
PAST_THE_CACHE = {"X-Muffin-Cache-Bypass": "1"}

#: An archive larger than this is refused unread (the edge's gate; Samsung's is 802 KB).
MAX_ARCHIVE_BYTES = 24 * 1024 * 1024

#: The first year of filings asked for, as the edge asked: DART's XBRL archives start in the
#: mid-2010s, so earlier annual reports carry nothing to read.
FIRST_YEAR = 2015

_KEY = re.compile(r"(crtfc_key=)[^&]*")


class DartRefused(RuntimeError):
    """DART answered with a status that refuses us (403, 429, 5xx). Never an absence."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"DART refused {url}: HTTP {status}")
        self.status = status


class DartTransport(RuntimeError):
    """The request never got a status. Never an absence."""


@dataclass(frozen=True)
class Answer:
    """What DART served, unchanged, with its status. `too_large` is a 200 not read."""

    document: Document
    status: int
    too_large: bool = False


def redacted(url: str) -> str:
    """A URL without our credential."""
    return _KEY.sub(r"\1REDACTED", url)


def _key() -> str:
    key = os.environ.get("DART_API_KEY", "")
    if not key:
        raise DartTransport("DART_API_KEY is not set")
    return key


def _base() -> str:
    return settings.provider_base("dart", ORIGIN)


def _get(
    path: str,
    params: dict[str, str],
    *,
    headers: dict[str, str] | None = None,
    timeout_s: float = 120.0,
    max_bytes: int | None = None,
) -> Answer:
    import httpx

    url = f"{_base()}/{path}"
    sent = {"User-Agent": settings.user_agent(), **(headers or {})}
    refused: DartRefused | None = None
    too_large = False
    with metrics.request("dart") as outcome:
        try:
            with (
                httpx.Client(timeout=timeout_s, follow_redirects=True) as client,
                client.stream(
                    "GET", url, params={"crtfc_key": _key(), **params}, headers=sent
                ) as response,
            ):
                full_url = redacted(str(response.url))
                body, too_large = _read_bounded(response, max_bytes)
        except httpx.HTTPError as exc:
            raise DartTransport(f"{path}: {type(exc).__name__}: {redacted(str(exc))}") from exc
        if response.status_code == 404:
            outcome["outcome"] = "absent"
        elif response.status_code != 200:
            outcome["outcome"] = "throttled" if response.status_code == 429 else "refused"
            refused = DartRefused(response.status_code, full_url)
        elif too_large:
            outcome["outcome"] = "too_large"
    # RAISED OUTSIDE THE METRICS BLOCK, which records any exception escaping it as `transport`.
    if refused is not None:
        raise refused
    return Answer(
        document=Document(
            url=full_url,
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


def corp_codes(timeout_s: float = 600.0) -> Answer:
    """The whole registry, one ZIP (346 s from the node on 2026-11-16)."""
    return _get("api/corpCode.xml", {}, headers=PAST_THE_CACHE, timeout_s=timeout_s)


def filings_params(corp_code: str, page: int, today: str) -> dict[str, str]:
    """One page of a company's periodic disclosures (`pblntf_ty=A`), as the edge asked."""
    return {
        "corp_code": corp_code,
        "bgn_de": f"{FIRST_YEAR}0101",
        "end_de": today,
        "pblntf_ty": "A",
        "page_count": "100",
        "page_no": str(page),
    }


def filings(corp_code: str, page: int = 1, timeout_s: float = 120.0) -> Answer:
    """One page of a company's periodic disclosures, fresh."""
    today = datetime.now(UTC).strftime("%Y%m%d")
    return _get(
        "api/list.json",
        filings_params(corp_code, page, today),
        headers=PAST_THE_CACHE,
        timeout_s=timeout_s,
    )


def instance(rcept_no: str, timeout_s: float = 240.0) -> Answer:
    """A filing's XBRL archive, served from the cache when held (a filing never changes)."""
    return _get(
        "api/fnlttXbrl.xml",
        {"rcept_no": rcept_no, "reprt_code": ANNUAL_REPORT_CODE},
        timeout_s=timeout_s,
        max_bytes=MAX_ARCHIVE_BYTES,
    )
