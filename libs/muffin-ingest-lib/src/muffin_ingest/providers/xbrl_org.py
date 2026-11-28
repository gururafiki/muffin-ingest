"""filings.xbrl.org: XBRL International's index of ESEF annual financial reports, each converted to
xBRL-JSON (Phase 6c step 7).

Two requests, two grains, measured 2026-11-28 from this machine and the node:

  * one filer's FILINGS (`/api/filings?filter=[entity.identifier eq <LEI>]`): every report the
    index holds for that LEI, ANNUAL AND INTERIM ALIKE, in one JSON:API page (L'Oréal 5, Inditex
    10; the `links` carry no `next`, so a filer's list arrives whole). Each row names the period
    end, the country, the date published and the report's `json_url`;
  * one REPORT (`json_url`): the whole xBRL-JSON, every fact with its dimensions. 2-27 MB
    (L'Oréal 6.7 MB, Enel 27 MB). A report is immutable once indexed; a correction is a new
    filing with a higher `fxo_id` suffix.

NO KEY. THE RATE LIMIT IS REAL AND UNSTATED: ~15 requests in quick succession earn a 429, and 8 s
between requests held across the survey. A 429 stops the run and is never an absence.

DE AND CH ARE NOT INDEXED, so their companies list nothing: an empty list there is the index's
coverage, not the company's.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document

ORIGIN = "https://filings.xbrl.org"
#: The `market.data_source` row and the http-cache location.
PROVIDER = "xbrl-org"

#: A report larger than this is refused unread. The largest seen is Enel's 27 MB.
MAX_REPORT_BYTES = 64 * 1024 * 1024

#: Sent with a filer's list, so the newest list is read whatever a cache holds.
PAST_THE_CACHE = {"X-Muffin-Cache-Bypass": "1"}


class XbrlOrgRefused(RuntimeError):
    """The index answered with a status that refuses us (403, 429, 5xx). Never an absence."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"filings.xbrl.org refused {url}: HTTP {status}")
        self.status = status


class XbrlOrgTransport(RuntimeError):
    """The request never got a status. Never an absence."""


@dataclass(frozen=True)
class Answer:
    """What the index served, unchanged, with its status. `too_large` is a 200 not read."""

    document: Document
    status: int
    too_large: bool = False


def _base() -> str:
    return settings.provider_base(PROVIDER, ORIGIN)


def filings_params(lei: str) -> dict[str, str]:
    """The query naming one filer's reports, newest period first."""
    return {
        "filter": json.dumps([{"name": "entity.identifier", "op": "eq", "val": lei}]),
        "sort": "-period_end",
        "page[size]": "100",
    }


def _get(
    path: str,
    params: dict[str, str] | None = None,
    *,
    headers: dict[str, str] | None = None,
    timeout_s: float = 120.0,
    max_bytes: int | None = None,
) -> Answer:
    import httpx

    url = f"{_base()}/{path.lstrip('/')}"
    sent = {"User-Agent": settings.user_agent(), "Accept": "application/json", **(headers or {})}
    refused: XbrlOrgRefused | None = None
    too_large = False
    with metrics.request(PROVIDER) as outcome:
        try:
            with (
                httpx.Client(timeout=timeout_s, follow_redirects=True) as client,
                client.stream("GET", url, params=params, headers=sent) as response,
            ):
                full_url = str(response.url)
                body, too_large = _read_bounded(response, max_bytes)
        except httpx.HTTPError as exc:
            raise XbrlOrgTransport(f"{path}: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 404:
            outcome["outcome"] = "absent"
        elif response.status_code != 200:
            outcome["outcome"] = "throttled" if response.status_code == 429 else "refused"
            refused = XbrlOrgRefused(response.status_code, full_url)
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


def filings(lei: str, timeout_s: float = 120.0) -> Answer:
    """Every report the index holds for one LEI, fresh."""
    return _get("api/filings", filings_params(lei), headers=PAST_THE_CACHE, timeout_s=timeout_s)


def report(json_url: str, timeout_s: float = 300.0) -> Answer:
    """One report's xBRL-JSON, by the `json_url` its filing row names."""
    return _get(json_url, timeout_s=timeout_s, max_bytes=MAX_REPORT_BYTES)
