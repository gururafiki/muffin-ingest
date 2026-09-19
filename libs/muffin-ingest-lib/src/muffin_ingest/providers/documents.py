"""Whole-file providers: one request, one document, no paging and no backlog.

WHY THESE ARE THEIR OWN SHAPE. `sec-cik-map` and `in-symbols` are backlog-driven resources in the
edge function purely because of the 90-second worker — the CIK map reached 6,645 of ~27,000 rows
and *restarted from zero every run*, and the NSE list was pressed into the same mould beside it.
Neither is incremental by nature: SEC publishes one 776 KB file listing ~10,400 filers, NSE one
CSV of 2,570 equities, and the file IS the answer. There is nothing to page and nothing to resume.

So the request grain is "the whole file", which the partition table in the design doc maps to **no
partition at all** — an unpartitioned asset on a schedule, with a freshness policy standing in for
everything the backlog used to carry.

A `Document` is returned rather than parsed rows, because the parse belongs to stage 2: a header
change at NSE (which throws, deliberately) must cost a re-parse and never a re-fetch.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from muffin_ingest import metrics, settings


class DocumentRefused(RuntimeError):
    """The provider did not answer. NEVER the same fact as "the document was empty"."""


@dataclass(frozen=True)
class Document:
    """What a provider served, unchanged, plus who asked and when.

    `fetched_at` is here rather than derived later because THE DATE TRAVELS WITH THE DATA: neither
    of these files carries one of its own, so the only honest timestamp is when it was read, and
    it has to be recorded in the raw artifact at the moment of reading.
    """

    url: str
    body: bytes
    content_type: str
    fetched_at: datetime

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()

    def as_row(self, run_id: str) -> dict[str, Any]:
        """The raw row. A document is a row with a `body` column — see `ParquetIOManager`.

        Measured: pyarrow infers `binary` for `bytes` and round-trips it identically, and a 40 KB
        XML compresses to 3.8 KB on the way. So documents need no I/O manager of their own, and
        the provenance travels as columns rather than a sidecar that can go missing or go stale.
        """
        return {
            "url": self.url,
            "body": self.body,
            "content_type": self.content_type,
            "sha256": self.sha256,
            "fetched_at": self.fetched_at.isoformat(),
            "run_id": run_id,
        }


#: NSE refuses a bare client. Both headers are required and the Referer is not decoration — the
#: archives host rejects a request that did not come from the site.
NSE_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)


def _get(url: str, *, provider: str, headers: dict[str, str], timeout_s: float) -> Document:
    import httpx

    # THE PROVIDER IS NAMED, NEVER READ OFF THE URL. This took the label from the URL's host, which
    # is the provider's host only when nothing sits in front of it. In production every base URL is
    # `http://http-cache:8080/<location>`, so the first SEC request driven after the roll counted as
    # `provider="http-cache:8080"` — and NSE would have joined it, one series for every document
    # provider. It is the nginx lesson reproduced one layer up: `$provider` is an explicit variable
    # per location, never `$proxy_host`. `yahoo_chart` and the openbb routes already named theirs.
    with metrics.request(provider) as outcome:
        try:
            response = httpx.get(url, headers=headers, timeout=timeout_s, follow_redirects=True)
        except httpx.HTTPError as exc:  # transport: never an absence
            raise DocumentRefused(f"{url}: {type(exc).__name__}: {exc}") from exc
        if response.status_code != 200:
            # A REFUSAL IS NOT AN ABSENCE. SEC answers 403 to a User-Agent it dislikes and the
            # body explains nothing; recording that as `empty` would make a config error look
            # like a provider that has stopped publishing.
            outcome["outcome"] = "refused"
            raise DocumentRefused(f"{url}: HTTP {response.status_code}")
    return Document(
        url=url,
        body=response.content,
        content_type=response.headers.get("content-type", "application/octet-stream"),
        fetched_at=datetime.now(UTC),
    )


def sec_company_tickers(timeout_s: float = 30.0) -> Document:
    """SEC's ticker→CIK file. ~776 KB, ~10,400 filers, keyless.

    SEC MANDATES A DESCRIPTIVE User-Agent and refuses a generic one. `settings.user_agent()` is
    that header and had no caller until now — the one HTTP provider in the repo sent its own
    browser string instead, so the SEC-mandated path was dead code.
    """
    base = settings.provider_base("sec", "https://www.sec.gov")
    return _get(
        f"{base}/files/company_tickers.json",
        provider="sec",
        headers={"User-Agent": settings.user_agent(), "Accept": "application/json"},
        timeout_s=timeout_s,
    )


def nse_equity_list(timeout_s: float = 30.0) -> Document:
    """NSE's published equity list. 2,570 rows, keyless, the join key for every Indian filer."""
    archives = settings.provider_base("nse-archives", "https://nsearchives.nseindia.com")
    site = settings.provider_base("nse", "https://www.nseindia.com")
    return _get(
        f"{archives}/content/equities/EQUITY_L.csv",
        provider="nse",
        headers={"User-Agent": NSE_UA, "Referer": f"{site}/"},
        timeout_s=timeout_s,
    )
