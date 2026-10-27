"""SEC EDGAR through edgartools: its identity, pacing and client, with its own cache switched off.

DECIDED 2026-10-24 (Phase 5 decisions 2 and 9). edgartools is the transport for every SEC request
of the SEC family, and its parsers read submissions (`extract_company_filings_table`) and Form 4
(`Form4.parse_xml`). Company facts are read by our own reader, because its concept map carries no
`ifrs-full` concept and its facts parser turns every exception into `None`, an empty answer.

THIS IS THE ONLY MODULE THAT IMPORTS `edgar`, AND IT DOES SO LAZILY, for two reasons:

  * IMPORTING IT WRITES AND DECIDES. `edgar.httpclient` builds `HTTP_MGR` at import with a file
    cache that keeps `/Archives/edgar/data` for ever, creates `<EDGAR_LOCAL_DATA_DIR>/_tcache`, and
    computes its cache rules from `EDGAR_BASE_URL` read at that moment. So every variable it reads
    is set first, and the manager is then replaced by an uncached one. A cache here would be a
    second, invisible store beside raw, serving last week's company facts to a refresh a filing
    triggered.
  * The Dagster definitions load without edgartools installed (the CI `definitions` job installs
    neither extra), so nothing reachable from `definitions.py` may import it at module level.

Every request names its provider (`sec` for www.sec.gov, `sec-data` for data.sec.gov) and goes
through http-cache's matching location PAST the cache: `X-Muffin-Cache-Bypass` skips a stored
entry and `X-Muffin-Cache-No-Store` keeps this answer out of it. Raw is the store, and http-cache
keeps a `sec-data` 200 for 7 days, which is exactly the staleness a filing-triggered refresh exists
to avoid.

THREE OUTCOMES, NEVER TWO. A 200 is an answer and a 404 is SEC saying it holds nothing at that URL;
both return, with the status beside the document, and the caller decides what a 404 means. A 403,
429 or 5xx is SEC refusing us, raised as `SecRefused`. A request that never got a status is
`SecTransport`. A refusal and a transport failure are never an absence.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from types import ModuleType
from typing import Any, Literal, cast

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document

#: SEC's two hosts, which are http-cache's `sec` and `sec-data` locations.
WWW_ORIGIN = "https://www.sec.gov"
DATA_ORIGIN = "https://data.sec.gov"

#: A filer whose company facts and submissions answer every day: Apple. An absence is filed only
#: when this answered in the same run, past the cache, so a run SEC was refusing or a cache serving
#: a stale error cannot record a filer as having nothing.
CONTROL_CIK = 320193

#: Sent on every request (see the module docstring).
PAST_THE_CACHE = {"X-Muffin-Cache-Bypass": "1", "X-Muffin-Cache-No-Store": "1"}

Provider = Literal["sec", "sec-data"]


class SecRefused(RuntimeError):
    """SEC answered with a status that refuses us (403, 429, 5xx). Never an absence."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"SEC refused {url}: HTTP {status}")
        self.status = status


class SecTransport(RuntimeError):
    """The request never got a status. Never an absence."""


@dataclass(frozen=True)
class SecAnswer:
    """What SEC served, unchanged, and the status it served it with: 200 or 404."""

    document: Document
    status: int


_lock = threading.Lock()
_edgar: ModuleType | None = None


def edgar() -> ModuleType:
    """The `edgar` module, configured for this process before its first import.

    Call this instead of `import edgar` anywhere in the codebase.
    """
    global _edgar
    if _edgar is not None:
        return _edgar
    with _lock:
        if _edgar is None:
            _edgar = _configure()
    return _edgar


def _configure() -> ModuleType:
    agent = settings.user_agent()
    rate = settings.sec_requests_per_second()
    os.environ["EDGAR_BASE_URL"] = settings.provider_base("sec", WWW_ORIGIN)
    os.environ["EDGAR_DATA_URL"] = settings.provider_base("sec-data", DATA_ORIGIN)
    os.environ["EDGAR_LOCAL_DATA_DIR"] = settings.edgar_scratch_dir()
    os.environ["EDGAR_RATE_LIMIT_PER_SEC"] = str(rate)
    # THE USER-AGENT. edgartools reads it through `get_identity()` whenever it builds a client, and
    # that prompts on stdin when it is unset, which hangs a run instead of failing it. Set here and
    # never through `set_identity`, which logs the identity at INFO; the agent is never printed.
    os.environ["EDGAR_IDENTITY"] = agent

    import edgar as module
    from edgar import httpclient

    # THE MANAGER BUILT AT IMPORT IS REPLACED, NOT RECONFIGURED: its client is built once, and after
    # that `http_client()` ignores every argument, so a flag passed later would be silently dropped.
    httpclient.HTTP_MGR = httpclient.get_http_mgr(cache_enabled=False, request_per_sec_limit=rate)
    if httpclient.HTTP_MGR.cache_mode != "Disabled":  # pragma: no cover - an edgartools change
        raise RuntimeError(f"edgartools' cache is {httpclient.HTTP_MGR.cache_mode!r}, not Disabled")
    # edgartools ships no type information, so mypy sees the module as `Any`.
    return cast(ModuleType, module)


def fetch(url: str, *, provider: Provider, timeout_s: float = 60.0) -> SecAnswer:
    """One SEC request, past http-cache, paced by edgartools' limiter. 200 and 404 return."""
    import httpx

    httpclient = edgar().httpclient
    refused: SecRefused | None = None
    with metrics.request(provider) as outcome:
        try:
            with httpclient.http_client() as client:
                response = client.get(url, headers=PAST_THE_CACHE, timeout=timeout_s)
        except httpx.HTTPError as exc:
            raise SecTransport(f"{url}: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 404:
            outcome["outcome"] = "absent"
        elif response.status_code != 200:
            outcome["outcome"] = "throttled" if response.status_code == 429 else "refused"
            refused = SecRefused(response.status_code, url)
    # RAISED OUTSIDE THE METRICS BLOCK, which records any exception escaping it as `transport`.
    if refused is not None:
        raise refused
    return SecAnswer(
        document=Document(
            url=url,
            body=response.content,
            content_type=response.headers.get("content-type", "application/octet-stream"),
            fetched_at=datetime.now(UTC),
        ),
        status=response.status_code,
    )


def _www() -> str:
    return settings.provider_base("sec", WWW_ORIGIN)


def _data() -> str:
    return settings.provider_base("sec-data", DATA_ORIGIN)


def companyfacts_url(cik: int) -> str:
    return f"{_data()}/api/xbrl/companyfacts/CIK{cik:010d}.json"


def submissions_url(cik: int) -> str:
    return f"{_data()}/submissions/CIK{cik:010d}.json"


def submissions_page_url(name: str) -> str:
    """An older page of a filer's submissions, named by the main document's `filings.files`."""
    return f"{_data()}/submissions/{name}"


def _quarter(day: date) -> int:
    return (day.month - 1) // 3 + 1


def daily_index_url(day: date) -> str:
    return (
        f"{_www()}/Archives/edgar/daily-index/{day.year}/QTR{_quarter(day)}/form.{day:%Y%m%d}.idx"
    )


def quarter_listing_url(day: date) -> str:
    """The directory listing of a quarter's daily indexes, as JSON: which days SEC published."""
    return f"{_www()}/Archives/edgar/daily-index/{day.year}/QTR{_quarter(day)}/index.json"


def filing_text_url(cik: int, accession: str) -> str:
    """A filing's complete submission text file, which carries a Form 4's XML inline."""
    return f"{_www()}/Archives/edgar/data/{cik}/{accession.replace('-', '')}/{accession}.txt"


def insider_dataset_url(year: int, quarter: int) -> str:
    """SEC's quarterly Insider Transactions Data Set (Forms 3, 4 and 5)."""
    return (
        f"{_www()}/files/structureddata/data/insider-transactions-data-sets/"
        f"{year}q{quarter}_form345.zip"
    )


def filings_table(table: Mapping[str, Any]) -> list[dict[str, Any]]:
    """One filer's filings, read by edgartools' `extract_company_filings_table` (decision 9).

    `table` is the parallel arrays SEC publishes: the main submissions document's
    `filings.recent`, or an older page whole. One dict per filing comes back in SEC's order, with
    `filing_date` a `date` and every other field as SEC sent it. It reads 13 of the 16 arrays
    (not `core_type`, `isXBRLNumeric` or `filmNumber`); raw keeps all 16. NO NETWORK: this only
    configures edgar, so stage 2 may call it.
    """
    edgar()
    from edgar.entity.data import extract_company_filings_table

    rows: list[dict[str, Any]] = extract_company_filings_table(dict(table)).to_pylist()
    return rows


def describe() -> dict[str, Any]:
    """What the configured client will do, for a run's metadata and a test's assertions."""
    httpclient = edgar().httpclient
    return {
        "cache_mode": httpclient.HTTP_MGR.cache_mode,
        "rate_per_second": settings.sec_requests_per_second(),
        "base_url": os.environ["EDGAR_BASE_URL"],
        "data_url": os.environ["EDGAR_DATA_URL"],
    }
