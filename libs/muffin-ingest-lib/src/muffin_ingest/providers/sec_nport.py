"""SEC EDGAR access for N-PORT: the fund directory, the full-text search, the primary document.

WHY THESE ARE SEPARATE FROM THE PARSE. `facets/nport.py` runs in stage 2 against bytes already
on disk, so a header change at EDGAR (which the parse throws on, deliberately) costs a re-parse
and never a re-fetch. These three fetches are the only network calls in the whole Discovery lane:
one per series per day for the search, one per filing for the document.

SEC ACCESS RULES, which are not optional:
  * a descriptive User-Agent with contact details is required — `settings.user_agent()` is that
    header and refuses to run without a valid value, having been the one value in production that
    could only ever have 403'd;
  * fair access is ~10 req/s; this lane is far below it and paces its own calls;
  * a throttle is REAL and surfaces as a non-ok response — `_get` raises `DocumentRefused` for
    any non-200, and a refusal must never be read as "this series does not file".
"""

from __future__ import annotations

from urllib.parse import quote

from muffin_ingest import settings
from muffin_ingest.providers.documents import Document, _get


def _headers() -> dict[str, str]:
    return {"User-Agent": settings.user_agent(), "Accept": "application/json"}


def fund_directory(timeout_s: float = 60.0) -> Document:
    """SEC's `company_tickers_mf.json`. ~1.2 MB, ~28,500 fund share classes, keyless.

    THE DISCOVERY DIRECTORY: it is the only keyless map from the tracked-fund `symbol` an operator
    adds to the `(cik, seriesId)` the FTS and the Archives path need. `company_tickers.json` (the
    registries lane) lists operating companies, not series — ETFs are series within a trust, and one
    CIK covers many funds.
    """
    base = settings.provider_base("sec", "https://www.sec.gov")
    return _get(
        f"{base}/files/company_tickers_mf.json",
        provider="sec",
        headers=_headers(),
        timeout_s=timeout_s,
    )


def search_index(
    series_id: str,
    *,
    start: str,
    end: str,
    timeout_s: float = 30.0,
) -> Document:
    """EDGAR FULL-TEXT SEARCH for one series' NPORT-P filings — one request per series.

    THE SERIES ID APPEARS VERBATIM IN THE DOCUMENT BODY, so searching for it is exact rather than
    heuristic. The probe-walk alternative (open each trust's submissions and stream a KB of each
    candidate's document to match the series) cannot reach a given fund in a big trust at all —
    measured: a 24-probe walk found XLK, EWJ, EWZ but could not find MCHI or EWU, while FTS finds
    all five in one hop. The response is the fixture `sec_nport_fts_ewj.json`.
    """
    base = settings.provider_base("sec-fts", "https://efts.sec.gov")
    url = (
        f"{base}/LATEST/search-index"
        f"?q=%22{quote(series_id)}%22&forms=NPORT-P&startdt={start}&enddt={end}"
    )
    return _get(url, provider="sec", headers=_headers(), timeout_s=timeout_s)


def primary_doc(cik: str, accession: str, timeout_s: float = 90.0) -> Document:
    """One filing's `primary_doc.xml`, byte for byte.

    THE FILER CIK IS THE DIRECTORY'S, NOT THE ONE THE ACCESSION EMBEDS — measured 2026-09-12, the
    same accession served under the accession-embedded CIK is a 404 and under the directory's CIK
    is the full document. This is why the Discovery sensor queues `f"{cik}:{accession}"` partition
    keys rather than bare accessions.
    """
    base = settings.provider_base("sec-data", "https://www.sec.gov")
    url = f"{base}/Archives/edgar/data/{int(cik)}/{accession}/primary_doc.xml"
    return _get(url, provider="sec", headers=_headers(), timeout_s=timeout_s)
