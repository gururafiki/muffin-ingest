"""N-PORT's three requests: the fund directory, the full-text search, the primary document.

WHY THESE ARE SEPARATE FROM THE PARSE. `facets/nport.py` runs in stage 2 against bytes already
on disk, so a header change at EDGAR (which the parse throws on, deliberately) costs a re-parse
and never a re-fetch. These three fetches are the only network calls in the whole Discovery lane:
one per series per day for the search, one per filing for the document.

ALL THREE GO THROUGH `sec_edgar`, the one SEC client (since 2026-11-11; before, plain httpx). That
gives them what every other SEC request has:
  * the identity SEC requires, set once where the client is configured;
  * the shared limiter, so the search's burst of one request per tracked fund is paced;
  * three outcomes: a refusal (403, 429, 5xx) is `SecRefused`, a request with no status is
    `SecTransport`, and a 404 is `SecNotFound`. None of them is ever read as "this series does not
    file";
  * a fresh answer, past http-cache. The directory and the search answer "what is listed now",
    and a filing's document is fetched once and kept in raw.

This module names no host: the URLs are `sec_edgar`'s.
"""

from __future__ import annotations

from muffin_ingest.providers import sec_edgar
from muffin_ingest.providers.documents import Document


def fund_directory(timeout_s: float = 60.0) -> Document:
    """SEC's `company_tickers_mf.json`. ~1.2 MB, ~28,500 fund share classes, keyless.

    THE DISCOVERY DIRECTORY: it is the only keyless map from the tracked-fund `symbol` an operator
    adds to the `(cik, seriesId)` the search and the Archives path need. `company_tickers.json`
    (the registries lane) lists operating companies, not series: ETFs are series within a trust,
    and one CIK covers many funds.
    """
    return sec_edgar.document(sec_edgar.fund_tickers_url(), provider="sec", timeout_s=timeout_s)


def search_index(
    series_id: str,
    *,
    start: str,
    end: str,
    timeout_s: float = 30.0,
) -> Document:
    """EDGAR FULL-TEXT SEARCH for one series' NPORT-P filings: one request per series.

    THE SERIES ID APPEARS VERBATIM IN THE DOCUMENT BODY, so searching for it is exact rather than
    heuristic. The probe-walk alternative (open each trust's submissions and stream a KB of each
    candidate's document to match the series) cannot reach a given fund in a big trust at all.
    Measured: a 24-probe walk found XLK, EWJ, EWZ but could not find MCHI or EWU, while the search
    finds all five in one hop. The response is the fixture `sec_nport_fts_ewj.json`.

    THE LIMITER SETS THE PACE, NOT SEC. The search answers in ~15 ms, so the sensor's pass over 74
    funds took 14.8 s at five requests a second (measured 2026-11-11; 27 s on a slower pass). A
    sensor evaluation is cut at 60 s, so one pass holds up to about 250 funds; past that the
    sensor needs a cursor.
    """
    return sec_edgar.document(
        sec_edgar.nport_search_url(series_id, start=start, end=end),
        provider="sec-fts",
        timeout_s=timeout_s,
    )


def primary_doc(cik: str, accession: str, timeout_s: float = 90.0) -> Document:
    """One filing's `primary_doc.xml`, byte for byte.

    THE FILER CIK IS THE DIRECTORY'S, NOT THE ONE THE ACCESSION EMBEDS. Measured 2026-09-12, the
    same accession served under the accession-embedded CIK is a 404 and under the directory's CIK
    is the full document. This is why the Discovery sensor queues `f"{cik}:{accession}"` partition
    keys rather than bare accessions, and why a 404 here is raised (`SecNotFound`) and never filed
    as a fund with no filing.
    """
    return sec_edgar.document(
        sec_edgar.nport_primary_doc_url(cik, accession), provider="sec", timeout_s=timeout_s
    )
