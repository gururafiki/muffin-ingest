"""OpenFIGI, called DIRECTLY — free, and keyed when a key is configured.

WHY DIRECT AND NOT openbb. OpenFIGI is a REST endpoint openbb does not wrap usefully; the edge
functions have always called it straight. Two endpoints, TWO RATE BUCKETS — `/v3/filter` (a venue
sweep) answered a 429 on request 21 while `/v3/mapping` (identifier resolution) stayed free, which
is why they get different Dagster pools.

THE RESPONSE BODY IS STORED WHOLE, exactly as served. `parse_filter` runs in stage 2 against a
file; a changed response contract costs a re-parse and never a re-fetch. The sweep is incremental
and resumes from a cursor because the budget is finite either way.

THE KEY MULTIPLIES BOTH BUDGETS AND WE WERE NOT SENDING IT. Measured 2026-09-21, the same hour, on
this account's key:

    /v3/filter    anonymous   refused on request 6 at 2.5 s pacing (~5 a minute)
    /v3/filter    keyed       15 pages in 15.0 s at 0.3 s pacing, no 429
    /v3/mapping   anonymous   HTTP 413 at 11 jobs: "Request may only contain 10 mapping jobs."
    /v3/mapping   keyed       100 jobs answered (5.06 MB); 413 at 101, naming the new limit

So the provider states both ceilings itself rather than us inferring them, which is why they are
constants here and not a guess. A venue pass goes from ~209 minutes to ~5, and the symbology
ladder's 5,697 ticker subjects from ~570 requests to 57.

A 429 IS NOT AN ABSENCE AND NOT A FAILURE: it is the provider refusing the MINUTE, and it must
never make a venue's sweep read as complete. It has its own exception because the caller has to
treat it differently from both a transport failure and an empty venue.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document

#: The real origin. `settings.provider_base` points it at http-cache when one is configured, which
#: is what makes the cache removable without an outage.
REAL_ORIGIN = "https://api.openfigi.com"


#: Jobs one `/v3/mapping` request may carry. Both numbers are the provider's own 413 text rather
#: than documentation: 11 jobs unkeyed answers "Request may only contain 10 mapping jobs", and 101
#: keyed answers the same sentence with 100. Keep them as two constants and CHOOSE — a single
#: number is wrong in one of the two configurations, and the wrong direction is a 413 that fails
#: the whole run.
MAPPING_JOBS_ANONYMOUS = 10
MAPPING_JOBS_KEYED = 100

#: Seconds to wait between `/v3/filter` pages. The anonymous figure is the one measured on
#: 2026-09-20 (12 s, after 2.5 s was refused on request 6); the keyed figure is 0.3 s, measured
#: sustaining 15 pages with no 429 — deliberately slower than the key's documented allowance,
#: because a sweep is a background job and nothing here is waiting for it.
SWEEP_PACING_ANONYMOUS = 12.0
SWEEP_PACING_KEYED = 0.3


def has_api_key() -> bool:
    """Whether this deployment can use the larger budgets. Read at CALL time, like every setting."""
    return bool(settings.openfigi_api_key())


def mapping_jobs_per_request() -> int:
    return MAPPING_JOBS_KEYED if has_api_key() else MAPPING_JOBS_ANONYMOUS


def sweep_pacing_s() -> float:
    return SWEEP_PACING_KEYED if has_api_key() else SWEEP_PACING_ANONYMOUS


class OpenFigiRefused(RuntimeError):
    """The request did not produce an answer — a transport fault or an unrecognised body, never an
    absence."""


class OpenFigiThrottled(RuntimeError):
    """The provider refused the MINUTE (HTTP 429). The caller must stop, resume later, and record
    the venue as UNFINISHED — a throttled sweep reading as complete is the 8,300-security mistake
    again, one venue at a time."""


def _post(path: str, body: Any, timeout_s: float) -> Document:
    base = settings.provider_base("openfigi", REAL_ORIGIN)
    url = f"{base}{path}"
    headers = {"Content-Type": "application/json"}
    # THE HEADER IS THE WHOLE CHANGE. It is added only when a key is configured, so an unkeyed
    # deployment sends exactly what it sent before rather than an empty credential — which
    # OpenFIGI answers with a 401 that would read as an outage.
    #
    # NOTE FOR THE CACHE: a request header is NOT part of http-cache's key (CLAUDE.md records the
    # same fact for SEC's User-Agent), so keyed and unkeyed callers share an entry. That is
    # harmless here — the key changes the BUDGET, never the answer — but it means a cached page
    # cannot be attributed to one or the other.
    api_key = settings.openfigi_api_key()
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    with metrics.request("openfigi") as outcome:
        try:
            response = httpx.post(
                url,
                json=body,
                headers=headers,
                timeout=timeout_s,
            )
        except httpx.HTTPError as exc:
            raise OpenFigiRefused(f"{type(exc).__name__}: {exc}") from exc
        if response.status_code == 429:
            outcome["outcome"] = "throttled"
            raise OpenFigiThrottled(
                f"openfigi 429 on {path} — the minute's budget is spent; stop and resume later"
            )
        if response.status_code != 200:
            outcome["outcome"] = "refused"
            raise OpenFigiRefused(f"openfigi {response.status_code} on {path}")
    # THE REQUEST BODY IS THE PROVENANCE — the response says nothing about what it was asked, and
    # the two parameters (`exchCode`, `securityType2`, `start`) decide what a page even contains.
    return Document(
        url=url,
        body=response.content,
        content_type=response.headers.get("content-type", "application/json"),
        fetched_at=datetime.now(UTC),
    )


def filter_exchange(
    exch_code: str,
    *,
    cursor: str | None = None,
    security_type2: str = "Common Stock",
    timeout_s: float = 20.0,
) -> Document:
    """One page of a venue's listings — `/v3/filter`, 100 rows plus a `next` cursor.

    `securityType2='Common Stock'` IS THE FILTER, not decoration: unfiltered, "Samsung Electronics"
    returns 8,725 hits that are nearly all derivatives. ADRs are a SEPARATE coarse type
    (`Depositary Receipt`), which is why a future sweep of the US must ask for both — a
    Common-Stock-only sweep loses every ADR, and TSM/NVO/BABA are exactly that.
    """
    body: dict[str, object] = {"exchCode": exch_code, "securityType2": security_type2}
    if cursor:
        body["start"] = cursor
    return _post("/v3/filter", body, timeout_s)


def mapping(jobs: list[dict[str, Any]], *, timeout_s: float = 20.0) -> Document:
    """One `/v3/mapping` request — THE RESPONSE IS POSITIONAL: entry j answers job j.

    Each job is `{"idType": "ID_ISIN", "idValue": "<isin>"}` and may carry `exchCode: "US"` to
    restrict to the US listing (the SEC-usable ticker) or not (every venue, for the local line).
    The body is stored whole; `facets/symbology.parse_mapping` reads it back positionally, and a
    reordering there — or here — would attach one company's listing to another's.
    """
    return _post("/v3/mapping", list(jobs), timeout_s)
