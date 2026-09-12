"""OpenFIGI, called DIRECTLY — the free, keyless symbology API.

WHY DIRECT AND NOT openbb. OpenFIGI is a REST endpoint openbb does not wrap usefully; the edge
functions have always called it straight. Two endpoints, TWO RATE BUCKETS — `/v3/filter` (a venue
sweep) answered a 429 on request 21 while `/v3/mapping` (identifier resolution) stayed free, which
is why they get different Dagster pools.

THE RESPONSE BODY IS STORED WHOLE, exactly as served. `parse_filter` runs in stage 2 against a
file; a changed response contract costs a re-parse and never a re-fetch. OpenFIGI's 25-request/
minute anonymous budget is exactly why the sweep is incremental and resumes from a cursor rather
than re-asking.

A 429 IS NOT AN ABSENCE AND NOT A FAILURE: it is the provider refusing the MINUTE, and it must
never make a venue's sweep read as complete. It has its own exception because the caller has to
treat it differently from both a transport failure and an empty venue.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document

#: The real origin. `settings.provider_base` points it at http-cache when one is configured, which
#: is what makes the cache removable without an outage.
REAL_ORIGIN = "https://api.openfigi.com"


class OpenFigiRefused(RuntimeError):
    """The request did not produce an answer — a transport fault or an unrecognised body, never an
    absence."""


class OpenFigiThrottled(RuntimeError):
    """The provider refused the MINUTE (HTTP 429). The caller must stop, resume later, and record
    the venue as UNFINISHED — a throttled sweep reading as complete is the 8,300-security mistake
    again, one venue at a time."""


def _post(path: str, body: dict[str, object], timeout_s: float) -> Document:
    base = settings.provider_base("openfigi", REAL_ORIGIN)
    url = f"{base}{path}"
    with metrics.request("openfigi") as outcome:
        try:
            response = httpx.post(
                url,
                json=body,
                headers={"Content-Type": "application/json"},
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
