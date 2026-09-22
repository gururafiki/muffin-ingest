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

import json
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

#: Seconds to wait between `/v3/filter` pages. The anonymous figure was measured on 2026-09-20
#: (12 s, after 2.5 s was refused on request 6).
#:
#: THE KEYED FIGURE WAS 0.3 s AND THAT WAS MEASURED JUST UNDER A CLIFF. The probe that set it
#: stopped, satisfied, at 15 pages; production then said what the ceiling actually was —
#: `openfigi throttled US after 20 pages`, and `after 0 pages` on the pass relaunched immediately,
#: so passes alternated 20/0. Re-measured 2026-09-22 by walking a 44-page venue until the provider
#: REFUSED rather than until the probe was satisfied:
#:
#:     paced 1.0 s   REFUSED at page 21 in  33.1 s   (~38 req/min)
#:     paced 2.0 s   REFUSED at page 21 in  50.9 s   (~25 req/min)
#:     paced 3.0 s   44 pages, NO refusal in 151.7 s (~17 req/min)
#:
#: Refusing at page 21 under both 1 s and 2 s is a ~20-request bucket whose refill sits between 17
#: and 25 a minute. 3 s is the measured-safe side of that, and still four times the anonymous rate.
#: **Probe until the provider refuses, not until you are satisfied.**
SWEEP_PACING_ANONYMOUS = 12.0
SWEEP_PACING_KEYED = 3.0


#: An error body naming a FIELD OF OUR REQUEST is ours and is permanent — retrying cannot help,
#: and a sweep that resumed for ever on it would hide a malformed request. Measured: OpenFIGI
#: answers `{"error": "Invalid key 'includeEntitlements'."}` with HTTP 200 for a bad parameter.
#:
#: ANYTHING ELSE IS TREATED AS THEIRS, which is the safe default in this direction. A transient
#: read as ours fails the run; ours read as a transient leaves the venue unfinished, and
#: `venue_sweep_reached_its_last_page` NAMES an unfinished venue with the provider's exact words in
#: the log. One needs a human at 3am, the other is on a dashboard.
_OUR_FAULT = ("invalid ",)


def _is_our_fault(message: str) -> bool:
    return message.strip().lower().startswith(_OUR_FAULT)


def _error_in(body: bytes) -> str | None:
    """The error OpenFIGI reported inside a 200, or None for an ordinary answer.

    THIS IS A TRANSPORT QUESTION, NOT A PARSE. It asks only "is this an answer at all?", the same
    thing the status code is asked two lines above; the body is still stored whole and every
    narrowing still belongs to stage 2.
    """
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    if isinstance(parsed, dict) and "error" in parsed and "data" not in parsed:
        return str(parsed["error"])
    return None


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


class OpenFigiUnavailable(RuntimeError):
    """The provider answered, and its answer was that it could not answer.

    OpenFIGI reports a transient server fault as **HTTP 200 with an error body**, so nothing about
    the status line distinguishes it from a page of listings. The caller must treat it like a 429 —
    stop, stay resumable, leave the subject unfinished — because it is the same fact: no answer
    this time, try later. Raising instead would fail the run and need a human.
    """


class OpenFigiThrottled(RuntimeError):
    """The provider refused the MINUTE (HTTP 429). The caller must stop, resume later, and record
    the venue as UNFINISHED — a throttled sweep reading as complete is the 8,300-security mistake
    again, one venue at a time."""


def _post(path: str, body: Any, timeout_s: float, *, bypass_cache: bool = False) -> Document:
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
    if bypass_cache:
        # http-cache maps this to `proxy_cache_bypass`, which SKIPS the stored entry and STORES
        # the fresh answer over it. Both halves matter: OpenFIGI's transient error arrives as a
        # cacheable 200, and `proxy_cache_valid 200 90d` then replays it for three months —
        # measured 2026-09-21, two venues unsweepable until December off one bad second.
        headers["X-Muffin-Cache-Bypass"] = "1"
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
        error = _error_in(response.content)
        if error is not None:
            outcome["outcome"] = "refused"
    if error is not None:
        # OURS IS PERMANENT, so it is raised without spending a second request: the cache key
        # contains the request body, so a corrected request cannot collide with this entry anyway.
        if _is_our_fault(error):
            raise OpenFigiRefused(f"openfigi {path} refused the request: {error}")
        # THEIRS MIGHT BE A CACHED ONE SECOND OF TROUBLE. Ask exactly once more, past the cache —
        # which also overwrites the stored error, so the next caller is not still reading it.
        if not bypass_cache:
            return _post(path, body, timeout_s, bypass_cache=True)
        raise OpenFigiUnavailable(f"openfigi {path} could not answer: {error}")
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
