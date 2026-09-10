"""One HTTP client, so a provider call is counted, paced and bounded in exactly one place.

WHAT THIS EXISTS TO PREVENT, all of it measured in the system it replaces:

* A MISSING TIMEOUT IS A DEAD WORKER, NOT A SLOW RESPONSE. `openbbFetcher` had none, so
  `security-profiles` ran past the worker limit the moment its symbols became foreign listings and
  returned a bare 502 — a resource that catches provider errors per batch never got the chance.
  Every request here carries one, and a per-call timeout is bounded by the REMAINING budget rather
  than a fixed value: a deadline that only gates whether to START a call lets one begin at 34.9s of
  a 35s budget and run 20s past it.
* NOTHING COUNTED PROVIDER CALLS AT THE CALLER. `http-cache` sees the direct providers but not the
  calls openbb makes inside its own process, and that is the rate limit that matters most; the only
  signal was an error string in a report.
* A RETRY ON A 4xx SPENDS BUDGET TO LEARN THE SAME ANSWER. Retries are for transport only.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

import httpx
from prometheus_client import Counter, Histogram

from muffin_ingest import settings
from muffin_ingest.limiter import TokenBucket

REQUESTS = Counter(
    "muffin_ingest_requests_total",
    "Provider requests, counted at the caller.",
    ["provider", "outcome"],
)
DURATION = Histogram(
    "muffin_ingest_request_seconds",
    "Provider request duration.",
    ["provider"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 45, 90),
)
BUCKET_WAIT = Histogram(
    "muffin_ingest_limiter_wait_seconds",
    "Time spent waiting for a provider's rate limiter — the cost of the budget, made visible.",
    ["provider"],
    buckets=(0, 0.05, 0.25, 1, 5, 15, 60),
)


class ProviderTransportError(RuntimeError):
    """We never got an answer. Says nothing about the subject, which is the whole point of having
    its own type: a caller must not be able to mistake it for an absence."""


class Client:
    """A provider-scoped HTTP client: one limiter, one base URL, one set of counters."""

    def __init__(
        self,
        provider: str,
        base_url: str,
        limiter: TokenBucket,
        *,
        default_timeout_s: float = 20.0,
        transport_retries: int = 2,
        client: httpx.Client | None = None,
    ) -> None:
        self.provider = provider
        self.base_url = base_url.rstrip("/")
        self.limiter = limiter
        self.default_timeout_s = default_timeout_s
        self.transport_retries = transport_retries
        self._client = client or httpx.Client(
            headers={"User-Agent": settings.user_agent()}, follow_redirects=True
        )

    def get(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        deadline: float | None = None,
        timeout_s: float | None = None,
    ) -> httpx.Response:
        """One GET, paced and bounded.

        `deadline` is a monotonic instant. The timeout is the SMALLER of the per-call ceiling and
        what is left of it, so a call cannot outlive the budget that was meant to contain it.
        """
        budget = timeout_s or self.default_timeout_s
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProviderTransportError(f"{self.provider}: deadline passed before the request")
            budget = min(budget, remaining)

        BUCKET_WAIT.labels(self.provider).observe(self.limiter.take())

        url = f"{self.base_url}/{path.lstrip('/')}"
        last: Exception | None = None
        for attempt in range(self.transport_retries + 1):
            started = time.monotonic()
            try:
                response = self._client.get(url, params=params, timeout=budget)
            except httpx.HTTPError as e:
                # TRANSPORT ONLY. A 4xx is an answer and is returned to the caller to classify —
                # retrying it spends budget to learn the same thing, and on a 25-a-day provider
                # that is a measurable share of the day.
                last = e
                DURATION.labels(self.provider).observe(time.monotonic() - started)
                REQUESTS.labels(self.provider, "transport").inc()
                if attempt < self.transport_retries:
                    self.limiter.take()
                    continue
                raise ProviderTransportError(f"{self.provider}: {e}") from e

            DURATION.labels(self.provider).observe(time.monotonic() - started)
            REQUESTS.labels(self.provider, _outcome_label(response.status_code)).inc()
            return response

        raise ProviderTransportError(f"{self.provider}: {last}")  # pragma: no cover - unreachable


def _outcome_label(status: int) -> str:
    """Labels a response by what it MEANS, not by its exact code.

    `429` is its own label rather than folded into `4xx`, because throttle pressure is the series
    anyone actually watches and burying it in a bucket with 404s makes it unreadable.
    """
    if status == 429:
        return "throttled"
    if status == 204:
        return "empty"
    if 200 <= status < 300:
        return "ok"
    if 400 <= status < 500:
        return "client_error"
    return "server_error"
