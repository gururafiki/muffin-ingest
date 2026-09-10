"""The HTTP client, against a stub transport.

Each test names the failure it prevents. The two that matter most are the timeout being bounded by
the REMAINING budget — a missing or unbounded timeout is a dead worker rather than a slow response,
which is how `security-profiles` returned a bare 502 with nothing naming the call — and a 4xx never
being retried, because on a 25-calls-a-day provider a wasted retry is a measurable share of the day.
"""

from __future__ import annotations

import httpx
import pytest

from muffin_ingest.http.client import Client, ProviderTransportError, _outcome_label
from muffin_ingest.limiter import TokenBucket


def _client(handler: httpx.MockTransport, **kw: object) -> Client:
    return Client(
        "testprov",
        "https://example.test/api",
        TokenBucket(rate_per_sec=1000.0, burst=1000),
        client=httpx.Client(transport=handler),
        **kw,  # type: ignore[arg-type]
    )


def test_a_successful_get_is_returned() -> None:
    c = _client(httpx.MockTransport(lambda r: httpx.Response(200, json={"ok": True})))
    assert c.get("/thing").status_code == 200


def test_a_4xx_is_an_answer_and_is_not_retried() -> None:
    """Retrying spends budget to learn the same thing."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, text="nope")

    c = _client(httpx.MockTransport(handler))
    assert c.get("/thing").status_code == 404
    assert calls["n"] == 1, "a 404 must be returned to the caller, not retried"


def test_a_transport_failure_is_retried_then_raises_its_own_type() -> None:
    """Its own type so a caller cannot mistake "we never got an answer" for "the provider has
    nothing" — the distinction this whole pipeline turns on."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("refused")

    c = _client(httpx.MockTransport(handler), transport_retries=2)
    with pytest.raises(ProviderTransportError):
        c.get("/thing")
    assert calls["n"] == 3, "the initial call plus two retries"


def test_the_timeout_is_bounded_by_what_is_left_of_the_deadline() -> None:
    """A deadline that only gates whether to START a call lets one begin at 34.9s of a 35s budget
    and run 20s past it."""
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["timeout"] = request.extensions.get("timeout", {}).get("read")
        return httpx.Response(200)

    import time as _t

    c = _client(httpx.MockTransport(handler), default_timeout_s=20.0)
    c.get("/thing", deadline=_t.monotonic() + 3.0)
    assert isinstance(seen["timeout"], float)
    assert seen["timeout"] <= 3.0, "the per-call ceiling must not outlive the remaining budget"


def test_a_passed_deadline_refuses_before_spending_a_request() -> None:
    """Against a rate-limited provider, a call that cannot be recorded is worse than no call."""
    import time as _t

    c = _client(httpx.MockTransport(lambda r: httpx.Response(200)))
    with pytest.raises(ProviderTransportError, match="deadline"):
        c.get("/thing", deadline=_t.monotonic() - 1)


@pytest.mark.parametrize(
    ("status", "label"),
    [(200, "ok"), (204, "empty"), (429, "throttled"), (404, "client_error"), (503, "server_error")],
)
def test_outcomes_are_labelled_by_meaning(status: int, label: str) -> None:
    """`429` is its own label rather than folded into `4xx`: throttle pressure is the series anyone
    actually watches, and burying it with 404s makes it unreadable."""
    assert _outcome_label(status) == label
