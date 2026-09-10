"""The token bucket, on a fake clock so the test is about the arithmetic and not about waiting.

MUTATION RESULTS, RECORDED BECAUSE TWO OF THE THREE DETECT BY HANGING RATHER THAN FAILING.

  * removing the `min(burst, …)` clamp — an idle resource banks credit and spends it in one burst,
    which is the exact thing this and the Dagster pool exist to prevent — turns a test RED.
  * removing the `sleep` so `take` refuses instead of waiting, and removing the burst guard, both
    make `take` spin for ever, so the suite HANGS.

A hang is a detection, and it is a poor one: it needs a timeout to read as a failure rather than as
a stuck CI job. Both are deadlocks by construction, so there is nothing to assert that would fire
first — run these under `pytest --timeout` if that ever becomes tempting to ignore.
"""

from __future__ import annotations

import pytest

from muffin_ingest.limiter import TokenBucket


class Clock:
    """A clock that only moves when something sleeps, so elapsed time is exactly what was asked."""

    def __init__(self) -> None:
        self.t = 0.0
        self.slept: list[float] = []

    def now(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.slept.append(s)
        self.t += s


def test_the_burst_is_free_and_then_the_rate_binds() -> None:
    c = Clock()
    b = TokenBucket(rate_per_sec=2.0, burst=2, now=c.now, sleep=c.sleep)
    assert b.take() == 0.0
    assert b.take() == 0.0, "the burst is available immediately"
    assert b.take() == pytest.approx(0.5), "then one token every half second"


def test_it_waits_rather_than_refusing() -> None:
    """Returning "no" pushes the decision back to a caller measured getting it wrong: every refused
    request pushes a provider's limit further out."""
    c = Clock()
    b = TokenBucket(rate_per_sec=1.0, burst=1, now=c.now, sleep=c.sleep)
    b.take()
    b.take()
    assert c.slept, "it slept rather than raising"


def test_tokens_accrue_while_nothing_is_asking() -> None:
    c = Clock()
    b = TokenBucket(rate_per_sec=10.0, burst=5, now=c.now, sleep=c.sleep)
    for _ in range(5):
        b.take()
    c.t += 1.0  # a second passes with no calls
    assert b.take() == 0.0, "a quiet second refills the bucket"


def test_it_never_accrues_beyond_the_burst() -> None:
    """Otherwise an idle resource banks an hour of credit and spends it in one go, which is the
    burst the pool and this exist to prevent."""
    c = Clock()
    b = TokenBucket(rate_per_sec=1.0, burst=2, now=c.now, sleep=c.sleep)
    c.t += 3600.0
    b.take()
    b.take()
    assert b.take() > 0.0, "the third call in an instant must still wait"


def test_a_rate_of_zero_is_refused() -> None:
    """A rate of zero is a stopped resource, and it should say so at construction rather than
    hang on the first call."""
    with pytest.raises(ValueError, match="positive"):
        TokenBucket(rate_per_sec=0)


def test_taking_more_than_the_burst_is_refused_rather_than_deadlocking() -> None:
    b = TokenBucket(rate_per_sec=1.0, burst=2)
    with pytest.raises(ValueError, match="burst"):
        b.take(3)
