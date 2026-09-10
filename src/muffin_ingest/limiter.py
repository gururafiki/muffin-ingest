"""Requests per unit time, which is the constraint that actually binds here.

THE BINDING CONSTRAINT IS REQUESTS PER UNIT TIME, so throughput improves only by cutting requests
PER SECURITY — never by raising a page size. Measured: `security-fundamentals` at 60 calls a run
tripped yfinance's limit while `security-industries` at 30 only partly did, and raising a page
raises calls per run and trips it sooner, buying nothing. The one optimisation that IS a win is the
opposite shape — batching `security-statements` to ten symbols a call made the same 90 requests
cover five times the securities.

TWO MECHANISMS, AND NEITHER REPLACES THE OTHER. A Dagster pool bounds CONCURRENCY: one run per
provider at a time, which is what the five-minute rotation was approximating. This bounds RATE, per
second and per day. A pool cannot express "25 calls a day" and a rate limiter cannot stop two runs
starting at once, and yfinance has punished both mistakes.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class TokenBucket:
    """A per-provider rate, refilled continuously.

    In-process, and that is sound because a Dagster pool of one means at most one run per provider
    is live. The DAILY quota deliberately lives in `ingest.provider_budget` instead: it must survive
    a process, since alpha_vantage's 25-a-day is spent across runs and a bucket that resets when the
    worker restarts would spend it several times over.
    """

    rate_per_sec: float
    burst: int = 1
    _tokens: float = field(default=0.0, init=False)
    _updated: float = field(default=0.0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    now: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep

    def __post_init__(self) -> None:
        if self.rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be positive; a rate of zero is a stopped resource")
        self._tokens = float(self.burst)
        self._updated = self.now()

    def take(self, n: int = 1) -> float:
        """Block until `n` requests may be made. Returns how long that took, for the metric.

        Waiting is the point. Returning "no" would push the decision back to a caller that has
        already been measured getting it wrong — every refused request pushes a provider's limit
        further out, so the resources stop themselves on the first throttle rather than retrying.
        """
        if n > self.burst:
            raise ValueError(f"cannot take {n} from a bucket whose burst is {self.burst}")
        waited = 0.0
        while True:
            with self._lock:
                now = self.now()
                self._tokens = min(
                    float(self.burst), self._tokens + (now - self._updated) * self.rate_per_sec
                )
                self._updated = now
                if self._tokens >= n:
                    self._tokens -= n
                    return waited
                shortfall = (n - self._tokens) / self.rate_per_sec
            self.sleep(shortfall)
            waited += shortfall
