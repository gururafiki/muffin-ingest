"""The spacing between the starts of two requests to one provider, shared by the lanes asking it.

Two lanes ask Yahoo one request at a time: the nightly price sweep (`raw_price_chart`) and the
company lane (`raw_yahoo_company`). The `yfinance` pool runs them one at a time, so the allowance is
spent at one pace whichever is running, and the pace is one number,
`prices.partitions.CHART_SECONDS_BETWEEN_REQUESTS`.
"""

from __future__ import annotations

import time
from collections.abc import Callable


class Pacer:
    """Waits until `spacing()` seconds have passed since the previous request started.

    `spacing` is read at every wait rather than once, so a test can set the module constant to 0.
    """

    def __init__(self, spacing: Callable[[], float]) -> None:
        self._spacing = spacing
        self._last = 0.0

    def wait(self) -> None:
        if self._last:
            gap = self._spacing() - (time.monotonic() - self._last)
            if gap > 0:
                time.sleep(gap)
        self._last = time.monotonic()
