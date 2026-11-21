"""Nasdaq's earnings calendar, one date per request, asked directly rather than through openbb.

WHY NOT openbb. Nasdaq answers a date with no reports as `data: null` (code 1002, "Earnings
Calendar: No record found."), and openbb's nasdaq adapter dereferences that null and raises
`AttributeError`.
The edge's `earnings-history` stalled on exactly such a window (2016-06-28..30) from 2026-09-06,
reporting `ok` 52 times a day. When the wrapper hides the field that carries the meaning, go around
it (umbrella CLAUDE.md).

ONE REQUEST PER DATE IS NASDAQ'S OWN GRAIN, which is why the lane is partitioned by day. A day's
answer lists the companies reporting that day: forecasts for a day to come, actuals and surprises
for a day that has passed. So one lane serves both the calendar and the history the edge asked for
with two resources. Measured 2026-11-21: p50 2.25 s a request; 2015 answers as readily as 2026.

THE CACHE: a day that has passed long ago no longer changes, so it may be served from http-cache's
30-day entry. A day inside the forward window changes daily (a report is added, rescheduled or
filled in), so it is asked past the cache, which still stores the fresh answer.
"""

from __future__ import annotations

from datetime import date

from muffin_ingest import settings
from muffin_ingest.providers.documents import Document, _get

#: Nasdaq's own host. `settings.provider_base` points it at http-cache when one is configured.
REAL_ORIGIN = "https://api.nasdaq.com"

#: The name these requests are counted under, and the `source_code` the rows are written under.
PROVIDER = "nasdaq"

#: Nasdaq answers a bare client with a refusal; the validation asked with a browser User-Agent.
BROWSER_UA = "Mozilla/5.0 (compatible; muffin-market-data)"


def url_for(day: date) -> str:
    base = settings.provider_base(PROVIDER, REAL_ORIGIN)
    return f"{base}/api/calendar/earnings?date={day.isoformat()}"


def fetch(day: date, *, fresh: bool, timeout_s: float = 30.0) -> Document:
    """One date's calendar, the response body whole. A non-200 raises `DocumentRefused`.

    `fresh` asks past the cache, for a day whose answer can still change.
    """
    headers = {"User-Agent": BROWSER_UA, "Accept": "application/json, text/plain, */*"}
    if fresh:
        headers["X-Muffin-Cache-Bypass"] = "1"
    return _get(url_for(day), provider=PROVIDER, headers=headers, timeout_s=timeout_s)
