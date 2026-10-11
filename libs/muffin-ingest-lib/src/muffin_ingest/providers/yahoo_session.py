"""Yahoo's cookie-and-crumb session: what quoteSummary demands, and nothing else here does.

MEASURED 2026-10-10 FROM THE NODE, which is the whole design:

  * quoteSummary without a crumb answers **401 `Invalid Crumb`**, through http-cache and direct.
  * `GET https://fc.yahoo.com` answers 404 AND STILL SETS the `A3` cookie; `getcrumb` with that
    cookie returns an 11-character crumb. Two requests per session.
  * The chart and fundamentals-timeseries endpoints need neither, so only the summary uses this.

THE COOKIE IS SENT AS A HEADER, NEVER THROUGH A COOKIE JAR. A jar scopes `A3` to `.yahoo.com`, and
the cache is `http-cache:8080`, so a client with a jar sends no cookie to the cache and getcrumb
answers 401 `Invalid Cookie` through it while working direct. So the cookie comes from
`fc.yahoo.com` directly (it has no cache location and must not: a cached cookie is someone else's
session), and the crumb and the data go through the cache with an explicit `Cookie` header.

NEITHER VALUE IS EVER LOGGED, STORED OR PUT IN AN ERROR. Both are credentials: `Session` hides them
from `repr`, every error names a status rather than a URL, and the summary provider redacts the
crumb from the URL it stores.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from muffin_ingest import metrics, settings
from muffin_ingest.providers import yahoo_chart

#: Where the `A3` cookie comes from. Not a cache location, deliberately: see the module docstring.
COOKIE_ORIGIN = "https://fc.yahoo.com"


class SessionRefused(yahoo_chart.YahooRefused):
    """No session could be opened. A transport fault, never a statement about any symbol."""


@dataclass(frozen=True)
class Session:
    """A cookie and the crumb minted for it. Neither appears in `repr`."""

    cookie: str = field(repr=False)
    crumb: str = field(repr=False)
    opened_at: datetime

    def scrub(self, text: str) -> str:
        """`text` with the crumb and the cookie replaced, for anything that might be printed: the
        whole `A3=…` header value and the bare value too."""
        out = text
        for secret in (self.crumb, self.cookie, self.cookie.partition("=")[2]):
            if secret:
                out = out.replace(secret, "redacted")
        return out


def open_session(*, timeout_s: float = 20.0) -> Session:
    """The cookie from `fc.yahoo.com`, then a crumb for it. Raises `SessionRefused` on any failure.

    `fc.yahoo.com` answering 404 is the NORMAL case, so the status is not checked: the cookie is.
    """
    # EACH REFUSAL IS RAISED OUTSIDE ITS METRICS BLOCK: an exception escaping the block is recorded
    # as `transport`, which is wrong for a request that answered with a refusal.
    with metrics.request("yahoo-session") as outcome:
        try:
            first = httpx.get(
                COOKIE_ORIGIN,
                headers={"User-Agent": yahoo_chart.BROWSER_UA},
                timeout=timeout_s,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise SessionRefused(f"cookie: {type(exc).__name__}") from exc
        a3 = first.cookies.get("A3")
        if not a3:
            outcome["outcome"] = "refused"
    if not a3:
        raise SessionRefused(f"cookie: HTTP {first.status_code} set no A3 cookie")

    cookie = f"A3={a3}"
    base = settings.provider_base("yahoo", yahoo_chart.REAL_ORIGIN)
    with metrics.request("yahoo-session") as outcome:
        try:
            second = httpx.get(
                f"{base}/v1/test/getcrumb",
                headers={"User-Agent": yahoo_chart.BROWSER_UA, "Cookie": cookie},
                timeout=timeout_s,
            )
        except httpx.HTTPError as exc:
            raise SessionRefused(f"crumb: {type(exc).__name__}") from exc
        crumb = second.text.strip()
        # A CRUMB IS A SHORT TOKEN. A 200 carrying an HTML page or an error object is not one, and
        # sending it on every summary request would turn one bad answer into a run of 401s.
        usable = (
            second.status_code == 200
            and 0 < len(crumb) <= 64
            and not any(c in crumb for c in "<{ ")
        )
        if not usable:
            outcome["outcome"] = "refused"
    if not usable:
        raise SessionRefused(f"crumb: HTTP {second.status_code}")
    return Session(cookie=cookie, crumb=crumb, opened_at=datetime.now(UTC))
