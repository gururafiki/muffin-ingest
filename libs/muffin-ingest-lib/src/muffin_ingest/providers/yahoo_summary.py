"""Yahoo's quoteSummary: one request per company, every module the company lane reads, stored whole.

WHY ONE REQUEST AND NOT openbb's FIVE ROUTES. The edge asked openbb for a company's profile,
metrics, share statistics, price targets and officers separately, and each of those routes calls
yfinance's `get_info()`, which is itself three Yahoo URLs. One quoteSummary request naming every
module answers all of them: measured 2026-10-10, 20-68 KB for most equities and 294 KB for AAPL,
30-80 ms.

TWO FUNCTIONS, AS IN `yahoo_chart`. `fetch` is the only network call and returns the body byte for
byte; `parse` and `value` are stage-2 code living here so Yahoo's payload shape is described once.

WHAT AN ANSWER IS, measured from the node:

  * 200 with `quoteSummary.result[0]`: the modules Yahoo holds. A module it does not hold is simply
    absent; it never fails the request (non-US listings lack `upgradeDowngradeHistory` and
    `secFilings`, and `esgScores` was absent for every symbol, so it is not asked for).
  * **404 `Quote not found for symbol: BDMS-F.BK`**: an absence that NAMES THE SYMBOL. Kept as an
    answer, like the chart's `No data found`, so stage 2 can count it.
  * a 404 that names no symbol (a wrong route): a transport fault, never an absence.
  * **401 `Invalid Crumb`** or `Invalid Cookie`: the session expired. `CrumbRejected`, so the caller
    opens a new session once rather than reading the rest of the run as refusals.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import httpx

from muffin_ingest import metrics, settings
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document
from muffin_ingest.providers.yahoo_session import Session

#: EVERY MODULE ASKED FOR, in one request. The ones stage 2 reads today, plus the ones a later
#: phase may adopt (earnings history and trend, analyst actions, holders, insiders, SEC filings):
#: they cost bytes and no requests, and raw keeps them for whoever reads them later. Not
#: `esgScores` (absent for every symbol measured), not `summaryProfile` (`assetProfile` without
#: the officers), and not the `*StatementHistory*` modules, which Yahoo has emptied in favour of
#: fundamentals-timeseries.
#:
#: THE ORDER IS PART OF THE URL, so it is fixed here and never derived.
MODULES: tuple[str, ...] = (
    "assetProfile",
    "price",
    "quoteType",
    "summaryDetail",
    "defaultKeyStatistics",
    "financialData",
    "calendarEvents",
    "majorHoldersBreakdown",
    "earnings",
    "earningsHistory",
    "earningsTrend",
    "recommendationTrend",
    "upgradeDowngradeHistory",
    "insiderHolders",
    "insiderTransactions",
    "netSharePurchaseActivity",
    "institutionOwnership",
    "fundOwnership",
    "majorDirectHolders",
    "secFilings",
)


class CrumbRejected(yahoo_chart.YahooRefused):
    """Yahoo refused the session (401). Open a new one; this says nothing about the symbol."""


def _error_of(body: bytes) -> dict[str, Any] | None:
    """The `quoteSummary.error` object, if the body is JSON and carries one."""
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    for top in ("quoteSummary", "finance"):
        block = parsed.get(top)
        if isinstance(block, dict) and isinstance(block.get("error"), dict):
            error: dict[str, Any] = block["error"]
            return error
    return None


def names_an_absence(body: bytes) -> bool:
    """Does Yahoo say, in its own words, that it does not carry THIS symbol?

    Only the summary's own error wording counts. A route 404 also arrives as JSON with a `Not Found`
    code, but its description is `HTTP 404 Not Found` and names no symbol, so reading the code alone
    would record a wrong URL as a missing company.
    """
    error = _error_of(body)
    description = str((error or {}).get("description") or "").lower()
    return "not found for symbol" in description


def _refusal(status: int, body: bytes) -> yahoo_chart.YahooRefused | None:
    """What a non-answer is, by its status. None for an answer, including a named absence."""
    if status == 401:
        return CrumbRejected("HTTP 401: the session was refused")
    if status != 200 and not names_an_absence(body):
        return yahoo_chart.refusal(status, "quoteSummary")
    return None


def fetch(symbol: str, session: Session, *, timeout_s: float = 20.0) -> Document:
    """The response body, exactly as Yahoo sent it, with the crumb redacted from the stored URL.

    A refusal raises; an answer, including one stating that the symbol does not exist, is returned.
    The status decides the throttle verdict, not the message text: a symbol like `4290.T` contains
    `429`, and a refusal message carrying it would read as a throttle and stop a whole run.
    """
    base = settings.provider_base("yahoo", yahoo_chart.REAL_ORIGIN)
    url = f"{base}/v10/finance/quoteSummary/{yahoo_chart.path_segment(symbol)}"
    # `formatted=false`, AS yfinance ASKS: numbers arrive bare (`0.0133`) rather than as
    # `{"raw": 0.0133, "fmt": "1.33%"}`, so the body is smaller. Not complete, though: officers'
    # pay stays wrapped either way, so `value` reads both shapes.
    params = {"modules": ",".join(MODULES), "formatted": "false", "crumb": session.crumb}
    refusal: yahoo_chart.YahooRefused | None = None
    with metrics.request("yahoo-summary") as outcome:
        try:
            response = httpx.get(
                url,
                params=params,
                headers={"User-Agent": yahoo_chart.BROWSER_UA, "Cookie": session.cookie},
                timeout=timeout_s,
            )
        except httpx.HTTPError as exc:
            raise yahoo_chart.YahooRefused(session.scrub(f"{type(exc).__name__}: {exc}")) from exc
        refusal = _refusal(response.status_code, response.content)
        if refusal is not None:
            outcome["outcome"] = "refused"
    # RAISED OUTSIDE THE METRICS BLOCK: an exception escaping it is recorded as `transport`, which
    # is right for a request that never answered and wrong for one that answered with a refusal.
    if refusal is not None:
        raise refusal
    return Document(
        url=str(httpx.URL(url, params={**params, "crumb": "redacted"})),
        body=response.content,
        content_type=response.headers.get("content-type", "application/json"),
        fetched_at=datetime.now(UTC),
    )


# --- stage 2 -------------------------------------------------------------------------------------


class Summary:
    """One parsed answer: the modules Yahoo returned, or its statement that it has none."""

    def __init__(self, modules: dict[str, Any], error: dict[str, Any] | None) -> None:
        self.modules = modules
        self.error = error

    def module(self, name: str) -> dict[str, Any]:
        found = self.modules.get(name)
        return found if isinstance(found, dict) else {}


def parse(body: bytes) -> Summary:
    """Bytes to modules. Raises `YahooRefused` only for a body that is not the summary's JSON."""
    try:
        parsed: Any = json.loads(body)
    except ValueError as exc:
        raise yahoo_chart.YahooRefused(f"unparseable quoteSummary body: {exc}") from exc
    block = parsed.get("quoteSummary") if isinstance(parsed, dict) else None
    if not isinstance(block, dict):
        raise yahoo_chart.YahooRefused("quoteSummary body has no quoteSummary object")
    error = block.get("error")
    if error:
        return Summary({}, error if isinstance(error, dict) else {"description": str(error)})
    results = block.get("result") or []
    first = results[0] if results and isinstance(results[0], dict) else {}
    return Summary(dict(first), None)


def value(module: dict[str, Any], key: str) -> Any:
    """A field's value: Yahoo wraps numbers as `{"raw": 0.0133, "fmt": "1.33%"}` and leaves strings
    bare. An empty wrapper `{}` is Yahoo saying it has no value, and reads as None."""
    found = module.get(key)
    if isinstance(found, dict):
        return found.get("raw")
    return found
