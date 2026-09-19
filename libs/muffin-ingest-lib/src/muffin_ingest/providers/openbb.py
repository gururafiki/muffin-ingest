"""The hub, IMPORTED rather than called over HTTP — and that is the point of the module.

`openbb-api` is a REST service in front of exactly this library, and the hop is where this
pipeline's most expensive defect class is manufactured. Alpha Vantage answers an exhausted quota
with `200` plus an `Information` field; through the REST hop that arrives as an empty `204`, which
is BYTE-IDENTICAL to "this symbol has no data" — the confusion that recorded ~8,300 securities as
permanently unanswerable in one afternoon. A throttled yfinance is the same shape.

In-process, two things survive that the hop destroys:

  * `OBBject.warnings`, where a provider says it is degraded while still returning a 200. Nothing
    downstream of the REST hop can see these at all.
  * the provider's own exception text, instead of a status code chosen by a FastAPI wrapper.

So `classify()` has something real to read, and `THROTTLED` stays distinguishable from `EMPTY` at
the source rather than by string-matching a flattened body.

CALLS ARE DIRECT AND TYPED, AND THERE IS NO ROUTE TABLE ANY MORE. There used to be a `ROUTES` dict
walked with `getattr`, justified by the REST convention being irregular —
`/equity/price/performance` sits beside `/etf/price_performance`. That irregularity is in the URL,
and we stopped using URLs: measured, all 26 entries mapped a string to ITSELF, so the table encoded
nothing and cost a layer of indirection plus a runtime failure mode.

Written out, the call reads as what it is. IT IS NOT MORE TYPE-CHECKABLE, THOUGH, AND THE FIRST
VERSION OF THIS COMMENT CLAIMED IT WAS. openbb ships `py.typed`, which is why the claim was
plausible — but measured against the real hub:

    >>> inspect.signature(obb.equity.price.historical)
    (symbol, start_date, end_date, provider, **kwargs)

THE ROUTE TAKES `**kwargs`, so a misspelled keyword binds happily, is swallowed, and the call
silently uses the default — returning a plausible wrong answer rather than an error. mypy cannot see
that, `inspect.signature(...).bind(...)` cannot see it, and neither could the route table this
replaced. What catches it is behavioural: ask for one day, get the provider's default range, and the
window filter's `outside_window` count moves.

The import stays function-local because it is ~250 MB and ~2 s — `dagster definitions validate`, the
unit tests and the CI `checks` job all run without openbb present.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from muffin_ingest import metrics
from muffin_ingest.providers.outcome import Outcome
from muffin_ingest.providers.vocab import no_data_for_subject, throttled


class ProviderRefused(Exception):
    """The provider answered, and the answer was that it will not serve us right now."""


@dataclass
class Answer:
    """One call's result, with the two things the REST hop throws away kept alongside the rows."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    #: `OBBject.warnings` — a provider degrading itself while still returning 200. This is the
    #: field that makes an in-process hub worth its 250 MB.
    warnings: list[str] = field(default_factory=list)
    provider: str | None = None


def _rows(results: Any) -> list[dict[str, Any]]:
    """`OBBject.results` to plain dicts, WITHOUT going through pandas.

    `to_df()` exists and would pull a DataFrame into memory for what is a list of pydantic models;
    on a 5,000-bar history that is the difference between a page and a worker. A single result is
    returned bare rather than in a list by some routes, which is the same shape problem as a
    provider adding a `symbol` column only when several symbols are requested.
    """
    if results is None:
        return []
    items = results if isinstance(results, list) else [results]
    out: list[dict[str, Any]] = []
    for item in items:
        dump = getattr(item, "model_dump", None)
        out.append(dump() if callable(dump) else dict(item))
    return out


def answer_from(result: Any) -> Answer:
    """The one thing worth sharing between call sites: read the warnings before the rows.

    Raises `ProviderRefused` when a warning says the provider is throttling us. That is the whole
    reason for an in-process hub: over HTTP the same call returns 200 with no rows, which is
    indistinguishable from a symbol the provider genuinely does not carry, and a caller that cannot
    tell those apart eventually marks the wrong thing.

    An EMPTY answer is NOT an error here — openbb answers `204 No Content` when a provider
    legitimately has nothing, and strictness belongs at the caller that requires rows, never in the
    fetcher: treating 204 as a failure once failed 22 of 24 batches over a few listings yfinance
    does not carry.
    """
    warnings = [
        w.message for w in (getattr(result, "warnings", None) or []) if getattr(w, "message", None)
    ]
    for message in warnings:
        if throttled(message):
            raise ProviderRefused(message)

    return Answer(
        rows=_rows(getattr(result, "results", None)),
        warnings=warnings,
        provider=getattr(result, "provider", None),
    )


def classify(text: str) -> Outcome:
    """What a message from the hub or a provider beneath it MEANS.

    Order matters and is not arbitrary: THROTTLE IS CHECKED FIRST. A provider refusing us often
    also says something that reads like an absence, and calling that an absence is what negative-
    caches real companies for a month. `vocab.py` asserts the two sets share no terms, so the order
    is belt and braces rather than the thing holding it together.
    """
    if throttled(text):
        return Outcome.THROTTLED
    if no_data_for_subject(text):
        return Outcome.DEAD_SUBJECT
    return Outcome.TRANSPORT


# ── the calls themselves ───────────────────────────────────────────────────────────────────────
#
# One named function per route this pipeline uses. Each is a thin, TYPED call: the keyword names and
# their types are openbb's, checked where mypy can see the hub, rather than a string looked up in a
# table and a `**params` that nothing validates until it runs.


def price_history(
    symbols: Sequence[str],
    *,
    start: date,
    end: date,
    provider: str = "yfinance",
    interval: str = "1d",
) -> Answer:
    """Daily bars for one or more symbols.

    COMMA-JOINED, AND THAT IS NOT A BATCHED REQUEST. `openbb_yfinance` calls
    `yf.download(tickers=symbol, ..., threads=False)`, so the vendor sees ONE REQUEST PER SYMBOL,
    serially. Joining collapses our call count, not theirs — which is why pacing is denominated in
    symbols and why "batching saves the provider budget" is false.
    """
    from openbb import obb

    # COUNTED HERE BECAUSE IT CANNOT BE COUNTED ANYWHERE ELSE. `http-cache` sits in front of
    # openbb-api and yfinance fetches through `curl_cffi`, ignoring our base URLs entirely — so
    # this request is invisible to nginx's `$provider` metrics by construction. Importing the hub
    # in-process moved that blind spot inside our own worker; this closes it.
    #
    # Labelled with the PROVIDER, not the route: the thing being rate-limited is yfinance.
    with metrics.request(provider):
        return answer_from(
            obb.equity.price.historical(
                symbol=",".join(symbols),
                provider=provider,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                interval=interval,
            )
        )


def sector_performance(provider: str = "finviz") -> Answer:
    """Sector performance, published as NUMBERS rather than bars.

    THE ONLY ACQUISITION HERE THAT DOES NOT RETURN A SERIES, which is why it exists as its own route
    instead of being folded into `price_history`. There is no ETF behind a muffin sector, so nothing
    can be computed — the provider's figure is the figure.

    IT IS US-LISTED ONLY, worth stating rather than relabelling as global: finviz screens US
    listings, so "Technology, +2.1% this month" is a statement about the US technology sector.

    NOTE THE RETURNS ARE FRACTIONS (-0.0366 = -3.66%) and the day change hides under the literal key
    `Change %` while `performance_1d` is present and ALWAYS NULL — measured 0 of 11 populated
    against 11 of 11. `facets/indices.py` owns both conversions; this function only fetches.
    """
    from openbb import obb

    with metrics.request(provider):
        return answer_from(
            obb.equity.compare.groups(group="sector", metric="performance", provider=provider)
        )
