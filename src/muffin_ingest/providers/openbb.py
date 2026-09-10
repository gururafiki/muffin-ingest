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

Costs, accepted deliberately: the import is ~250 MB and ~2 s, so it happens on FIRST CALL and not
at module import — `dagster definitions validate`, the unit tests and the CI `checks` job must all
run without openbb installed. And `openbb-core` is AGPL-3.0, which is why this repo is.

THE ROUTE TABLE IS DATA BECAUSE THE CONVENTION IS NOT REGULAR. `obb.x.y.z` maps to `/api/v1/x/y/z`
*mostly*: `/equity/price/performance` sits beside `/etf/price_performance`. Every route here was
taken from the deployed `/openapi.json` or from a call this pipeline already makes; none is
inferred from the pattern.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from muffin_ingest.providers.outcome import Outcome
from muffin_ingest.providers.vocab import no_data_for_subject, throttled

#: Logical name -> the dotted path on the hub. Kept as data so a facet names a ROUTE and never a
#: path, and so the irregular ones (`etf.price_performance`) cannot be "corrected" into the pattern.
ROUTES: dict[str, str] = {
    "equity.price.historical": "equity.price.historical",
    "equity.price.performance": "equity.price.performance",
    "equity.profile": "equity.profile",
    "equity.fundamental.metrics": "equity.fundamental.metrics",
    "equity.fundamental.income": "equity.fundamental.income",
    "equity.fundamental.balance": "equity.fundamental.balance",
    "equity.fundamental.cash": "equity.fundamental.cash",
    "equity.fundamental.dividends": "equity.fundamental.dividends",
    "equity.fundamental.management": "equity.fundamental.management",
    "equity.calendar.earnings": "equity.calendar.earnings",
    "equity.estimates.consensus": "equity.estimates.consensus",
    "equity.estimates.price_target": "equity.estimates.price_target",
    "equity.compare.groups": "equity.compare.groups",
    # NOT `etf.price.performance`. finviz's per-symbol variant is broken upstream (it mangles the
    # symbol: AAPL -> 'AAAPL' is not in list) and fmp's is premium, which is why country returns
    # are computed from `etf.historical` instead.
    "etf.price_performance": "etf.price_performance",
    "etf.historical": "etf.historical",
    "economy.cpi": "economy.cpi",
    "economy.gdp.real": "economy.gdp.real",
    "economy.gdp.nominal": "economy.gdp.nominal",
    "economy.unemployment": "economy.unemployment",
    "economy.fred_series": "economy.fred_series",
    "fixedincome.government.yield_curve": "fixedincome.government.yield_curve",
    "fixedincome.rate.effr": "fixedincome.rate.effr",
    "fixedincome.rate.sofr": "fixedincome.rate.sofr",
    "index.price.historical": "index.price.historical",
    "crypto.price.historical": "crypto.price.historical",
    "derivatives.futures.historical": "derivatives.futures.historical",
}


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

    @property
    def empty(self) -> bool:
        return not self.rows


def _load_hub() -> Any:
    """Import the hub. Slow and large, so never at module import.

    Called on the first fetch of a run subprocess. Each Dagster run is its own process, so this is
    paid once per run rather than once per call.
    """
    # Deliberately a function-local import; see the module docstring.
    from openbb import obb

    return obb


def _resolve(hub: Any, route: str) -> Callable[..., Any]:
    """Walk the dotted path to the callable, refusing a route that is not in the table.

    An unknown route must fail HERE, naming itself, rather than as an AttributeError deep inside
    the hub that reads like an openbb version problem.
    """
    if route not in ROUTES:
        raise KeyError(f"unknown route {route!r}; add it to ROUTES with the source it came from")
    node = hub
    walked: list[str] = []
    for part in ROUTES[route].split("."):
        # A MISSING ROUTER LOOKS LIKE A BROKEN HUB, AND THE BARE AttributeError SAYS SO:
        # `'App' object has no attribute 'equity'`, which names neither the cause nor the fix. An
        # extension supplying the DATA (openbb-yfinance) is not the one supplying the NAMESPACE you
        # reach it through (openbb-equity), and installing only the first leaves a hub that imports
        # perfectly and cannot serve a single route.
        if not hasattr(node, part):
            namespace = ".".join([*walked, part]) or part
            raise AttributeError(
                f"route {route!r} needs `{namespace}`, which this hub does not have — the router "
                f"extension for it is probably not installed (openbb-{part} for a top-level "
                f"namespace). Installed providers supply data, not namespaces."
            )
        node = getattr(node, part)
        walked.append(part)
    if not callable(node):
        raise TypeError(f"route {route!r} resolved to {type(node).__name__}, not a callable")
    return node  # type: ignore[no-any-return]


def _records(results: Any) -> list[dict[str, Any]]:
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


def fetch(route: str, *, hub: Any | None = None, **params: Any) -> Answer:
    """Call one route and return rows plus the warnings the REST hop would have discarded.

    Raises `ProviderRefused` when a warning says the provider is throttling us. That is the whole
    reason for this module: over HTTP the same call returns 200 with no rows, indistinguishable
    from a symbol the provider genuinely does not carry, and a caller that cannot tell those apart
    eventually marks the wrong thing.

    Everything else is left to the caller. An EMPTY answer is not an error here — openbb answers
    `204 No Content` when a provider legitimately has nothing, and strictness belongs at the caller
    that requires rows, never in the fetcher: treating 204 as a failure once failed 22 of 24
    batches over a few listings yfinance does not carry.
    """
    node = _resolve(hub if hub is not None else _load_hub(), route)
    result = node(**params)

    warnings = [
        w.message for w in (getattr(result, "warnings", None) or []) if getattr(w, "message", None)
    ]
    for message in warnings:
        if throttled(message):
            raise ProviderRefused(f"{route}: {message}")

    return Answer(
        rows=_records(getattr(result, "results", None)),
        warnings=warnings,
        provider=getattr(result, "provider", None),
    )
