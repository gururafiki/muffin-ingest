"""The hub is imported rather than called over HTTP, and these are the reasons.

Every test drives a FAKE hub. That is not only speed: the CI `checks` job deliberately does not
install openbb (it is ~250 MB and AGPL), and a seam that can only be tested with the real thing
installed is a seam nobody tests.
"""

import re
from dataclasses import dataclass
from typing import Any

import pytest

from muffin_ingest.providers import openbb
from muffin_ingest.providers.outcome import Outcome


@dataclass
class FakeWarning:
    message: str
    category: str = "OpenBBWarning"


class FakeRow:
    """Stands in for a pydantic `Data` model, which is what `OBBject.results` holds."""

    def __init__(self, **fields: Any) -> None:
        self._fields = fields

    def model_dump(self) -> dict[str, Any]:
        return dict(self._fields)


class FakeResult:
    def __init__(self, results: Any = None, warnings: list[FakeWarning] | None = None) -> None:
        self.results = results
        self.warnings = warnings
        self.provider = "yfinance"


class FakeHub:
    """A hub whose dotted paths resolve, recording what it was called with."""

    def __init__(self, result: FakeResult) -> None:
        self._result = result
        self.calls: list[dict[str, Any]] = []

        hub = self

        class Node:
            def __init__(self, depth: int = 0) -> None:
                self._depth = depth

            def __getattr__(self, name: str) -> "Node":
                return Node(self._depth + 1)

            def __call__(self, **params: Any) -> FakeResult:
                hub.calls.append(params)
                return hub._result

        self._root = Node()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._root, name)


def test_a_throttle_stated_in_a_warning_is_not_an_empty_answer() -> None:
    """THE WHOLE REASON THIS MODULE EXISTS.

    Alpha Vantage answers an exhausted quota with 200 plus an `Information` field, and openbb's
    REST hop turns that into an empty 204 — byte-identical to "this symbol has no data". That
    conflation once recorded ~8,300 securities as permanently unanswerable in an afternoon.

    In-process the warning survives, so the call raises instead of returning a plausible nothing.
    """
    hub = FakeHub(
        FakeResult(
            results=[],
            warnings=[
                FakeWarning(
                    "Thank you for using Alpha Vantage! Our standard API rate limit is 25 requests"
                )
            ],
        )
    )
    with pytest.raises(openbb.ProviderRefused):
        openbb.fetch("equity.fundamental.metrics", hub=hub, symbol="MSFT")


def test_an_empty_answer_with_no_warning_is_returned_not_raised() -> None:
    """openbb answers 204 when a provider legitimately has nothing, and that is an ANSWER.

    Strictness belongs at the caller that requires rows. Treating 204 as a failure in the fetcher
    once failed 22 of 24 batches over a few listings yfinance does not carry, losing the good
    symbols batched alongside them.
    """
    answer = openbb.fetch("equity.profile", hub=FakeHub(FakeResult(results=[])), symbol="ICT.PS")
    assert answer.empty
    assert answer.warnings == []


def test_a_warning_that_is_not_a_throttle_is_carried_not_raised() -> None:
    """A degraded provider is not a refusing one, and the caller may still want the rows."""
    hub = FakeHub(
        FakeResult(
            results=[FakeRow(symbol="AAPL", close=1.0)],
            warnings=[FakeWarning("Data for 1 symbol was not returned")],
        )
    )
    answer = openbb.fetch("equity.price.historical", hub=hub, symbol="AAPL")
    assert answer.rows == [{"symbol": "AAPL", "close": 1.0}]
    assert answer.warnings == ["Data for 1 symbol was not returned"]


def test_a_single_result_is_still_a_list_of_rows() -> None:
    """Some routes return one object rather than a list of them.

    The same shape hazard as a provider adding a `symbol` column only when several symbols are
    requested: code written against the many-case silently mishandles the one-case.
    """
    answer = openbb.fetch(
        "equity.profile", hub=FakeHub(FakeResult(results=FakeRow(symbol="AAPL"))), symbol="AAPL"
    )
    assert answer.rows == [{"symbol": "AAPL"}]


def test_no_results_at_all_is_no_rows_rather_than_a_crash() -> None:
    assert openbb.fetch("equity.profile", hub=FakeHub(FakeResult(results=None))).rows == []


def test_an_unknown_route_names_itself() -> None:
    """An AttributeError deep in the hub reads like an openbb version problem.

    The route table is data precisely so a typo fails here, saying which entry is missing.
    """
    with pytest.raises(KeyError, match=re.escape("equity.price.perfomance")):
        openbb.fetch("equity.price.perfomance", hub=FakeHub(FakeResult()))


def test_the_irregular_route_is_in_the_table_under_its_real_name() -> None:
    """`obb.x.y.z` -> `/api/v1/x/y/z` is NOT perfectly regular, and this is the case that proves it.

    `/equity/price/performance` sits beside `/etf/price_performance`. Anyone "correcting" the
    second into the pattern gets an AttributeError at runtime, on a route that had been working.
    """
    assert openbb.ROUTES["etf.price_performance"] == "etf.price_performance"
    assert "etf.price.performance" not in openbb.ROUTES


def test_classify_reads_a_throttle_before_an_absence() -> None:
    """A message carrying BOTH vocabularies is a throttle, not an absence.

    THE FIRST VERSION OF THIS TEST COULD NOT FAIL. It asserted a throttle message and an absence
    message separately, and swapping the order of the two checks passed clean — because
    `vocab.py` guarantees the sets are disjoint, so on any single-vocabulary message the order
    genuinely does not matter. A fixture where the candidate rules agree cannot tell them apart.

    A message carrying both is not hypothetical: this file already records Tiingo saying "run over
    your hourly request limit", where two throttle terms overlap, and a provider under load
    reporting a limit AND a missing series in one breath is the realistic shape. Getting it wrong
    costs a month of negative cache on a company that is perfectly fine, so throttle wins.
    """
    both = "Rate limit reached; no dividend data found for TSLA"
    assert openbb.classify(both) is Outcome.THROTTLED, (
        "a message that says both must be read as the provider refusing us, never as an absence"
    )

    throttle = "Error: You have run over your hourly request limit"
    assert openbb.classify(throttle) is Outcome.THROTTLED
    assert openbb.classify("No dividend data found for TSLA") is Outcome.DEAD_SUBJECT
    assert openbb.classify("Could not find CIK for symbol") is Outcome.DEAD_SUBJECT
    # Says nothing about the subject, so it must back off and mark NOTHING.
    assert openbb.classify("connection refused") is Outcome.TRANSPORT


def test_the_hub_is_not_imported_until_a_call_needs_it() -> None:
    """~250 MB and ~2 s at import would be paid by `dagster definitions validate`, by every unit
    test, and by the CI job that deliberately does not install openbb at all."""
    import sys

    assert "openbb" not in sys.modules, "importing this module must not import the hub"
    openbb.fetch("equity.profile", hub=FakeHub(FakeResult(results=[])), symbol="AAPL")
    assert "openbb" not in sys.modules, "passing a hub must not trigger the import either"
