"""What the in-process hub keeps that the REST hop destroys, and how a message is read.

Every test drives a FAKE result. That is not only speed: the CI `checks` job deliberately does not
install openbb (~250 MB, AGPL), and a seam that can only be tested with the real thing installed is
a seam nobody tests.

THERE IS NO ROUTE TABLE TO TEST ANY MORE. There used to be a `ROUTES` dict walked with `getattr`,
and two tests here asserted its contents. Measured before deleting it: all 26 entries mapped a
string to ITSELF, and its stated justification — the REST convention being irregular — was about a
URL this pipeline stopped using when the hub moved in-process. The calls are written out and typed
now, so the question those tests asked is answered by mypy where the hub is installed.
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


def test_a_throttle_stated_in_a_warning_is_not_an_empty_answer() -> None:
    """THE WHOLE REASON THE HUB IS IMPORTED. Over HTTP this same call is a 200 with no rows —
    byte-identical to a symbol the provider does not carry — and a caller that cannot tell those
    apart eventually records a real company as permanently unanswerable."""
    result = FakeResult(
        results=[],
        warnings=[FakeWarning("YFRateLimitError: Too Many Requests. Rate limited.")],
    )
    with pytest.raises(openbb.ProviderRefused):
        openbb.answer_from(result)


def test_an_empty_answer_with_no_warning_is_returned_not_raised() -> None:
    """openbb answers 204 when a provider legitimately has nothing. Strictness belongs at the
    caller that requires rows — treating 204 as a failure once failed 22 of 24 batches over a few
    listings yfinance does not carry."""
    answer = openbb.answer_from(FakeResult(results=[]))
    assert answer.rows == []
    assert answer.warnings == []


def test_a_warning_that_is_not_a_throttle_is_carried_not_raised() -> None:
    """A provider can degrade itself while still answering. The message travels with the rows so a
    caller can record it, rather than being turned into a failure nobody asked for."""
    result = FakeResult(
        results=[FakeRow(date="2026-09-09", close=1.0)],
        warnings=[FakeWarning("Symbol Error: No data found for FOO")],
    )
    answer = openbb.answer_from(result)
    assert len(answer.rows) == 1
    assert "No data found" in answer.warnings[0]


def test_a_single_result_is_still_a_list_of_rows() -> None:
    """Some routes return one object rather than a list — the same shape problem as a provider
    adding a `symbol` column only when several symbols are requested."""
    answer = openbb.answer_from(FakeResult(results=FakeRow(symbol="AAPL", name="Apple")))
    assert answer.rows == [{"symbol": "AAPL", "name": "Apple"}]


def test_no_results_at_all_is_no_rows_rather_than_a_crash() -> None:
    assert openbb.answer_from(FakeResult(results=None)).rows == []


def test_classify_reads_a_throttle_before_an_absence() -> None:
    """ORDER IS NOT ARBITRARY. A provider refusing us often also says something that reads like an
    absence, and calling that an absence negative-caches real companies for a month."""
    both = "Rate limited: no data found for AAPL"
    assert openbb.classify(both) is Outcome.THROTTLED, (
        "a message carrying BOTH vocabularies must read as the provider refusing us, never as a "
        "statement about the symbol"
    )
    assert openbb.classify("YFRateLimitError") is Outcome.THROTTLED
    assert openbb.classify("No dividend data found for TSLA") is Outcome.DEAD_SUBJECT
    assert openbb.classify("Could not find CIK for symbol") is Outcome.DEAD_SUBJECT
    assert openbb.classify("connection refused") is Outcome.TRANSPORT


def test_the_hub_is_not_imported_by_importing_this_module() -> None:
    """~250 MB and ~2 s would otherwise be paid by `dagster definitions validate`, by every unit
    test, and by the CI job that deliberately has no openbb installed. The import lives inside the
    call that needs it, so this module and its callers stay cheap to load."""
    import pathlib

    text = pathlib.Path(openbb.__file__ or "").read_text()
    module_level = text.split("def price_history")[0]
    assert not re.search(r"^from openbb import", module_level, re.M), (
        "the hub import must be function-local"
    )
    assert not re.search(r"^import openbb", module_level, re.M)
