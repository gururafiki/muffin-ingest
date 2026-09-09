"""The isolation rule, driven against a stub provider.

Both failure directions are silent in production, which is why this is tested rather than reviewed:
too eager writes a rate limit down as thousands of dead companies (it once cost 1,369), too cautious
stalls a backlog for ever on a batch it refuses to judge.
"""

from __future__ import annotations

from collections.abc import Sequence

from muffin_ingest.providers.isolation import fetch_with_isolation

ROW: dict[str, object] = {"ok": True}


class Stub:
    """A provider that answers, empties or raises per subject, and counts what it was asked."""

    def __init__(
        self,
        answers: dict[str, list[dict[str, object]]] | None = None,
        raises: set[str] | None = None,
        batch_raises: str | None = None,
        batch_empty: bool = True,
    ) -> None:
        self.answers = answers or {}
        self.raises = raises or set()
        self.batch_raises = batch_raises
        self.batch_empty = batch_empty
        self.asked: list[tuple[str, ...]] = []

    def __call__(self, subjects: Sequence[str], timeout_s: float) -> list[dict[str, object]]:
        self.asked.append(tuple(subjects))
        if len(subjects) > 1:
            if self.batch_raises:
                raise RuntimeError(self.batch_raises)
            return [] if self.batch_empty else [ROW]
        (s,) = subjects
        if s in self.raises:
            raise RuntimeError(f"no data for {s}")
        return self.answers.get(s, [])


def _clock() -> float:
    return 0.0


FAR = 10_000.0


def test_a_batch_that_answers_is_returned_untouched() -> None:
    stub = Stub(batch_empty=False)
    v = fetch_with_isolation(stub, ["A", "B"], 5, FAR, control="CTL", now=_clock)
    assert v.rows == [ROW] and v.dead == [] and len(stub.asked) == 1


def test_an_empty_batch_triggers_isolation_not_only_a_throw() -> None:
    """A throttled yfinance returns 200-with-no-rows rather than raising. A rule that isolates only
    on an exception is blind to the commonest failure this pipeline has."""
    stub = Stub(answers={"A": [ROW]}, batch_empty=True)
    v = fetch_with_isolation(stub, ["A", "B"], 5, FAR, control="CTL", now=_clock)
    assert stub.asked[0] == ("A", "B")
    assert ("A",) in stub.asked and ("B",) in stub.asked
    assert v.rows == [ROW] and v.dead == ["B"]


def test_isolation_keeps_what_it_recovers() -> None:
    """A batch is often empty because ONE member is bad. Discarding the others' rows turns the fix
    into a slower version of the bug."""
    stub = Stub(answers={"A": [ROW], "C": [ROW]}, batch_empty=True)
    v = fetch_with_isolation(stub, ["A", "B", "C"], 5, FAR, control="CTL", now=_clock)
    assert len(v.rows) == 2 and v.dead == ["B"]


def test_a_throttle_blames_nobody() -> None:
    """Draining too hard once tripped yfinance's limit; every symbol then failed alone and 1,369
    ordinary securities were recorded as permanently unanswerable."""
    stub = Stub(batch_raises="YFRateLimitError: Too Many Requests")
    v = fetch_with_isolation(stub, ["A", "B"], 5, FAR, control="CTL", now=_clock)
    assert v.dead == []
    assert v.throttled_out is True
    assert v.isolated is False, "a throttled verdict must never justify a mark"


def test_nothing_answered_and_no_control_is_an_outage_not_bad_symbols() -> None:
    stub = Stub(raises={"A", "B"})  # the control answers nothing either
    v = fetch_with_isolation(stub, ["A", "B"], 5, FAR, control="CTL", now=_clock)
    assert v.dead == [], "an outage must not be recorded as two dead subjects"
    assert v.control_answered is False


def test_nothing_answered_but_the_control_did_blames_the_symbols() -> None:
    stub = Stub(answers={"CTL": [ROW]}, raises={"A", "B"})
    v = fetch_with_isolation(stub, ["A", "B"], 5, FAR, control="CTL", now=_clock)
    assert v.dead == ["A", "B"]
    assert v.control_answered is True
    assert v.isolated is True, "each subject was asked alone, which is what permits a mark"


def test_a_subject_the_deadline_cut_short_is_never_marked() -> None:
    """Marking one would record OUR deadline as a fact about the company."""
    # The loop reads the clock ONCE per subject before asking it, so the third read is what must
    # land inside the tail reserve. Getting this wrong the first time let all three be asked and the
    # test failed for the right reason.
    ticks = iter([0.0, 0.0, 9_999.5])

    def clock() -> float:
        return next(ticks, 9_999.5)

    stub = Stub(answers={"A": [ROW]}, batch_empty=True)
    v = fetch_with_isolation(stub, ["A", "B", "C"], 5, FAR, control="CTL", now=clock)
    assert "C" not in v.dead
    assert v.isolated is False, "a partial sweep cannot justify marking anything"


def test_a_single_subject_batch_is_not_isolated_twice() -> None:
    """One subject IS the isolated case; re-asking would double the cost of every dead symbol."""
    stub = Stub(answers={}, batch_empty=True)
    v = fetch_with_isolation(stub, ["A"], 5, FAR, control="CTL", now=_clock)
    assert len(stub.asked) == 1 and v.rows == [] and v.dead == []
