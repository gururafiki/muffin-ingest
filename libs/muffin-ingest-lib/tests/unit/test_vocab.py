"""The vocabularies, and the property that keeps them safe.

Every message here was quoted from a real response on this deployment; the point of the test is
that a future edit cannot make one set shadow the other.
"""

from __future__ import annotations

import pytest

from muffin_ingest.providers.vocab import (
    NO_DATA_PAIRS,
    NO_DATA_TERMS,
    THROTTLE_TERMS,
    no_data_for_subject,
    throttled,
)

# --- messages measured in production --------------------------------------------------------

THROTTLE_MESSAGES = [
    "YFRateLimitError: Too Many Requests. Rate limited. Try after a while.",
    # Tiingo; matched none of the four terms the set originally had
    "Error: You have run over your hourly request limit",
    "HTTP 429 Too Many Requests",
    "Thank you for using Alpha Vantage! Our standard API rate limit is 25 requests per day",
]

NO_DATA_MESSAGES = [
    "No dividend data found for TSLA",  # a US non-payer
    "'NoneType' object has no attribute 'empty'",  # ICT.PS, FAB.AE, WARBABAN.KW
    "Could not find CIK for symbol: BRK/B",
    "Unexpected Error -> ContentTypeError -> 404, "
    "message='Attempt to decode JSON with unexpected mimetype'",
]


@pytest.mark.parametrize("message", THROTTLE_MESSAGES)
def test_throttle_messages_are_classified_as_throttles(message: str) -> None:
    assert throttled(message)
    assert not no_data_for_subject(message), (
        "a throttle classified as an absence is the incident that recorded ~8,300 securities "
        "as permanently unanswerable in one afternoon"
    )


@pytest.mark.parametrize("message", NO_DATA_MESSAGES)
def test_absence_messages_are_classified_as_absences(message: str) -> None:
    assert no_data_for_subject(message)
    assert not throttled(message), (
        "an absence classified as a throttle stalls a weight-ordered backlog on its own head"
    )


def test_the_two_vocabularies_share_no_term() -> None:
    """Neither set may contain a term of the other, NOR a substring of one.

    A substring is the dangerous case: it makes one classifier silently shadow the other for every
    message containing it, and no single test message would reveal it.
    """
    flat_no_data = set(NO_DATA_TERMS) | {part for pair in NO_DATA_PAIRS for part in pair}
    for throttle_term in THROTTLE_TERMS:
        for absence_term in flat_no_data:
            assert throttle_term not in absence_term, (
                f"throttle term {throttle_term!r} is a substring of absence term {absence_term!r}"
            )
            assert absence_term not in throttle_term, (
                f"absence term {absence_term!r} is a substring of throttle term {throttle_term!r}"
            )


def test_a_pair_needs_both_halves() -> None:
    """`contenttypeerror` and `404` mean an absence together and nothing apart.

    A bare 404 is any transport failure and would mark innocent subjects; `contenttypeerror` alone
    is a parsing complaint.
    """
    assert no_data_for_subject("ContentTypeError -> 404")
    assert not no_data_for_subject("404 Not Found")
    assert not no_data_for_subject("ContentTypeError: unexpected mimetype")


def test_the_terms_deliberately_overlap_and_no_single_one_is_required() -> None:
    """WHAT THESE TESTS DO NOT PROVE, measured by mutating them.

    Deleting `"run over your"` — the term added specifically because Tiingo's wording matched none
    of the original four — leaves every test green, because Tiingo's actual message ("Error: You
    have run over your hourly request limit") also contains `"request limit"`, which was added in
    the same batch. So the fixture cannot tell those two rules apart, and no test here pins an
    individual term.

    That is defence in depth rather than a defect, and it is recorded because the alternative is
    believing the suite is stronger than it is. The properties that ARE load-bearing and ARE
    mutation-proven: the two sets share no vocabulary (mutation: adding a throttle term to the
    absence set turns it red), and a pair needs both halves (mutation: matching on either half
    turns it red).

    The failure no test can catch is a wording never seen. That is why every term is quoted from a
    measured response and why an unrecognised message classifies as neither.
    """
    tiingo = "Error: You have run over your hourly request limit"
    matching = {term for term in THROTTLE_TERMS if term in tiingo.lower()}
    assert len(matching) >= 2, (
        f"expected Tiingo's wording to be caught by more than one term, got {matching}"
    )


def test_an_unrecognised_message_is_neither() -> None:
    """The default is to claim nothing, so an unclassified failure backs off instead of marking."""
    message = "Segmentation fault"
    assert not throttled(message)
    assert not no_data_for_subject(message)
