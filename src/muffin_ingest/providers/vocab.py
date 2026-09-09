"""How a provider SAYS it is refusing us, versus how it says a subject has no data.

Ported verbatim from `resources.ts`'s `throttled()` and `noDataForSymbol()` in the edge function,
including the wordings that were added only after they had cost something. Every entry was quoted
from a body seen in production on this deployment; none is a guess at what a provider might say.

THE TWO SETS MUST SHARE NO VOCABULARY, and `test_vocab.py` asserts it. Marking a throttled symbol
absent is the incident that recorded ~8,300 securities as permanently unanswerable in an afternoon;
failing to classify a genuine absence is what stalled `pending_dividends` at 9,221 with `written: 0`
on every run for a day.
"""

from __future__ import annotations

#: The provider is refusing us. Never evidence about a subject.
#:
#: The first four were the original guess at vocabulary. The last three are why a guess is not a
#: classifier: Tiingo says "Error: You have run over your hourly request limit", which matches none
#: of the first four, so six consecutive `security-corporate-actions` runs were rate-limited with
#: `throttledOut` never set — leaving the throttle-pressure panel and its alert blind to that
#: provider entirely.
THROTTLE_TERMS: frozenset[str] = frozenset(
    {
        "ratelimit",
        "rate limit",
        "too many requests",
        "429",
        "run over your",
        "request limit",
        "quota",
    }
)

#: This SUBJECT has no data, as a settled fact. Simple substrings.
#:
#: The first two mean the same thing through two code paths in openbb's yfinance adapter, measured
#: 2026-09-06: `TSLA` (a US non-payer) answers "No dividend data found for TSLA", while `ICT.PS`,
#: `FAB.AE` and `WARBABAN.KW` answer "'NoneType' object has no attribute 'empty'" — the adapter
#: dereferencing a frame it never received. The second reads like a bug in our code and is a
#: statement about the symbol, and because only the first was classified, 60 securities of the
#: Philippines, the UAE, Kuwait and Chile sat at the head of `pending_dividends` failing eight
#: times a day.
#:
#: The third is SEC via openbb: it resolves symbol -> CIK through SEC's own ticker map, so a US OTC
#: foreign-ordinary line absent from that map cannot be served however valid the company is.
NO_DATA_TERMS: frozenset[str] = frozenset(
    {
        "no dividend data found",
        "'nonetype' object has no attribute",
        "could not find cik for symbol",
    }
)

#: The same fact stated as a pair that must BOTH appear. SEC via openbb answers an unmapped symbol
#: with `Unexpected Error -> ContentTypeError -> 404, message='Attempt to decode JSON …'`, measured
#: on `AIBRF` and `BWAGF` with AAPL answering in the same seconds. Neither half alone is safe: a
#: bare "404" is any transport failure, and `contenttypeerror` alone is a parsing complaint.
NO_DATA_PAIRS: frozenset[tuple[str, ...]] = frozenset({("contenttypeerror", "404")})


def throttled(message: str) -> bool:
    """Is this provider refusing us, rather than answering about a subject?"""
    m = message.lower()
    return any(term in m for term in THROTTLE_TERMS)


def no_data_for_subject(message: str) -> bool:
    """Does this error say THIS SUBJECT has no data, rather than that the provider is unwell?

    A throttle is deliberately NOT in this list and must never be. `throttled()` is consulted first
    and breaks the loop before this is reached; that separation is what makes it safe to mark on
    per-symbol evidence without stacking a run-level tally on top — a tally that, once the
    answerable head of a backlog has drained, can never become true and guarantees the stall it was
    meant to prevent.
    """
    m = message.lower()
    if any(term in m for term in NO_DATA_TERMS):
        return True
    return any(all(part in m for part in pair) for pair in NO_DATA_PAIRS)
