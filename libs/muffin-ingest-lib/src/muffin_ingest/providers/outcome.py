"""What a provider's answer MEANS, which is the distinction this whole pipeline turns on.

A request that FAILED and a request that ANSWERED NOTHING are different facts. Conflating them is
the single most expensive defect this codebase has had, in both directions:

* treating a throttle as an absence recorded ~8,300 securities as permanently unanswerable in one
  afternoon, including ordinary Nasdaq lines, while every count read as healthy progress;
* treating an absence as a failure stalled weight-ordered backlogs on their own head for weeks,
  because the same unanswerable securities returned at the front of every page.

So the classifier returns an `Outcome`, never a boolean, and `ledger.mark()` is the only code
allowed to turn one into a stored fact.
"""

from __future__ import annotations

from enum import StrEnum


class Outcome(StrEnum):
    """The five things a provider can tell us, plus the two ways we can fail to ask."""

    ANSWERED = "answered"
    """Rows came back. The only outcome that clears a task."""

    EMPTY = "empty"
    """The provider answered and had nothing. Evidence about the SYMBOL — but only ever enough to
    mark it absent when the symbol was asked ALONE and a control symbol proved the provider healthy
    in the same run. openbb returns `204 No Content` for this, which is a legitimate answer."""

    THROTTLED = "throttled"
    """The provider is refusing us. Evidence about the PROVIDER and never about a symbol; marking
    anything on this is the incident above. yfinance also expresses it as a 200 with no rows, which
    is why `EMPTY` alone can never justify a mark."""

    DEAD_SUBJECT = "dead_subject"
    """The provider named this subject as one it cannot serve — a delisted line, a venue it does
    not cover, a symbol absent from SEC's registrant map. Settled, so it earns a negative cache."""

    UNSUPPORTED_VENUE = "unsupported_venue"
    """The endpoint does not cover this market at all (finviz outside US listings, Tiingo outside
    US corporate actions). Distinct from `DEAD_SUBJECT` because it is a property of the ROUTE, so
    re-asking under a corrected symbol cannot help."""

    TRANSPORT = "transport"
    """We never got an answer: connection refused, TLS failure, timeout, unparseable body. Says
    nothing about the subject, so it backs off and marks nothing. A thirty-second provider restart
    must not cost a week of staleness."""

    PARSER_KILLED = "parser_killed"
    """We got the document and could not read it inside the memory or time we allow. Ours, not the
    provider's — and it must still stamp, or the document returns at the head of the queue for ever.
    That is exactly how `security-cn-segments` stopped for two days: a killed worker writes no
    record at all, so it goes silent rather than red."""


#: Outcomes after which a task may be recorded as permanently (for `absent_ttl`) unanswerable.
#: Deliberately a set of two: everything else is either success or a statement about us.
MARKABLE: frozenset[Outcome] = frozenset({Outcome.DEAD_SUBJECT, Outcome.UNSUPPORTED_VENUE})

#: Outcomes that mean the provider is unwell, so the whole page backs off and NOTHING is marked.
PROVIDER_UNWELL: frozenset[Outcome] = frozenset({Outcome.THROTTLED, Outcome.TRANSPORT})
