"""Ask a batch; if it answers nothing, find out WHO is at fault before blaming anyone.

A faithful port of `fetchWithIsolation` from the edge function, including the two rules that were
each added after an incident and the one that was added after the FIX for an incident.

It returns EVIDENCE and never writes. Deciding what the evidence means is `ledger.mark()`'s job, and
recording it is `ingest.mark_absent()`'s, which refuses without the two facts this produces:
whether the subject was asked ALONE, and whether a control subject proved the provider healthy in
the same attempt.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from muffin_ingest.providers.vocab import throttled

#: Below this, do not start another isolated call — an answer that cannot be recorded is worse than
#: no answer, because a subject we never asked must not look like one that failed.
TAIL_RESERVE_S = 2.0

#: A control probe needs room to complete, or a slow probe becomes a false "the provider is down"
#: and nothing is marked when it should be.
CONTROL_RESERVE_S = 4.0


class Fetcher(Protocol):
    """Asks a provider for a set of subjects. Raises on transport failure, returns [] on an empty
    answer — and the whole point of this module is that those are different facts."""

    def __call__(self, subjects: Sequence[str], timeout_s: float) -> list[dict[str, object]]: ...


@dataclass(frozen=True)
class BatchVerdict:
    """What one batch established. `dead` is populated ONLY when it can be justified."""

    rows: list[dict[str, object]] = field(default_factory=list)
    dead: list[str] = field(default_factory=list)
    error: str | None = None
    #: True when every subject in `dead` was asked on its own. `ingest.mark_absent` refuses without
    #: it, because a run-wide tally is only ever a floor on the provider's health.
    isolated: bool = False
    #: True only when a known-good subject answered in this same attempt. None when not probed.
    control_answered: bool | None = None
    throttled_out: bool = False


def fetch_with_isolation(
    fetcher: Fetcher,
    subjects: Sequence[str],
    timeout_s: float,
    deadline: float,
    control: str | None = "AAPL",
    now: Callable[[], float] = time.monotonic,
) -> BatchVerdict:
    """Try the batch; on a throw OR an empty answer, re-ask each subject alone.

    AN EMPTY ANSWER MUST TRIGGER ISOLATION, not just a throw. A throttled yfinance returns
    200-with-no-rows rather than raising, so a rule that only isolates on an exception is blind to
    the single most common failure — six marking sites once recorded "the provider has nothing for
    this security" when the truth was "the provider is not talking to us".
    """
    try:
        rows = fetcher(subjects, timeout_s)
        if rows or len(subjects) <= 1:
            return BatchVerdict(rows=rows)
        reason = "batch answered with no rows"
    except Exception as e:  # noqa: BLE001 - any transport failure is evidence, not a crash
        reason = str(e)[:200]

    return _isolate(fetcher, subjects, timeout_s, deadline, control, reason, now)


def _isolate(
    fetcher: Fetcher,
    subjects: Sequence[str],
    timeout_s: float,
    deadline: float,
    control: str | None,
    reason: str,
    now: Callable[[], float],
) -> BatchVerdict:
    rows: list[dict[str, object]] = []
    failed_alone: list[str] = []
    asked_all = True

    for subject in subjects:
        remaining = deadline - now()
        if remaining < TAIL_RESERVE_S:
            # A SUBJECT WE RAN OUT OF TIME TO ASK IS NOT A DEAD SUBJECT. Marking one would record
            # our own deadline as a fact about the company.
            asked_all = False
            break
        try:
            got = fetcher([subject], min(timeout_s, remaining))
        except Exception:  # noqa: BLE001
            failed_alone.append(subject)
        else:
            if got:
                rows.extend(got)
            else:
                failed_alone.append(subject)

    # 1. A THROTTLE BLAMES NOBODY. Draining a backlog too hard once tripped yfinance's limit, every
    #    batch then failed, every symbol failed alone, and the isolation pass concluded that 1,369
    #    securities were permanently unanswerable — including ordinary Nasdaq lines.
    if throttled(reason):
        return BatchVerdict(
            rows=rows,
            dead=[],
            error=f"{reason} (provider is RATE-LIMITING — no subject blamed)",
            isolated=False,
            throttled_out=True,
        )

    # 2. IF NOTHING ANSWERED, PROVE THE PROVIDER IS UP BEFORE BLAMING THE SUBJECTS. Without this,
    #    an outage is indistinguishable from a page of genuinely dead symbols.
    if not rows and failed_alone:
        if control is not None and deadline - now() > CONTROL_RESERVE_S:
            try:
                probe = fetcher([control], min(timeout_s, deadline - now()))
            except Exception:  # noqa: BLE001
                probe = []
            if probe:
                return BatchVerdict(
                    rows=rows,
                    dead=failed_alone,
                    error=f"{reason} (control {control} answered — the provider is up "
                    f"and these {len(failed_alone)} subjects are unanswerable)",
                    isolated=asked_all,
                    control_answered=True,
                )
        return BatchVerdict(
            rows=rows,
            dead=[],
            error=f"{reason} (all {len(failed_alone)} failed individually and the control did not "
            "answer — treated as a provider outage, not bad subjects)",
            isolated=False,
            control_answered=False,
        )

    # 3. SOME ANSWERED, SO THE PROVIDER IS DEMONSTRABLY UP and the rest are evidence about
    #    themselves — but ONLY the ones actually asked alone. ISOLATION MUST KEEP WHAT IT RECOVERS:
    #    a batch is often empty because ONE member is bad, and discarding the others' rows turns the
    #    fix into a slower version of the bug.
    return BatchVerdict(
        rows=rows,
        dead=failed_alone,
        error=reason,
        isolated=asked_all,
        control_answered=True if rows else None,
    )
