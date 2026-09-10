"""The queue, as the library sees it.

Thin on purpose. Every rule that can be wrong in a way that costs a month lives in SQL — in
`ingest.mark_absent()`, which REFUSES without an attempt proving the subject was asked alone and a
control subject answered — so this module cannot get them wrong by forgetting them. What is here is
the shape of a run: open an attempt, claim a page, record what each subject's answer meant, close
the attempt.

THE ATTEMPT IS OPENED BEFORE THE PROVIDER IS CALLED AND CLOSED IN A `finally`. A worker killed by
the supervisor writes nothing at all — it goes silent rather than red — so the row that says
"something started here" has to exist before the thing that might kill us. `ingest.reap()` closes
whatever is left open, which is how a killed run stops being invisible.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from muffin_ingest.providers.isolation import BatchVerdict
from muffin_ingest.providers.outcome import Outcome


# WHAT THIS MODULE ACTUALLY NEEDS FROM A DATABASE, as a protocol rather than a driver import.
#
# Two things follow that are worth more than the tidiness. The ledger can be driven in a test with
# a fake that records the SQL, so "which function does an empty answer reach" is a unit test rather
# than something reviewed by eye — and it is exactly the question that has cost this pipeline the
# most. And nothing in `muffin_ingest` imports psycopg, so a driver change is one line in the
# caller.
class DbCursor(Protocol):
    def __enter__(self) -> DbCursor: ...
    def __exit__(self, *exc: object) -> None: ...
    def execute(self, sql: str, params: Sequence[Any] = ()) -> None: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...


class DbConn(Protocol):
    def cursor(self) -> DbCursor: ...
    def commit(self) -> None: ...


@dataclass(frozen=True)
class Task:
    """One unit of work: a subject that owes a facet."""

    facet: str
    subject: str
    security_id: str | None
    asked_with: str | None
    watermark: object | None
    version: int


class Attempt:
    """One provider call, recorded from before it happens until after it ends."""

    def __init__(self, conn: DbConn, attempt_id: int, facet: str) -> None:
        self._conn = conn
        self.attempt_id = attempt_id
        self.facet = facet

    def close(
        self,
        outcome: Outcome,
        *,
        rows_written: int = 0,
        error: str | None = None,
        duration_ms: int | None = None,
        isolated: bool = False,
        control_answered: bool | None = None,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                update ingest.attempt
                   set finished_at = now(), outcome = %s, rows_written = %s,
                       error = left(%s, 2000), duration_ms = %s,
                       isolated = %s, control_answered = %s
                 where attempt_id = %s
                """,
                (
                    outcome.value,
                    rows_written,
                    error,
                    duration_ms,
                    isolated,
                    control_answered,
                    self.attempt_id,
                ),
            )


@contextmanager
def attempt(
    conn: DbConn, run_id: str, facet: str, provider: str, subjects: Sequence[str]
) -> Iterator[Attempt]:
    """Open an attempt row, hand it out, and guarantee it is closed.

    On an exception the outcome is `transport` rather than anything about the subjects: a crash on
    our side says nothing about the company, and recording it as an absence is how an outage becomes
    a population of permanently dead securities.
    """
    started = time.monotonic()
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingest.attempt (run_id, facet, provider_code, subjects, asked_with)
            values (%s, %s, %s, %s, %s)
            returning attempt_id
            """,
            (run_id, facet, provider, len(subjects), list(subjects)),
        )
        attempt_id = (cur.fetchone() or (0,))[0]
    conn.commit()  # so the row survives even if this process is killed next

    a = Attempt(conn, attempt_id, facet)
    try:
        yield a
    except Exception as e:
        a.close(
            Outcome.TRANSPORT,
            error=str(e)[:2000],
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        conn.commit()
        raise


def sync_population(conn: DbConn, facet: str) -> int:
    """Enqueue what this facet owes and does not already hold. Returns how many were added.

    Called before every claim, so `backlog_size` is always the real depth of the queue rather than
    a number that only becomes true after some other job has run.

    The anti-join, the stored `round` and the shape check all live in the database function — not
    here — because they are invariants of the queue rather than of this client. `round` in
    particular has to be assigned once and never recomputed: a window function over the
    OUTSTANDING rows renumbers as the queue drains and hands the same company the head for ever,
    which is a defect no counter in the old system could see.
    """
    with conn.cursor() as cur:
        cur.execute("select ingest.sync_population(%s)", (facet,))
        return int((cur.fetchone() or (0,))[0])


def cool_down(conn: DbConn, provider_code: str, seconds: int | None = None) -> None:
    """Put a provider to sleep because it said it is refusing us.

    Separate from the daily quota on purpose: the budget can be untouched and the provider still
    unwilling, and a run that cannot tell those apart marks securities absent during an outage —
    which is how 1,369 ordinary tickers were once negative-cached for a month in one afternoon.
    """
    with conn.cursor() as cur:
        if seconds is None:
            cur.execute("select ingest.cool_down(%s)", (provider_code,))
        else:
            cur.execute(
                "select ingest.cool_down(%s, make_interval(secs => %s))", (provider_code, seconds)
            )


def claim(conn: DbConn, facet: str, limit: int, lease_seconds: int, run_id: str) -> list[Task]:
    """Take a page. Ordering, leasing and reaping are all the database's, not ours."""
    with conn.cursor() as cur:
        cur.execute(
            "select facet, subject, security_id, asked_with, watermark, version "
            "from ingest.claim(%s, %s, make_interval(secs => %s), %s)",
            (facet, limit, lease_seconds, run_id),
        )
        return [
            Task(
                facet=r[0],
                subject=r[1],
                security_id=r[2],
                asked_with=r[3],
                watermark=r[4],
                version=r[5],
            )
            for r in cur.fetchall()
        ]


def record(
    conn: DbConn,
    facet: str,
    tasks: Sequence[Task],
    verdict: BatchVerdict,
    attempt_obj: Attempt,
    *,
    rows_per_subject: dict[str, int] | None = None,
) -> None:
    """Turn one batch's evidence into task state.

    THE ONLY PLACE THAT DECIDES, and it decides nothing that SQL will not re-check. A subject in
    `verdict.dead` is offered to `ingest.mark_absent()`, which refuses unless the attempt says it
    was asked alone AND a control answered — so an over-eager caller is stopped by the database
    rather than by review.
    """
    written = rows_per_subject or {}
    dead = set(verdict.dead)

    with conn.cursor() as cur:
        for task in tasks:
            if task.subject in dead:
                # Offered, not asserted. The refusal lives in SQL.
                cur.execute(
                    "select ingest.mark_absent(%s, %s, %s, %s)",
                    (facet, task.subject, attempt_obj.attempt_id, task.asked_with),
                )
                continue

            if written.get(task.subject, 0) > 0:
                outcome = Outcome.ANSWERED
            elif verdict.throttled_out:
                outcome = Outcome.THROTTLED
            elif verdict.error is not None:
                outcome = Outcome.TRANSPORT
            else:
                # Asked, answered nothing, and NOT justified as an absence — so it backs off and is
                # asked again. This is the branch that must never quietly become a mark.
                outcome = Outcome.EMPTY

            cur.execute(
                "select ingest.complete(%s, %s, %s, %s, null, %s)",
                (facet, task.subject, outcome.value, task.asked_with, verdict.error),
            )


def backlog_size(conn: DbConn, facet: str) -> int:
    """How deep the QUEUE is, never how much of this page is left.

    Nine resources once computed `remaining` as the page minus what it covered — correct security
    counts answering the wrong question, so `security-prices` reported `remaining: 0` against a
    backlog of 9,013.
    """
    with conn.cursor() as cur:
        cur.execute(
            "select count(*) from ingest.task "
            "where facet = %s and status in ('due','backoff') and next_due_at <= now()",
            (facet,),
        )
        return int((cur.fetchone() or (0,))[0])
