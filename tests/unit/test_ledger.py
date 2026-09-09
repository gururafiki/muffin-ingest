"""The ledger client, driven against a fake cursor.

There is no database here on purpose. What this module must get right is WHICH SQL FUNCTION it calls
for each outcome — because `ingest.mark_absent` refuses the unjustified cases itself, the client's
only real responsibility is not to route an unjustified subject there in the first place, and not to
route a justified one somewhere else.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from muffin_ingest import ledger
from muffin_ingest.providers.isolation import BatchVerdict
from muffin_ingest.providers.outcome import Outcome


class FakeCursor:
    def __init__(self, sink: list[tuple[str, Sequence[Any]]]) -> None:
        self.sink = sink

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *a: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self.sink.append((" ".join(sql.split()), params))

    def fetchone(self) -> tuple[Any, ...] | None:
        return (1,)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []


class FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Sequence[Any]]] = []
        self.commits = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.calls)

    def commit(self) -> None:
        self.commits += 1


def _task(subject: str) -> ledger.Task:
    return ledger.Task(
        facet="f", subject=subject, security_id=None, asked_with=subject, watermark=None, version=0
    )


def test_a_dead_subject_is_offered_to_mark_absent() -> None:
    conn = FakeConn()
    verdict = BatchVerdict(dead=["B"], isolated=True, control_answered=True)
    ledger.record(conn, "f", [_task("B")], verdict, ledger.Attempt(conn, 7, "f"))
    sql, params = conn.calls[0]
    assert "ingest.mark_absent" in sql
    assert params[2] == 7, "the ATTEMPT is passed, because that is what justifies the mark"


def test_a_throttled_subject_is_never_offered_to_mark_absent() -> None:
    """The mark would be a statement about the company; a throttle is a statement about us."""
    conn = FakeConn()
    verdict = BatchVerdict(dead=[], error="rate limit", throttled_out=True)
    ledger.record(conn, "f", [_task("A")], verdict, ledger.Attempt(conn, 7, "f"))
    joined = " ".join(s for s, _ in conn.calls)
    assert "mark_absent" not in joined
    assert Outcome.THROTTLED.value in str(conn.calls)


def test_an_empty_answer_backs_off_rather_than_marking() -> None:
    """The branch that must never quietly become a mark: asked, answered nothing, not justified."""
    conn = FakeConn()
    ledger.record(conn, "f", [_task("A")], BatchVerdict(), ledger.Attempt(conn, 7, "f"))
    joined = " ".join(s for s, _ in conn.calls)
    assert "mark_absent" not in joined
    assert Outcome.EMPTY.value in str(conn.calls)


def test_a_subject_with_rows_is_answered() -> None:
    conn = FakeConn()
    ledger.record(
        conn,
        "f",
        [_task("A")],
        BatchVerdict(rows=[{}]),
        ledger.Attempt(conn, 7, "f"),
        rows_per_subject={"A": 3},
    )
    assert Outcome.ANSWERED.value in str(conn.calls)


def test_the_attempt_row_is_committed_before_the_provider_is_called() -> None:
    """A killed worker writes nothing at all, so the row saying "something started here"
    must already be durable before the risky work begins."""
    conn = FakeConn()
    with ledger.attempt(conn, "run-1", "f", "p", ["A"]):
        assert conn.commits == 1, "the attempt must be durable before the risky work begins"


def test_a_crash_closes_the_attempt_as_transport_not_as_an_absence() -> None:
    """A crash on our side says nothing about the company."""
    conn = FakeConn()
    with pytest.raises(RuntimeError), ledger.attempt(conn, "run-1", "f", "p", ["A"]):
        raise RuntimeError("boom")
    assert Outcome.TRANSPORT.value in str(conn.calls)
    assert Outcome.DEAD_SUBJECT.value not in str(conn.calls)


def test_backlog_size_counts_the_queue_not_the_page() -> None:
    """Nine resources once reported the page's remainder and called it the backlog."""
    conn = FakeConn()
    ledger.backlog_size(conn, "f")
    sql, _ = conn.calls[0]
    assert "count(*)" in sql and "ingest.task" in sql
    assert "limit" not in sql.lower(), "a count, never a page"
