"""Four rules the writer enforces so a caller cannot forget them.

Every one cost something in production, and every fixture below is built so the WRONG
implementation is distinguishable rather than merely absent.
"""

from typing import Any

import pytest

from muffin_ingest.writers import (
    WriterError,
    dedupe_by,
    numeric_or_none,
    replace_scope,
    upsert,
)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[Any]]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = ()) -> None:
        self.calls.append((" ".join(sql.split()), list(params)))

    def fetchone(self) -> tuple[Any, ...] | None:
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []


def test_a_conflict_key_appearing_twice_is_collapsed_not_sent() -> None:
    """SQLSTATE 21000 fails the WHOLE statement, so the batch and the resource go with it.

    Four occurrences here, and the symptom is a bare 502 that reads as a size problem: a page of
    10 and 20 succeeded while 40 and 300 did not, because only a large page was likely to contain
    a duplicate.
    """
    cur = FakeCursor()
    rows = [
        {"security_id": "s1", "period": "2025", "value": 1},
        {"security_id": "s1", "period": "2025", "value": 2},
        {"security_id": "s2", "period": "2025", "value": 3},
    ]
    result = upsert(cur, "market.thing", rows, conflict=["security_id", "period"], update=["value"])

    assert result.written == 2
    assert result.collapsed == 1, "the collapse must be REPORTED — a silent one hides a source bug"
    sql, params = cur.calls[0]
    assert sql.count("(%s, %s, %s)") == 2, "only two value tuples reach the server"
    # LAST wins: a later row is the newer statement, so s1 carries 2 rather than 1.
    assert 2 in params and 1 not in params


def test_do_nothing_still_dedupes() -> None:
    """Postgres tolerates duplicates under DO NOTHING, which is exactly why this is easy to skip.

    Sending rows the server will discard is waste, and the collapse count is the signal about the
    source either way.
    """
    cur = FakeCursor()
    rows = [{"k": 1, "v": "a"}, {"k": 1, "v": "b"}]
    result = upsert(cur, "market.thing", rows, conflict=["k"])
    assert (result.written, result.collapsed) == (1, 1)
    assert "do nothing" in cur.calls[0][0]


def test_an_upsert_without_a_conflict_target_is_refused() -> None:
    with pytest.raises(WriterError, match="conflict target"):
        upsert(FakeCursor(), "market.thing", [{"k": 1}], conflict=[])


def test_a_conflict_column_absent_from_the_rows_is_refused() -> None:
    """Otherwise the statement names a column the VALUES list does not carry, and the error comes
    back from the server naming SQL rather than the facet that built the rows."""
    with pytest.raises(WriterError, match="period_type"):
        upsert(
            FakeCursor(),
            "market.security_statement",
            [{"security_id": "s1", "period_ending": "2025-12-31"}],
            conflict=["security_id", "period_ending", "period_type"],
            update=["value"],
        )


def test_replace_scope_deletes_even_when_there_is_nothing_to_write() -> None:
    """A filing that now discloses no segments must WITHDRAW what it used to.

    An upsert alone cannot: the rows it stops producing stay in a served partition for ever,
    looking freshly written. A parser rule that DEMOTES a member self-heals; one that DROPS a
    member does not, which is how a subtotal stayed beside its own children after the fix that
    was meant to remove it.
    """
    cur = FakeCursor()
    result = replace_scope(
        cur, "market.security_segment", [], scope={"accession_number": "0001-25"}, conflict=["id"]
    )
    assert cur.calls, "the delete must run even with no rows"
    sql, params = cur.calls[0]
    assert sql.startswith("delete from market.security_segment where accession_number = %s")
    assert params == ["0001-25"]
    assert result.retracted == 1 and result.written == 0


def test_replace_scope_refuses_an_empty_scope() -> None:
    """With no scope the delete has no WHERE clause and empties the table."""
    with pytest.raises(WriterError, match="scope"):
        replace_scope(FakeCursor(), "market.thing", [{"id": 1}], scope={}, conflict=["id"])


def test_a_numeric_looking_string_is_not_a_number() -> None:
    """`"1234"` casts cleanly in SQL and stores silently; `"n/a"` raises and, inside a migration
    applied --single-transaction, aborts the whole deploy."""
    assert numeric_or_none(12.5) == 12.5
    assert numeric_or_none(7) == 7.0
    assert numeric_or_none("1234") is None, "a string that looks numeric is the dangerous case"
    assert numeric_or_none("n/a") is None
    assert numeric_or_none(None) is None
    # `bool` is an `int` in Python, so True would otherwise store as 1.
    assert numeric_or_none(True) is None


def test_dedupe_keeps_the_last_and_says_how_many_it_dropped() -> None:
    rows = [{"k": 1, "n": "first"}, {"k": 2, "n": "other"}, {"k": 1, "n": "last"}]
    out, collapsed = dedupe_by(rows, lambda r: r["k"])
    assert [r["n"] for r in out] == ["last", "other"], "position is kept, content is the newest"
    assert collapsed == 1


def test_a_table_must_be_schema_qualified() -> None:
    """`market` and `api` and `ingest` are different schemas with different grants; an unqualified
    name resolves against search_path, which is not the same thing twice."""
    with pytest.raises(WriterError, match="schema-qualified"):
        upsert(FakeCursor(), "security_price", [{"k": 1}], conflict=["k"])
