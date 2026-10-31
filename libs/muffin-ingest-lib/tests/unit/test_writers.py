"""Four rules the writer enforces so a caller cannot forget them.

Every one cost something in production, and every fixture below is built so the WRONG
implementation is distinguishable rather than merely absent.
"""

from typing import Any

import pytest

from muffin_ingest import writers
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


def test_a_write_too_large_for_one_statement_is_split_rather_than_refused() -> None:
    """POSTGRES SENDS THE PARAMETER COUNT AS AN int16, so one statement carries at most 65,535 bind
    parameters. It is a PROTOCOL limit, not a tunable, and a multi-VALUES insert spends one per
    column per row — so the ceiling is a row count that moves with how wide the table is.

    Nothing here had met it: Lane A writes one trading day and its largest statement was a few
    thousand parameters. The first history backfill of 25 securities was ~178,000 rows x 8 columns
    = 1.4 M, and psycopg refused the WHOLE statement with

        number of parameters must be between 0 and 65535

    naming neither the table nor the row count — after the provider had been paid and the raw files
    were already on disk, for the third time in one afternoon.
    """
    columns = 8
    rows = [
        {
            "security_id": f"s{i}",
            "trade_date": "2026-09-10",
            **{f"c{c}": c for c in range(columns - 2)},
        }
        for i in range(20_000)
    ]
    cur = FakeCursor()
    result = upsert(
        cur, "market.price_bar", rows, conflict=["security_id", "trade_date"], update=["c0"]
    )

    assert result.written == len(rows), (
        "chunking must not change what the caller is told was written"
    )
    assert len(cur.calls) > 1, (
        "20,000 rows of 8 columns is 160,000 parameters; one statement cannot carry them"
    )
    for sql, params in cur.calls:
        assert len(params) <= writers.MAX_BIND_PARAMS, (
            f"a chunk carrying {len(params)} parameters is exactly the statement Postgres refuses"
        )
        assert sql.startswith("insert into market.price_bar"), "every chunk is the same statement"
        assert "on conflict (security_id, trade_date) do update" in sql

    sent = sum(len(p) for _, p in cur.calls)
    assert sent == len(rows) * columns, "every row reaches the database exactly once"


def test_chunking_does_not_move_the_dedupe_and_a_repeated_key_is_still_collapsed_once() -> None:
    """THE ORDER MATTERS: dedupe the whole set, THEN chunk.

    Splitting first would let one conflict key survive in two chunks, and the second statement would
    overwrite the first with whichever copy landed last — a different answer from `dedupe_by`'s
    (last wins, once, and it is COUNTED). The collapse count is the tell, and it is a statement
    about the SOURCE rather than a repair to be pleased about.
    """
    # THE TWO COPIES MUST LAND IN DIFFERENT CHUNKS OR THE FIXTURE CANNOT TELL THE RULES APART, and
    # the first version of this test could not: three columns gives 65,535 // 3 = 21,845 rows per
    # chunk, so 20,001 rows were ONE statement, per-chunk dedupe gave the same answer, and the
    # mutation passed clean. 25,000 puts the repeat in the SECOND chunk, where only a whole-set
    # dedupe can see the first.
    rows = [{"security_id": f"s{i}", "trade_date": "2026-09-10", "close": i} for i in range(25_000)]
    # A value no ordinary row carries — `close: i` means 999 is legitimately security s999's.
    rows.append({"security_id": "s0", "trade_date": "2026-09-10", "close": -1.5})

    cur = FakeCursor()
    result = upsert(
        cur, "market.price_bar", rows, conflict=["security_id", "trade_date"], update=["close"]
    )

    assert result.collapsed == 1, "the duplicate is collapsed, not sent twice in two chunks"
    assert result.written == 25_000
    closes = [p for _, params in cur.calls for p in params if p == -1.5]
    assert closes == [-1.5], "the LAST copy is the one that survives, exactly once"

    # THE ASSERTION THAT ACTUALLY SEPARATES THE TWO RULES, and without it this test was decorative.
    # Under a chunk-then-dedupe the repeat reaches the server in a SECOND statement, `do update`
    # quietly overwrites the first, and the final stored value is the same — so asserting on the
    # value alone certifies both rules. What differs is how many rows were SENT: 25,001 against
    # 25,000, which is one wasted round trip here and a collapse count of 0 reporting nothing about
    # a source that is repeating itself.
    sent = sum(len(params) for _, params in cur.calls) // 3
    assert sent == result.written, (
        f"{sent} rows sent for {result.written} written — a key deduped per chunk rather than "
        f"across the whole set is sent once per chunk it appears in"
    )


class CountingCursor(FakeCursor):
    """A cursor that reports a rowcount per statement, as psycopg does."""

    def __init__(self, counts: list[int]) -> None:
        super().__init__()
        self._counts = list(counts)
        self.rowcount = -1

    def execute(self, sql: str, params: Any = ()) -> None:
        super().execute(sql, params)
        self.rowcount = self._counts.pop(0)


def test_an_update_skips_a_row_whose_values_did_not_change() -> None:
    """`do update set c = excluded.c` writes a new row version even when nothing changed.

    The 2026-10-06 night upserted 11.6 M price bars for ~170 k changes, and `price_bar` had taken
    202.6 M updates against 59.5 M inserts in 26 days. The guard compares the SETTERS only (the
    conflict key is equal by construction), and with `is distinct from`, because `<>` against a
    NULL is NULL, so a value that became null or stopped being null would never be written.
    """
    cur = FakeCursor()
    rows = [{"security_id": "s1", "trade_date": "2026-10-05", "close": 1, "currency_code": None}]
    upsert(
        cur,
        "market.price_bar",
        rows,
        conflict=["security_id", "trade_date"],
        update=["close", "currency_code"],
    )
    sql = cur.calls[0][0]
    assert sql.startswith("insert into market.price_bar as stored_row ("), sql
    assert sql.endswith(
        "do update set close = excluded.close, currency_code = excluded.currency_code "
        "where (stored_row.close, stored_row.currency_code) is distinct from "
        "(excluded.close, excluded.currency_code)"
    ), sql
    assert "stored_row.security_id" not in sql, "the conflict key is equal by construction"


def test_compare_decides_the_update_and_every_setter_is_still_written() -> None:
    """An `as_of` compared like a value rewrites every unchanged statement on each company facts
    re-read, and the requeue trigger then re-derives them all. Left out of `compare`, an unchanged
    row is untouched, and a changed one still takes the new `as_of`."""
    cur = FakeCursor()
    rows = [{"k": 1, "data": "{}", "currency": "USD", "as_of": "2026-10-29"}]
    upsert(
        cur,
        "market.thing",
        rows,
        conflict=["k"],
        update=["data", "currency", "as_of"],
        compare=["data", "currency"],
    )
    assert cur.calls[0][0].endswith(
        "do update set data = excluded.data, currency = excluded.currency, as_of = excluded.as_of "
        "where (stored_row.data, stored_row.currency) is distinct from "
        "(excluded.data, excluded.currency)"
    ), cur.calls[0][0]


@pytest.mark.parametrize("compare", [[], ["k"], ["missing"]])
def test_compare_names_only_columns_being_updated(compare: list[str]) -> None:
    """An empty list would compare nothing (always true), and a column not updated, the key
    included, would decide an update by a value that cannot change."""
    with pytest.raises(WriterError, match="compare"):
        upsert(
            FakeCursor(),
            "market.thing",
            [{"k": 1, "v": "a", "missing": 1}],
            conflict=["k"],
            update=["v"],
            compare=compare,
        )


def test_do_nothing_carries_no_guard() -> None:
    """DO NOTHING has no update to guard; a `where` there is a syntax error."""
    cur = FakeCursor()
    upsert(cur, "market.thing", [{"k": 1, "v": "a"}], conflict=["k"])
    assert cur.calls[0][0].endswith("on conflict (k) do nothing")


def test_changed_is_what_the_driver_says_was_inserted_or_updated() -> None:
    """`written` counts rows SENT; `changed` counts rows the database wrote, summed per chunk.

    The gap between the two is the no-op work the guard saves, and on a night that re-publishes
    whole files it is most of the rows.
    """
    rows = [{"security_id": f"s{i}", "trade_date": "2026-10-05", "close": i} for i in range(25_000)]
    cur = CountingCursor([7, 3])
    result = upsert(
        cur, "market.price_bar", rows, conflict=["security_id", "trade_date"], update=["close"]
    )
    assert len(cur.calls) == 2, "25,000 rows of 3 columns is two statements"
    assert (result.written, result.changed) == (25_000, 10)


def test_a_cursor_that_cannot_count_leaves_changed_unknown() -> None:
    """A partial sum would read as a real one, so one unknown chunk makes the count unknown."""
    rows = [{"security_id": f"s{i}", "trade_date": "2026-10-05", "close": i} for i in range(25_000)]
    result = upsert(
        CountingCursor([7, -1]),
        "market.price_bar",
        rows,
        conflict=["security_id", "trade_date"],
        update=["close"],
    )
    assert result.changed is None
    assert upsert(FakeCursor(), "market.thing", [{"k": 1}], conflict=["k"]).changed is None


# --- options for a table two writers share -------------------------------------------------------

STATEMENT = {
    "security_id": "s",
    "statement": "income",
    "period_ending": "2025-12-31",
    "period_type": "annual",
    "data": '{"ebit": 1}',
    "source_code": "yfinance",
}
STATEMENT_KEY = ["security_id", "statement", "period_ending", "period_type"]


def test_only_replacing_updates_a_conflicting_row_only_if_it_is_the_writer_s_own() -> None:
    """A period SEC holds keeps SEC's row: the key leaves the source out on purpose, so the guard
    lives in the update, and its value is a parameter rather than SQL text."""
    cur = FakeCursor()
    writers.upsert(
        cur,
        "market.security_statement",
        [STATEMENT],
        conflict=STATEMENT_KEY,
        update=["data", "source_code"],
        only_replacing={"source_code": "yfinance"},
    )
    sql, params = cur.calls[0]
    assert sql.endswith(
        "is distinct from (excluded.data, excluded.source_code) and stored_row.source_code = %s"
    )
    assert params[-1] == "yfinance" and "yfinance" not in sql
    with pytest.raises(writers.WriterError):
        writers.upsert(
            cur, "market.x", [STATEMENT], conflict=STATEMENT_KEY, only_replacing={"a": 1}
        )


def test_only_replacing_with_a_list_replaces_any_named_source_and_no_other() -> None:
    """A retired source's rows are superseded by the writer that replaces it (Tiingo by Yahoo,
    2026-10-25), and every other source's rows still stay. The list is ONE array parameter."""
    cur = FakeCursor()
    writers.upsert(
        cur,
        "market.security_corporate_action",
        [STATEMENT],
        conflict=STATEMENT_KEY,
        update=["data", "source_code"],
        only_replacing={"source_code": ["yfinance", "tiingo"]},
    )
    sql, params = cur.calls[0]
    assert sql.endswith("and stored_row.source_code = any(%s)")
    assert params[-1] == ["yfinance", "tiingo"] and "tiingo" not in sql


def test_merge_json_lays_the_offered_values_over_the_stored_object() -> None:
    """Yahoo's statement lines roll per line: a narrower answer must not erase a stored figure."""
    cur = FakeCursor()
    writers.upsert(
        cur,
        "market.security_statement",
        [STATEMENT],
        conflict=STATEMENT_KEY,
        update=["data", "source_code"],
        merge_json=["data"],
    )
    sql, _ = cur.calls[0]
    merged = "coalesce(stored_row.data, '{}'::jsonb) || jsonb_strip_nulls(excluded.data)"
    assert f"set data = {merged}" in sql
    assert f"is distinct from ({merged}, excluded.source_code)" in sql, "compared as written"
    with pytest.raises(writers.WriterError):
        writers.upsert(cur, "market.x", [STATEMENT], conflict=STATEMENT_KEY, merge_json=["data"])
    with pytest.raises(writers.WriterError):
        writers.upsert(
            cur,
            "market.x",
            [STATEMENT],
            conflict=STATEMENT_KEY,
            update=["source_code"],
            merge_json=["data"],
        )


def test_update_existing_never_inserts_never_erases_and_casts_every_value() -> None:
    cur = FakeCursor()
    result = writers.update_existing(
        cur,
        "market.security",
        [
            {"security_id": "a", "reporting_currency": "EUR", "provider_country_iso2": None},
            {"security_id": "b", "reporting_currency": None, "provider_country_iso2": None},
            {"security_id": "a", "reporting_currency": "USD", "provider_country_iso2": "GB"},
        ],
        key="security_id",
        types={
            "security_id": "uuid",
            "reporting_currency": "text",
            "provider_country_iso2": "text",
        },
    )
    assert (result.written, result.collapsed) == (1, 1), "b states nothing; a's later row wins"
    sql, params = cur.calls[0]
    assert sql.startswith(
        "update market.security as stored_row set provider_country_iso2 = coalesce("
    )
    assert "(values (%s::uuid, %s::text, %s::text)) as v" in sql
    assert "as v (security_id, provider_country_iso2, reporting_currency)" in sql
    assert "insert" not in sql
    assert params == ["a", "GB", "USD"]
    with pytest.raises(writers.WriterError):
        writers.update_existing(
            cur, "market.security", [{"security_id": "a", "x": 1}], key="security_id", types={}
        )
    with pytest.raises(writers.WriterError):
        writers.update_existing(
            cur,
            "market.security",
            [{"security_id": "a", "x": 1}],
            key="security_id",
            types={"security_id": "uuid", "x": "text; drop table market.security"},
        )


KEY_TYPES = {"statement": "text", "period_ending": "date", "period_type": "text"}


def test_retract_unproduced_deletes_only_the_scope_s_rows_the_answer_no_longer_produces() -> None:
    cur = FakeCursor()
    produced = [
        {"statement": "income", "period_ending": "2025-09-27", "period_type": "annual", "v": 1},
        {"statement": "balance", "period_ending": "2025-09-27", "period_type": "quarter", "v": 2},
    ]
    writers.retract_unproduced(
        cur,
        "market.security_statement",
        produced,
        scope={"security_id": "s1", "source_code": "sec-xbrl"},
        key=["statement", "period_ending", "period_type"],
        types=KEY_TYPES,
    )
    [(sql, params)] = cur.calls
    assert sql == (
        "delete from market.security_statement as stored_row where stored_row.security_id = %s"
        " and stored_row.source_code = %s and not exists (select 1 from unnest(%s::text[],"
        " %s::date[], %s::text[]) as p (statement, period_ending, period_type) where"
        " stored_row.statement = p.statement and stored_row.period_ending = p.period_ending"
        " and stored_row.period_type = p.period_type)"
    )
    assert params == [
        "s1",
        "sec-xbrl",
        ["income", "balance"],
        ["2025-09-27", "2025-09-27"],
        ["annual", "quarter"],
    ]


def test_retract_unproduced_is_one_statement_however_many_rows_are_produced() -> None:
    """A NOT EXISTS split across statements would delete each chunk's complement: everything."""
    cur = FakeCursor()
    produced = [
        {"statement": "income", "period_ending": f"d{i}", "period_type": "quarter"}
        for i in range(writers.MAX_BIND_PARAMS)
    ]
    writers.retract_unproduced(
        cur,
        "market.security_statement",
        produced,
        scope={"security_id": "s1"},
        key=["statement", "period_ending", "period_type"],
        types=KEY_TYPES,
    )
    assert len(cur.calls) == 1
    assert len(cur.calls[0][1]) == 4


def test_retract_unproduced_with_nothing_produced_empties_the_scope_and_only_it() -> None:
    cur = FakeCursor()
    writers.retract_unproduced(
        cur,
        "market.security_statement",
        [],
        scope={"security_id": "s1"},
        key=["statement"],
        types=KEY_TYPES,
    )
    [(sql, params)] = cur.calls
    assert "stored_row.security_id = %s" in sql
    assert params == ["s1", []]


def test_retract_unproduced_refuses_what_could_delete_more_than_it_means() -> None:
    def retract(**kwargs: Any) -> None:
        args: dict[str, Any] = {
            "scope": {"security_id": "s1"},
            "key": ["statement"],
            "types": KEY_TYPES,
        }
        args.update(kwargs)
        writers.retract_unproduced(
            FakeCursor(), "market.security_statement", kwargs.pop("rows", []), **args
        )

    with pytest.raises(WriterError, match="scope"):
        retract(scope={})
    with pytest.raises(WriterError, match="key"):
        retract(key=[])
    with pytest.raises(WriterError, match="type"):
        retract(key=["statement", "fiscal_year"])
    # A produced row missing a key column would match nothing, so its stored row would be deleted.
    with pytest.raises(WriterError, match="lacks key"):
        writers.retract_unproduced(
            FakeCursor(),
            "market.security_statement",
            [{"statement": None}],
            scope={"security_id": "s1"},
            key=["statement"],
            types=KEY_TYPES,
        )


def test_retract_unproduced_reports_the_rows_the_driver_deleted() -> None:
    cur = CountingCursor([3])
    result = writers.retract_unproduced(
        cur, "market.t", [], scope={"security_id": "s1"}, key=["statement"], types=KEY_TYPES
    )
    assert result.retracted == 3
    assert result.written == 0


def test_nothing_sent_is_nothing_changed() -> None:
    """An empty upsert executes no statement, so the count is a known zero, not unknown.

    Unknown made an empty Form 4 day read `inserted=-1`, and the I/O manager, which sums `changed`
    across a run's partitions, turned a whole run's count unknown for one empty partition.
    """
    cur = CountingCursor([])
    result = upsert(cur, "market.thing", [], conflict=["k"], update=["v"])
    assert (result.written, result.changed) == (0, 0)
    assert cur.calls == [], "nothing is sent"
