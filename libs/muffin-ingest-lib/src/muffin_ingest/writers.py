"""Getting provider rows into `market` without the four ways this pipeline has got it wrong.

Each rule here cost something, and each is enforced by the writer rather than remembered by the
caller — because "a rule written at one call site is not a rule" is the most repeated correction
in this codebase, and the reason `fetchWithIsolation`'s outage rule lived in a comment on one
handler while three other handlers negative-cached thousands of securities without it.

  1. THE SAME CONFLICT KEY TWICE FAILS THE WHOLE STATEMENT. Postgres rejects an
     `INSERT … ON CONFLICT DO UPDATE` whose statement carries a key twice — SQLSTATE 21000,
     `ON CONFLICT DO UPDATE command cannot affect row a second time` — and the batch, the page and
     the resource go with it. It has happened four times here (fund-holding lots, the sector views,
     `security-industries`, `pending_industry` yielding a row per (security, sector)), and the
     symptom is a bare 502 that looks exactly like a size or timeout problem: a page of 10 and 20
     succeeded while 40 and 300 did not, because only a large page was likely to contain a
     duplicate. So `upsert` dedupes on its OWN conflict key and reports how many it collapsed. A
     caller cannot forget, and a rising collapse count is a signal about the source rather than a
     silent repair.

  2. AN UPSERT CANNOT RETRACT. A period a run stops producing keeps whatever was written last time,
     for ever, looking freshly written. Securities served `1d = 0.00%` for four days that way,
     because the guard that stopped PRODUCING a number could never REMOVE the stale one. Anything
     whose source restates a whole scope — a filing's segments, a symbol's periods — writes with
     `replace_scope`, which deletes that scope first, or upserts and then calls
     `retract_unproduced`, which deletes only what the answer no longer produces.

  3. A NUMERIC-LOOKING STRING IS NOT A NUMBER. Promoting out of provider jsonb, `"1234"` casts
     cleanly and stores a wrong type in silence, while `"n/a"` RAISES — and inside a migration
     applied `--single-transaction` that aborts the whole deploy. `numeric_or_none` is the gate.

  4. MONEY WITHOUT ITS CURRENCY IS NOT A FIGURE — AND THAT RULE BELONGS IN THE DDL, NOT HERE.
     Alibaba's CNY 1,023,670,000,000 revenue rendered as "$1.02T" against a true ~$141B, and the
     bug was INFERRING a currency rather than lacking one. This module used to carry a
     `require_currency` helper that refused any money row without one; wiring it into `upsert` was
     tried and REVERTED, because `market.price_bar.currency_code` is deliberately nullable — 425 of
     10,894 askable equities have no currency from any source, and refusing them a price row is
     worse than the unlabelled number the app already renders correctly. A blanket rule here would
     have silently dropped exactly those securities.

     `not null` on the column is the enforcement that cannot be forgotten and cannot be wrong about
     which tables mean it. The serving layer withholds a label it cannot justify; that is where the
     Alibaba fix actually lives.

  5. AN UPDATE THAT CHANGES NOTHING STILL WRITES. `do update set c = excluded.c` writes a new row
     version, its index entries and the WAL for both, even when every value is identical. Stage 2
     of the price lane publishes each partition from its whole raw file, so the 2026-10-06 night
     upserted 11.6 M bars for ~170 k changes, and `price_bar` had taken 202.6 M updates (8.0 M of
     them HOT) against 59.5 M inserts in 26 days. So `upsert` updates only a row whose values
     differ, and reports `changed` beside `written`, because `written` counts rows SENT.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Protocol


# WHAT A WRITER ACTUALLY NEEDS FROM A DATABASE, as a protocol rather than a driver import.
#
# Two things follow that are worth more than the tidiness. A writer can be driven in a test with a
# fake that records the SQL, so "which statement does an empty answer reach" is a unit test rather
# than something reviewed by eye — and it is exactly the question that has cost this pipeline the
# most. And nothing in `muffin_ingest` imports psycopg, so a driver change is one line in the
# caller. (These lived in the ingest ledger until it was retired on 2026-10-04.)
class DbCursor(Protocol):
    def __enter__(self) -> DbCursor: ...

    # THE STANDARD THREE-ARGUMENT FORM, not `*exc: object`. A protocol method declared with `*args`
    # promises to accept ANY call, so a concrete `__exit__` taking exactly three positionals is
    # NARROWER and does not satisfy it — psycopg's real cursor was rejected by the very protocol
    # written to describe it, the first time one was passed in. The fakes in the tests use `*exc`
    # and satisfy this form too, because an implementation may always be more permissive than the
    # protocol it fulfils.
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
        /,
    ) -> None: ...
    # POSITIONAL-ONLY, AND RETURNING `object`. Both were written from imagination rather than
    # from a driver, and neither survived the first real cursor: psycopg names the first parameter
    # `query` (so a protocol naming it `sql` cannot be satisfied by keyword) and RETURNS the cursor
    # (so a protocol demanding `-> None` cannot be satisfied at all). The fakes in the tests use
    # ordinary named parameters and satisfy this form too, because an implementation may always be
    # more permissive than the protocol it fulfils.
    def execute(self, sql: str, params: Sequence[Any] = (), /) -> object: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...


class DbConn(Protocol):
    def cursor(self) -> DbCursor: ...
    def commit(self) -> None: ...


#: Table and column names come from facet modules, never from provider data — but a typo that
#: reaches string interpolation is worth failing loudly rather than sending to the server.
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")

#: The alias an upsert gives its target, so the update can compare the STORED row with the one
#: offered (`excluded`). Not a column name anywhere in `market` or `ingest`, and not a keyword.
_STORED = "stored_row"


class WriterError(Exception):
    """A row or a target this writer refuses to send to the database."""


def _ident(name: str) -> str:
    if not _IDENT.match(name):
        raise WriterError(f"{name!r} is not a plain lowercase identifier")
    return name


def _qualified(table: str) -> str:
    parts = table.split(".")
    if len(parts) != 2:
        raise WriterError(f"table must be schema-qualified, got {table!r}")
    return ".".join(_ident(p) for p in parts)


@dataclass(frozen=True)
class WriteResult:
    """What a write did, including the part a caller would otherwise never see."""

    written: int
    #: Rows dropped because a later row carried the same conflict key. Zero is the normal case;
    #: a number that grows is a statement about the SOURCE — a backlog view yielding one row per
    #: (security, sector) rather than per security, say — not a repair to be pleased about.
    collapsed: int = 0
    retracted: int = 0
    #: Rows the statements actually inserted or updated, from the driver's rowcount. `written`
    #: counts rows SENT, and a row that arrives identical to the stored one is skipped, so the gap
    #: between the two is the no-op work this writer no longer does. None when the cursor cannot
    #: say, as a test's fake cannot.
    changed: int | None = None


def dedupe_by(
    rows: Sequence[Mapping[str, Any]], key: Callable[[Mapping[str, Any]], Hashable]
) -> tuple[list[Mapping[str, Any]], int]:
    """Collapse rows sharing a key, KEEPING THE LAST.

    Last, not first, because these arrive in provider order and a later row is the newer statement
    — a restated period supersedes the original, and a filing's second mention of a member is the
    one it means. Returns the count collapsed so the caller can report it; a silent collapse is how
    a source defect becomes invisible.
    """
    seen: dict[Hashable, int] = {}
    out: list[Mapping[str, Any]] = []
    collapsed = 0
    for row in rows:
        k = key(row)
        if k in seen:
            out[seen[k]] = row
            collapsed += 1
        else:
            seen[k] = len(out)
            out.append(row)
    return out, collapsed


def numeric_or_none(value: Any) -> float | None:
    """A number, or nothing — never a plausible-looking wrong type.

    `"1234"` casts cleanly in SQL and stores silently; `"n/a"` raises and, inside a migration,
    aborts the deploy. Booleans are refused explicitly because `bool` is an `int` in Python and
    `True` would otherwise store as 1.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


#: Postgres' extended-query protocol sends the parameter count as an int16, so a single statement
#: may carry at most 65,535 bind parameters — a PROTOCOL limit, not a tunable. A multi-VALUES insert
#: spends one per column per row, so the ceiling is a ROW count that depends on how wide the table
#: is, which is why nothing in this pipeline had met it: Lane A writes one day at a time and its
#: largest statement was a few thousand parameters. A history backfill of 25 securities is ~178,000
#: rows x 8 columns = 1.4 M, and psycopg refuses the whole statement with
#:
#:     number of parameters must be between 0 and 65535
#:
#: naming neither the table nor the row count. Chunking belongs HERE and not at any call site: every
#: facet writes through this function, and a rule written at one call site is not a rule.
MAX_BIND_PARAMS = 65535


def _values_sql(columns: Sequence[str], count: int) -> str:
    one = "(" + ", ".join(["%s"] * len(columns)) + ")"
    return ", ".join([one] * count)


def _flatten(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> list[Any]:
    return [row.get(c) for row in rows for c in columns]


def upsert(
    cur: DbCursor,
    table: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    conflict: Sequence[str],
    update: Sequence[str] | None = None,
    only_replacing: Mapping[str, Any] | None = None,
    merge_json: Sequence[str] = (),
    compare: Sequence[str] | None = None,
) -> WriteResult:
    """Insert many rows, updating on conflict — and dedupe on the conflict key first, always.

    `update=None` means DO NOTHING, which Postgres tolerates duplicates in; the dedupe still runs,
    because sending rows the server will discard is waste and the collapse count is worth seeing
    either way.

    THE CONFLICT TARGET MUST BE THE REAL KEY. A partial unique index is NOT covered by
    `on conflict (a, b)`: `unique … where is_primary` still throws and fails the whole statement,
    which is why moving such a flag takes two statements rather than an upsert.

    `only_replacing` LIMITS WHICH STORED ROWS AN UPDATE MAY TOUCH, for a table two writers share a
    key in. `{"source_code": "yfinance"}` updates a conflicting row only if it was Yahoo's: a
    period SEC holds keeps SEC's row, and the offered row is simply not written. The key leaves
    the source out on purpose (one row per period, whoever stated it), so the guard has to live in
    the update rather than in the key. A row it declines is in `written` and not in `changed`.
    A LIST names every source the writer may replace (`["yfinance", "tiingo"]`): a retired source
    whose rows this writer supersedes, while any other source's rows stay untouched.

    `merge_json` NAMES jsonb COLUMNS AN UPDATE MERGES INTO RATHER THAN REPLACES: the stored object,
    with the offered one's NON-NULL values laid over it. For a provider whose window rolls per
    field, where a later answer covering the same row with fewer fields is narrower rather than a
    correction (Yahoo's statement lines, measured 2026-10-11: Samsung's 2025-03-31 quarter keeps
    ten of the ~180 lines the edge stored in August). A value the provider restates still wins.

    `compare` NAMES THE COLUMNS WHOSE DIFFERENCE DECIDES WHETHER A STORED ROW IS UPDATED (default:
    every updated column). For a column that records WHEN rather than WHAT. SEC's company facts are
    a filer's whole history, re-read whenever it files, so an `as_of` compared like a value would
    rewrite every unchanged statement on every re-read, and `security_statement`'s trigger re-queues
    a row for the metric derivation whenever its `as_of` moves. Left out of `compare`, the row keeps
    the `as_of` of the read that last changed it, and still takes the offered one when a compared
    column changes.
    """
    if not rows:
        return WriteResult(written=0)
    if not conflict:
        raise WriterError("an upsert needs a conflict target; without one this is a plain insert")
    if only_replacing is not None and (update is None or not only_replacing):
        raise WriterError("only_replacing guards an update; it needs `update` and a column")
    if merge_json and update is None:
        raise WriterError("merge_json merges on update; with DO NOTHING there is nothing to merge")
    if compare is not None and update is None:
        raise WriterError("compare guards an update; with DO NOTHING there is nothing to compare")

    target = _qualified(table)
    keys = [_ident(c) for c in conflict]
    deduped, collapsed = dedupe_by(rows, lambda r: tuple(r.get(k) for k in keys))

    columns = sorted({c for row in deduped for c in row})
    for c in columns:
        _ident(c)
    missing = [k for k in keys if k not in columns]
    if missing:
        raise WriterError(f"conflict column(s) {missing} are not present in the rows")

    if update is None:
        action = "do nothing"
    else:
        setters = [c for c in update if c not in keys]
        if not setters:
            raise WriterError(
                "every updatable column is part of the conflict key, so this can only DO NOTHING"
            )
        merged = {_ident(c) for c in merge_json}
        unknown = merged - set(setters)
        if unknown:
            raise WriterError(f"merge_json column(s) {sorted(unknown)} are not being updated")
        compared = list(setters) if compare is None else [_ident(c) for c in compare]
        if not compared or set(compared) - set(setters):
            raise WriterError(
                f"compare must name at least one updated column, and only those: {compare!r}"
            )

        def offered_value(c: str) -> str:
            if c in merged:
                return f"coalesce({_STORED}.{c}, '{{}}'::jsonb) || jsonb_strip_nulls(excluded.{c})"
            return f"excluded.{c}"

        assignments = ", ".join(f"{_ident(c)} = {offered_value(c)}" for c in setters)
        # ONLY A ROW WHOSE VALUES DIFFER IS UPDATED (rule 5). `is distinct from` rather than `<>`,
        # because a NULL on either side makes `<>` NULL, and NULL is not true: a value that became
        # null, or stopped being null, would never be written. The conflict key is left out; it
        # is equal by construction.
        stored = ", ".join(f"{_STORED}.{_ident(c)}" for c in compared)
        offered = ", ".join(offered_value(c) for c in compared)
        action = f"do update set {assignments} where ({stored}) is distinct from ({offered})"
        # The guard's VALUES are parameters like any other, appended after the rows' own: a
        # source code is data, never SQL text.
        for column, value in (only_replacing or {}).items():
            many = isinstance(value, (list, tuple))
            action += f" and {_STORED}.{_ident(column)} = {'any(%s)' if many else '%s'}"
    guard_params = [
        list(v) if isinstance(v, (list, tuple)) else v for v in (only_replacing or {}).values()
    ]

    # CHUNKED, AND THE DEDUPE STAYS WHOLE-SET. Splitting first and deduping per chunk would let one
    # conflict key survive in two chunks — the second statement would then silently overwrite the
    # first with whichever copy happened to land last, which is a different answer from the one
    # `dedupe_by` gives (last wins, once, and it is COUNTED). Deduping first also means the chunks
    # hold distinct keys, so they cannot collide with each other.
    per_chunk = max(1, (MAX_BIND_PARAMS - len(guard_params)) // max(1, len(columns)))
    changed: int | None = 0
    for start in range(0, len(deduped), per_chunk):
        chunk = deduped[start : start + per_chunk]
        sql = (
            f"insert into {target} as {_STORED} ({', '.join(columns)}) "
            f"values {_values_sql(columns, len(chunk))} "
            f"on conflict ({', '.join(keys)}) {action}"
        )
        cur.execute(sql, _flatten(chunk, columns) + guard_params)
        # INSERTED PLUS UPDATED, which is what psycopg reports for `insert … on conflict`. A
        # cursor that cannot say (-1, or no attribute at all) makes the whole count unknown rather
        # than a partial sum that would read as a real one.
        count = getattr(cur, "rowcount", None)
        if changed is not None and isinstance(count, int) and count >= 0:
            changed += count
        else:
            changed = None
    return WriteResult(written=len(deduped), collapsed=collapsed, changed=changed)


def replace_scope(
    cur: DbCursor,
    table: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    scope: Mapping[str, Any],
    conflict: Sequence[str],
) -> WriteResult:
    """Delete a scope and write what the source now says it contains.

    For anything whose source restates a WHOLE scope — a filing's segment facts, a symbol's return
    periods. An upsert alone leaves behind whatever the source has stopped producing, which then
    outlives the fix that stopped producing it: a parser rule that DEMOTES a member self-heals,
    because the member is still emitted and overwrites its own row, while a rule that DROPS one
    leaves the last row written in a served partition for ever, looking fresh.

    The scope is deleted even when there is nothing to write, because a filing that now discloses
    no segments must withdraw what it used to. The caller is responsible for not calling this on a
    THROW — retracting on an outage empties the universe.
    """
    if not scope:
        raise WriterError("replace_scope needs a scope; with none this would delete the table")

    target = _qualified(table)
    where = " and ".join(f"{_ident(c)} = %s" for c in scope)
    cur.execute(f"delete from {target} where {where}", list(scope.values()))

    every_column = sorted({c for r in rows for c in r})
    result = upsert(cur, table, rows, conflict=conflict, update=every_column)
    return WriteResult(
        written=result.written, collapsed=result.collapsed, retracted=1, changed=result.changed
    )


#: A SQL type named in a cast. Plain names only (`uuid`, `text`, `numeric`, `date`): this is
#: interpolated, so it is held to the same rule as an identifier.
_TYPE = re.compile(r"^[a-z][a-z0-9_]*$")


def retract_unproduced(
    cur: DbCursor,
    table: str,
    produced: Sequence[Mapping[str, Any]],
    *,
    scope: Mapping[str, Any],
    key: Sequence[str],
    types: Mapping[str, str],
) -> WriteResult:
    """Within a scope, delete the rows whose key the newest whole answer no longer produces.

    `replace_scope`'s retraction without its rewrite. For a source re-read often and restated
    rarely: SEC's company facts are a filer's whole history, re-read whenever it files, and
    deleting and rewriting a few thousand statement rows to change three would cost the dead
    tuples, the WAL and the derivation re-queue that rule 5 exists to avoid. The caller upserts
    what the answer produces (changes only) and calls this for what it no longer does. Called only
    after an answered and parsed document; never on a throw, which would retract the universe.

    THE KEYS TRAVEL AS ONE TYPED ARRAY PER COLUMN (`unnest(%s::date[], …)`), so a filer with tens
    of thousands of rows is still one statement of `len(key)` parameters. A `values` list would hit
    the bind-parameter limit, and splitting a NOT EXISTS across statements would delete each
    chunk's complement: everything.

    An empty `produced` deletes the whole scope: the answer says there is nothing there.
    `retracted` is the number of rows deleted, or 0 when the cursor cannot say.
    """
    if not scope:
        raise WriterError("retract_unproduced needs a scope; with none it would empty the table")
    if not key:
        raise WriterError("retract_unproduced needs the key a produced row is matched on")
    target = _qualified(table)
    for c in [*scope, *key]:
        _ident(c)
    for c in key:
        if c not in types or not _TYPE.match(types[c]):
            raise WriterError(f"key column {c!r} needs a plain SQL type in `types`")
    for row in produced:
        missing = [c for c in key if row.get(c) is None]
        if missing:
            raise WriterError(f"a produced row lacks key column(s) {missing}: {dict(row)!r}")

    where = " and ".join(f"{_STORED}.{c} = %s" for c in scope)
    arrays = ", ".join(f"%s::{types[c]}[]" for c in key)
    matched = " and ".join(f"{_STORED}.{c} = p.{c}" for c in key)
    sql = (
        f"delete from {target} as {_STORED} where {where} and not exists ("
        f"select 1 from unnest({arrays}) as p ({', '.join(key)}) where {matched})"
    )
    cur.execute(sql, [*scope.values(), *([row[c] for row in produced] for c in key)])
    count = getattr(cur, "rowcount", None)
    return WriteResult(written=0, retracted=count if isinstance(count, int) and count > 0 else 0)


def update_existing(
    cur: DbCursor,
    table: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    key: str,
    types: Mapping[str, str],
) -> WriteResult:
    """Set columns on rows that already exist. Never inserts, and never erases.

    For facts one lane observes about an entity another lane creates: a company's reporting
    currency and operating country belong on `market.security`, which discovery owns. An upsert
    would mint a security from a partial row; this touches only rows that are there.

    NONE MEANS "NOT STATED", NEVER "ERASE". Each column is set to `coalesce(offered, stored)`, so a
    document that omits a company's country leaves the country the company already has. A lane that
    must retract a value says so with a different tool (`replace_scope`), not with a null here.

    ONLY A ROW WHOSE VALUES DIFFER IS WRITTEN (rule 5), so `changed` counts real changes.

    EVERY VALUE IS CAST, to `types[column]`. A VALUES list in a FROM clause types an untyped
    parameter as `text`, and `uuid = text` has no operator: without the casts the statement fails
    on the key comparison before it reaches a row.
    """
    if not rows:
        return WriteResult(written=0)
    target = _qualified(table)
    _ident(key)
    columns = sorted({c for row in rows for c in row} - {key})
    if not columns:
        raise WriterError("an update needs at least one column besides the key")
    for c in [key, *columns]:
        _ident(c)
        if c not in types or not _TYPE.match(types[c]):
            raise WriterError(f"column {c!r} needs a plain SQL type in `types`")
    # A row stating nothing would be a no-op on the server; it is dropped here so `written` counts
    # rows that carried a fact.
    stating = [
        r for r in rows if r.get(key) is not None and any(r.get(c) is not None for c in columns)
    ]
    deduped, collapsed = dedupe_by(stating, lambda r: r.get(key))
    if not deduped:
        return WriteResult(written=0, collapsed=collapsed)

    ordered = [key, *columns]
    one = "(" + ", ".join(f"%s::{types[c]}" for c in ordered) + ")"
    assignments = ", ".join(f"{c} = coalesce(v.{c}, {_STORED}.{c})" for c in columns)
    stored = ", ".join(f"{_STORED}.{c}" for c in columns)
    offered = ", ".join(f"coalesce(v.{c}, {_STORED}.{c})" for c in columns)
    per_chunk = max(1, MAX_BIND_PARAMS // len(ordered))
    changed: int | None = 0
    for start in range(0, len(deduped), per_chunk):
        chunk = deduped[start : start + per_chunk]
        sql = (
            f"update {target} as {_STORED} set {assignments} "
            f"from (values {', '.join([one] * len(chunk))}) as v ({', '.join(ordered)}) "
            f"where {_STORED}.{key} = v.{key} and ({stored}) is distinct from ({offered})"
        )
        cur.execute(sql, _flatten(chunk, ordered))
        count = getattr(cur, "rowcount", None)
        if changed is not None and isinstance(count, int) and count >= 0:
            changed += count
        else:
            changed = None
    return WriteResult(written=len(deduped), collapsed=collapsed, changed=changed)
