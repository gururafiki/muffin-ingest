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
     `replace_scope`, which deletes that scope first.

  3. A NUMERIC-LOOKING STRING IS NOT A NUMBER. Promoting out of provider jsonb, `"1234"` casts
     cleanly and stores a wrong type in silence, while `"n/a"` RAISES — and inside a migration
     applied `--single-transaction` that aborts the whole deploy. `numeric_or_none` is the gate.

  4. MONEY WITHOUT ITS CURRENCY IS NOT A FIGURE. Alibaba's CNY 1,023,670,000,000 revenue rendered
     as "$1.02T" against a true ~$141B. Defaulting to dollars is how that started, so a money row
     that cannot state its currency is refused here rather than labelled later.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from muffin_ingest.ledger import DbCursor

#: Table and column names come from facet modules, never from provider data — but a typo that
#: reaches string interpolation is worth failing loudly rather than sending to the server.
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


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


def require_currency(rows: Iterable[Mapping[str, Any]], amount: str, currency: str) -> None:
    """Refuse money that cannot say what it is denominated in.

    Withholding the label is the correct rendering of an unknown currency and it looks like a bug;
    inferring one is how a CNY figure became "$1.02T". So the refusal is here, at the write, where
    the source is still visible — not at the page, where only a symbol is left to guess from.
    """
    for row in rows:
        if row.get(amount) is not None and not row.get(currency):
            raise WriterError(
                f"row states {amount}={row[amount]!r} with no {currency}; money must carry its "
                f"currency, and defaulting to USD is how Alibaba's revenue rendered as dollars"
            )


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
) -> WriteResult:
    """Insert many rows, updating on conflict — and dedupe on the conflict key first, always.

    `update=None` means DO NOTHING, which Postgres tolerates duplicates in; the dedupe still runs,
    because sending rows the server will discard is waste and the collapse count is worth seeing
    either way.

    THE CONFLICT TARGET MUST BE THE REAL KEY. A partial unique index is NOT covered by
    `on conflict (a, b)`: `unique … where is_primary` still throws and fails the whole statement,
    which is why moving such a flag takes two statements rather than an upsert.
    """
    if not rows:
        return WriteResult(written=0)
    if not conflict:
        raise WriterError("an upsert needs a conflict target; without one this is a plain insert")

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
        assignments = ", ".join(f"{_ident(c)} = excluded.{_ident(c)}" for c in setters)
        action = f"do update set {assignments}"

    sql = (
        f"insert into {target} ({', '.join(columns)}) "
        f"values {_values_sql(columns, len(deduped))} "
        f"on conflict ({', '.join(keys)}) {action}"
    )
    cur.execute(sql, _flatten(deduped, columns))
    return WriteResult(written=len(deduped), collapsed=collapsed)


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
    return WriteResult(written=result.written, collapsed=result.collapsed, retracted=1)
