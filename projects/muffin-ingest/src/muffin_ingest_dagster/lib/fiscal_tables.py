"""Writing a security's fiscal calendar: replaced whole, and only when it changed.

TWO LANES WRITE CALENDARS (the SEC filer lane, and the Yahoo lane for companies SEC has none for),
so the rule lives here once.

REPLACED WHOLE, NOT UPSERTED. A calendar is one answer about a company: a regime can disappear (a
transition re-read as an ordinary year) and a period can change which regime is canonical. An upsert
cannot retract, and flipping `is_canonical` row by row can collide with the partial unique index on
canonical labels part-way through. Deleting the security's calendar cascades to its periods.

ONLY WHEN IT CHANGED. The Yahoo lane re-derives every visited company's calendar, ~1,800 a day, and
nearly all are the same as yesterday's. Rewriting them would cost the dead tuples and WAL of
~100,000 rows a day to say nothing new, which is the upsert rule (rule 5) at the scale of a table.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from muffin_ingest.writers import WriteResult, replace_scope, upsert

#: The columns that make a calendar what it is. `as_of` is when it was written, not what it says.
CALENDAR_COLUMNS = (
    "regime",
    "source_code",
    "year_end_month",
    "year_end_day",
    "is_52_53_week",
    "week_end_isodow",
    "first_year_end",
    "last_year_end",
    "change_kind",
)
PERIOD_COLUMNS = (
    "regime",
    "fiscal_year",
    "fiscal_period",
    "period_start",
    "period_end",
    "is_canonical",
    "is_observed",
    "is_recast",
    "source_code",
)

STORED_CALENDARS = (
    f"select security_id::text, {', '.join(CALENDAR_COLUMNS)} from market.fiscal_calendar "
    "where security_id = any(%s::uuid[])"
)
STORED_PERIODS = (
    f"select security_id::text, {', '.join(PERIOD_COLUMNS)} from market.fiscal_period "
    "where security_id = any(%s::uuid[])"
)

Rows = Sequence[Mapping[str, Any]]


def _shape(values: Sequence[Any]) -> tuple[str | None, ...]:
    """A row as text, so a stored `date` and an offered ISO string compare equal."""
    return tuple(None if v is None else str(v) for v in values)


def _stored(cur: Any, sql: str, sids: Sequence[str]) -> dict[str, set[tuple[str | None, ...]]]:
    cur.execute(sql, (list(sids),))
    out: dict[str, set[tuple[str | None, ...]]] = {sid: set() for sid in sids}
    for row in cur.fetchall():
        out.setdefault(str(row[0]), set()).add(_shape(row[1:]))
    return out


def replace_calendars(cur: Any, wanted: Mapping[str, tuple[Rows, Rows]]) -> Counter[str]:
    """Write each security's calendar and periods (`fiscal.rows`), unless the stored one is equal.

    `wanted` maps a security to its calendar rows and period rows. Returns `calendar_replaced`,
    `calendar_unchanged` and the period rows written.
    """
    stats: Counter[str] = Counter()
    if not wanted:
        return stats
    sids = sorted(wanted)
    calendars_held = _stored(cur, STORED_CALENDARS, sids)
    periods_held = _stored(cur, STORED_PERIODS, sids)
    for sid in sids:
        calendars, periods = wanted[sid]
        same = calendars_held[sid] == {
            _shape([c[k] for k in CALENDAR_COLUMNS]) for c in calendars
        } and periods_held[sid] == {_shape([p[k] for k in PERIOD_COLUMNS]) for p in periods}
        if same:
            stats["calendar_unchanged"] += 1
            continue
        replace_scope(
            cur,
            "market.fiscal_calendar",
            calendars,
            scope={"security_id": sid},
            conflict=["security_id", "regime"],
        )
        written: WriteResult = upsert(
            cur,
            "market.fiscal_period",
            periods,
            conflict=["security_id", "regime", "fiscal_year", "fiscal_period"],
            update=[
                c for c in PERIOD_COLUMNS if c not in ("regime", "fiscal_year", "fiscal_period")
            ]
            + ["as_of"],
        )
        stats["calendar_replaced"] += 1
        stats["calendar_period_rows"] += written.written
    return stats
