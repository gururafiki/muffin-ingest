"""What a directory query's stored walk says about itself — read from its raw file, nowhere else.

A walk is the pages one query's partition holds: the current one, finished or under way
(`raw_exchange_sweep`). Three readers ask it the same questions — the finished check, the cap check
and the absence mark — and must agree, so the answers are computed here once. Two copies of the cap
rule would drift, and the mark acting on a looser one than the check reports would mark lines past
a window the check calls covered.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from muffin_ingest_dagster.defs.discovery.raw import raw_exchange_sweep
from muffin_ingest_dagster.lib.io_managers import RawStore

#: How far a walk that ENDED may fall short of the provider's own `total` and still count as
#: finished. `total` is re-counted on every page, so a venue that gains or loses a listing during a
#: walk ends a few rows off: measured 2026-09-27, GR held 14,202 against 14,205. The cap it exists
#: to catch is thousands (the US held 15,000 of 20,096).
CAP_DRIFT_ROWS = 25


@dataclass(frozen=True)
class Walk:
    """One query's stored walk, as its pages describe it."""

    key: str
    pages: int
    #: It holds pages and the last carries no cursor.
    complete: bool
    #: The earliest page's fetch time. A walk is only ever REPLACED by a new one, so this is when
    #: the walk began, and any line it returned was stamped at or after it.
    started_at: datetime | None
    #: Results across the pages, counted as the provider sent them.
    held: int
    #: The provider's own count, read off the newest page.
    total: int | None
    #: The largest FIGI the walk holds. Walks are ordered by FIGI, so for a capped walk this is the
    #: edge of what it could have returned.
    window_end: str | None

    @property
    def capped(self) -> bool:
        """Finished, and short of the provider's own total by more than drift."""
        return (
            self.complete
            and self.total is not None
            and self.total - self.held > max(CAP_DRIFT_ROWS, self.total // 1000)
        )

    def as_fact(self) -> dict[str, Any]:
        """What `market.mark_venue_absence` needs to know about this walk."""
        return {
            "complete": self.complete,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "capped": self.capped,
            "window_end": self.window_end if self.capped else None,
        }


def is_finished(raw_store: RawStore, key: str) -> bool:
    """The cheap form of `Walk.complete`: one column of the file, no bodies."""
    stored = list(raw_store.stored_rows_for(raw_exchange_sweep.key, key, columns=["cursor_at"]))
    return bool(stored) and not stored[-1].get("cursor_at")


def read_walk(raw_store: RawStore, key: str) -> Walk:
    """Every fact the readers need, from one pass over the walk's pages."""
    stored = list(
        raw_store.stored_rows_for(
            raw_exchange_sweep.key, key, columns=["body", "cursor_at", "fetched_at", "page"]
        )
    )
    held, total, window_end = _counts(stored)
    seen = [_when(row.get("fetched_at")) for row in stored]
    fetched = [at for at in seen if at is not None]
    return Walk(
        key=key,
        pages=len(stored),
        complete=bool(stored) and not stored[-1].get("cursor_at"),
        started_at=min(fetched) if fetched else None,
        held=held,
        total=total,
        window_end=window_end,
    )


def _counts(stored: Sequence[Mapping[str, Any]]) -> tuple[int, int | None, str | None]:
    """Results held, the provider's total off the newest page, and the largest FIGI held.

    Counts results as the provider sent them, including any `parse_filter` cannot key, because the
    provider's `total` counts them too. A body that cannot be read counts as nothing.
    """
    held = 0
    total: int | None = None
    largest: str | None = None
    for row in stored:
        try:
            parsed = json.loads(bytes(row["body"]))
        except (KeyError, TypeError, json.JSONDecodeError):
            continue
        if not isinstance(parsed, dict):
            continue
        data = parsed.get("data")
        if isinstance(data, list):
            held += len(data)
            for line in data:
                figi = line.get("figi") if isinstance(line, dict) else None
                # PYTHON ORDERS STRINGS BY CODE POINT, which is Postgres' `collate "C"` and
                # OpenFIGI's order. The database compares the window under that collation.
                if isinstance(figi, str) and (largest is None or figi > largest):
                    largest = figi
        page_total = parsed.get("total")
        if isinstance(page_total, int) and not isinstance(page_total, bool):
            total = page_total
    return held, total, largest


def _when(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None
