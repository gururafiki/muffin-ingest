"""Fiscal calendars: written whole and only when changed, by the SEC lane and by the Yahoo lane.

The database is a recording fake that answers with the stored calendar a test sets.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date
from typing import Any

import dagster as dg
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import fiscal

from muffin_ingest_dagster.defs.companies.core import yahoo_fiscal_calendar
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.lib.fiscal_tables import (
    CALENDAR_COLUMNS,
    PERIOD_COLUMNS,
    replace_calendars,
)
from muffin_ingest_dagster.lib.resources import Postgres

#: A December filer's three years and two quarters, as Yahoo would publish them.
ANNUAL = [date(2023, 12, 31), date(2024, 12, 31), date(2025, 12, 31)]
QUARTER = [date(2026, 3, 31), date(2026, 6, 30)]

CALLS: list[tuple[str, list[Any]]] = []
#: What the fake database answers each read with.
STORED: dict[str, list[tuple[Any, ...]]] = {}


class Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        CALLS.append((text, list(params)))
        self.rows, self.rowcount = [], 0
        for marker, rows in STORED.items():
            if marker in text:
                self.rows = list(rows)
                return
        if text.startswith(("insert into", "delete from")):
            self.rowcount = 1

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        return None


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    CALLS.clear()
    STORED.clear()
    yield


def _rows(sid: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    calendar = fiscal.calendar_from_periods(ANNUAL, QUARTER)
    return fiscal.rows(calendar, security_ids=[sid], source="yfinance", as_of="2026-10-29T00:00Z")


def _as_stored(sid: str, rows: list[dict[str, object]], columns: Sequence[str]) -> list[Any]:
    """Rows as the database returns them: dates as `date`, not ISO strings."""

    def value(v: object) -> object:
        if isinstance(v, str) and len(v) == 10 and v[4] == "-":
            return date.fromisoformat(v)
        return v

    return [(sid, *(value(r[c]) for c in columns)) for r in rows]


def _writes() -> list[str]:
    return [sql for sql, _ in CALLS if sql.startswith(("insert", "delete", "update"))]


# --- the shared writer ---------------------------------------------------------------------------


def test_a_calendar_equal_to_the_stored_one_is_not_written() -> None:
    """Stored dates come back as `date` and the rows hold ISO strings; equal is still equal."""
    calendars, periods = _rows("s1")
    STORED["from market.fiscal_calendar where"] = _as_stored("s1", calendars, CALENDAR_COLUMNS)
    STORED["from market.fiscal_period where"] = _as_stored("s1", periods, PERIOD_COLUMNS)
    stats = replace_calendars(Cursor(), {"s1": (calendars, periods)})
    assert stats["calendar_unchanged"] == 1 and stats["calendar_replaced"] == 0
    assert _writes() == []


def test_a_calendar_that_changed_is_replaced_whole() -> None:
    """One period moved: the security's calendar is deleted (its periods cascade) and rewritten,
    never patched row by row past the partial unique index on canonical labels."""
    calendars, periods = _rows("s1")
    stored = [dict(p) for p in periods]
    stored[0]["period_end"] = "2023-12-30"
    STORED["from market.fiscal_calendar where"] = _as_stored("s1", calendars, CALENDAR_COLUMNS)
    STORED["from market.fiscal_period where"] = _as_stored("s1", stored, PERIOD_COLUMNS)
    stats = replace_calendars(Cursor(), {"s1": (calendars, periods)})
    assert stats["calendar_replaced"] == 1
    writes = _writes()
    assert writes[0] == "delete from market.fiscal_calendar where security_id = %s"
    assert writes[1].startswith("insert into market.fiscal_calendar as stored_row")
    assert writes[2].startswith("insert into market.fiscal_period as stored_row")


def test_a_security_with_no_stored_calendar_is_written() -> None:
    calendars, periods = _rows("s1")
    stats = replace_calendars(Cursor(), {"s1": (calendars, periods)})
    assert stats["calendar_replaced"] == 1 and stats["calendar_period_rows"] == len(periods)


# --- the Yahoo lane ------------------------------------------------------------------------------


def materialize(keys: Sequence[str]) -> dg.ExecuteInProcessResult:
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, list(keys))
        return dg.materialize(
            [yahoo_fiscal_calendar],
            instance=instance,
            tags={
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
            },
            resources={"postgres": FakePostgres()},
            raise_on_error=False,
        )


def test_yahoo_writes_a_calendar_only_where_sec_has_none_and_labels_every_company() -> None:
    """`s-sec` has a SEC calendar: it is not touched, and still labelled, so its Yahoo rows take
    SEC's labels. `s-yahoo` gets one from its period ends. `s-none` has no annual period."""
    STORED["source_code <> 'yfinance'"] = [("s-sec",)]
    STORED["array_agg(distinct period_ending"] = [
        ("s-sec", "annual", ANNUAL),
        ("s-yahoo", "annual", ANNUAL),
        ("s-yahoo", "quarter", QUARTER),
        ("s-none", "quarter", QUARTER),
    ]
    STORED["select market.label_fiscal_periods"] = [(7,)]
    result = materialize(["s-none", "s-sec", "s-yahoo"])
    assert result.success, result.get_step_failure_events()

    deletes = [p for sql, p in CALLS if sql.startswith("delete from market.fiscal_calendar")]
    assert deletes == [["s-yahoo"]]
    [inserted] = [p for sql, p in CALLS if sql.startswith("insert into market.fiscal_calendar")]
    assert "yfinance" in inserted and "s-sec" not in inserted
    [labels] = [p for sql, p in CALLS if sql.startswith("select market.label_fiscal_periods")]
    assert labels == [["s-none", "s-sec", "s-yahoo"]]

    # One materialisation per partition of the run, each carrying the run's metadata.
    event, *_ = result.get_asset_materialization_events()
    meta = {k: v.value for k, v in event.step_materialization_data.materialization.metadata.items()}
    assert (meta["calendar_from_sec"], meta["no_annual_periods"], meta["calendar_replaced"]) == (
        1,
        1,
        1,
    )
    assert meta["labelled"] == 7
