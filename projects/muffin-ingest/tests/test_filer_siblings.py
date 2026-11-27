"""`security_filer_siblings`: a company's regulator filer onto every listing, and its history.

The RULE lives in the database (`market.issuer_sibling_links`, proven by muffin-deployment's
`a-company-s-filer-reaches-every-listing.sql`); this asset only runs it. What is tested here is
what the asset owns: the extension runs once and commits before any copy, each copy commits on its
own (so one holder's failure cannot undo another's), and the counters add up to what was done.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import dagster as dg
import pytest

from muffin_ingest_dagster.defs.segments import core
from muffin_ingest_dagster.lib.resources import Postgres

#: (statement, params) in order, and "commit" where the asset committed.
LOG: list[Any] = []

#: What `filer_holders_to_copy` names, and what each copy answers.
TO_COPY = [("s-petr4", "sec"), ("s-petr3", "sec"), ("s-icbc-h", "cninfo")]
COPIED = {"s-petr4": (4496, 379), "s-petr3": (4496, 379), "s-icbc-h": (61, 0)}


class Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        LOG.append((sql, tuple(params)))
        if sql == core.SIBLINGS:
            self.rows = [(143, 2, 4)]
        elif sql == core.TO_COPY:
            self.rows = list(TO_COPY)
        elif sql == core.COPY:
            self.rows = [COPIED[str(params[0])]]
        else:
            raise AssertionError(f"unexpected statement: {sql}")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        LOG.append("commit")


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clear() -> None:
    LOG.clear()


def test_the_extension_commits_before_any_copy_and_each_copy_commits_alone() -> None:
    result = core.security_filer_siblings(dg.build_asset_context(), postgres=FakePostgres())
    statements = [e if e == "commit" else e[0] for e in LOG]
    assert statements == [
        core.SIBLINGS, "commit", core.TO_COPY,
        core.COPY, "commit", core.COPY, "commit", core.COPY, "commit",
    ]  # fmt: skip
    assert [e[1] for e in LOG if e != "commit" and e[0] == core.COPY] == list(TO_COPY)

    assert isinstance(result, dg.MaterializeResult) and result.metadata is not None
    assert dict(result.metadata) == {
        "linked": 143, "retracted": 2, "refused": 4,
        "copied_holders": 3, "copied_cninfo": 1, "copied_sec": 2,
        "copied_filings": 4496 * 2 + 61, "copied_segments": 379 * 2,
    }  # fmt: skip


def test_nothing_to_copy_still_reports_the_extension() -> None:
    TO_COPY_SAVED = list(TO_COPY)
    TO_COPY.clear()
    try:
        result = core.security_filer_siblings(dg.build_asset_context(), postgres=FakePostgres())
    finally:
        TO_COPY.extend(TO_COPY_SAVED)
    assert isinstance(result, dg.MaterializeResult) and result.metadata is not None
    meta = dict(result.metadata)
    assert (meta["linked"], meta["copied_holders"], meta["copied_filings"]) == (143, 0, 0)
