"""Stand-ins for driving a branch's assets against PRODUCTION without writing to it.

Imported by a harness that `branch_on_node.sh` ships to the node beside the branch's source. Every
insert, update and delete is recorded in `SKIPPED` and never sent, and the connection is ALSO
read-only, so a statement that slipped past the interception would fail rather than write.
`Capture` stands in for `PostgresIOManager`: what stage 2 produced lands in `CAPTURED`, ready to
compare with what production holds.

    from readonly import CAPTURED, SKIPPED, Capture, SafePostgres
    resources = {"postgres": SafePostgres(), "postgres_io": Capture(),
                 "parquet_io": ParquetIOManager(tmp), "raw_store": RawStore(base_path=tmp)}

A cursor answering an intercepted statement returns no rows, so a retraction counts 0 and a
`returning` clause yields nothing. Count what WOULD have been retracted separately, by comparing
`CAPTURED` with the table.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg
import psycopg
from muffin_ingest import settings
from muffin_ingest_dagster.lib.resources import Postgres

#: The first words of every write that was intercepted, in order.
SKIPPED: list[str] = []

#: Every row stage 2 handed to the core I/O manager.
CAPTURED: list[dict[str, Any]] = []


class SafeCursor:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        self._skipped = False

    def __enter__(self) -> SafeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        self._cursor.close()

    def execute(self, sql: Any, params: Any = None) -> Any:
        text = " ".join(str(sql).split()).lower()
        self._skipped = text.startswith(("insert", "update", "delete", "merge", "truncate"))
        if self._skipped:
            SKIPPED.append(text[:60])
            return None
        return self._cursor.execute(sql, params)

    def fetchall(self) -> list[Any]:
        return [] if self._skipped else list(self._cursor.fetchall())

    def fetchone(self) -> Any:
        return None if self._skipped else self._cursor.fetchone()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class SafeConn:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def cursor(self, *args: Any, **kwargs: Any) -> SafeCursor:
        return SafeCursor(self._conn.cursor(*args, **kwargs))

    def commit(self) -> None:
        return None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class SafePostgres(Postgres):
    """`Postgres`, read-only twice over, with a statement timeout of its own."""

    @contextmanager
    def connect(self) -> Iterator[Any]:
        with psycopg.connect(settings.database_url()) as conn:
            conn.read_only = True
            conn.execute("set statement_timeout = 60000")
            yield SafeConn(conn)


class Capture(dg.ConfigurableIOManager):
    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        CAPTURED.extend(obj)

    def load_input(self, context: dg.InputContext) -> Any:
        raise NotImplementedError("nothing reads back out of a captured write")
