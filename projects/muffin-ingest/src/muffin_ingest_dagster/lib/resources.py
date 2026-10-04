"""The one resource everything needs: a direct connection to Postgres.

A fresh connection per `connect()`, committed when the block exits cleanly and rolled back when it
raises. `autocommit` is deliberately OFF: a writer's statements land together or not at all — the
symbology lane writes identifiers, symbols and probes in one transaction, so a run that dies
half-way leaves the subject to be asked again rather than half-recorded.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import dagster as dg
from muffin_ingest import settings

if TYPE_CHECKING:
    from psycopg import Connection


# `ConfigurableResource` is generic in this Dagster version and its own stubs are partial, so
# strict mode wants arguments it does not document. Ignored narrowly by code rather than
# blanket-ignored, so a DIFFERENT error here still fails the build.
class Postgres(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Direct SQL. Not PostgREST — see `muffin_ingest.settings.database_url` for why."""

    @contextmanager
    def connect(self) -> Iterator[Connection[Any]]:
        import psycopg

        with psycopg.connect(settings.database_url()) as conn:
            yield conn
