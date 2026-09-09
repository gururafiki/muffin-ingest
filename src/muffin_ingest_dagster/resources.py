"""The one resource everything needs: a direct connection to Postgres.

A pool rather than a connection per op, because a run is a subprocess and Dagster may execute
several ops in it. `autocommit` is deliberately OFF — the ledger's whole value is that claiming,
writing and completing happen in one transaction, so a facet that half-succeeds leaves the task
claimable again rather than silently done.
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
