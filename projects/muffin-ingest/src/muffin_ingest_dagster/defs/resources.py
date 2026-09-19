"""Resource bindings — one per key, shared by every family."""

import dagster as dg
from muffin_ingest import settings

from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, PostgresIOManager
from muffin_ingest_dagster.lib.resources import Postgres


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "postgres": Postgres(),
            # ONE MANAGER PER STORAGE CLASS, never one per asset — which is what makes the writers'
            # rules apply to every facet without any of them remembering.
            "parquet_io": ParquetIOManager(settings.raw_root()),
            # The writer takes the SAME resource every reader uses, rather than opening its own
            # connection — see `PostgresIOManager`'s docstring for what that silently skipped.
            "postgres_io": PostgresIOManager(postgres=Postgres()),
        }
    )
