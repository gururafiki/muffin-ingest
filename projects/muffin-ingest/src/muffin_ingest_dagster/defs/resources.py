"""Resource bindings — one per key, shared by every family."""

import dagster as dg
from muffin_ingest import settings

from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, PostgresIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres

#: Bound under two keys below; built once so the write path and the read path cannot diverge.
_raw_store = ParquetIOManager(settings.raw_root())


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "postgres": Postgres(),
            # ONE MANAGER PER STORAGE CLASS, never one per asset — which is what makes the writers'
            # rules apply to every facet without any of them remembering.
            "parquet_io": _raw_store,
            # THE SAME BASE PATH UNDER A SECOND KEY, deliberately. An asset that EXTENDS a
            # partition has to know how far it got BEFORE it fetches, and an I/O manager only
            # speaks at the output seam. `RawStore` delegates to the same manager, so there is one
            # path convention: it reads exactly the file `parquet_io` wrote.
            "raw_store": RawStore(base_path=settings.raw_root()),
            # The writer takes the SAME resource every reader uses, rather than opening its own
            # connection — see `PostgresIOManager`'s docstring for what that silently skipped.
            "postgres_io": PostgresIOManager(postgres=Postgres()),
        }
    )
