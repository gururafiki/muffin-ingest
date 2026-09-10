"""Where an asset's output goes, so no asset opens a connection or a file of its own.

TWO STORAGE CLASSES, ONE MANAGER EACH — never one per asset. Raw lands as Parquet on the block
volume; core lands in Postgres through the writers that already refuse the four shapes this pipeline
has been wrong about. An asset RETURNS rows; what happens to them is not its business, which is
what makes the same rules apply to every facet without any of them remembering.

WHY PYARROW AND NOT POLARS, decided by measurement rather than by the plan.

The design reserved ~47 MB for polars on the grounds that an 88 M-row transform must not hold a
frame in memory. It never does: BOTH acquisition lanes are chunked by construction — the daily
cross-section is one day for ~10,894 symbols, and the history lane is one batch of ten securities at
a time, ~73 k bars. The largest frame either lane holds is four orders of magnitude below the number
that justified the dependency, so `scan_parquet`'s laziness buys nothing here and pyarrow alone
serialises what we have. `UPathIOManager` and `upath` come with dagster itself, so the whole raw
path costs one wheel.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

import dagster as dg

from muffin_ingest import settings
from muffin_ingest.writers import upsert

if TYPE_CHECKING:
    from upath import UPath

#: Rows in, rows out. Deliberately not a pydantic model at this boundary: Parquet and the upsert
#: both want plain mappings, and every rule a model would enforce here (a number that is really a
#: string, money with no currency, a duplicated conflict key) is already enforced by `writers`,
#: where it applies to every caller rather than to every author who remembered to declare a type.
Rows = Sequence[Mapping[str, Any]]


class ParquetIOManager(dg.UPathIOManager):
    """Raw: exactly what the provider said, one file per partition.

    A partition's file is REPLACED, not appended to. Raw is immutable in the sense that we never
    edit a row — but a partition re-materialises when we re-ask, and the newer answer is the one the
    provider now gives. Appending would make a re-run silently double the data, which is the
    upsert-cannot-retract failure moved to the filesystem.
    """

    extension: str | None = ".parquet"

    def __init__(self, base_path: str) -> None:
        # `UPathIOManager` wants a `UPath` and fails with `'str' object has no attribute
        # 'joinpath'` deep inside partition-path resolution when handed a string — a message that
        # names neither the argument nor the manager. Taking a plain path here means every caller
        # passes what it has (an env var, a tmp_path) and none of them has to know.
        from upath import UPath

        super().__init__(base_path=UPath(base_path))

    def dump_to_path(self, context: dg.OutputContext, obj: Rows, path: UPath) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        rows = list(obj)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not rows:
            # AN EMPTY PARTITION IS A RESULT, AND IT IS WRITTEN. A market holiday produces no bars,
            # and the difference between "we collected that day and there was nothing" and "we never
            # collected that day" is the whole reason the cross-section is partitioned by date.
            # Writing nothing would make the two indistinguishable on disk.
            pq.write_table(pa.table({}), path.path if hasattr(path, "path") else str(path))
            return

        columns = sorted({key for row in rows for key in row})
        table = pa.table({column: [row.get(column) for row in rows] for column in columns})
        pq.write_table(table, path.path if hasattr(path, "path") else str(path))

    def load_from_path(self, context: dg.InputContext, path: UPath) -> list[dict[str, Any]]:
        import pyarrow.parquet as pq

        table = pq.read_table(path.path if hasattr(path, "path") else str(path))
        return table.to_pylist() if table.num_columns else []


class PostgresIOManager(dg.ConfigurableIOManager):
    """Core: typed rows, written through `writers.upsert`.

    THE TARGET AND ITS CONFLICT KEY COME FROM THE ASSET'S METADATA, so a facet declares where its
    rows go beside the asset rather than in a connection it opened itself. That is what makes
    `dedupe_by` on the conflict key — the rule that has failed whole statements with SQLSTATE 21000
    four separate times — unavoidable rather than remembered.
    """

    def handle_output(self, context: dg.OutputContext, obj: Rows) -> None:
        import psycopg

        meta = context.definition_metadata or {}
        table = meta.get("table")
        conflict = meta.get("conflict")
        if not table or not conflict:
            raise dg.DagsterInvariantViolationError(
                f"{context.asset_key.to_user_string()} writes through postgres_io but declares no "
                f"`table`/`conflict` metadata; without them this manager would have to guess where "
                f"the rows go and on what key they collide"
            )

        rows = list(obj)
        with psycopg.connect(settings.database_url()) as conn:
            with conn.cursor() as cur:
                result = upsert(
                    cur,
                    str(table),
                    rows,
                    conflict=list(conflict),
                    update=_updatable(rows, conflict),
                )
            conn.commit()

        context.add_output_metadata(
            {
                "rows": result.written,
                # A RISING COLLAPSE COUNT IS A STATEMENT ABOUT THE SOURCE, not a repair to be
                # pleased about — a backlog yielding one row per (security, sector) rather than per
                # security is how this was first noticed.
                "collapsed": result.collapsed,
                "table": str(table),
            }
        )

    def load_input(self, context: dg.InputContext) -> None:
        raise dg.DagsterInvariantViolationError(
            "core tables are read by SQL, not loaded back into an asset — stage 3 reads them "
            "through views and functions, which is what keeps the heavy work in the database"
        )


def _updatable(rows: Rows, conflict: Sequence[str]) -> list[str]:
    """Every column present that is not part of the key.

    Derived from the ROWS rather than declared, so a facet that starts producing a new column cannot
    silently keep writing the old value on conflict — the same reason the retraction list in
    `clear_symbol_caches` had to stop being hand-maintained after a tenth column never joined it.
    """
    return sorted({key for row in rows for key in row} - set(conflict))
