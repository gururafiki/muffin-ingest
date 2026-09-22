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

import contextvars
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

import dagster as dg
from muffin_ingest.writers import WriteResult, replace_scope, upsert

from muffin_ingest_dagster.lib.partitioned import Complete
from muffin_ingest_dagster.lib.resources import Postgres

if TYPE_CHECKING:
    from upath import UPath

#: Rows in, rows out. Deliberately not a pydantic model at this boundary: Parquet and the upsert
#: both want plain mappings, and every rule a model would enforce here (a number that is really a
#: string, money with no currency, a duplicated conflict key) is already enforced by `writers`,
#: where it applies to every caller rather than to every author who remembered to declare a type.
Rows = Sequence[Mapping[str, Any]]


class ParquetIOManager(dg.UPathIOManager):
    """Raw: exactly what the provider said, one file per partition.

    BY DEFAULT a partition's file is REPLACED, not appended to. Raw is immutable in the sense that
    we never edit a row — but a partition re-materialises when we re-ask, and the newer answer is
    the one the provider now gives. Appending blindly would make a re-run silently double the data,
    which is the upsert-cannot-retract failure moved to the filesystem.

    AN ASSET MAY DECLARE `merge_on` INSTEAD, and then the file means something different: not "the
    latest answer, replacing the last" but "every answer we hold for this subject, newest winning
    per key". That is what lets a run ask only for the extension — a day, not ten years — without
    destroying what earlier runs stored, and it is the difference between resuming a refused night
    and losing its first half. The key is declared beside the asset rather than assumed here,
    because every family keys its rows differently and a wrong key silently collapses them.

    Say which it is at the asset:

        @dg.asset(io_manager_key="parquet_io", metadata={"merge_on": ["symbol", "date"]})

    Without `merge_on` the behaviour is exactly as before, so no existing lane changes.
    """

    extension: str | None = ".parquet"

    #: PER-PARTITION TALLIES, EMITTED ONCE. `dump_to_path` runs once per partition, and
    #: `add_output_metadata` may be called only ONCE per output — a second call with the same keys
    #: raises `DagsterInvalidMetadata: Tried to add metadata for key(s) that already have
    #: metadata`. So a range run failed on its SECOND partition, and only a range run could: every
    #: test and every hand-run materialised one partition at a time, which is the one shape that
    #: cannot reach the bug. Measured 2026-09-22 — the first night `nightly_prices` emitted its
    #: bounded runs, all 66 failed this way and `market.price_bar` published nothing for four days.
    #:
    #: A ContextVar rather than an attribute because one manager instance serves concurrent steps;
    #: an attribute would let two outputs' tallies merge into each other.
    _tally: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
        "parquet_io_tally", default=None
    )

    def _record(self, tally_entry: dict[str, Any]) -> None:
        """Add one partition's counters to the run's tally, if a write is in progress.

        A `dump_to_path` reached outside `handle_output` — there is no such caller today, and the
        default of None rather than a fresh list is what makes a future one fail loudly here
        instead of silently dropping its numbers.
        """
        tally = self._tally.get()
        if tally is None:
            raise dg.DagsterInvariantViolationError(
                "ParquetIOManager wrote a partition outside handle_output, so its metadata has "
                "nowhere to go."
            )
        tally.append(tally_entry)

    @staticmethod
    def _summarise(tally: list[dict[str, Any]]) -> dict[str, Any]:
        """One metadata dict for however many partitions the run wrote.

        THE COUNTS ARE SUMS AND SAY SO. A range run's `rows_total` is the range's, not any one
        partition's — per-partition metadata is not expressible here, and a number that silently
        described only the last partition would be worse than a total.
        """
        if not tally:
            return {}
        replaced = [t for t in tally if t.get("replaced")]
        out: dict[str, Any] = {
            "merged_on": tally[0]["merged_on"],
            "rows_fetched": sum(int(t["rows_fetched"]) for t in tally),
            "rows_kept": sum(int(t["rows_kept"]) for t in tally),
            "rows_superseded": sum(int(t["rows_superseded"]) for t in tally),
            "rows_unkeyable": sum(int(t["rows_unkeyable"]) for t in tally),
            "rows_total": sum(int(t["rows_total"]) for t in tally),
            "partitions_replaced": len(replaced),
            "partitions_merged": len(tally) - len(replaced),
        }
        if replaced:
            out["replaced_because"] = "the run fetched these subjects' whole history"
        return out

    def __init__(self, base_path: str) -> None:
        # `UPathIOManager` wants a `UPath` and fails with `'str' object has no attribute
        # 'joinpath'` deep inside partition-path resolution when handed a string — a message that
        # names neither the argument nor the manager. Taking a plain path here means every caller
        # passes what it has (an env var, a tmp_path) and none of them has to know.
        from upath import UPath

        super().__init__(base_path=UPath(base_path))

    def handle_output(self, context: dg.OutputContext, obj: Rows | Mapping[str, Rows]) -> None:
        """Write one file per partition, including when a single run covers several.

        `UPathIOManager` REFUSES A MULTI-PARTITION OUTPUT OUTRIGHT:

            The current IO manager does not support persisting an output associated with multiple
            partitions. This error is likely occurring because a backfill was launched using the
            'single run' option.

        and the ways out it suggests are both worse. A multi-run policy turns one backfill of 96
        securities into 96 runs — and because this provider is asked one batch at a time, that is
        ten calls becoming ninety-six, a tenfold increase in provider spend for the same data.
        Opting out of I/O managers entirely puts a file path back inside every asset.
        WITHOUT THIS OVERRIDE `BackfillPolicy.single_run()` IS DECORATIVE, which is how it reached
        production: the asset ran, fetched all 96 securities' history, and died at the write.

        So a run covering several partitions returns a MAPPING of partition key to its rows, and
        each lands in its own file — which is what the partition means.
        """
        tally: list[dict[str, Any]] = []
        token = self._tally.set(tally)
        try:
            self._write(context, obj, tally)
        finally:
            self._tally.reset(token)

    def _write(
        self,
        context: dg.OutputContext,
        obj: Rows | Mapping[str, Rows],
        tally: list[dict[str, Any]],
    ) -> None:
        if context.has_asset_partitions and len(context.asset_partition_keys) > 1:
            paths = self._get_paths_for_partitions(context)
            if not isinstance(obj, Mapping):
                raise dg.DagsterInvariantViolationError(
                    f"{context.asset_key.to_user_string()} covers "
                    f"{len(context.asset_partition_keys)} partitions in one run, so it must return "
                    f"a mapping of partition key to rows — one file is written per partition. Got "
                    f"{type(obj).__name__}."
                )
            missing = set(obj) - set(paths)
            if missing:
                raise dg.DagsterInvariantViolationError(
                    f"{context.asset_key.to_user_string()} returned rows for partition(s) it was "
                    f"not asked about: {sorted(missing)[:5]}. A partition is a claim about a "
                    f"slice; writing one the run was not asked for makes that claim for it."
                )
            for key, path in paths.items():
                self.make_directory(path.parent)
                # A PARTITION IN THE RANGE THAT PRODUCED NOTHING STILL GETS A FILE. That is the
                # difference between "collected, and there was nothing" and "never collected", and
                # it is the whole reason the cross-section is partitioned by date.
                self.dump_to_path(context=context, obj=obj.get(key, []), path=path)
            context.add_output_metadata(
                {"partitions_written": len(paths), **self._summarise(tally)}
            )
            return

        super().handle_output(context, obj)
        # THE SINGLE-PARTITION PATH GOES THROUGH `UPathIOManager`, which calls `dump_to_path`
        # itself — so its tally is emitted here rather than inside the write, for the same
        # one-call-per-output reason.
        summary = self._summarise(tally)
        if summary:
            context.add_output_metadata(summary)

    def dump_to_path(self, context: dg.OutputContext, obj: Rows, path: UPath) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        rows = list(obj)
        merge_on = [str(c) for c in ((context.definition_metadata or {}).get("merge_on") or ())]
        # A COMPLETE ANSWER REPLACES; AN EXTENSION MERGES. The asset knows which it asked for and
        # says so per partition (`partitioned.Complete`), because one run holds both: the price
        # sweep loads a never-collected security in full and extends its neighbour, in the same run.
        #
        # AND AN EMPTY ONE REPLACES NOTHING, enforced here rather than trusted of every caller. No
        # rows means a dead symbol, a refusal or a holiday — writing that over a stored history
        # would delete it to record a quiet day, which is the failure the merge exists to prevent.
        if merge_on and isinstance(obj, Complete) and rows:
            stored = self._stored_rows(path)
            self._record(
                {
                    "merged_on": ", ".join(merge_on),
                    "rows_fetched": len(rows),
                    "rows_kept": 0,
                    "rows_superseded": len(stored),
                    "rows_unkeyable": 0,
                    "rows_total": len(rows),
                    "replaced": True,
                }
            )
        elif merge_on:
            rows = self._merge_with_stored(context, rows, path, merge_on)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not rows:
            # AN EMPTY PARTITION IS A RESULT, AND IT IS WRITTEN. A market holiday produces no bars,
            # and the difference between "we collected that day and there was nothing" and "we never
            # collected that day" is the whole reason the cross-section is partitioned by date.
            # Writing nothing would make the two indistinguishable on disk.
            #
            # WITH A COLUMN, THOUGH — `pa.table({})` produces a file with ZERO columns, which this
            # manager could read back and nothing else could: DuckDB refuses it outright with "Need
            # at least one non-root column in the file". Found by the offline replay test the first
            # time it asked a question of an empty partition. A zero-row file that other tools can
            # open is the difference between raw being inspectable and raw being ours alone.
            #
            # A glob spanning empty and populated partitions therefore needs `union_by_name=true`,
            # which is the honest cost of recording the distinction at all.
            pq.write_table(
                pa.table({"collected_nothing": pa.array([], type=pa.bool_())}),
                path.path if hasattr(path, "path") else str(path),
            )
            return

        columns = sorted({key for row in rows for key in row})
        table = pa.table({column: [row.get(column) for row in rows] for column in columns})
        pq.write_table(table, path.path if hasattr(path, "path") else str(path))

    def _merge_with_stored(
        self, context: dg.OutputContext, rows: Rows, path: UPath, merge_on: list[str]
    ) -> list[Mapping[str, Any]]:
        """What this run fetched, on top of what the partition already held.

        NEWEST WINS PER KEY, and "newest" is this run by construction — a re-ask of a subject is the
        provider restating it, so the stored copy goes. Anything the run did NOT re-ask about is
        kept, which is the whole point: the asset fetches an extension and the partition still holds
        the history.

        AN EMPTY RESULT IS NOT AN EMPTY PARTITION HERE. For a replacing asset, no rows means "we
        collected and there was nothing", and the zero-row file records it. For a merging asset it
        means "no extension today" — a market holiday, a symbol with no new bar — so returning the
        stored rows unchanged is the honest outcome. Writing the marker instead would delete the
        history to record a quiet day.
        """
        stored = self._stored_rows(path)
        if not stored:
            return list(rows)

        def key(row: Mapping[str, Any]) -> tuple[Any, ...]:
            return tuple(row.get(column) for column in merge_on)

        # A ROW THE RUN CANNOT KEY WOULD COLLAPSE EVERY OTHER UNKEYED ROW ONTO IT. `row.get` returns
        # None for a missing column, so a `merge_on` naming a field the provider does not send makes
        # every row share the key `(None, ...)` — one row would survive and the rest would be
        # dropped, silently, on data that is perfectly good. Refuse instead.
        missing = sorted({c for row in rows for c in merge_on if c not in row})
        if missing:
            raise dg.DagsterInvariantViolationError(
                f"{context.asset_key.to_user_string()} declares merge_on={merge_on} but its rows "
                f"do not carry {missing}. Every row must carry every merge column, or rows that "
                f"lack it all collapse onto one key and only one survives."
            )

        try:
            fresh_keys = {key(row) for row in rows}
        except TypeError as exc:  # a provider field that is a list or a dict
            raise dg.DagsterInvariantViolationError(
                f"{context.asset_key.to_user_string()} declares merge_on={merge_on}, but one of "
                f"those columns holds an unhashable value. Merge on scalars the provider keys its "
                f"own answer by."
            ) from exc

        # UNKEYABLE STORED ROWS ARE KEPT, NEVER DROPPED. A file written before the key existed, or
        # by an older shape, cannot be compared — and "I cannot tell whether this was superseded"
        # must resolve to keeping it. Counted so a non-zero value is visible rather than inferred.
        kept: list[Mapping[str, Any]] = []
        unkeyable = 0
        for row in stored:
            if any(column not in row for column in merge_on):
                kept.append(row)
                unkeyable += 1
                continue
            if key(row) not in fresh_keys:
                kept.append(row)

        merged = kept + list(rows)
        self._record(
            {
                "merged_on": ", ".join(merge_on),
                "rows_fetched": len(rows),
                "rows_kept": len(kept),
                "rows_superseded": len(stored) - len(kept),
                "rows_unkeyable": unkeyable,
                "rows_total": len(merged),
                "replaced": False,
            }
        )
        return merged

    def stored_rows_for(
        self, asset_key: dg.AssetKey, partition_key: str
    ) -> Sequence[Mapping[str, Any]]:
        """What a partition already holds, asked OUTSIDE a run's output context.

        An asset that extends rather than replaces has to know how far it got BEFORE it fetches,
        and Dagster offers nothing for it: self-dependency (`TimeWindowPartitionMapping`) is
        time-window only, so it cannot express "this same subject partition, as it was before".

        THE PATH IS BUILT HERE AND WRITTEN BY `UPathIOManager`, WHICH IS A DRIFT WAITING TO HAPPEN —
        an expensive one, because a lookup that silently misses reads as "nothing stored" and makes
        the asset re-fetch a whole history it already had. `test_the_public_lookup_reads_what_the_
        manager_wrote` drives the real seam and compares, so the two cannot diverge unnoticed.
        """
        path = self._base_path.joinpath(*asset_key.path, partition_key)
        return self._stored_rows(self._with_extension(path))

    def _stored_rows(self, path: UPath) -> Sequence[Mapping[str, Any]]:
        """What the partition already holds, or nothing if it holds nothing yet."""
        import pyarrow.parquet as pq

        if not path.exists():
            return []
        table = pq.read_table(path.path if hasattr(path, "path") else str(path))
        if table.column_names == ["collected_nothing"]:
            return []
        stored: list[dict[str, Any]] = table.to_pylist()
        return stored

    def load_from_path(self, context: dg.InputContext, path: UPath) -> list[dict[str, Any]]:
        import pyarrow.parquet as pq

        table = pq.read_table(path.path if hasattr(path, "path") else str(path))
        # The empty-partition marker is not data; a partition that collected nothing loads as no
        # rows, which is what every downstream stage should see.
        if table.column_names == ["collected_nothing"]:
            return []
        rows: list[dict[str, Any]] = table.to_pylist()
        return rows


class RawStore(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Read-only access to what a raw partition already holds, for an asset that EXTENDS it.

    A thin Pythonic wrapper rather than a second implementation: it delegates to `ParquetIOManager`,
    so there is exactly one path convention and the read cannot drift from the write. It exists at
    all because Dagster will not mix `required_resource_keys` with typed resource parameters on one
    asset — *"Cannot specify resource requirements in both @asset decorator and as arguments to the
    decorated function"* — and a `UPathIOManager` passed as a typed parameter is read as an asset
    INPUT, failing with `Input asset "["raw_store"]" is not produced by any of the provided asset
    ops`, which names neither the resource nor the cause.
    """

    base_path: str

    def stored_rows_for(
        self, asset_key: dg.AssetKey, partition_key: str
    ) -> Sequence[Mapping[str, Any]]:
        return ParquetIOManager(base_path=self.base_path).stored_rows_for(asset_key, partition_key)


class PostgresIOManager(dg.ConfigurableIOManager):
    """Core: typed rows, written through `writers.upsert`.

    THE TARGET AND ITS CONFLICT KEY COME FROM THE ASSET'S METADATA, so a facet declares where its
    rows go beside the asset rather than in a connection it opened itself. That is what makes
    `dedupe_by` on the conflict key — the rule that has failed whole statements with SQLSTATE 21000
    four separate times — unavoidable rather than remembered.

    IT TAKES THE `Postgres` RESOURCE RATHER THAN CALLING `psycopg.connect` ITSELF. It was the one
    place in the orchestration layer that opened its own connection, which meant the resource was
    the single place that decides HOW to connect for everything EXCEPT the writes — so a statement
    timeout, a pool, or a read replica added there would have silently skipped every core write,
    and nothing would have reported it.
    """

    postgres: Postgres

    def handle_output(self, context: dg.OutputContext, obj: Rows) -> None:
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
        # AN UPSERT CANNOT RETRACT, and for anything whose source restates a WHOLE scope that is a
        # defect rather than a limitation: a period a run stops producing keeps whatever was written
        # last time, for ever, looking freshly written. Securities served `1d = 0.00%` for four days
        # that way, because the guard that stopped PRODUCING a number could never REMOVE the stale
        # one. An asset declaring `replace_scope` gets delete-then-insert per scope value instead.
        scope_columns = [str(c) for c in (meta.get("replace_scope") or [])]

        with self.postgres.connect() as conn:
            with conn.cursor() as cur:
                if scope_columns:
                    result = _replace_scopes(cur, str(table), rows, scope_columns, list(conflict))
                else:
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
                "scopes_retracted": result.retracted,
                "table": str(table),
            }
        )

    def load_input(self, context: dg.InputContext) -> None:
        raise dg.DagsterInvariantViolationError(
            "core tables are read by SQL, not loaded back into an asset — stage 3 reads them "
            "through views and functions, which is what keeps the heavy work in the database"
        )


def _replace_scopes(
    cur: Any, table: str, rows: Rows, scope_columns: Sequence[str], conflict: Sequence[str]
) -> WriteResult:
    """Delete each scope this run produced, then write what it now says that scope contains.

    ONLY THE SCOPES THIS RUN TOUCHED. Deleting every scope and rewriting would make a bounded run —
    one page of securities — retract everything it did not happen to cover, which turns a page size
    into data loss. A security absent from `rows` is one this run said nothing about, and saying
    nothing is not the same as saying "no periods".
    """
    written = collapsed = retracted = 0
    by_scope: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in rows:
        by_scope.setdefault(tuple(row.get(c) for c in scope_columns), []).append(row)

    for values, scope_rows in by_scope.items():
        result = replace_scope(
            cur,
            table,
            scope_rows,
            scope=dict(zip(scope_columns, values, strict=True)),
            conflict=list(conflict),
        )
        written += result.written
        collapsed += result.collapsed
        retracted += result.retracted
    return WriteResult(written=written, collapsed=collapsed, retracted=retracted)


def _updatable(rows: Rows, conflict: Sequence[str]) -> list[str]:
    """Every column present that is not part of the key.

    Derived from the ROWS rather than declared, so a facet that starts producing a new column cannot
    silently keep writing the old value on conflict — the same reason the retraction list in
    `clear_symbol_caches` had to stop being hand-maintained after a tenth column never joined it.
    """
    return sorted({key for row in rows for key in row} - set(conflict))
