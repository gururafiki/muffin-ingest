"""The two shapes a `single_run` / `multi_run` lane has at its seams, in one place.

WHY THIS MODULE EXISTS. A backfill policy that hands an asset every partition at once changes the
shape at EVERY seam, not just the first one that fails — and the fix was discovered twice, one
stage apart, then hand-copied into three asset modules with the key expression inlined differently
each time. Two of those copies are wrong today: `assets/fx.py::raw_fx_spot` and
`assets/indices.py::raw_index_bars` both declare `BackfillPolicy.single_run()` and return a flat
list, so the first multi-day backfill of either dies at the WRITE, after the provider has been
paid. Nobody has run one yet, which is the only reason it has not been seen.

A rule written at one call site is not a rule. This is the call site.
"""

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from dagster import AssetExecutionContext

Row = dict[str, Any]
Rows = list[Row]


def by_partition(
    context: AssetExecutionContext,
    rows: Rows,
    *,
    key: Callable[[Row], str],
) -> Rows | dict[str, Rows]:
    """One object per partition when a run covers several; the rows themselves when it covers one.

    THE SHAPE IS DICTATED BY THE I/O MANAGER AND IT IS NOT OPTIONAL. A `single_run` backfill hands
    the asset every partition at once, and one file has to be written per partition — so the output
    has to say which rows belong to which. Returning a flat list works for a single partition and
    dies at the WRITE for a range, after the provider has already been paid: the first 96-security
    history backfill fetched everything and then failed on

        does not support persisting an output associated with multiple partitions

    Keeping the single-partition case a plain list is deliberate: it is the overwhelmingly common
    path, and `UPathIOManager` already handles it.

    `key` maps a row to the partition it belongs to. It is a parameter rather than a convention
    because the three lanes key on three different columns (`trade_date`, `security_id`,
    `currency_code`) — and inlining it per lane is exactly how the two broken copies happened.
    """
    # `has_asset_partitions` is an OutputContext attribute, not an asset one — reaching for it here
    # fails with a bare AttributeError inside the op. `has_partition_key` is true for exactly one
    # key; a run covering a range sets `has_partition_key_range` instead.
    if context.has_partition_key or not context.has_partition_key_range:
        return rows

    keys = list(context.partition_keys)
    # A RANGE OF EXACTLY ONE IS NOT A RANGE, AND THE I/O MANAGER DISAGREES ABOUT WHICH IT IS.
    #
    # `multi_run` groups CONTIGUOUS partitions, so a backfill whose keys are scattered through the
    # partition set produces one run per partition — each with a `partition_key_range` whose start
    # equals its end. This function then returned a MAPPING while `UPathIOManager` took its
    # single-partition path and handed the mapping to the writer, which died on
    #
    #     AttributeError: 'str' object has no attribute 'get'
    #
    # naming neither the partition nor the shape. Measured in production: 250 requested partitions
    # became 250 single-partition runs and the first three all failed this way.
    if len(keys) == 1:
        return rows

    # EVERY KEY IN THE RANGE GETS AN ENTRY, including the ones that produced nothing — the I/O
    # manager writes an empty file for those, and that is what makes "we collected that slice and
    # there was nothing" distinguishable from "we never collected it".
    out: dict[str, Rows] = {k: [] for k in keys}
    known = set(keys)
    for row in rows:
        placed = key(row)
        # A ROW THE RUN WAS NOT ASKED FOR IS STILL THE PROVIDER'S ANSWER, so it is FILED rather
        # than dropped. `equity.price.historical` widens a degenerate range (asking for 09-09
        # alone returns 09-09 AND 09-10) and a call made mid-session brings back a bar for a day
        # still trading. Discarding those made stage 1 the place that decides what belongs to a
        # window — and a row deleted at fetch is a row no re-parse can recover.
        #
        # It goes in the LAST partition of the run, which is the one whose request reached
        # furthest forward, and it keeps its own date so stage 2 can see it does not belong.
        # The I/O manager rejects a key outside the range, so there is nowhere else it could go.
        out[placed if placed in known else keys[-1]].append(row)
    return out


def loaded_rows(loaded: Any) -> Rows:
    """The MIRROR of `by_partition`, on the way back in — and it was missing, which cost a run.

    `UPathIOManager.load_input` reads one file per partition and, when the downstream step covers
    SEVERAL, hands back a `{partition_key: obj}` mapping rather than the obj. So a `single_run`
    lane has two shapes, not one, and BOTH stages have to know it. Annotating the input
    `list[dict[str, Any]]` makes Dagster type-check the mapping against a list and fail the step
    with

        Type check failed for step input "raw_price_history" - expected type "[Dict[String,Any]]"

    — after the provider has been paid and the raw files are already on disk. That is the same
    defect as the write refusing a multi-partition output, one stage downstream, and it survived
    the fix for that one because the fix only looked at the OUTPUT side.

    The input is therefore annotated `Any` at the asset — Dagster derives a DagsterType from the
    annotation and refuses a union, so there is no way to spell "either of these two" that it
    accepts. What recovers the guarantee is this function being the only way in: the rows are
    flattened here, and nothing downstream sees the difference.
    """
    if isinstance(loaded, Mapping):
        # Ordered by key so a multi-partition run writes in a stable order — the upsert does not
        # care, but a diff of two runs does.
        return [row for _, rows in sorted(loaded.items()) for row in rows]
    if isinstance(loaded, Sequence):
        return list(loaded)
    raise TypeError(
        f"raw input is {type(loaded).__name__}, expected a mapping of partition key to rows "
        f"or a sequence of rows — the I/O manager and the asset disagree about the run's shape"
    )


def to_every_partition(context: AssetExecutionContext, rows: Rows) -> Rows | dict[str, Rows]:
    """File the same rows under EVERY partition the run covers. For an artifact that covers a
    RANGE rather than belonging to a day.

    WHY THIS IS NOT `by_partition` WITH A CLEVERER KEY. `by_partition` asks a row which partition
    it belongs to, which presumes the row carries a date — true for a bar, false for a whole
    provider response. Yahoo's chart body for `range=5d` is one document covering five sessions:
    there is no honest way to file it under one of them, and filing it under the day we happened
    to ask would date the artifact by the clock, which is the thing this pipeline keeps being
    bitten by.

    So it is filed under each day in the run, and the claim each partition makes stays true:
    *this is the provider's answer covering that day*. Stage 2 reads the body and decides which
    points fall where — meaning a corrected timezone or window rule costs a re-parse of files
    already on disk rather than ten years of refetching per currency.

    THE DUPLICATION IS THE POINT, not a cost being tolerated. A partition that does not contain
    its own evidence cannot be re-read on its own, and re-reading one partition in isolation is
    what the whole split is for.
    """
    if context.has_partition_key or not context.has_partition_key_range:
        return rows
    keys = list(context.partition_keys)
    # A RANGE OF EXACTLY ONE IS NOT A RANGE — see `by_partition`; `UPathIOManager` takes its
    # single-partition path and would hand the mapping straight to the writer.
    if len(keys) == 1:
        return rows
    return {k: list(rows) for k in keys}


def rows_per_partition(context: AssetExecutionContext, loaded: Any) -> dict[str, Rows]:
    """The loaded input KEPT per partition — for a stage whose rule is per partition.

    `loaded_rows` flattens, which is right when nothing downstream cares which file a row came
    from. A stage that publishes "each partition's own day" does care: raw keeps whatever the
    provider sent, so one partition's file can hold a bar for a day that belongs to ANOTHER — a
    range widened forward, a session still trading. Windowing the flattened run would offer that
    stray beside the real bar from its own file, and the writer's last-wins dedupe would then pick
    between them by FILE ORDER: right only by accident of sorting.

    Keyed by partition so the caller applies each partition's own window to its own file, after
    which a stray cannot be published at all.
    """
    if isinstance(loaded, Mapping):
        return {str(key): list(rows) for key, rows in sorted(loaded.items())}
    keys = list(context.partition_keys)
    if isinstance(loaded, Sequence) and len(keys) == 1:
        return {keys[0]: list(loaded)}
    raise TypeError(
        f"raw input is {type(loaded).__name__} for {len(keys)} partition(s) — the I/O manager "
        f"hands over a mapping for several and a sequence for exactly one"
    )
