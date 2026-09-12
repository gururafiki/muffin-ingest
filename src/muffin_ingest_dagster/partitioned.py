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
    for row in rows:
        out.setdefault(key(row), []).append(row)
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
