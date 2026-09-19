"""Asset checks of the prices family."""

import dagster as dg

from muffin_ingest_dagster.defs.prices.raw import raw_price_bars


@dg.asset_check(asset=raw_price_bars, blocking=False)
def every_askable_security_was_asked(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """A DAY-PARTITIONED ASSET CLAIMS ITS WHOLE CROSS-SECTION, so a run that stopped early lies.

    This is the claim the entire partitioning argument rests on. "Did the collection run on
    Tuesday?" is unanswerable from `price_bar` — a security with no bar looks identical whether its
    market was shut, its symbol is dead, or nothing ran at all — so MATERIALISING THE PARTITION is
    the answer. A run that spends its budget with subjects still unasked materialises the partition
    anyway, and the claim quietly becomes false.

    Measured: 200 securities took 244 s, so ~1.22 s each and the 11,446 askable equities are **3.9
    hours**. The budget was one hour, which would have covered a quarter of them and reported
    success — the exact shape of `remaining: 0` against a backlog of 9,013.

    WARN, NOT ERROR. A short night is worth seeing and not worth failing a pipeline over, and a
    check that takes the lane down because a provider was slow is a check someone disables.
    """
    key = raw_price_bars.key
    event = context.instance.get_latest_materialization_events([key]).get(key)
    materialization = event.asset_materialization if event is not None else None
    unasked = 0
    partition = "none"
    subjects = 0
    if materialization is not None:
        partition = materialization.partition or "none"
        unasked = int(getattr(materialization.metadata.get("unasked"), "value", 0) or 0)
        subjects = int(getattr(materialization.metadata.get("subjects"), "value", 0) or 0)

    return dg.AssetCheckResult(
        passed=unasked == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "unasked": unasked,
            "subjects": subjects,
            "partition": partition,
            "note": "a partition claims its whole cross-section; unasked subjects make that false",
        },
    )
