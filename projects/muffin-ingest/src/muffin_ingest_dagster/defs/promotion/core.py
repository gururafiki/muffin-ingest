"""Stage 2 of the promotion family: the verdicts, recorded, and the priced lines promoted."""

from collections import Counter
from datetime import datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import promotion
from muffin_ingest.writers import upsert

from muffin_ingest_dagster.defs.prices.partitions import PROVIDER, SECURITY_PARTITION
from muffin_ingest_dagster.defs.promotion.partitions import wave_weeks
from muffin_ingest_dagster.defs.promotion.raw import capacity, read_policy
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

#: A verdict that is a statement by Yahoo about the line. `unreadable` is not: a gateway page says
#: nothing about the symbol, so it is counted and never recorded.
STATEMENTS = frozenset(promotion.OUTCOMES) - {"unreadable"}


@dg.asset(
    partitions_def=wave_weeks,
    # EAGER: it follows its probe, in a run of its own on the `sql` pool, so the probe's ~25
    # minutes on the `yfinance` pool never hold the database pool too.
    automation_condition=dg.AutomationCondition.eager(),
    # A WAVE A WEEK, paused or not: a paused policy still materialises the week with its reason, so
    # a stale wave means the schedule stopped, not that nothing was due.
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=8)),
    pool="sql",
    group_name="universe",
    kinds={"postgres"},
    description=(
        "The week's promotion wave: records what Yahoo said about each line it was asked, and "
        "promotes the share classes whose line Yahoo priced. No provider call."
    ),
)
def promotion_wave(
    context: AssetExecutionContext, postgres: Postgres, raw_promotion_probe: Any
) -> "dg.MaterializeResult[None]":
    """Bodies on disk to verdicts, then `market.promote_share_classes(priced lines, capacity)`.

    AN ABSENCE IS RECORDED ONLY WHEN THE CONTROL PRICED IN THE SAME RUN. A run Yahoo was throttling
    or answering with an error page would otherwise record a week's candidates as unpriceable and
    keep them out of waves for 90 days: the 1,369-security incident, in a new table. A `priced`
    verdict is positive evidence and is recorded whatever the control said.

    A LINE IS ITS WALK OVER ITS SPELLINGS. It is priced under the first spelling Yahoo priced, and
    promoted under that spelling (a KOSDAQ line as `.KQ`). It is absent only when every spelling
    was asked and Yahoo knew none: a walk cut short by a failed request or a throttle says nothing,
    and is counted `unfinished` and never recorded.

    THE PRICED LINES ARE HANDED OVER IN THE ORDER THEY WERE ASKED, which is the wave's order, and
    the function re-checks each against the candidates: a class promoted since stage 1 is skipped.
    """
    rows = partitioned.loaded_rows(raw_promotion_probe)
    control = [r for r in rows if r.get("role") == "control"]
    candidates = [r for r in rows if r.get("role") == "candidate"]
    control_priced = any(_verdict(r) == promotion.PRICED for r in control)

    walks: dict[str, list[dict[str, Any]]] = {}
    for row in candidates:
        walks.setdefault(str(row["figi"]), []).append(row)

    outcomes: Counter[str] = Counter()
    probes: list[dict[str, Any]] = []
    priced: list[tuple[str, str]] = []
    respelled = 0
    for figi, walk in walks.items():
        walk.sort(key=lambda r: int(r.get("spelling_rank") or 0))
        verdicts = [(_verdict(r), r) for r in walk]
        hit = next(((v, r) for v, r in verdicts if v == promotion.PRICED), None)
        verdict, row = hit if hit is not None else verdicts[-1]
        if hit is not None:
            priced.append((figi, str(row["asked_symbol"])))
            respelled += int(int(row.get("spelling_rank") or 0) > 0)
        elif promotion.asks_the_next_spelling(verdict) and len(walk) < int(
            row.get("spellings") or 1
        ):
            verdict = "unfinished"
        outcomes[verdict] += 1
        if verdict in STATEMENTS and (verdict == promotion.PRICED or control_priced):
            probes.append(
                {
                    "figi": figi,
                    "outcome": verdict,
                    "symbol": row["asked_symbol"],
                    "source_code": PROVIDER.code,
                    "observed_at": row["fetched_at"],
                }
            )

    keys = len(context.instance.get_dynamic_partitions(SECURITY_PARTITION))
    with postgres.connect() as conn, conn.cursor() as cur:
        if probes:
            upsert(
                cur,
                "market.promotion_probe",
                probes,
                conflict=["figi"],
                update=["outcome", "symbol", "source_code", "observed_at"],
            )
        policy = read_policy(cur)
        limit = capacity(policy, keys) if policy is not None else 0
        cur.execute(
            "select market.promote_share_classes(%s::text[], %s::text[], %s)",
            ([figi for figi, _ in priced], [symbol for _, symbol in priced], limit),
        )
        reported = cur.fetchone()
    report: dict[str, Any] = reported[0] if reported else {}
    context.log.info(
        "promotion wave: %s promoted of %s priced (%s asked; control priced: %s); outcomes %s; "
        "by venue %s; %s candidates left; %s",
        report.get("promoted"),
        len(priced),
        len(walks),
        control_priced,
        dict(outcomes),
        report.get("by_venue"),
        report.get("candidates_left"),
        report.get("reason") or "ok",
    )
    return dg.MaterializeResult(
        metadata={
            "promoted": report.get("promoted"),
            "asked": len(walks),
            "priced": len(priced),
            # Priced under a spelling other than the line's first: a KOSDAQ `.KQ`, a padded Hong
            # Kong number, a Canadian Venture line.
            "priced_respelled": respelled,
            "symbol_held": report.get("symbol_held"),
            "control_priced": control_priced,
            # NAMED, NOT JUST COUNTED: what the directory offered that Yahoo would not price.
            "outcomes": dg.MetadataValue.json(dict(sorted(outcomes.items()))),
            "probes_recorded": len(probes),
            "capacity": limit,
            "securities_in_sweep": keys,
            "by_venue": dg.MetadataValue.json(report.get("by_venue") or {}),
            "candidates_left": report.get("candidates_left"),
            # A wave that promoted nothing says why: paused, no capacity, or nothing priced.
            "reason": report.get("reason") or "none",
        }
    )


def _verdict(row: dict[str, Any]) -> str:
    fetched = row["fetched_at"]
    fetched_at = fetched if isinstance(fetched, datetime) else datetime.fromisoformat(str(fetched))
    return promotion.verdict(bytes(row["body"]), fetched_at)
