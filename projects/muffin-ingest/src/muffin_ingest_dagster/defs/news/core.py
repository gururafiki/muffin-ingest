"""Stage 2 of the news family: the stored documents read into the news tables."""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from muffin_ingest.facets import news
from muffin_ingest.writers import WriteResult, upsert

from muffin_ingest_dagster.defs.companies.partitions import COMPANY_PARTITIONS_PER_RUN
from muffin_ingest_dagster.defs.news.raw import FRESH
from muffin_ingest_dagster.defs.prices.partitions import security_partitions
from muffin_ingest_dagster.lib import partitioned
from muffin_ingest_dagster.lib.resources import Postgres

RAW_NEWS = dg.AssetKey("raw_yahoo_news")


def _written(result: WriteResult) -> dict[str, Any]:
    changed = -1 if result.changed is None else result.changed
    return {"rows": result.written, "changed": changed, "collapsed": result.collapsed}


@dg.asset(
    ins={
        # A RE-PARSE MAY NAME A COMPANY STAGE 1 NEVER REACHED, whose partition has no file yet.
        "raw_yahoo_news": dg.AssetIn(key=RAW_NEWS, metadata={"allow_missing_partitions": True})
    },
    group_name="news",
    kinds={"postgres"},
    partitions_def=security_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(COMPANY_PARTITIONS_PER_RUN),
    pool="sql",
    freshness_policy=FRESH,
    metadata={"table": "market.news_article, market.news_security"},
    description="Articles from each company's newest RSS document, linked to the companies they "
    "came back for; articles past 90 days pruned.",
)
def company_news(
    context: AssetExecutionContext, postgres: Postgres, raw_yahoo_news: Any
) -> dg.MaterializeResult[None]:
    """No provider call. One transaction: the articles, their links, then the prune.

    INSERT-ONLY APART FROM THE PRUNE. An article a newer feed no longer lists has scrolled out of
    the newest twenty, not been withdrawn, so nothing is retracted for its absence. An article's
    title or summary is updated if the feed restates it differently; `source` is not, because the
    feed states no publisher and the edge's stored ones are the only ones there are.
    """
    documents = partitioned.loaded_rows(raw_yahoo_news)
    parsed = news.normalise(documents, now=datetime.now(UTC))
    with postgres.connect() as conn:
        with conn.cursor() as cur:
            articles = upsert(
                cur,
                "market.news_article",
                parsed.articles,
                conflict=["url"],
                update=["published_at", "title", "summary"],
            )
            links = upsert(
                cur, "market.news_security", parsed.links, conflict=["url", "security_id"]
            )
            cur.execute("select market.prune_news(%s)", (news.RETENTION_DAYS,))
            pruned = (cur.fetchone() or (0,))[0]
        conn.commit()
    if parsed.stats["documents_unknown"]:
        context.log.info(
            "%d companies' symbols are unknown to the feed", parsed.stats["documents_unknown"]
        )
    return dg.MaterializeResult(
        metadata={
            **parsed.stats,
            **{f"articles_{k}": v for k, v in _written(articles).items()},
            **{f"links_{k}": v for k, v in _written(links).items()},
            "pruned": int(pruned or 0),
        }
    )
