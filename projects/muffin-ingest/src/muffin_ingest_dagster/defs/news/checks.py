"""Asset checks of the news family."""

import dagster as dg

from muffin_ingest_dagster.defs.news.core import company_news
from muffin_ingest_dagster.lib.resources import Postgres

#: How old the newest article may be before the lane is judged stopped. The rotation visits a
#: seventh of the universe a day and the large companies have headlines most days, so three days
#: without one is a lane that stopped, as the edge's did for seven weeks without a word.
STALE_AFTER_DAYS = 3

NEWEST_FEED_ARTICLE = """
select max(a.published_at),
       count(*) filter (where a.published_at >= now() - make_interval(days => %s))::int
  from market.news_security ns
  join market.news_article a on a.url = ns.url
 where ns.source_code = %s
"""


@dg.asset_check(asset=company_news, blocking=False)
def news_is_current(postgres: Postgres) -> dg.AssetCheckResult:
    """Is the feed still bringing in news? The newest linked article must be under three days old.

    A WARNING, NOT A FAILURE: a quiet stretch of the rotation can leave it short for a day, and the
    run wrote correctly either way. Days of it are the outage the edge's `security-news` had.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(NEWEST_FEED_ARTICLE, (STALE_AFTER_DAYS, "yahoo-rss"))
        newest, recent = (cur.fetchone() or (None, 0))[:2]
    return dg.AssetCheckResult(
        passed=bool(recent),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "newest_article": str(newest) if newest is not None else "none",
            f"articles_last_{STALE_AFTER_DAYS}_days": int(recent or 0),
        },
    )
