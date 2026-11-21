"""Company news from Yahoo's per-company RSS feed (Phase 6b, decided 2026-11-21).

  `raw_yahoo_news`  one RSS document per company, the newest answer replacing the last.
  `company_news`    stage 2: `market.news_article` once per URL, `market.news_security` per company
          it came back for, then `market.prune_news` with its 90-day window.

Replaces the edge's `security-news`, whose openbb route has answered 204 since 2026-10-01.

THE GRID IS THE PRICE LANE'S, AND THE CADENCE IS THE COMPANY ROTATION'S: decision 2 ties news to the
weekly visit, so both assets join `yahoo_companies` once a live run on a tiny subset has read clean.
Until then they run only when launched. Design: umbrella docs/specs/2026-11-21-macro-and-events.md.
"""
