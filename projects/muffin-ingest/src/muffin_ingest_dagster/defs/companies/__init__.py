"""A company's facts and statements from Yahoo, refreshed weekly per security (Phase 4).

  `raw_yahoo_summary`     one quoteSummary document per company, the latest kept (`yahoo_companies`
          asks a seventh of the grid a day, so each company is visited weekly).
  `raw_yahoo_statements`  every fundamentals-timeseries document, appended: two requests (annual,
          quarterly) on a visit that finds a period Yahoo names and we do not hold, or 90 days on.
  stage 2                 `security_profile`, `security_officer`, `security_fundamentals`,
          `security_share_stats`, `security_estimate` and `yahoo_classification` from the summary;
          `yahoo_statement` from the timeseries. Tables only Yahoo writes are named after the table;
          a table another source shares is named `yahoo_*`.
  stage 3                 `derived_security_metrics`: the metric layer and the TTMs, paged under the
          worker's 120 s statement timeout instead of PostgREST's 8 s.

The corporate actions read the price lane's chart documents and live with it
(`prices.core.yahoo_corporate_action`).

THE GRID IS THE PRICE LANE'S: one key per security, kept in step by `new_securities_need_history`.
Design: umbrella docs/specs/2026-10-10-yahoo-company-data.md.
"""
