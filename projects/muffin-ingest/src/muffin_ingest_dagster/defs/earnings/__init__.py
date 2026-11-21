"""Earnings dates and surprises from nasdaq and Yahoo (Phase 6b, decided 2026-11-21).

  `raw_nasdaq_earnings`  nasdaq's calendar for one date, whole: forecasts for a day to come,
          actuals and surprises for a day that has passed. Daily partitions from 2012-01-01 to 21
          days ahead; `nasdaq_earnings_window` re-asks the last seven days and the next 21 daily.
  `nasdaq_earnings`      stage 2: `market.earnings_calendar` (dates within 90 days) and
          `market.security_eps_history` (reported quarters), under `nasdaq`.
  `yahoo_earnings`       stage 2 over the company lane's stored `raw_yahoo_summary`: Yahoo's next
          date (`calendarEvents`) and last four quarters (`earningsHistory`), under `yfinance`. No
          request: the company lane already fetched these documents.

Replaces the edge's `earnings-calendar` and `earnings-history`, which asked the same nasdaq endpoint
over two ranges through openbb. Both tables key on the provider since the cutover migration, so the
two sources' rows for one company sit side by side and the serving views choose per company.

SHIPPED STOPPED: the writers use the provider keys, which exist only once the cutover migration
deploys. Design: umbrella docs/specs/2026-11-21-macro-and-events.md.
"""
