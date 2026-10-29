"""The SEC family (Phase 5): filers' facts, filings and calendars, and insider trades.

  `raw_sec_daily_index`      SEC's daily form index, one file a day: the family's trigger.
  `raw_sec_form4`            each tracked issuer's Form 4s, by the day SEC listed them.
  `sec_form4_trade`          their lines in `insider_trade` (`sec-form4`), insert-only.
  `raw_sec_insider_dataset`  SEC's quarterly Insider Transactions Data Set, since 2006.
  `sec_insider_trade`        its lines in `insider_trade` (`sec-form345`), replacing both lanes'.

Every request goes through `muffin_ingest.providers.sec_edgar`, past http-cache, at 5 a second in
the `sec` pool. Both schedules ship STOPPED until the lanes have run live on a tiny subset.
Design: umbrella docs/specs/2026-10-24-sec-family.md.
"""
