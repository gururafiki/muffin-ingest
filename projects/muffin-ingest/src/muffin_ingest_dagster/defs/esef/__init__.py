"""Europe's segment lane (Phase 6c step 7): ESEF annual financial reports, as filings.xbrl.org
serves them in xBRL-JSON, read through their segment note.

  `security_esef_filer`  each European equity's LEI, from N-PORT's issuer, in the countries the
                         index covers; the SEC's companies are skipped (Phase 6c decision 2);
  `raw_esef_filings`     each filer's list and the annual reports not yet read, kept whole
                         (Phase 6c decision 3), one request every eight seconds;
  `esef_segment`         the reports into `security_filing`, and the splits each note's HTML tables
                         state, read against the report's own tagged figures (`xbrl-org`).
"""
