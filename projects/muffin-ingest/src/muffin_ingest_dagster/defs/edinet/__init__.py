"""Japan's segment lane (Phase 6c step 5): business lines from EDINET's annual reports.

  `raw_edinet_code_list`, `security_edinet_filer`
                      EDINET's code list, whole, and each Japanese equity's EDINET code; the SEC's
                      companies are skipped (decision 2).
  `raw_edinet_days`, `edinet_document`
                      each day's list of documents, whole, and its annual reports and amendments in
                      `market.edinet_document`; `file_edinet_filings` files a mapped company's
                      reports into `security_filing`. EDINET has no per-company listing, so a day's
                      list is how a filing is discovered.
  `raw_edinet_filings`, `edinet_segment`
                      each filer's report archives, whole, and the splits its segment note states,
                      read against the report's own tagged figures (`facets.segment_note`).

The note is a text block of HTML, not dimensioned XBRL, so the reader is driven by
`market.segment_note_rule`. Design: umbrella docs/specs/2026-11-26-more-segment-sources.md.
"""
