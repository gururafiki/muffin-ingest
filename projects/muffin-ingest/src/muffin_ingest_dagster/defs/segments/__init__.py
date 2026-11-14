"""The segments family (Phase 6): business lines from each regulator's own filings.

  `raw_sec_instance`  each SEC accounts filing's XBRL instance, by filer: the documents whole.
  `sec_segment`       their segments in `security_segment` (`sec-segments`), and the mark on
                      `security_filing` that the filing was read.

One parser for every regulator (`muffin_ingest.facets.segments`, ported from the edge), driven by
control tables. A filing is the unit of work and a filer is the partition: SEC alone has 226,000
accounts filings, more than a grid may hold. Which filings are left is `security_filing`'s own
marks, read per filer.
Design: umbrella docs/specs/2026-11-09-segments-and-regulators.md.
"""
