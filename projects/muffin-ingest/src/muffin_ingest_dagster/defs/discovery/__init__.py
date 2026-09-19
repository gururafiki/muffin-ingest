"""Discovery: where the universe comes from — SEC N-PORT filings, and the OpenFIGI venue sweep.

LANE SHAPES, FROM THE PARTITION TABLE:
  N-PORT   is a DOCUMENT per accession → a dynamic partition per `(cik, accession)`, sensor-seeded
           from EDGAR full-text search. Raw is the primary_doc.xml, byte for byte.
  SWEEP    is a COLLECTION per venue → a dynamic partition per `exch_code`, its cursor carried in
           the partition file (the last page's `next`). Raw is the `/v3/filter` pages, verbatim.

STAGE 2 IS THE ONLY PLACE THAT NARROWS. `facets/nport.py` and `facets/openfigi.py` run against
bytes already on disk, so a corrected parse costs a re-parse and never a re-fetch.

`discovered_security` writes THREE tables (security, security_identifier, issuer) through the
`Postgres` resource — the registries' deliberate exception to "assets return records". A filing
resolves onto all three at once, and they must land in one transaction: a security whose issuer is
FK'd but missing is as broken as an identifier pointing at a security that is not there. The
alternative — one postgres_io asset per table — commits them separately and leaves a half-filed
filing dangling.
"""
