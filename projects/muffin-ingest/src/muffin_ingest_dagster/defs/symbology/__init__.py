"""The identity ladder: OpenFIGI and Yahoo evidence per security → adopted symbols.

LANE SHAPES, FROM THE PARTITION TABLE:
  the ladder is a BATCH OF SUBJECTS WE CHOOSE → one dynamic partition per `security_id`, each
  materialisable whether the provider answered or had nothing. THE GRID IS THE QUEUE:
  unmaterialised = never asked; materialised with a probe hit/miss = asked; and a THROTTLED
  subject is NOT materialised at all — recording a throttle as a miss is the 8,300-security
  incident, one security at a time.

THREE EVIDENCE RUNGS — `raw_figi_ticker` (OpenFIGI's US lookup, the SEC-usable ticker),
`raw_figi_local_symbol` (OpenFIGI unfiltered, for the local line), `raw_yahoo_symbol` (Yahoo's ISIN
search). Each stores the provider's response whole, keyed to its subject by a `position` column —
the mapping response is POSITIONAL, so a body that covers ten ISINs still answers "what did the
provider say about MY isin" per partition.

`security_symbology` resolves one security's materialised rung files onto `security_identifier`,
`security_provider_symbol` and `identifier_probe` in one transaction — the registries exception,
repeated for the same reason: a hit and a miss are the same run's observations and must land
together, and neither is a permission boundary.

RE-ASK IS THE SENSOR'S JOB: a materialised MISS partition is deleted once its probe is
`REASK_AFTER` days, so the next sensor tick re-seeds it. This is the grid-is-the-queue behaviour
proven the design way — day 31 comes back — without a custom `AutomationCondition`.
"""
