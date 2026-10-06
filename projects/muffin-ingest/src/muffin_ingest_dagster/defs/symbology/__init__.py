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

RE-ASK IS AN AUTOMATION CONDITION, `ReAskAfter` in `conditions.py`: a materialised partition is
requested again once its miss is `REASK_AFTER_DAYS` old and the day is the subject's own day of a
`REASK_SPREAD_DAYS` cycle, or once its held symbol dies. It used to DELETE the partition so the
sensor re-seeded it, and that deleted from the price lane's grid as well (2026-09-20).
"""
