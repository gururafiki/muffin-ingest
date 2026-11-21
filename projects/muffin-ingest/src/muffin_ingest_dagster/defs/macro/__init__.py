"""The macro family: the catalogued series of `market.macro_indicator`, from openbb's providers.

  `raw_macro_series`   stage 1, one partition per catalogued series: openbb's rows for the last 400
                       days, kept whole; the newest answer replaces, an empty one replaces nothing
  `macro_observation`  stage 2: `market.macro_observation`, retracting within each answer's window

Replaces the edge's `macro-indicators` (Phase 6b, umbrella
docs/specs/2026-11-21-macro-and-events.md).
"""
