"""FX rates, in the same two lanes as prices — because the question splits the same way.

  LANE A  `raw_fx_spot`     DAILY partitions.
          Materialising a partition claims the whole cross-section of currencies was collected for
          that day. Nothing in `fx_rate` can answer that on its own: a currency with no row for
          Tuesday looks identical whether the pair is unquoted, the fetch failed, or nothing ran.

  LANE B  `raw_fx_history`  one partition PER CURRENCY.
          Ten years of weekly closes. The subject IS the slice, `min(as_of)` answers "is this one
          loaded", and a newly tracked currency costs one partition rather than a re-fetch of all
          43.

IT IS THE SAME SHAPE AS THE PRICE FAMILY ON PURPOSE. Forty-three currencies is small enough that one
lane would work, and building it the other way would make the standard something that holds only
when it is convenient. The parts that genuinely differ are the two that carry the domain: the
plausibility band, and subunits being DERIVED rather than fetched.
"""
