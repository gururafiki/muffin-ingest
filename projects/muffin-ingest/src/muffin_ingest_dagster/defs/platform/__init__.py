"""The code location itself: the ledger heartbeat and the automation sensor.

STORAGE RETENTION USED TO LIVE HERE AND WAS RETIRED 2026-09-19. `prune_dagster_storage` deleted runs
older than 90 days along with their events — and `get_materialized_partitions` reads exactly those
events, so it was deleting the partition grid, which the price lane now uses as its record of what
has been collected. Measured before removing it: the steady state is ~1 MB/day, so the job was
reclaiming about 365 MB a year to do that, and Dagster's partial index on
`(asset_key, dagster_event_type, partition, id)` serves the grid query whatever the row count.

Dagster OSS prunes nothing by itself — `dagster.yaml`'s `retention:` covers schedule, sensor and
auto-materialize TICKS only — so this is a deliberate unbounded table. Revisit past ~20 GB, and
prefer "keep the newest materialization per (asset, partition), prune the rest by age" over a flat
age cut: status needs exactly one row per partition, so that rule is flat forever where an age cut
is not. See `docs/specs/2026-09-19-partitioning-to-the-provider-grain.md` in the umbrella.
"""
