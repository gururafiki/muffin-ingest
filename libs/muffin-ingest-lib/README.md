# muffin-ingest-lib

The ingestion library: providers (how to ask a vendor and how to classify its answer), facets
(parsing raw into core rows), derivations, the `ingest` task-ledger client, writers, settings and
the metrics exporter.

**It imports no orchestrator.** The CI `checks` job installs this package with no Dagster present,
which is what proves that rather than a comment asserting it. The Dagster code location that calls
it lives in `../../projects/muffin-ingest`.

```bash
uv sync                      # runtime + dev, no hub
uv sync --extra hub          # adds openbb-core and the providers/routers the lanes call
uv run pytest                # the unit suite, over captured fixtures in tests/fixtures
```
