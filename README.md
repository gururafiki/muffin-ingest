# muffin-ingest

Market-data ingestion for [Muffin](https://github.com/gururafiki/muffin): a provider library, a
task ledger in Postgres, and Dagster assets over both.

It replaces the 7,891-line Deno edge function that ingests `market.*` today. The design, with the
measurements behind every decision, is
[`docs/superpowers/specs/2026-09-09-ingestion-rework-design.md`](https://github.com/gururafiki/muffin/blob/main/docs/superpowers/specs/2026-09-09-ingestion-rework-design.md)
in the umbrella repo.

## Layout

A **`dg` workspace**, so a dependency that cannot share an environment becomes a new project rather
than a repo-wide move. Each package locks its own environment with `uv`.

```
dg.toml                       the workspace
deployments/local/            the environment `dg` runs in, and DAGSTER_HOME for local runs
libs/muffin-ingest-lib/       muffin_ingest — the library, with NO Dagster dependency
projects/muffin-ingest/       the code location + Dockerfile; src/muffin_ingest_dagster/defs/<family>/
```

```bash
# The library: unit tests, no orchestrator installed
cd libs/muffin-ingest-lib && uv sync && uv run pytest

# The code location: loads, checks and asset tests
cd projects/muffin-ingest && uv sync
export DAGSTER_HOME=$PWD/../../deployments/local/dagster_home
uv run dagster definitions validate -m muffin_ingest_dagster.definitions
uv run dg check defs && uv run pytest

# The image (built from the workspace root, as CI does)
docker build -f projects/muffin-ingest/Dockerfile -t muffin-ingest:local .
```

Conventions — what belongs in which stage, and why a rename is a migration — are in
`.agents/skills/dagster-ingestion-best-practices`.

## The three parts, and why they are separate

- **`muffin_ingest`** is a plain Python library with no Dagster import anywhere in it. Providers
  know how to ask and how to classify an answer; the ledger knows what to ask next and what an
  answer means; parsers turn a document into rows. It is testable without an orchestrator, and it
  outlives any choice of one.
- **`muffin_ingest_dagster`** is the thin layer that makes each table an asset, each provider a
  concurrency pool, and each production invariant an asset check.
- **The ledger** (`ingest.task`, `ingest.attempt`, `ingest.facet`) lives in the database and is
  defined by a migration in `muffin-deployment`, because the schema belongs with the rest of the
  schema.

## Why the orchestrator does not own the queue

Dagster manages runs, schedules, retries, checks and freshness, and this repo uses it for all of
them. What it does not model is per-ITEM state — which of ~12,350 securities × ~40 facets is due,
absent, throttled or leased. Its only per-item primitive is partitions: Dagster documents 100,000
per asset, which is why a subject grid is the right shape for one facet's collection and still
cannot carry 12,350 × 40 of them, nor the leasing, backoff and absence rules around each. So the
ledger is the one deliberately custom piece, and it is three tables.

## Licence

**AGPL-3.0-or-later**, not GPLv3 like the rest of Muffin, because `openbb-core` is AGPL-3.0 and is
imported in-process rather than called over HTTP. That hop is where a throttled provider's answer
becomes an empty `204` indistinguishable from "this symbol has no data" — the confusion that has
repeatedly recorded a rate limit as thousands of permanently unanswerable securities.
