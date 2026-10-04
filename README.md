# muffin-ingest

Market-data ingestion for [Muffin](https://github.com/gururafiki/muffin): a provider library and
the Dagster code location that runs it.

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

## The two parts, and why they are separate

- **`muffin_ingest`** is a plain Python library with no Dagster import anywhere in it. Providers
  know how to ask and how to classify an answer; facets turn an answer into rows; parsers turn a
  document into rows. It is testable without an orchestrator, and it outlives any choice of one.
- **`muffin_ingest_dagster`** is the thin layer that makes each table an asset, each provider a
  concurrency pool, and each production invariant an asset check.

## Where per-item state lives

Dagster's per-item primitive is partitions, documented at 100,000 per asset. Where a provider is
asked about one subject at a time, the asset is partitioned by that subject, so a partition is
exactly what one ask claims: the price history lane has one partition per security (~12,350).

What a provider said about a subject's IDENTIFIER is a different fact. A symbol the provider
rejected when asked alone, with a control symbol answering in the same run, is an observation in
`market.identifier_probe`, keyed by the symbol it was asked with. The askable population skips a
miss younger than 30 days only while the security still carries that symbol, so a corrected symbol
is asked again with no step to remember.

Until 2026-10-04 a task ledger in the database (`ingest.task`, `ingest.attempt`, `ingest.facet`)
held that state. It served only the day-partitioned price lane and left with it; dropping the
schema is a deferred step in `muffin-deployment`.

## Licence

**AGPL-3.0-or-later**, not GPLv3 like the rest of Muffin, because `openbb-core` is AGPL-3.0 and is
imported in-process rather than called over HTTP. That hop is where a throttled provider's answer
becomes an empty `204` indistinguishable from "this symbol has no data" — the confusion that has
repeatedly recorded a rate limit as thousands of permanently unanswerable securities.
