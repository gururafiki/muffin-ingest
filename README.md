# muffin-ingest

Market-data ingestion for [Muffin](https://github.com/gururafiki/muffin): a provider library, a
task ledger in Postgres, and Dagster assets over both.

It replaces the 7,891-line Deno edge function that ingests `market.*` today. The design, with the
measurements behind every decision, is
[`docs/superpowers/specs/2026-09-09-ingestion-rework-design.md`](https://github.com/gururafiki/muffin/blob/main/docs/superpowers/specs/2026-09-09-ingestion-rework-design.md)
in the umbrella repo.

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
absent, throttled or leased. Its only per-item primitive is partitions, which are bounded at about
25,000 per asset and meant for time windows. So the ledger is the one deliberately custom piece, and
it is three tables.

## Licence

**AGPL-3.0-or-later**, not GPLv3 like the rest of Muffin, because `openbb-core` is AGPL-3.0 and is
imported in-process rather than called over HTTP. That hop is where a throttled provider's answer
becomes an empty `204` indistinguishable from "this symbol has no data" — the confusion that has
repeatedly recorded a rate limit as thousands of permanently unanswerable securities.
