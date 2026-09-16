---
name: dagster-ingestion-best-practices
description: >-
  Use when planning, designing, building, testing, deploying or reviewing data ingestion on Dagster
  in muffin-ingest — adding a provider or data family, migrating a resource off the market-refresh
  edge function, changing the market data model, or deciding partitions, backfills, sensors,
  schedules, I/O managers, asset checks or dependency isolation.
license: AGPL-3.0
metadata:
  author: muffin
  version: '2.0.0'
---

# Dagster ingestion in muffin

Raw is the provider's answer kept whole; everything after it is a re-runnable Dagster asset over a
model built for the long term; nothing is trusted until real data has been driven through it.

**REQUIRED BACKGROUND: `dagster-expert`** — Dagster's official skill, vendored beside this one and
pinned to the Dagster version the project locks. Read its reference for any Dagster or `dg` API;
never answer from memory. This skill is muffin's layer on top of it, and wins where they differ:

- **OSS, not Dagster+.** Ignore `dg api`, `dg plus`, the Dagster+ MCP server, alert policies, branch
  deployments and Insights. Alerts are Grafana; the deployed instance is reached as described in
  `muffin-reach-deployed-services`.
- **One self-hosted node.** Postgres run storage, runs are subprocesses of the code-location
  container, provider pools run at limit 1, code ships by image roll (`muffin-deploy`).

## Rules

1. **Dagster-native first.** Before writing machinery, find the feature
   ([dagster-native.md](references/dagster-native.md)). Build only what Dagster has no concept of,
   and say why in the spec.
2. **Model for the long term.** A wrong model is changed, not worked around: normalised core with a
   declared grain, serving through views. Replace a table expand/contract-style — new beside old,
   re-point readers, keep the old as a backup, deferred note to drop it
   ([data-modelling.md](references/data-modelling.md)).
3. **Raw first, as-is.** Stage 1 writes exactly what the provider sent to Parquet, converting only
   what Parquet requires — nothing dropped, cleaned, renamed, parsed, deduped or reshaped. A column
   may be *added* only if it is (a) a key mapping the answer to our model that cannot be derived,
   (b) a partition key that cannot be derived, or (c) metadata Dagster cannot hold. Credentials are
   redacted ([raw-layer.md](references/raw-layer.md)).
4. **Only stage 1 touches the network.** Re-running stage 2 or 3 after a logic change costs no
   provider request.
5. **Failed ≠ empty ≠ throttled ≠ dead subject.** Never record "the provider has nothing" unless the
   subject was asked alone and the provider was proven healthy in the same run.
6. **A materialized partition is a completeness claim.** Partition the question the data cannot
   answer about itself; never date × subject.
7. **Plan before code.** Work through the planning stages; at each, present options with trade-offs
   and a recommendation, and ask instead of assuming. Spec: `docs/specs/<yyyy>-<mm>-<dd>-<name>.md`
   in the umbrella.
8. **Prove it on real data twice** — a tiny subset locally, then a tiny subset live — and read every
   counter before scaling up.
9. **Ship through a PR** in deployable repos: PR → checks green → merge. Code ships by image roll;
   schema and config by deploy.
10. **Capture what you learn.** Generalised feedback → memory and the skill it concerns; a procedure
    that took iterations or failed first → a new skill; a later action → a `docs/deferred/` note plus
    a dated `todos.md` line.

## Planning — [planning.md](references/planning.md)

1. Review the existing ingestion and measure production
2. Architect the data model — extend or replace
3. High-level architecture — assets by stage, partitions, automation, pools, checks, freshness
4. Low-level design — calls, functions and their responsibilities, raw schema, outcomes, run width
5. Validate against the real provider; capture fixtures
6. Dependencies and isolation
7. UI changes the model implies
8. Metrics and alerts beyond Dagster's own
9. Review for gaps and resolve them with the user

## Implementation — [implementation.md](references/implementation.md)

1. Components — schema, library, then `defs/<family>/`
2. Tests over captured fixtures
3. Local run on a tiny subset
4. Parity against the data being replaced (migrations only)
5. PR → checks → merge → roll or deploy → live run on a tiny subset
6. Schedules and sensors on, backfill launched; do not wait for the drain — deferred note
7. Observability, UI, retirement of the old path
8. Docs, skills, review

## Where things go — [project-structure.md](references/project-structure.md)

```
muffin-ingest/                       dg workspace
  libs/muffin-ingest-lib/            muffin_ingest: providers, facets, derive, writers — no Dagster
  projects/muffin-ingest/            one code location
    src/muffin_ingest_dagster/
      definitions.py                 load_from_defs_folder
      lib/                           I/O managers, partition helpers, resource classes
      defs/resources.py              resource bindings
      defs/platform/                 heartbeat, storage retention, automation sensor
      defs/<family>/                 partitions.py raw.py core.py derived.py checks.py automation.py
    envs/<name>/                     Pipes venvs — only for a dependency that must be isolated
```

## Red flags

| Thought | Do instead |
|---|---|
| "Nothing reads this field, leave it out of raw" | Raw keeps it; stage 2 decides |
| "I'll parse the dates while fetching" | Stage 1 stores the provider's value; stage 2 parses |
| "A small table will track which subjects are done" | Partition status — dagster-native.md first |
| "The endpoint presumably returns X" | Call it, capture it, measure it (planning stage 5) |
| "CI is green, so ship it" | Tiny subset locally, then live, reading every counter |
| "The schema is awkward but it works" | Rule 2 — fix the model now, expand/contract |

## References

- [planning.md](references/planning.md) — the nine stages, what to ask, the spec template
- [implementation.md](references/implementation.md) — the stages, shipping, deferred notes
- [raw-layer.md](references/raw-layer.md) — what stage 1 may and may not do
- [data-modelling.md](references/data-modelling.md) — core and serving rules, expand/contract, migrations
- [dagster-native.md](references/dagster-native.md) — need → feature, I/O managers, partitions, isolation, upgrades
- [project-structure.md](references/project-structure.md) — layout, what goes where, names are state
- [pitfalls.md](references/pitfalls.md) — mistakes this pipeline already made, with measurements
