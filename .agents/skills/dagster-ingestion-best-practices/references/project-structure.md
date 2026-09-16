# Project structure

A `dg` workspace, so a dependency that cannot share an environment becomes a new project rather than a
repo-wide move. Status: **target layout** — muffin-ingest is being migrated to it (umbrella
`todos.md` › Ingestion rework).

```
muffin-ingest/
  dg.toml                               the workspace; lists its projects
  deployments/local/pyproject.toml      environment for running dg across the workspace
  libs/muffin-ingest-lib/               package muffin_ingest — NO Dagster dependency
    pyproject.toml  uv.lock             extras per dependency group
    src/muffin_ingest/
      providers/<vendor>.py             fetch (what the vendor sent), classify, spell, batch size
      providers/openbb.py               the hub in-process; one typed function per route
      facets/<family>.py                parse and normalise raw into core rows (pure)
      derive/<thing>.py                 computations over data already held (pure)
      writers.py  ledger.py  metrics.py  settings.py
    tests/                              unit tests, run with no Dagster installed
    tests/fixtures/<provider>/          captured payloads and how to re-capture them — the one copy
    scripts/                            capture scripts
  projects/muffin-ingest/               one code location, `muffin_ingest`
    pyproject.toml  uv.lock  Dockerfile [tool.dg] project; the library as an editable path source
    src/muffin_ingest_dagster/
      definitions.py                    starts the exporter; load_from_defs_folder
      lib/                              io_managers.py  partitioned.py  resources.py — classes only
      defs/resources.py                 @dg.definitions — the resource bindings
      defs/platform/                    heartbeat asset and checks, storage retention, automation sensor
      defs/<family>/
        partitions.py                   PartitionsDefinitions and constants the family shares
        raw.py                          stage 1 assets (pool = the provider)
        core.py                         stage 2 assets (pool = sql)
        derived.py                      stage 3 assets
        checks.py                       asset checks
        automation.py                   jobs, schedules, sensors
    envs/<name>/                        Pipes venvs (pyproject + uv.lock), only when needed
    tests/<family>/                     asset, check and sensor tests; offline replay
  .agents/skills/                       dagster-expert (vendored, pinned) and this skill
  skills-lock.json                      source, tag and hash of vendored skills
```

Leave out files a family does not need. Importing an asset into `automation.py` for a job selection is
safe: the loader de-duplicates the same object. A *different* object with the same key is an error.

## What goes where

| Code | Place | Rule |
|---|---|---|
| An HTTP or openbb call, outcome classification, symbol spelling | `libs/…/providers/` | no Dagster; return what the vendor sent |
| Parsing, normalising, windowing, dedupe | `libs/…/facets/` | pure; runs in stage 2 over raw |
| Arithmetic over held data | `libs/…/derive/` | pure; dates come from the data, never the clock |
| Writing rows, conflict keys, chunking | `libs/…/writers.py` | the only route into Postgres |
| An asset, check, schedule, sensor or job | `projects/…/defs/<family>/` | thin: call the library, return rows and metadata |
| I/O managers, partition-seam helpers, resource classes | `projects/…/muffin_ingest_dagster/lib/` | shared by every family |
| Resource instances | `defs/resources.py` | one binding per resource key |
| A dependency that conflicts | `envs/<name>/` or a new project | [the isolation ladder](dagster-native.md#dependency-isolation) |

## Names are state

Materialization history is keyed by **asset key**; schedule and sensor state by **name and code
location**; dynamic partitions by the **partitions-definition name**. Renaming any of them silently
orphans history or resets a cursor.

- Never rename an asset, check, job, schedule, sensor, partitions definition, pool or resource key as a
  side effect. A deliberate rename is a migration with its own plan.
- Asset keys are nouns naming the dataset or table (`raw_price_bars`, `price_bar`), with no
  `key_prefix`.
- Family → `group_name`; stage → `kinds` plus a `layer` tag (`raw`, `core`, `derived`, `serving`);
  provider → `pool`.
- A definitions snapshot test lists every name, and changes only on purpose.
- No `from __future__ import annotations` in a Dagster module: it turns the `context` annotation into
  a string and validation rejects it with a message that does not name the cause.

## Commands

With `deployments/local`'s environment active, from the workspace root:

- `dg list defs` — what loads
- `dg check defs` — every definition loads without error
- `dg launch --assets <selection> --partition <key>` — a local run (`--partition-range "a...b"`)
- `dg dev` — the local UI for every project

New projects come from a pinned `uvx create-dagster@<version> project projects/<name>`, never by hand.

## Adding a family

1. Spec approved ([planning.md](planning.md)).
2. Library: `providers/<vendor>.py` if the vendor is new, `facets/<family>.py`, fixtures, unit tests.
3. `defs/<family>/` by stage, with pools and partitions from the spec and metadata counters on every
   asset.
4. Tests under `projects/muffin-ingest/tests/<family>/`; add the new names to the snapshot test.
5. Continue with [implementation.md](implementation.md) from stage 3.
