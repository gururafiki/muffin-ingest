# Planning an ingestion change

Work through the stages in order, writing each result into the spec as you go. At **every** stage:
present at least two options with trade-offs and a recommendation, ask about anything you are not
sure of, and record the decision together with the options rejected. A stage that does not apply
gets one sentence in the spec saying why.

The spec lives at `docs/specs/<yyyy>-<mm>-<dd>-<name>.md` in the umbrella (template at the end). Plan
the next phase deeply and later phases as headings in `todos.md`.

## 1. Review the existing ingestion

**Do**
- Read what exists: the `market-refresh` handler, its `pending_*` view, `%_missing_at` and cursor
  columns, its `cron_resource` row; Dagster assets already touching the same tables; the family's
  lessons in CLAUDE.md and earlier specs.
- Measure production, with dates: row counts, `coverage_sample` for the affected facets, backlog depth
  and drain, empty/error rates, freshness.
- List every reader: muffin-ui hooks and views, Grafana panels, `market-verify` checks, matviews,
  other assets.

**Ask** what the data is for, what "done" means, what must not break.
**Spec:** *Current state* — measurements and the reader list.

## 2. Architect the data model

**Do** — for every table, extend or replace ([data-modelling.md](data-modelling.md)): grain, keys
and identity, units and currency, valid vs observed time, constraints, serving views, size from a
measured bytes-per-row, RLS policy and grants for `ingest_rw`, control-table seeds.
**Options** — extend in place vs a new table beside the old; view vs matview; append (point in time)
vs overwrite.
**Spec:** DDL sketch, one-sentence grain per table, migrations, how readers are re-pointed, what
becomes of the old tables.

## 3. High-level architecture

**Do** — draw the asset graph by stage (raw → core → derived → serving) and fill one row per asset:

| asset | stage | partitions | backfill policy | pool | automation | writes | checks | freshness | project |
|---|---|---|---|---|---|---|---|---|---|

Choose partitions by the provider's request grain and I/O manager vs resource by the rule in
[dagster-native.md](dagster-native.md). Name every job, schedule and sensor — names are state.
**Options** — partition schemes; schedule vs sensor vs eager; one lane vs a cross-section lane plus a
history lane.
**Spec:** the graph and the table.

## 4. Low-level design

**Do** — for each asset:
- the exact call — openbb route (with its router *and* provider extension) or direct endpoint;
  parameters; batching; how many **vendor** requests per subject;
- modules, functions and classes with one responsibility each — library `providers/<vendor>.py`
  (fetch, classify, spell), `facets/<family>.py` (parse, normalise), `derive/`; project asset bodies
  stay thin;
- the raw schema — provider fields untouched, plus each added column and the rule that allows it;
- outcome classification, isolation, and the control subject that proves the provider healthy;
- run width from a memory estimate; retries and cooldown;
- metadata emitted; idempotency key (upsert conflict or replaced scope); where every date comes from.

**Spec:** module/function table, raw schema, metadata keys.

## 5. Validate against reality

**Do** — call the provider before writing a parser:
- subjects that should work, subjects that should **fail**, one the provider has nothing for, and one
  whose response has a different shape (a single item vs a batch);
- capture whole bodies as fixtures (`tests/fixtures/<provider>/` plus a re-capture note);
- measure payload size, latency, rate limit and quota, paging, timezone and live-quote semantics,
  identifier spelling;
- confirm keys, licence and terms; openbb router + provider installed; the three http-cache wiring
  points for a direct provider; arm64 wheels.

Never probe an unbounded catalogue against shared production services — bound it or run it locally.
**Spec:** a *Measured* table with dates, the fixture list, and anything that changed the design.

## 6. Dependencies and isolation

**Do** — for every new dependency: why, licence (compatible with AGPL-3.0), arm64 wheel, size, the
pins it brings. Pick the lowest rung of the isolation ladder that works
([dagster-native.md](dagster-native.md#dependency-isolation)).
**Spec:** dependency table and the isolation decision.

## 7. UI changes

**Do** — which `market` views the app reads change, and with which filters; redefine in place (no UI
release) or add a surface (a muffin-ui PR); the column contract; anon 3 s latency for the conjunctions
the app sends (`check_anon_read_latency.py`); labels — currency, SAMPLE, freshness; release order so
neither half ships alone and broken.
**Spec:** UI impact table and PR order.

## 8. Metrics and alerts

**Do** — separate what Dagster already shows (runs, materializations and their metadata, partition
status, checks, freshness, backfills) from what it does not: provider requests and throttles by
outcome (the worker's exporter), rows written including zeros, coverage by dimension, partition drain
over time, raw bytes on disk, run duration and memory, serving latency. Extend an existing Grafana
dashboard before adding one (`muffin-grafana-dashboards`); alerts are Grafana email.
**Spec:** metric → source → panel → alert.

## 9. Review for gaps

**Do** — walk the blast radius: the app, the anon latency guard, the coverage model, Grafana samples,
`market-verify`, matviews, PostgREST's schema cache. Walk the definition of done: cache wiring, panel,
guard, UI, docs. Then rollback, retiring the old path, and deferred notes. Resolve every open question
with the user.
**Spec:** *Risks and rollback*, *Deferred*, *Open questions* — the last one empty before
implementation starts.

## Spec template

```markdown
# <Family or change> — design

Status: DRAFT | APPROVED <date>. Extends / supersedes: <links>.

## Context            why, what prompted it, the intended outcome
## Current state      measured <date>; readers
## Decisions          per stage: chosen, rejected, why
## Data model         grain, DDL sketch, migrations, re-pointing
## Architecture       asset graph and asset table
## Low-level design   calls, modules and functions, raw schema, outcomes, metadata
## Validation         measured facts with dates; fixtures
## Dependencies       table and isolation decision
## UI impact
## Observability      metrics, panels, alerts
## Rollout            the implementation stages for this change
## Risks and rollback
## Deferred           links to docs/deferred notes
## Open questions
```
