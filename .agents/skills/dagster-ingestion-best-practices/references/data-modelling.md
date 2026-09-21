# Data model

Choose the model that will still be right in a year over the smallest change. A migration is paid
once; a bad model is paid on every query, every guard and every UI section that reads it.

## Layers

| Layer | Where | Shape |
|---|---|---|
| raw | Parquet on `/mnt/data/ingest/raw` | the provider's answer ([raw-layer.md](raw-layer.md)) |
| core | `market.*` tables | normalised (3NF): one fact in one place, grain declared |
| serving | `market.*` views and matviews the app reads | shaped for the reader; denormalised is fine |

## Core

- **Declare the grain** in one sentence per table — "one row per security per trade date" — in the
  spec and as a table comment. A column that does not depend on the whole key belongs elsewhere.
- **Identity:** surrogate `security_id`; identifiers in their own table by kind and value. Never key a
  security on a placeholder (`<cusip>000000000</cusip>` collapsed four companies into one) or on an
  issuer-level id (an LEI is shared by share classes).
- **No polymorphic keys** (a `scope_id` that is sometimes a symbol and sometimes a sector), no
  sampling concept in a primary key, no pipeline state on a fact table — per-subject ingestion state
  belongs to Dagster.
- **Lookups are control tables with foreign keys** (`data_source`, `return_period`, `currency`), never
  CHECK lists or code constants. Editorial choices are rows, so they change without a deploy.
- **Units are schema:** money carries `currency_code`; a `_pct` column holds a percent, never a
  fraction (convert at the boundary); `period_type` is part of the key wherever an annual period and
  a Q4 share an end date.
- **Time:** valid time (`trade_date`, `period_ending`, `as_of`) is data. Where restatements matter —
  statements, metrics, segments, fundamentals — observed time (`observed_at`, `superseded_at`) is
  appended, never overwritten, and serving views pick the latest.
- **Constraints in the database:** primary keys, foreign keys, CHECKs. `NOT NULL` only after measuring
  the population it would refuse; otherwise nullable plus an asset check counting the nulls.
- **Indexes:** the primary key only, until a query plan asks for another.
- **Large time series** are range-partitioned by year.

## Serving

- The app reads views, so views are the compatibility boundary: tables underneath can change without
  a UI release.
- One definer per view; `create or replace view` can only append columns.
- Time every view **as `anon`** (3 s statement timeout) with the filter conjunctions the app actually
  sends — one filter at a time proves nothing. Whole-table readers get a matview whose refresh is an
  asset.
- Recreating a matview needs its unique index (for `refresh … concurrently`) and its grants re-issued.

## Changing a model — expand and contract

1. **Expand:** a migration creates the new tables beside the old; nothing reads them.
2. **Fill:** backfill from raw, or write both through Dagster.
3. **Verify:** parity, with every difference explained.
4. **Switch:** redefine the serving views on the new tables with the same names and columns, or ship
   the muffin-ui PR that adopts a new surface. **Never before a RUNNING lane fills the new base.**
   `market.untracked_listing` was re-pointed onto `venue_listing` on 2026-09-13 and deployed on
   09-17 while the OpenFIGI sweep that fills it had never run — its sensor ships STOPPED — so the
   view returned **0 rows against the old table's 148,782** and the Markets search was dead for
   three days with nothing reporting it. A migration can only see that the table exists. Before
   switching, check the lane's materializations, not its code.
5. **Keep the old tables** as a backup; stop writing them.
6. **Contract later:** a deferred note carrying the drop date and the query that proves nothing reads
   them.

## Migrations

- Declarative: edit `stack/supabase/schemas/`, generate with `supabase db diff -f <name>`. A data
  repair goes through `market.one_shot`, not a statement that re-runs.
- `ingest_rw` needs an **RLS policy** permitting the write *and* the grant — two independent gates;
  a correct grant can hide a missing policy.
- Seed every control row a writer depends on (a new `source_code`) in the same migration.
- A new table or view is invisible over PostgREST until its schema cache reloads — the deploy signals
  it.
