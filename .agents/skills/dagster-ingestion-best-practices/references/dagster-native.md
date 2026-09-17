# Dagster-native features, and muffin's rules for them

How an API works is `dagster-expert`'s job ([index](../../dagster-expert/SKILL.md)). This file says
which feature to reach for, and what muffin learned using it. Before relying on an API, check it
exists in the locked version: `python -c "import dagster; print(dagster.__version__)"` inside the
project environment.

## Need → feature

| Need | Use | Not |
|---|---|---|
| A table or raw dataset | `@dg.asset`, one per table; `@dg.multi_asset` only when one computation writes tables together | ops and jobs as the unit |
| Run on a clock | `ScheduleDefinition`, `build_schedule_from_partitioned_job` | pg_cron, cron offsets |
| Run after upstream changes | `AutomationCondition.eager()` **and** the automation sensor declared RUNNING in code — Dagster ships it stopped. Eager also needs **no upstream partition missing**, so it never fires for an unpartitioned asset over a partition set that is never complete (`security_return`; open: `docs/deferred/2026-09-17-security-return-never-auto-materialises.md`). Why it did not fire: `dagster.asset_daemon_asset_evaluations` | chained schedules |
| New subjects appear | a sensor issuing `AddDynamicPartitionsRequest` | backlog views |
| What was asked / what is missing | partition status; backfill over missing partitions | `pending_*` views, `%_missing_at` columns |
| One caller per provider | `pool="<provider>"` at limit 1 | mutex tables |
| Rate and daily quota | pacing inside the run; schedule × page size; a cooldown after a throttle | a distributed rate limiter |
| Data quality | `@dg.asset_check`, blocking where bad data must not propagate; partitioned checks for partitioned assets | separate verify scripts |
| Staleness | `FreshnessPolicy.time_window` or `.cron` on **scheduled** assets, not on backfill-only lanes | custom staleness queries |
| Re-run after a logic change | bump `code_version` → assets show Unsynced → backfill | parser-version columns |
| Transient transport failure | `RetryPolicy(max_retries, delay, backoff, jitter)` on the asset | retry loops in asset bodies |
| A provider throttling | stop asking, record `throttled`, cool down; `RetryRequested(seconds_to_wait=…)` only for a short stated wait | retrying into the limit |
| Runs that die | `run_monitoring` and `run_retries` in `dagster.yaml` — **not enabled yet**; evaluate before relying on either | assuming a killed run is visible |
| Per-run knobs (subset, limit) | `dg.Config` | environment-variable switches |
| Secrets | `dg.EnvVar` in resource config | `os.environ` in asset bodies |
| Run evidence | `MaterializeResult` metadata, plus `dagster/row_count`, `dagster/table_name`, `dagster/uri`, `dagster/column_schema` | log lines only |
| Lineage to what the app reads | `AssetSpec` for serving views, with `deps` on core assets (`is_virtual` once out of preview) | nothing |

## I/O managers or resources

Dagster's guidance: an I/O manager suits *load into memory → transform → write*. For SQL-managed
tables or data bigger than memory, use a resource and `deps`.

- **Raw:** `ParquetIOManager` — stage 1 returns rows or documents, stage 2 loads them.
- **Core rows:** `postgres_io` when one run's rows fit in memory; it applies dedupe, chunking under
  the 65,535-parameter limit, and `replace_scope` for every writer.
- **SQL derivations, matview refreshes, anything larger than a run's memory:** `deps=[…]` plus the
  `Postgres` resource.
- A backfill policy that gives a run several partitions changes the data shape at **both** seams:
  return `{partition_key: rows}` on write and accept a mapping on load
  (`partitioned.by_partition`, `partitioned.rows_per_partition`).

## Partitions follow the provider's request grain

| Request grain | The question the data cannot answer itself | Partition by |
|---|---|---|
| a period of a cross-section | did we collect period P? | time |
| a document | did we read document D? | dynamic, document id |
| a collection sweep | how far did we get through venue V? | venue, plus a cursor |
| one whole file | none — the file is the answer | nothing; schedule + freshness |
| a batch of subjects we choose | have we asked about S? | dynamic, subject id |
| one subject's whole history | the same | dynamic, subject id |

- Never date × subject; Dagster documents at most 100,000 partitions per asset.
- A source that cannot be asked about a past day stays unpartitioned; its consumers may still be.
- **Backfill policy:** time partitions → `BackfillPolicy.single_run()` (one batched sweep); subject
  partitions → `BackfillPolicy.multi_run(N)`, with N set by measured memory — nothing batches across
  subjects, since the vendor is asked per symbol anyway.

## Pools and rates

- One pool per provider, named after it. `dagster.yaml` has no per-pool map, so every pool is created
  on first use at `default_limit: 1` — a misspelled pool bounds nothing. Keep the pool names in a test.
- State budgets in the unit the **vendor** sees: openbb's yfinance adapter asks once per symbol, so
  batching lowers our call count, not theirs.
- Raising a pool above 1 makes in-run pacing N× looser — note it beside the pool.
- **`granularity: run` holds every pool a run names for the whole run.** Every stage-2 asset and the
  heartbeat share `sql`, so one long run queues every lane. Measured: the heartbeat ran 111 s late
  behind `daily_prices` (open: `docs/deferred/2026-09-16-sql-pool-run-granularity-blocks-every-lane.md`).
- Pace to what the vendor tolerates **per minute**, not per run. One observation each: a nightly run
  at ~42 calls/min was refused at call 329, and a backfill at ~14/min finished 601 calls. That is a
  starting point, not a known limit.

## Dependency isolation

A project is one process and one venv: everything under `defs/` is imported together, so two
conflicting dependencies cannot live in one project. Climb only as far as needed.

1. **One environment** — the default. Try pins or library extras first.
2. **Pipes, per asset.** The asset calls `PipesSubprocessClient` with the interpreter of a locked venv
   under `projects/<project>/envs/<name>/` (pyproject plus `uv.lock`, synced at image build, no
   network at run time). The script uses the library, writes raw to the path the I/O manager would use
   (or core rows through the library's writers), and reports metadata through Pipes. It gets no
   Dagster resources. Use `UvRunComponent` only after proving it runs offline against its lock.
3. **A new project in the workspace, per group of assets** — when the isolated code needs its own
   resources, sensors or schedules, the conflict is broad, or an image or licence boundary is wanted.
   Scaffold it with a pinned `create-dagster project projects/<name>`; it gets its own `uv.lock`,
   image, Swarm service and `workspace.yaml` entry. Assets keep their materialization history when
   moved (keyed by asset key); schedule and sensor state is keyed by code location, so re-check cursors
   after a move.

## Dagster state has a retention period

`prune_dagster_storage` deletes runs older than 90 days **along with their events**, and
`get_materialized_partitions` reads exactly those events. A partition materialized once will look
never-materialized after 90 days. Anything treating partition status as durable state must survive
this — open item in the umbrella: `docs/deferred/2026-09-16-dagster-pruning-erases-partition-state.md`.

## Upgrading Dagster

1. Bump the pins in the project's `pyproject.toml`; `uv lock`.
2. Re-vendor the official skill at the matching tag, then mirror it to the umbrella:
   `DO_NOT_TRACK=1 npx -p node@22.20.0 -p skills@<version> -- skills add "dagster-io/skills#v<dagster-version>" --skill dagster-expert -a universal -a claude-code -y`
   (the CLI needs Node ≥ 22.20; `-a universal` keeps the content in `.agents/skills` with a Claude
   symlink).
3. Read `MIGRATION.md` and `CHANGES.md` for removed APIs, and grep for them.
4. Compare `dagster/_core/storage/alembic/versions` between the two versions. New revisions mean
   `dagster instance migrate` must run before the roll.
5. The definitions snapshot test is unchanged; tiny subset locally; roll; live checks.
