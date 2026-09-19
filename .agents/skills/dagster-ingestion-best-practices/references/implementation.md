# Implementing an ingestion change

Stages in order. Each one ends with evidence — a test, a number read off a run, a row in the
database — never with "should work".

## 1. Components

1. **Schema, additive.** Edit `muffin-deployment/stack/supabase/schemas/`, generate with
   `supabase db diff -f <name>`. Seed every control row a writer needs (a new `source_code`) in the
   same migration; add the RLS policy and grants for `ingest_rw`. Nothing reads the new tables yet.
2. **Library** (`libs/muffin-ingest-lib`, no Dagster): the provider adapter (fetch returns what the
   vendor sent; classify outcomes), the facet parser and normaliser, derivations.
3. **Project** (`projects/muffin-ingest`): assets in `defs/<family>/` by stage, shared helpers from
   `muffin_ingest_dagster.lib`, a `code_version` on stage 2 and 3 assets, metadata counters on every
   asset, and `default_status` set in code for every schedule and sensor.

## 2. Tests

- **Library:** the rules over captured fixtures. When a rule could be subtly wrong, the fixture must
  make the right and wrong versions disagree — otherwise the test passes either way.
- **Dagster:** `dg.materialize([...], resources=fakes, partition_key=...)`; offline replay over
  captured bytes, asserting with DuckDB against the Parquet actually written; a multi-partition run
  through **both** seams (write and load); sensors with `build_sensor_context`.
- **SQL:** migration behaviour tests in muffin-deployment CI.
- **Static:** `dg check defs`, mypy for the library and the project, ruff; the definitions snapshot
  test changes only for a deliberate rename or addition.
- **Guards:** prove each one by deleting what it guards and watching it fail — and confirm the
  mutation actually applied, **reached the interpreter** (clear `__pycache__`, run with
  `PYTHONDONTWRITEBYTECODE=1`: a same-length edit within a second reuses the previous `.pyc`), and
  failed naming the object you changed.

## 3. Local run on a tiny subset

Against a throwaway Postgres with the migrations applied, raw written to a temporary directory, real
providers: `dg launch --assets <selection> --partition <key>` with config limiting the run to a few
subjects. Read every metadata counter, open the Parquet with DuckDB, query the rows it wrote. Skill:
`dagster-pipeline-local-test`.

## 4. Parity — only when replacing existing data

- Compare at a common anchor (the same trade date, the same period), with a tolerance, not equality.
- The gate is *every disagreement explained*. Adjudicate against the provider — the old pipeline may
  be the wrong one.
- Record the result and any unexplained residue in the spec.

## 5. Ship, then run live on a tiny subset

1. PR → wait for checks. Count them against a known-good PR (a conflicting PR runs no workflows at
   all) and read `statusCheckRollup`, not a watcher's exit code → squash merge → re-pin the submodule
   in the umbrella.
2. **Code** ships by image roll: wait for the image build of the merged sha, then `maintenance.yml`
   `roll-ingest`. **Schema and config** ship by `deploy.yml`. Details in `muffin-deploy`.
3. In the deployed Dagster, launch one partition or a handful of subjects; read the counters, the
   rows and the logs. Fix problems in the repo and ship again — never on the node by hand.

## 6. Turn it on and backfill

- Schedules and sensors RUNNING in code — including the automation sensor, if the family uses
  `AutomationCondition`.
- Launch the backfill over missing partitions. Run width is measured memory; the provider's pool
  paces it.
- Do **not** wait for it to drain. Write a deferred note: when to check, the query or panel, the
  expected drain rate, and what to do if it is flat.

## 7. Observability, UI, retirement

- Extend the dashboard chosen during planning (`muffin-grafana-dashboards`); panels read samples,
  never live backlog views; alerts live in Grafana.
- The UI change is a muffin-ui PR, with `check_anon_read_latency.py` covering the new conjunctions in
  the same change.
- Retire the old path: disable its `cron_resource` row, remove its handler and `pending_*` view. Old
  tables stay as a backup, with a deferred note carrying the drop date and the query that proves
  nothing reads them.

## 8. Docs, skills, review

- `docs/data-ingestion.md`, `docs/data-coverage.md`, measured lessons in CLAUDE.md, `todos.md`, the
  README.
- Rule 10: generalised feedback → memory and the relevant skill; a procedure that took iterations → a
  new skill.
- Review the diff; report what is done, what waits on a deploy or a drain, and what needs a decision.

## Deferred note

A file at `docs/deferred/<yyyy>-<mm>-<dd>-<name>.md` in the umbrella, plus one line in `todos.md`:

```
- [ ] due <yyyy-mm-dd> — <what> — docs/deferred/<file>
```

```markdown
# <What>

Created <date> · Due <date, or the event that triggers it> · Status: open

## Why it is deferred
## Context        spec, PRs, code pointers, dashboards, queries
## What to do     exact steps
## Done when      the measurement that closes it
```

Mark the `todos.md` line `[x]` with what was actually found, and set the note's status to done.
