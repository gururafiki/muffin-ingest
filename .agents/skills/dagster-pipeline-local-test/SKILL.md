---
name: dagster-pipeline-local-test
description:
  Use when running a muffin-ingest lane locally before shipping it — standing up Postgres for the
  pipeline, applying the committed migrations, materialising one partition of a tiny subset, and
  reading what it wrote. Also the way to find out what a database rebuilt from what is committed
  actually does.
license: AGPL-3.0-or-later
metadata:
  author: muffin
  version: '1.0.0'
---

# Run a lane locally, on a tiny subset

Implementation stage 3: **a local run on a tiny subset, before the PR.** It is not a substitute for
the live run (stage 5) — it is what stops the live run being the first time real data moves through
the code.

**Postgres runs in DOCKER, never installed on the host.** `scripts/local_stack.sh` beside this file
does the whole database half; it takes `up`, `roles`, `fix`, `apply`, `down`.

## 1. A database, built the way a rebuild builds one

```bash
scripts/local_stack.sh up      # container + WAIT ON A REAL QUERY
scripts/local_stack.sh roles   # ONLY the roles the Supabase image itself creates
scripts/local_stack.sh apply   # muffin-deployment/stack/supabase/migrations/*.sql, in order
```

- **`pg_isready` and `psql -c 'select 1'` both lie during init** — postgres runs a TEMPORARY server
  on the same socket, so both succeed before the real one is up. Wait on a query against the real
  database; the script does.
- **`docker exec` needs `-i` when the SQL arrives on stdin and `< /dev/null` when it is in `-c`.**
  Both directions fail silently: without `-i` a heredoc runs nothing, and with `-i` inside a script
  that is itself on stdin, psql eats the rest of the script.
- **Never pass `--platform`** unless you mean it. Forcing `linux/amd64` on an arm64 daemon makes
  docker fetch an image it cannot run, and it looks like a hang, not an error.

`apply` stopping is a RESULT, not a broken harness. Measured 2026-09-19: without `ingest_rw` and
`metrics_ro` the **baseline itself** refuses (`role "metrics_ro" does not exist`), which is the
whole content of `docs/deferred/2026-09-19-a-rebuilt-database-has-no-ingest-or-metrics-role.md`.
`scripts/local_stack.sh fix` creates them.

## 2. Seed only what the ingest learns at runtime

Some control tables are populated by the pipeline, not by a migration — `market.currency` is the
documented one — so a fresh database has none and a lane asks about nothing **while reporting
success**. Seed a handful by hand:

```sql
insert into market.currency (code, name) values ('USD','US Dollar'),('EUR','Euro'),('GBP','Pound')
on conflict (code) do nothing;
```

**And some are authored reference data the baseline dropped, which is a defect rather than a
fixture.** `market.data_source` is 0 locally against 24 in production, `index_scope` 0 against 73 —
see `docs/deferred/2026-09-19-a-rebuilt-database-has-no-reference-data.md`. Until that closes, a
local run needs the row the lane writes (`insert into market.data_source values ('yfinance', …)`),
and hitting the foreign key is the harness working:

```
ForeignKeyViolation: insert or update on table "fx_rate" violates "fx_rate_source_code_fkey"
DETAIL:  Key (source_code)=(yfinance) is not present in table "data_source".
```

## 3. Materialise one partition

```bash
cd projects/muffin-ingest
export DAGSTER_HOME=$PWD/../../deployments/local/dagster_home
export INGEST_DATABASE_URL="postgresql://postgres:muffin-local@localhost:55432/muffin"
export MUFFIN_RAW_ROOT=/tmp/muffin-raw            # default is /var/lib/muffin-ingest/raw

uv run dagster asset materialize --select ledger_health -m muffin_ingest_dagster.definitions
uv run dagster asset materialize --select raw_fx_spot --partition 2026-09-18 \
  --config-json '{"ops": {"raw_fx_spot": {"config": {"limit": 3}}}}' \
  -m muffin_ingest_dagster.definitions
uv run dagster asset materialize --select fx_rate --partition 2026-09-18 \
  -m muffin_ingest_dagster.definitions
```

- **`dagster asset materialize`, not `dg launch`** — in 1.13.22 `dg launch --help` prints six lines
  and does not take a config.
- **The config key is the OP name**, which equals the asset name: `{"ops": {"<asset>": {"config":
  {...}}}}`. Every lane's `Config` carries a `limit` precisely so a local run is three subjects
  rather than twelve thousand.
- Run stage 1 and stage 2 as SEPARATE commands. That is the two-stage split being real: re-running
  stage 2 costs no provider request, and if it needs one, something has leaked upstream.
- It runs the asset in a subprocess exactly as production does, which is how the
  `PROMETHEUS_MULTIPROC_DIR` exporter defect was reproduced locally.

## 4. Read what it wrote — three places, not one

```bash
find "$MUFFIN_RAW_ROOT" -name '*.parquet'        # stage 1: one file per partition
docker exec -i -e PGPASSWORD=muffin-local muffin-local-pg psql -U postgres -d muffin -tAF'|' <<'SQL'
select as_of, count(*), string_agg(currency_code || '=' || round(usd_per_unit::numeric,4), ' ') 
  from market.fx_rate group by 1 order by 1;
SQL
```

1. **The counters in the run's metadata** — sum the outcome counters against `subjects`. A gap is a
   branch that forgot to count, not a quiet day.
2. **The Parquet file exists and is the right size.** A run that materialised a partition while
   writing nothing is the failure this whole design is about.
3. **The rows in Postgres, counted per date.** A `single_run` range attaches the RUN's metadata to
   every partition in it, so metadata cannot tell you which day is empty.

A successful run is not a correct one: the 2026-09-18 FX run above published EUR 1.1476, GBP 1.3358,
JPY 0.0064 USD per unit — plausible is the point, and a JPY near 1.0 would have been a units defect
the exit code could never show.

## 5. Tear it down

`scripts/local_stack.sh down`. Leave nothing running on the host.
