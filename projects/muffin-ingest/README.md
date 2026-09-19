# muffin-ingest (Dagster code location)

Assets, checks, schedules and sensors over `muffin-ingest-lib`, autoloaded from
`src/muffin_ingest_dagster/defs/`: one package per family plus `platform/`. One image runs all
three Swarm services (code location, daemon, webserver).

```bash
uv sync                                     # the project env, with the library as an editable path
export DAGSTER_HOME=$PWD/../../deployments/local/dagster_home
uv run dagster definitions validate -m muffin_ingest_dagster.definitions
uv run pytest
```

From the workspace root, `dg list defs` and `dg check defs` ask the same questions through `dg`.
Conventions and the stage layout: `.agents/skills/dagster-ingestion-best-practices`.
