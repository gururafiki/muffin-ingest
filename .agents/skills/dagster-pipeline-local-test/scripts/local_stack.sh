#!/usr/bin/env bash
# Local Dagster harness — Postgres in DOCKER, never a host install.
#   up      start the container and wait on a REAL query (pg_isready lies during init)
#   roles   create only the roles the Supabase image itself creates
#   apply   apply muffin-deployment's committed migrations/ in order, stopping on the first error
#   fix     create ingest_rw + metrics_ro
#   down    remove the container and its volume
set -euo pipefail
NAME=muffin-local-pg
PORT=55432
PASS=muffin-local
# The committed migrations live in the muffin-deployment submodule. This skill is committed TWICE —
# canonically in muffin-ingest and mirrored at the umbrella root — so the two copies sit at
# different depths and a fixed number of `..` is right for exactly one of them. Walk UP until the
# directory is found instead, and resolve it only where it is needed, so `down` still works from
# anywhere.
find_migrations() {
  if [ -n "${MUFFIN_MIGRATIONS:-}" ]; then echo "$MUFFIN_MIGRATIONS"; return 0; fi
  local d
  d=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  while [ "$d" != "/" ]; do
    if [ -d "$d/muffin-deployment/stack/supabase/migrations" ]; then
      echo "$d/muffin-deployment/stack/supabase/migrations"; return 0
    fi
    d=$(dirname "$d")
  done
  echo "no muffin-deployment/stack/supabase/migrations above $(dirname "${BASH_SOURCE[0]}")" >&2
  echo "set MUFFIN_MIGRATIONS to point at it" >&2
  return 1
}
psql_() { docker exec -i -e PGPASSWORD=$PASS $NAME psql -U postgres -d muffin "$@"; }

case "${1:-}" in
up)
  docker rm -f $NAME >/dev/null 2>&1 || true
  docker run -d --name $NAME -e POSTGRES_PASSWORD=$PASS -e POSTGRES_DB=muffin \
    -p $PORT:5432 postgres:17-alpine >/dev/null
  # WAIT ON A REAL QUERY. postgres runs a TEMPORARY server on the same socket during init, so
  # pg_isready and even `psql -c 'select 1'` on the default db succeed before the real server is up.
  for i in $(seq 1 60); do
    if psql_ -tAc 'select 1' >/dev/null 2>&1; then echo "postgres up after ${i}s"; exit 0; fi
    sleep 1
  done
  echo "postgres never answered a query"; docker logs --tail 20 $NAME; exit 1 ;;
roles)
  # Exactly what the Supabase image's own initdb supplies — and deliberately NOT ingest_rw or
  # metrics_ro, which is the whole question.
  psql_ -v ON_ERROR_STOP=1 -q <<'SQL'
do $$ begin
  create role anon nologin noinherit;
  create role authenticated nologin noinherit;
  create role service_role nologin noinherit bypassrls;
  create role supabase_admin login password 'x' superuser createrole createdb replication bypassrls;
  create role authenticator login password 'x' noinherit;
  create role supabase_auth_admin login password 'x' createrole;
  create role supabase_storage_admin login password 'x' createrole;
exception when duplicate_object then null; end $$;
SQL
  psql_ -tAc "select rolname from pg_roles where rolname not like 'pg\_%' order by 1" | tr '\n' ' '; echo ;;
fix)
  psql_ -v ON_ERROR_STOP=1 -qc "create role ingest_rw nologin; create role metrics_ro nologin;"
  echo "created ingest_rw, metrics_ro" ;;
apply)
  MIG=$(find_migrations) || exit 2
  for f in $(ls "$MIG"/*.sql | sort); do
    printf '%-52s ' "$(basename "$f")"
    if out=$(psql_ -v ON_ERROR_STOP=1 -q < "$f" 2>&1); then echo OK
    else echo "FAILED"; echo "$out" | grep -E 'ERROR|LINE' | head -3; exit 1; fi
  done
  echo "every committed migration applied" ;;
down) docker rm -f $NAME >/dev/null 2>&1 && echo "removed $NAME" ;;
*) echo "usage: $0 up|roles|apply|fix|down"; exit 2 ;;
esac
