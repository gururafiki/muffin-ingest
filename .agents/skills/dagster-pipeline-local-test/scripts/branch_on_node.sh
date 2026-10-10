#!/usr/bin/env bash
# Run a harness on the node against PRODUCTION, READ-ONLY, with THIS checkout's muffin-ingest source.
#
#   scripts/branch_on_node.sh path/to/harness.py
#
# The harness runs in a throwaway container from the image the code location is running, in the
# code location's network namespace (so `supabase-db` and `http-cache` resolve), with the branch's
# `muffin_ingest` and `muffin_ingest_dagster` ahead of the installed ones on PYTHONPATH and
# `readonly.py` beside them. Writes are the harness's job to intercept: use `readonly.SafePostgres`
# and `readonly.Capture` for every resource that writes.
#
# NOT inside the code-location container: its memory limit is shared with the gRPC server and every
# run, and a probe there was OOM-killed on 2026-10-04. This one gets its own 1 GB and is removed.
# The two secrets travel through the environment of the docker CLI, never its argv, and are never
# printed.
set -euo pipefail

harness=${1:?usage: branch_on_node.sh path/to/harness.py}
here=$(cd "$(dirname "$0")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)
# The umbrella mirrors this skill; from there, the source lives in its submodule.
[ -d "$root/libs/muffin-ingest-lib" ] || root="$root/muffin-ingest"

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
cp -R "$root/libs/muffin-ingest-lib/src/muffin_ingest" "$stage/"
cp -R "$root/projects/muffin-ingest/src/muffin_ingest_dagster" "$stage/"
cp "$here/readonly.py" "$stage/"
cp "$harness" "$stage/harness.py"
find "$stage" -name __pycache__ -type d -prune -exec rm -rf {} +

tar cf - -C "$stage" . | ssh muffin '
  set -e
  ING=$(docker ps -qf name=muffin_muffin-ingest)
  IMG=$(docker inspect --format "{{.Config.Image}}" "$ING")
  export INGEST_DATABASE_URL="$(docker exec "$ING" printenv INGEST_DATABASE_URL)"
  export HTTP_CACHE_URL="$(docker exec "$ING" printenv HTTP_CACHE_URL)"
  docker run --rm -i -m 1g --network "container:$ING" \
    -e INGEST_DATABASE_URL -e HTTP_CACHE_URL --entrypoint sh "$IMG" -c \
    "mkdir -p /tmp/b && tar xf - -C /tmp/b && cd /tmp && PYTHONPATH=/tmp/b python /tmp/b/harness.py"'
