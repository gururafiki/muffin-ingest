#!/usr/bin/env bash
# Gate a reader change: run one harness over documents already on the node, twice, once with the
# library the code location is RUNNING and once with THIS checkout's, then compare per document.
#
#   scripts/gate_reader.sh path/to/harness.py /node/dir/one [/node/dir/two ...]
#   ssh muffin 'cat /tmp/claude-gate/DONE'                              # when both have finished
#   ssh muffin 'python3 /tmp/claude-gate/gate_compare.py /tmp/claude-gate/out'
#
# Each document directory is mounted read-only at /docs/<its basename>, so the comparison groups by
# it. The harness (see example_cninfo_gate.py) takes one argument, the label (`base` or `new`), and
# writes /work/gate_<label>.jsonl, one line per document: {"doc", "status", "facts", ...}.
#
# THE BASE IS THE RUNNING IMAGE, not `main`: a change merged and not yet rolled is in neither side's
# library, and a gate against it would report that change as part of yours. Roll first, or say so.
#
# It runs DETACHED on the node (two throwaway containers in parallel, 1.5 GB each, no network), so a
# dropped ssh session or this machine sleeping cannot kill it. 854 CNINFO PDFs took ~30 minutes.
# Nothing is fetched and nothing is written outside /tmp/claude-gate. PYTHONDONTWRITEBYTECODE=1,
# because a mutated-then-restored source of the same size can outlive its restore as bytecode.
set -euo pipefail

harness=${1:?usage: gate_reader.sh harness.py /node/docs/dir [...]}
shift
[ "$#" -gt 0 ] || { echo "name at least one document directory on the node" >&2; exit 2; }
here=$(cd "$(dirname "$0")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)
[ -d "$root/libs/muffin-ingest-lib" ] || root="$root/muffin-ingest"   # the umbrella's mirror

D=/tmp/claude-gate
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/libnew"
cp -R "$root/libs/muffin-ingest-lib/src/muffin_ingest" "$stage/libnew/"
find "$stage" -name __pycache__ -type d -prune -exec rm -rf {} +
cp "$harness" "$stage/gate.py"
cp "$here/gate_compare.py" "$stage/"

mounts=""
groups=""
for dir in "$@"; do
  group=$(basename "$dir")
  case " $groups " in *" $group "*)
    echo "two document directories are both named '$group'; link one under another name" >&2
    exit 2;;
  esac
  groups="$groups $group"
  mounts="$mounts -v $dir:/docs/$group:ro"
done

ssh muffin "rm -rf $D && mkdir -p $D/out && chmod a+rwx $D/out"
tar cf - -C "$stage" . | ssh muffin "tar xf - -C $D"
ssh muffin "cat > $D/run.sh" <<EOF
#!/bin/bash
ING=\$(docker ps -qf name=muffin_muffin-ingest)
IMG=\$(docker inspect -f '{{.Config.Image}}' "\$ING")
docker run --rm --name claude-gate-base --network none -m 1500m -e PYTHONDONTWRITEBYTECODE=1 \\
  -v $D/gate.py:/gate.py:ro -v $D/out:/work $mounts --entrypoint python "\$IMG" /gate.py base \\
  < /dev/null > $D/base.log 2>&1 &
docker run --rm --name claude-gate-new --network none -m 1500m -e PYTHONDONTWRITEBYTECODE=1 \\
  -e PYTHONPATH=/libnew -v $D/libnew:/libnew:ro \\
  -v $D/gate.py:/gate.py:ro -v $D/out:/work $mounts --entrypoint python "\$IMG" /gate.py new \\
  < /dev/null > $D/new.log 2>&1 &
wait
date -u +%FT%TZ > $D/DONE
EOF
# Two commands, not `chmod && setsid … &`: the `&` would background the whole list, whose stdout is
# this ssh session, and ssh would wait for the gate to finish.
ssh muffin "chmod +x $D/run.sh; setsid nohup $D/run.sh > /dev/null 2>&1 < /dev/null &"
echo "gate running on the node in $D; poll: ssh muffin 'cat $D/DONE; wc -l $D/out/*.jsonl'"
