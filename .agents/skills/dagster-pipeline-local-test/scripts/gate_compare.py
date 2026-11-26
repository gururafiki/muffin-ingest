"""Compare a reader gate's two runs per document: python3 gate_compare.py <out dir> [--quiet GROUP ...].

Each run is gate_<label>.jsonl with one line per document: {"doc", "status", "facts", "period"?}.
A document is grouped by the directory it was mounted from (/docs/<group>/...), so the golden set,
the cases the change is for and a random sample read as separate lines.

Categories, per group:
  same / same-empty   identical facts and period (empty = nothing read on either side)
  gained              nothing before, facts now
  LOST                facts before, nothing now        -> every one needs explaining
  CHANGED [period]    facts on both sides that differ  -> every one needs reading
  status a->b         one side raised

The changed documents are listed after the table, except for groups named with --quiet.
Needs only the standard library, so it runs with the node's own python3.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path


def load(path: Path) -> dict[str, dict]:
    with path.open(encoding="utf-8") as handle:
        return {row["doc"]: row for row in map(json.loads, handle)}


def category(b: dict, n: dict) -> str:
    if b.get("status") != n.get("status"):
        return f"status {b.get('status')}->{n.get('status')}"
    bf, nf = b.get("facts") or [], n.get("facts") or []
    if bf == nf and b.get("period") == n.get("period"):
        return "same" if bf else "same-empty"
    if not bf:
        return "gained"
    if not nf:
        return "LOST"
    return "CHANGED period" if b.get("period") != n.get("period") else "CHANGED"


def main(argv: list[str]) -> int:
    out = Path(argv[0])
    quiet = set(argv[argv.index("--quiet") + 1 :]) if "--quiet" in argv else set()
    base, new = load(out / "gate_base.jsonl"), load(out / "gate_new.jsonl")
    both = [d for d in base if d in new]
    counts: Counter[tuple[str, str]] = Counter()
    changed: list[tuple[str, str]] = []
    for doc in both:
        group = doc.split("/")[2] if doc.startswith("/docs/") else "-"
        kind = category(base[doc], new[doc])
        counts[(group, kind)] += 1
        if not kind.startswith("same") and group not in quiet:
            changed.append((kind, doc))
    for (group, kind), n in sorted(counts.items()):
        print(f"{group:20} {kind:22} {n}")
    print(f"compared {len(both)}; only in base {len(set(base) - set(new))}; "
          f"only in new {len(set(new) - set(base))}")
    for kind, doc in sorted(changed):
        b, n = base[doc], new[doc]
        print(f"  {kind:16} {doc}  {b.get('period')}->{n.get('period')}  "
              f"facts {len(b.get('facts') or [])}->{len(n.get('facts') or [])}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
