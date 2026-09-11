# Captured provider payloads

`price_history.json` is what yfinance actually returned through openbb on 2026-09-11, not a shape
anyone wrote out. It exists because the invented fixtures could not produce the two shapes that have
actually cost this pipeline something:

* a **single-symbol** response carries **no `symbol` column at all**, so a batch of one has to be
  told what it asked about — attributing a whole series to the wrong company is otherwise silent;
* an **unknown symbol raises** `EmptyDataError` rather than answering with no rows, so "the provider
  had nothing" and "the call failed" arrive through different channels for the same underlying fact.

## Re-capturing

Run against the node, where the hub and its credentials already live:

```bash
ssh muffin 'docker exec -i $(docker ps -qf name=muffin_muffin-ingest) python -' <<'PY'
import json
from openbb import obb
GROUPS = {
    "batch_mixed_venues": ["AAPL", "005930.KS", "QIBK.QA", "SQM-B.SN"],
    "single_symbol_no_symbol_column": ["NESN.SW"],
    "provider_has_nothing": ["ZZZZ.NOPE"],
}
out = {}
for name, syms in GROUPS.items():
    entry = {"symbols": syms, "rows": [], "warnings": [], "error": None}
    try:
        r = obb.equity.price.historical(symbol=",".join(syms), provider="yfinance",
                                        start_date="2026-09-08", end_date="2026-09-09",
                                        interval="1d")
        res = r.results or []
        items = res if isinstance(res, list) else [res]
        entry["rows"] = [i.model_dump() for i in items]
        entry["warnings"] = [w.message for w in (r.warnings or []) if getattr(w, "message", None)]
    except Exception as e:
        entry["error"] = f"{type(e).__name__}: {e}"
    out[name] = entry
print(json.dumps(out, default=str))
PY
```

**The symbols are chosen, not arbitrary.** A US line, a Korean one whose session sits on the far
side of UTC, and two the old pipeline disagreed with the provider about (`QIBK.QA`, `SQM-B.SN`) —
so a re-capture keeps covering the cases that have already gone wrong once.
