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

## `fx_chart.json` — Yahoo's chart endpoint, captured 2026-09-11

Five cases, chosen so each answers a question the others cannot. Every one of them turned out to
carry a fact the code had guessed at:

| case | what it pins |
|---|---|
| `eur_spot` | 6 points over a 5-day window with **a null among them** — `closes[-1]` is not safe |
| `ils_history` | **524** weekly points over ten years; the subunit parent for ILA |
| `gel_history` | **ONE** point, dated today — a history fetch that SUCCEEDS and loads nothing |
| `unknown_pair` | **HTTP 404** with `{"code": "Not Found", "description": "No data found…"}` |
| `twd_inverted` | `USDTWD=X` returns **31.60**, which the plausibility band must refuse |

**The `meta` block is part of the wire shape, not an extra**, and the first capture omitted it —
which cost a production run. Two facts live only there:

* `gmtoffset: 3600` / `exchangeTimezoneName: "Europe/London"`. An FX daily bar is stamped at the
  session's **open** in the exchange's timezone, so the 2026-09-10 session arrives as
  `1788994800` — **2026-09-09T23:00Z**. Reading `.date()` in UTC dates every bar a day early; a
  partition filtering to its own window then reported `outside_window=190` (38 currencies × 5
  points, all of them) and wrote nothing while reporting success.
* `regularMarketTime`. The **last point is often a live quote rather than a completed bar**, and it
  is exactly identifiable: its timestamp *equals* this field, measured to the second on three
  separate series. `GELUSD=X` is the extreme — its only point is the live quote, so Yahoo has no
  completed weekly bar for the lari at all.

`unknown_pair` is why the capture was worth taking. The provider was written to raise on any
non-200, which would have reported every unquoted currency as a *transport* failure — and since a
transport failure must never mark a subject absent, the negative cache could never fill and those
pairs would be re-asked for ever. The status says 404 and the body says the symbol has no data;
only reading the body tells them apart.

Re-capture: run this against the deployed image, which already carries `httpx`.

```bash
ssh muffin 'docker exec -i $(docker ps -qf name=muffin_muffin-ingest) python -' \
  < scripts/capture_fx.py > tests/fixtures/fx_chart.json
```

The arrays are truncated to 600 points; `ils_history` fits whole.
