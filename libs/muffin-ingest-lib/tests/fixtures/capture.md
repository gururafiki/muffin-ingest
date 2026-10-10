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

## `openfigi_mapping_local_ums.json` — an every-venue ISIN answer, captured 2026-09-26

One entry of a real `/v3/mapping` body the local rung stored in production
(`raw_figi_local_symbol`, UMS Holdings, `SG1J94892465`, fetched 2026-09-24), kept verbatim and
wrapped as the one-job positional array a one-subject request returns. It is the shape the
US-restricted captures cannot show:

* **26 lines across 25 venues** — Singapore's own `UMSH SP`, the OTC `UMSSF US`, and the European
  composite's many trading lines — so a picker has to choose by venue, not take the first line;
* **one share class (`BBG001SGJVC9`) on 25 of them and none on the 26th** (`UMSHSGD X1`). Lines
  without a class are ordinary — 2,340 of them sat in the stored answers, nearly all common stock —
  so the share-class planner must ignore them rather than read them as disagreement.

Measured over all 1,518 answers stored at the time: 1,393 named exactly one class, 125 none, and
none named two. Re-capture from the node by reading any local-rung partition:

```bash
ssh muffin 'docker exec -i $(docker ps -qf name=muffin_muffin-ingest) python -' <<'PY'
import json, pyarrow.parquet as pq
r = pq.read_table("/var/lib/muffin-ingest/raw/raw_figi_local_symbol/<security_id>.parquet").to_pylist()[0]
print(json.dumps([json.loads(bytes(r["body"]))[int(r["position"])]]))
PY
```

## `yahoo_chart_*.body` for the price lane — Yahoo's chart, captured 2026-10-10

Response bodies, byte for byte, fetched from the node through http-cache with the price lane's own
parameters (`interval=1d&includeAdjustedClose=true&events=div,split`, a `period1`/`period2` window).
Each pins a shape `facets.price_chart` has a rule for:

| file | window | what it pins |
|---|---|---|
| `amrm_ta_break` | 2026-04-20..06-20 | Tel Aviv's change from shekels to agorot on 2026-05-18, a **96.6x** step, every bar labelled `ILA` |
| `vod_jo_bounce` | 2024-12-20..2025-02-01 | 2025-01-10 quoted in rand among cents (**0.01x** then **98.9x**) — a bounce, not a change |
| `vod_l_ext` | 2026-09-26..10-10 | `GBp`, pence: `.upper()` turns it into pounds |
| `npn_jo_ext` | same | `ZAc`, cents |
| `alg_kw_week` | 2026-09-20..10-10 | `KWF`, fils (a thousandth of a dinar), and Kuwait's Sunday sessions |
| `7203_t_ext` | 2026-09-26..10-10 | Tokyo, stamped at 00:00 UTC, with a **dividend** event (not a split) in the window |
| `bhp_ax_ext` | same | on a Saturday, a **live point dated Friday beside Friday's completed bar** |
| `aapl_ext` | same | an ordinary US body |
| `aapl_max` | `range=max` | **`dataGranularity: 3mo`**, 169 points since 1984, although `interval=1d` was asked |
| `bdms_f_bk_404` | `range=max` | HTTP 404, `"No data found, symbol may be delisted"` — a named absence |

`aapl_max` is why the lane never asks `range=max`: Yahoo downsamples a long `max` silently (AAPL
`3mo`, VOD.L `3mo`, NPN.JO `1mo`, AMRM.TA `1wk`), while `period1=0` returned AAPL's 11,549 daily
bars, every one on a date the stored openbb history also holds, closes within 5e-15.

Re-capture from the node, through the cache, with the lane's parameters:

```bash
ssh muffin 'docker exec -i $(docker ps -qf name=muffin_muffin-ingest) python -' <<'PY'
import datetime as dt, httpx, pathlib
from muffin_ingest import settings
base = settings.provider_base("yahoo", "https://query2.finance.yahoo.com")
def ep(d): return str(int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.UTC).timestamp()))
params = {"period1": ep(dt.date(2026, 4, 20)), "period2": ep(dt.date(2026, 6, 20)),
          "interval": "1d", "includeAdjustedClose": "true", "events": "div,split"}
r = httpx.get(f"{base}/v8/finance/chart/AMRM.TA", params=params,
              headers={"User-Agent": "Mozilla/5.0 (compatible; muffin-market-data)"}, timeout=30)
pathlib.Path("/tmp/amrm.body").write_bytes(r.content)
PY
```
