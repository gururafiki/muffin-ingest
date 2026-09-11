import json
import os
import sys
from datetime import UTC, datetime

import httpx

BASE = os.environ.get("YAHOO_BASE_URL") or "https://query2.finance.yahoo.com"
UA = "Mozilla/5.0 (compatible; muffin-market-data)"


def grab(symbol, range_, interval):
    r = httpx.get(
        f"{BASE}/v8/finance/chart/{symbol}",
        params={"range": range_, "interval": interval},
        headers={"User-Agent": UA},
        timeout=25.0,
    )
    return r.status_code, (
        r.json() if r.headers.get("content-type", "").startswith("application/json") else None
    )


out = {}
for name, symbol, range_, interval in [
    ("eur_spot", "EURUSD=X", "5d", "1d"),
    ("ils_history", "ILSUSD=X", "10y", "1wk"),
    ("gel_history", "GELUSD=X", "10y", "1wk"),
    ("unknown_pair", "ZZZUSD=X", "5d", "1d"),
    ("twd_inverted", "USDTWD=X", "5d", "1d"),
]:
    try:
        status, body = grab(symbol, range_, interval)
    except Exception as e:
        out[name] = {
            "symbol": symbol,
            "range": range_,
            "interval": interval,
            "status": None,
            "transport_error": f"{type(e).__name__}: {e}",
        }
        print(name, "TRANSPORT", file=sys.stderr)
        continue
    ch = (body or {}).get("chart") or {}
    res = (ch.get("result") or [{}])[0] or {}
    stamps = res.get("timestamp") or []
    q = (res.get("indicators") or {}).get("quote") or [{}]
    closes = (q[0] or {}).get("close") or []
    meta = res.get("meta") or {}
    out[name] = {
        "symbol": symbol,
        "range": range_,
        "interval": interval,
        "status": status,
        "chart_error": ch.get("error"),
        # WITHOUT THESE THE DATES ARE A DAY EARLY AND THE LAST POINT IS NOT A BAR. `gmtoffset`
        # puts a stamp of 23:00Z on the session it belongs to (midnight in Europe/London), and a
        # stamp equal to `regularMarketTime` is a LIVE quote rather than a completed bar.
        "meta": {
            k: meta.get(k)
            for k in (
                "gmtoffset",
                "exchangeTimezoneName",
                "timezone",
                "regularMarketTime",
                "dataGranularity",
                "instrumentType",
            )
        },
        # The wire shape, kept as Yahoo sends it: PARALLEL ARRAYS that can carry nulls.
        "timestamp": stamps[:600],
        "close": closes[:600],
        "first_date": datetime.fromtimestamp(stamps[0], tz=UTC).date().isoformat()
        if stamps
        else None,
        "last_date": datetime.fromtimestamp(stamps[-1], tz=UTC).date().isoformat()
        if stamps
        else None,
        "nulls": sum(1 for c in closes if c is None),
    }
    print(
        name, "status", status, "points", len(stamps), "nulls", out[name]["nulls"], file=sys.stderr
    )
print(json.dumps(out, indent=1))
