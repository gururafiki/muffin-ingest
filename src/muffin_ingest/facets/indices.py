"""Index returns: how a country, a classification group or a sector has moved.

THREE SCOPE KINDS, TWO ACQUISITION SHAPES, and the split is not arbitrary.

* **country** (45) and **group** (17) are backed by a PROXY ETF, so their returns are computed from
  bars with the same rules as a security's — one implementation, so a sector page and a stock page
  cannot disagree about what "3-month return" means.
* **sector** (11) has no ETF. finviz publishes the numbers directly through
  `equity/compare/groups`, which is also the only source here that is US-listed-only — a fact worth
  stating rather than relabelling as global.

THE PROXY SYMBOL IS NOT COPIED. `countries.etf_symbol` and `classification_groups.etf` already hold
it, and duplicating either into `index_scope.proxy_symbol` would be the same fact in two places,
which this schema has watched drift four times. The column stays as an editorial OVERRIDE —
`coalesce(override, the source table)` — exactly as `security.price_symbol` overrides a ticker.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date
from typing import Any

#: Provider sector label -> muffin sector id.
#:
#: finviz does NOT use GICS names: "Consumer Defensive"/"Consumer Cyclical"/"Financial"/
#: "Healthcare"/"Basic Materials"/"Technology" against GICS's Consumer Staples/Consumer
#: Discretionary/Financials/Health Care/Materials/Information Technology. All eleven map 1:1.
#:
#: `Financial Services` is here TOO, and deliberately: yfinance's `equity/profile` uses that label
#: for the same sector, so one map means a security classified from a profile lands in the same
#: bucket as a sector read from the sector endpoint — the entire point of having sector ids.
#:
#: AN UNMAPPED LABEL IS REPORTED, NEVER INSERTED UNDER A GUESSED ID. A provider rename should
#: degrade one row, not silently file a sector's performance under a neighbour's name.
SECTOR_LABELS: dict[str, str] = {
    "Financial Services": "financials",
    "Financial": "financials",
    "Energy": "energy",
    "Utilities": "utilities",
    "Real Estate": "real-estate",
    "Consumer Defensive": "consumer-staples",
    "Communication Services": "communication-services",
    "Healthcare": "health-care",
    "Consumer Cyclical": "consumer-discretionary",
    "Industrials": "industrials",
    "Technology": "information-technology",
    "Basic Materials": "materials",
}

#: finviz response field -> period code.
#:
#: THE DAY CHANGE IS UNDER THE LITERAL KEY `Change %` AND `performance_1d` IS ALWAYS NULL —
#: measured on all eleven sectors, 0 of 11 populated against 11 of 11 for `Change %`. A field that
#: exists, is named exactly what you want and is never filled is the worst shape there is: reading
#: it yields no 1d return at all, with nothing anywhere saying why.
#:
#: There is NO 3y/5y/10y here, so those periods stay ABSENT for sectors rather than being computed
#: from a shorter window and labelled as though they were not.
FINVIZ_PERIODS: dict[str, str] = {
    "Change %": "1d",
    "performance_1w": "1w",
    "performance_1m": "1m",
    "performance_3m": "3m",
    "performance_6m": "6m",
    "performance_ytd": "ytd",
    "performance_1y": "1y",
}


def to_percent(value: Any) -> float | None:
    """A FRACTION to a percent, because openbb reports performance as a fraction.

    `-0.0366` means -3.66%. Storing it raw yields numbers a hundred times too small that still
    render as entirely plausible values — the same confusion that once put NVIDIA at a 46% dividend
    yield on a deployed page. Rounded to four decimals, matching what the old resource stored so the
    parity comparison is about the DATA rather than about float formatting.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    # CHECKED BEFORE ROUNDING, NOT AFTER, AND THE TEST CAUGHT THAT. `round()` on a NaN raises
    # `ValueError: cannot convert float NaN to integer`, so a guard placed after it never runs —
    # a provider sending NaN would have taken the whole asset down instead of dropping one figure.
    if not math.isfinite(float(value)):
        return None
    return round(float(value) * 1_000_000) / 10_000


#: Scopes backed by a proxy ETF, with the symbol resolved from wherever it actually lives.
#:
#: `index_scope.proxy_symbol` FIRST, as an editorial override; then the source table. A group's code
#: is `group:<scheme>:<id>` because a group id is NOT unique across schemes — MSCI and FTSE both
#: have `developed`, backed by DIFFERENT funds (URTH vs VEA), and keying on the bare id would file
#: one's performance under the other.
PROXIED_SCOPES = """
select s.index_code,
       coalesce(s.proxy_symbol, c.etf_symbol, g.etf) as symbol
  from market.index_scope s
  left join market.countries c
    on s.scope_kind = 'country' and c.iso2 = s.country_iso2
  left join market.classification_groups g
    on s.scope_kind = 'group' and s.index_code = 'group:' || g.scheme_id || ':' || g.id
  left join market.tracked_fund tf
    on tf.symbol = coalesce(s.proxy_symbol, c.etf_symbol, g.etf)
 where s.enabled
   and s.scope_kind in ('country', 'group')
   and coalesce(s.proxy_symbol, c.etf_symbol, g.etf) is not null
   -- A FUND MARKED DEAD HAS NO PRICE SERIES EITHER. FM stopped filing in 2024 because it was
   -- liquidated, and asking for its bars is how this resource once spent a day returning 502s —
   -- one dead symbol takes its whole batch with it.
   and coalesce(tf.enabled, true)
 order by s.index_code
"""


def proxied_scopes(conn: Any) -> list[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(PROXIED_SCOPES)
        return [(str(code), str(symbol)) for code, symbol in cur.fetchall()]


def sector_rows(
    results: Sequence[dict[str, Any]], *, run_id: str, taken: date
) -> list[dict[str, Any]]:
    """What finviz said, flattened to one row per (label, period) — the artifact, not the answer.

    THE PROVIDER'S OWN LABEL IS KEPT, not the muffin id it maps to. Mapping is interpretation and
    belongs in stage 2; keeping the raw label means a rename can be diagnosed from the stored bytes
    rather than by re-asking the provider.
    """
    out: list[dict[str, Any]] = []
    for row in results:
        label = row.get("name")
        if not isinstance(label, str):
            continue
        for field, period in FINVIZ_PERIODS.items():
            value = row.get(field)
            if value is None:
                continue
            out.append(
                {
                    "provider_label": label,
                    "period_code": period,
                    "fraction": float(value),
                    # THE DAY THE SNAPSHOT WAS TAKEN, recorded here because the provider does not
                    # state one. Without it the only date available downstream is a partition key,
                    # and a re-run of an old partition would stamp today's figures with a past day.
                    "taken": taken.isoformat(),
                    "provider": "finviz",
                    "run_id": run_id,
                }
            )
    return out


def normalise_sectors(
    rows: Sequence[dict[str, Any]], *, source_code: str = "finviz"
) -> tuple[list[dict[str, Any]], list[str]]:
    """Raw finviz rows to `market.index_return` rows, plus the labels nothing could map.

    Returns the unmapped labels rather than logging them, because a caller that cannot see them
    cannot fail on them — and a provider rename that silently drops a sector is exactly the failure
    this pipeline keeps finding in the resource it replaces.
    """
    out: list[dict[str, Any]] = []
    unmapped: set[str] = set()
    for row in rows:
        label = str(row.get("provider_label", ""))
        sector = SECTOR_LABELS.get(label)
        if sector is None:
            unmapped.add(label)
            continue
        pct = to_percent(row.get("fraction"))
        if pct is None:
            continue
        out.append(
            {
                "index_code": f"sector:{sector}",
                "period_code": str(row["period_code"]),
                # FROM THE ROW, never from a caller's idea of "now" — the raw artifact records when
                # the snapshot was taken and that is the only honest date for a figure the provider
                # publishes without one.
                "as_of": str(row["taken"])[:10],
                "price_return_pct": pct,
                # NEVER COALESCED TO THE PRICE RETURN. finviz publishes price performance only, so
                # a total return here is NOT KNOWN — and a column filled with the price return
                # erases the difference between "paid no income" and "we did not measure it".
                "total_return_pct": None,
                "source_code": source_code,
            }
        )
    return out, sorted(unmapped)
