"""Daily bars: reading what the provider said, and refusing what it cannot have meant.

Everything in this module is PURE — it takes rows and returns rows. The network call lives in the
Dagster asset, the write lives in an I/O manager, and the per-subject verdict lives in the ledger.
That separation is what makes these rules testable against frozen bytes instead of against a
provider, which is how the transformation half of this pipeline stops costing requests to fix.

THREE SHAPES THE PROVIDER RESPONSE HAS THAT LOOK LIKE ONE:

  1. `symbol` IS PRESENT ONLY WHEN SEVERAL SYMBOLS WERE REQUESTED. A single-symbol batch comes back
     with no symbol column at all, so it has to be TOLD which symbol it asked about. Getting this
     wrong attributes a whole series to the wrong company and nothing downstream can see it.
  2. Dividends and splits ride on the SAME response. openbb's yfinance provider defaults
     `include_actions` to true, so they cost no extra call and were previously discarded.
  3. A split ratio is RECORDED, NEVER APPLIED. The bars are already split-adjusted; feeding a ratio
     back into a price would corrupt it. Its value is coverage — Tiingo is US-only, so a Tokyo or
     Zurich split was invisible.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class Bar:
    """One security's close on one day, plus whatever actions the same response carried."""

    trade_date: date
    close: float
    volume: int | None = None
    #: Cash dividend with THIS bar's date as the ex-date, when the provider reported one.
    dividend: float | None = None
    #: Split ratio on this date. Recorded, never applied — see the module docstring.
    split_ratio: float | None = None


def _as_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and len(value) >= 10:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    to_date = getattr(value, "date", None)
    return to_date() if callable(to_date) else None


def _positive(value: Any) -> float | None:
    """A number strictly greater than zero, or nothing.

    A ZERO CLOSE IS NOT A PRICE, and admitting one is not a small error: it yields -100% on every
    period at once, which was a 1,078-row defect. Booleans are refused explicitly because `bool` is
    an `int` in Python and `True` would otherwise store as 1.0.
    """
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, int | float):
        return None
    number = float(value)
    return number if number > 0 else None


def _optional_number(value: Any) -> float | None:
    """A number that may legitimately be zero or negative — an action, not a price."""
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, int | float):
        return None
    return float(value)


def bar_from(row: Mapping[str, Any], sole_symbol: str) -> tuple[str, Bar] | None:
    """One provider row to `(symbol, Bar)`, or None if it is not a usable bar.

    `sole_symbol` is what to attribute the row to when the response carries no `symbol` column —
    which is the single-symbol case, not an error.
    """
    trade_date = _as_date(row.get("date"))
    close = _positive(row.get("close"))
    if trade_date is None or close is None:
        return None

    volume = row.get("volume")
    return str(row.get("symbol") or sole_symbol).upper(), Bar(
        trade_date=trade_date,
        close=close,
        volume=int(volume)
        if isinstance(volume, int | float) and not isinstance(volume, bool)
        else None,
        dividend=_optional_number(row.get("dividend")),
        split_ratio=_optional_number(row.get("split_ratio")),
    )


def bars_by_symbol(rows: Sequence[Mapping[str, Any]], sole_symbol: str) -> dict[str, list[Bar]]:
    """Group a batched response by symbol, sorted ascending by date.

    SORTED HERE AND NOT AT THE CALLER, because every rule downstream — the previous bar for `1d`,
    the anchor for a lookback, the comparability cut — reads the series positionally, and a provider
    that changes its ordering would otherwise change the numbers rather than fail.
    """
    out: dict[str, list[Bar]] = {}
    for row in rows:
        parsed = bar_from(row, sole_symbol)
        if parsed is None:
            continue
        symbol, bar = parsed
        out.setdefault(symbol, []).append(bar)
    for series in out.values():
        series.sort(key=lambda b: b.trade_date)
    return out


# ---------------------------------------------------------------------------------------------
# Who to ask, and how to turn an answer into a core row
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Subject:
    """One security this provider can be asked about, and the name to ask with."""

    security_id: str
    symbol: str
    #: Fund weight, so the head of any bounded run is the part of the universe anyone looks at.
    weight: float


#: Securities that are equities, that this provider has a name for, and that the ledger has not
#: recorded as unanswerable.
#:
#: THE LEFT JOIN IS THE WHOLE POINT AND IT IS CORRECT BEFORE THE FACET EXISTS. `ingest.task` holds
#: no `price_daily` rows until a cutover migration seeds them, and a LEFT JOIN excludes nothing when
#: there is nothing to exclude — so this query is right on day one and stays right afterwards,
#: rather than needing to be revisited at exactly the moment it starts to matter.
#:
#: `coalesce(provider_symbol, ticker)`, never the ticker first: OpenFIGI's US lookup is a thin OTC
#: foreign-ordinary line for most foreign companies, and pricing off it prices a different
#: instrument.
ASKABLE_SUBJECTS = """
select s.security_id::text,
       coalesce(ps.symbol, sym.symbol) as symbol,
       coalesce(max(h.weight), 0)::float as weight
  from market.security s
  join market.security_symbol sym on sym.security_id = s.security_id
  left join market.security_provider_symbol ps
         on ps.security_id = s.security_id and ps.provider_code = %s
  left join market.fund_holding_current h on h.security_id = s.security_id
  left join ingest.task t
         on t.facet = %s and t.security_id = s.security_id and t.status = 'absent'
 where s.security_type_code = 'equity'
   and coalesce(ps.symbol, sym.symbol) is not null
   and t.subject is null
 group by s.security_id, coalesce(ps.symbol, sym.symbol)
 order by weight desc, s.security_id
"""


def askable_subjects(
    conn: Any, *, provider: str = "yfinance", facet: str = "price_daily", limit: int | None = None
) -> list[Subject]:
    """The universe this run will ask about, heaviest holdings first."""
    sql = ASKABLE_SUBJECTS + (" limit %s" if limit is not None else "")
    params: list[Any] = [provider, facet]
    if limit is not None:
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [Subject(security_id=r[0], symbol=r[1], weight=r[2]) for r in cur.fetchall()]


def currency_by_security(conn: Any) -> dict[str, str]:
    """What each security's prices are denominated in, from its listing or its own column.

    Measured 2026-09-10: 10,469 of 10,894 askable equities have one and 425 have neither, which is
    why `market.price_bar.currency_code` is nullable — refusing those securities a bar would be
    worse than the unlabelled number the app already renders correctly.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select s.security_id::text,
                   coalesce(
                     (select l.currency_code from market.listing l
                       where l.security_id = s.security_id and l.currency_code is not null
                       order by l.is_primary desc limit 1),
                     s.currency_code)
              from market.security s
             where s.security_type_code = 'equity'
            """
        )
        return {row[0]: row[1] for row in cur.fetchall() if row[1]}


def raw_rows(
    subject: Subject, bars: Sequence[Bar], *, provider: str, run_id: str, observed: str
) -> list[dict[str, Any]]:
    """One provider answer as raw rows — what was asked, who it was asked for, and what came back.

    `security_id` is recorded even though it is OURS rather than the provider's, and that is
    deliberate: it is a fact about the REQUEST, exactly as `ingest.attempt.asked_with` is. Without
    it, stage 2 would have to re-resolve a symbol to a security using today's mapping, so a symbol
    repaired between the fetch and the transform would silently re-attribute a whole series.
    """
    return [
        {
            "security_id": subject.security_id,
            "asked_symbol": subject.symbol,
            "observed_symbol": observed,
            "provider": provider,
            "run_id": run_id,
            "trade_date": bar.trade_date.isoformat(),
            "close": bar.close,
            "volume": bar.volume,
            "dividend": bar.dividend,
            "split_ratio": bar.split_ratio,
        }
        for bar in bars
    ]


def normalise(
    rows: Sequence[Mapping[str, Any]], currencies: Mapping[str, str], *, source_code: str
) -> list[dict[str, Any]]:
    """Raw rows to `market.price_bar` rows. No provider call, so a fix here is free to re-run.

    DEDUPED ON THE CONFLICT KEY BY THE WRITER, not here — but a security appearing twice in one
    partition (asked under two symbols after a repair) would otherwise fail the whole statement with
    SQLSTATE 21000, which has happened four times in this pipeline and reads as a size problem.
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        close = row.get("close")
        security_id = row.get("security_id")
        trade_date = row.get("trade_date")
        if security_id is None or trade_date is None or not isinstance(close, int | float):
            continue
        if isinstance(close, bool) or close <= 0:
            continue
        out.append(
            {
                "security_id": security_id,
                "trade_date": trade_date,
                "close": float(close),
                "volume": row.get("volume"),
                "currency_code": currencies.get(str(security_id)),
                "source_code": source_code,
            }
        )
    return out


def securities_with_bars(
    conn: Any, *, page: int = 500, limit: int | None = None
) -> Iterator[list[str]]:
    """Securities that have bars, heaviest holdings first, in pages.

    PAGED BECAUSE THE BARS ARE, NOT THE SECURITIES. One page's worth of daily history over the
    longest lookback is the thing held in memory — 500 securities is ~650k rows — so the page is a
    memory budget wearing a row count's clothes.
    """
    sql = """
    select pb.security_id::text
      from market.price_bar pb
      left join market.fund_holding_current h on h.security_id = pb.security_id
     group by pb.security_id
     order by max(coalesce(h.weight, 0)) desc, pb.security_id
    """
    if limit is not None:
        sql += " limit %s"
    with conn.cursor() as cur:
        cur.execute(sql, [limit] if limit is not None else [])
        ids = [r[0] for r in cur.fetchall()]
    for i in range(0, len(ids), page):
        yield ids[i : i + page]


def bars_for(conn: Any, security_ids: Sequence[str], *, since: date) -> dict[str, list[Bar]]:
    """Each security's daily series since a date, oldest first.

    SORTED IN SQL AND TRUSTED HERE. Every rule downstream reads the series positionally — the
    previous bar for `1d`, the anchor for a lookback, the cut after a discontinuity — so an
    unordered result would change the numbers rather than fail.

    THE DIVIDEND IS JOINED, AND WITHOUT IT THE TOTAL RETURN WOULD BE A LIE. `market.price_bar` holds
    no dividend — it is a price table — so a series built from it alone reinvests nothing and
    `total_returns` returns exactly the price return, wearing a different column's name. That is the
    conflation `security_return.total_return_pct` is documented to refuse: NULL means "not
    computed", and a number that merely equals the price return erases the difference between "paid
    no income" and "we do not know".
    """
    if not security_ids:
        return {}
    out: dict[str, list[Bar]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            select pb.security_id::text, pb.trade_date, pb.close, pb.volume, ca.value
              from market.price_bar pb
              left join market.security_corporate_action ca
                     on ca.security_id = pb.security_id
                    and ca.ex_date = pb.trade_date
                    and ca.kind = 'dividend'
             where pb.security_id = any(%s::uuid[]) and pb.trade_date >= %s
             order by pb.security_id, pb.trade_date
            """,
            (list(security_ids), since),
        )
        for security_id, trade_date, close, volume, dividend in cur.fetchall():
            out.setdefault(security_id, []).append(
                Bar(
                    trade_date=trade_date,
                    close=float(close),
                    volume=volume,
                    dividend=float(dividend) if dividend is not None else None,
                )
            )
    return out
