"""Daily bars: reading what the provider said, and refusing what it cannot have meant.

Everything in this module is PURE — it takes rows and returns rows. The network call lives in the
Dagster asset, the write lives in an I/O manager, and the per-subject verdict is an
`identifier_probe` row (`symbol_probes`).
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

import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from muffin_ingest.providers.isolation import BatchVerdict


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

    FINITE AS WELL, because yfinance sends `close: NaN` for a session it has not closed yet — 60 of
    61 index proxies at 00:00:34 UTC on 2026-09-17, with open/high/low/volume populated. NaN is a
    `float`, so a bare type check admits it, and Postgres cannot refuse it later: `'NaN'::numeric`
    sorts above every number, so `close > 0` holds. `inf > 0` holds too.
    """
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) and number > 0 else None


def close_of(row: Mapping[str, Any]) -> float | None:
    """The one rule for what counts as a close in a provider row. Every lane reads it from here,
    because each hand-written copy of it admitted NaN."""
    return _positive(row.get("close"))


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
    close = close_of(row)
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


#: Securities that are equities, that this provider has a name for, and whose symbol this provider
#: has not rejected on its own within `DEAD_FOR_DAYS`.
#:
#: THE REJECTION IS BOUND TO THE SYMBOL IT WAS ASKED WITH. `asked_with` must equal the symbol this
#: query would ask with now, so a corrected spelling is askable the moment it is written — the
#: contract `clear_symbol_caches` used to enforce by hand, made structural. A miss also EXPIRES: a
#: security can gain a listing, and a symbol repair must be allowed to prove the provider wrong.
#: The ledger this replaced had to be taught the expiry after it locked securities out for ever;
#: here it is the predicate.
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
         on ps.security_id = s.security_id and ps.provider_code = %(provider)s
  left join market.fund_holding_current h on h.security_id = s.security_id
 where s.security_type_code = 'equity'
   and coalesce(ps.symbol, sym.symbol) is not null
   and not exists (
         select 1 from market.identifier_probe p
          where p.security_id = s.security_id
            and p.scheme = %(scheme)s and p.provider = %(provider)s
            and p.outcome = 'miss'
            and p.asked_with = coalesce(ps.symbol, sym.symbol)
            and p.observed_at > now() - make_interval(days => %(dead_for_days)s))
 group by s.security_id, coalesce(ps.symbol, sym.symbol)
 order by weight desc, s.security_id
"""

#: The probe scheme a price lane's verdict on a symbol is recorded under — the same scheme the
#: symbology lane uses for the symbol it adopts, with this provider as `provider`, so the two never
#: share a key (`identifier_probe` is keyed by security, scheme and provider).
SYMBOL_SCHEME = "symbol"

#: How long a symbol the provider rejected alone stays unasked. The ledger's `absent_ttl` for this
#: facet, carried over unchanged.
DEAD_FOR_DAYS = 30


def askable_subjects(
    conn: Any, *, provider: str = "yfinance", limit: int | None = None
) -> list[Subject]:
    """The universe this run will ask about, heaviest holdings first."""
    sql = ASKABLE_SUBJECTS + (" limit %(limit)s" if limit is not None else "")
    params: dict[str, Any] = {
        "provider": provider,
        "scheme": SYMBOL_SCHEME,
        "dead_for_days": DEAD_FOR_DAYS,
        "limit": limit,
    }
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [Subject(security_id=r[0], symbol=r[1], weight=r[2]) for r in cur.fetchall()]


def symbol_probes(
    by_symbol: Mapping[str, Subject],
    verdict: BatchVerdict,
    *,
    provider: str,
    observed_at: datetime,
) -> list[dict[str, Any]]:
    """What one batch ESTABLISHED about each subject's symbol, as `identifier_probe` rows.

    THREE OUTCOMES BECOME NOTHING, and each is a fact this pipeline has paid for confusing:
      * a throttled batch was REFUSED, so it established nothing about anybody;
      * a subject the batch did not answer and did not isolate is unknown, not dead;
      * a transport failure is ours or the provider's, never the symbol's.

    A MISS NEEDS BOTH PROOFS: the subject was asked ALONE, and a control subject answered in the
    same attempt. `fetch_with_isolation` populates `dead` only then, and this checks the two flags
    anyway, because the ledger this replaced kept the rule in SQL precisely so an over-eager caller
    could not mark an outage as an absence — the 1,369-security incident's shape. The rule is here
    now, beside the only code that writes the row, and a test holds it.

    A HIT IS RECORDED TOO. It replaces an earlier miss under the same key, so a symbol that starts
    answering again is visibly alive rather than merely expired.
    """
    if verdict.throttled_out:
        return []
    answered = _answered_symbols(by_symbol, verdict)
    proven = verdict.isolated and verdict.control_answered is True
    dead = {d.upper() for d in verdict.dead} if proven else set()
    rows: list[dict[str, Any]] = []
    for symbol, subject in by_symbol.items():
        if symbol.upper() in answered:
            outcome, value = "hit", symbol
        elif symbol.upper() in dead:
            outcome, value = "miss", None
        else:
            continue
        rows.append(
            {
                "security_id": subject.security_id,
                "scheme": SYMBOL_SCHEME,
                "provider": provider,
                "asked_with": symbol,
                "value": value,
                "outcome": outcome,
                "observed_at": observed_at,
            }
        )
    return rows


def _answered_symbols(by_symbol: Mapping[str, Subject], verdict: BatchVerdict) -> set[str]:
    """The upper-cased symbols with at least one row. A single-symbol batch carries no `symbol`
    column, so its rows belong to the one symbol asked (`bars_by_symbol` reads it the same way)."""
    answered: set[str] = set()
    for row in verdict.rows:
        symbol = str(row.get("symbol") or "").upper()
        if symbol:
            answered.add(symbol)
        elif len(by_symbol) == 1:
            answered.add(next(iter(by_symbol)).upper())
    return answered


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


def row_date(row: Mapping[str, Any]) -> date | None:
    """The provider row's own trade date, parsed. Shared so the window filter and the raw writer
    cannot disagree about which partition a row belongs to."""
    return _as_date(row.get("date"))


# --- one security, one listing's history ----------------------------------------------------------
#
# A SECURITY'S SYMBOL CAN CHANGE UNDER ITS STORED HISTORY, AND THE EXTENSION THEN APPENDS ANOTHER
# LISTING TO IT. The lane asks with `coalesce(provider symbol, ticker)`, so a security priced
# through its ticker (OpenFIGI's US line, usually a thin OTC foreign-ordinary line) moves to its
# home line the moment symbology adopts one. Measured 2026-10-04: 69 history partitions held two
# asked symbols, years of the OTC line in dollars followed by days of the home line: Geely as
# `GELYF` from 2007 then `0175.HK`, SATS as `SPASF` then `S58.SI`, Wuxi Biologics as `WXIBF` then
# `2269.HK`. Each series jumps in price and currency at the switch, and every return across it is
# wrong. The extension never looked at the symbol, only at the newest date.


def asked_with_another_symbol(stored: Sequence[Mapping[str, Any]], symbol: str) -> bool:
    """Whether a stored history holds a row asked with a symbol other than `symbol`.

    A row with no `asked_symbol` says nothing about which symbol it came from, so it counts as
    another. Measured 2026-10-04: every non-empty partition records one, so this reloads exactly the
    mixed ones.
    """
    return any(row.get("asked_symbol") != symbol for row in stored)


def dates_by_security(rows: Sequence[Mapping[str, Any]]) -> dict[str, set[date]]:
    """Each security's raw trade dates, every row counted, the ones stage 2 refuses included.

    A row whose close stage 2 refuses (an unclosed session's NaN) still says the provider has that
    day, so a bar already published for it is not retracted on the strength of a glitch.
    """
    out: dict[str, set[date]] = {}
    for row in rows:
        day, sid = row_date(row), row.get("security_id")
        if day is None or sid is None:
            continue
        out.setdefault(str(sid), set()).add(day)
    return out


#: WITHIN THE RANGE A RAW HISTORY COVERS, `price_bar` MIRRORS IT. Measured 2026-10-04: 95
#: securities held 5,097 bars on dates their raw history lacks, and every one of the twelve largest
#: was a Hong Kong home line (2018.HK, 2331.HK, 2688.HK…) carrying its OTC line's dollar bars on
#: the days Hong Kong was shut. Lancashire's are the UK bank holidays: about 7.5 against a pence
#: series of ~640, so its chart falls 99% each Easter Monday. They are the old listing, left behind
#: when the raw history was reloaded under the new one, because stage 2 only ever upserts.
#:
#: OUTSIDE THAT RANGE THE RAW HISTORY SAYS NOTHING, and the bars stay. The same measurement found
#: 37,123 bars before their raw history's first date, and they are not all one thing: AREN's 7,457
#: run continuously into its raw history (1.05 on 07-16, 1.00 on 07-17) — a history the provider
#: stopped returning, not another listing's. A rule that mirrored raw outright would delete them.
RETRACT_BARS_ABSENT_FROM_RAW = """
delete from market.price_bar b
 using unnest(%s::uuid[], %s::date[], %s::date[]) as r(security_id, first_date, last_date)
 where b.security_id = r.security_id
   and b.trade_date between r.first_date and r.last_date
   and not exists (select 1 from unnest(%s::uuid[], %s::date[]) as h(security_id, trade_date)
                    where h.security_id = b.security_id and h.trade_date = b.trade_date)
returning b.security_id::text
"""


def retract_bars_absent_from_raw(cur: Any, held: Mapping[str, set[date]]) -> int:
    """Delete each security's bars on dates inside its raw range that its raw history lacks.

    A security with no raw rows is not in `held` and is left alone: an empty history is a dead
    symbol or a refusal, and retracting on it would delete a history to record a quiet night.
    """
    ids, firsts, lasts, pair_ids, pair_dates = [], [], [], [], []
    for sid, days in sorted(held.items()):
        if not days:
            continue
        ids.append(sid)
        firsts.append(min(days))
        lasts.append(max(days))
        for day in sorted(days):
            pair_ids.append(sid)
            pair_dates.append(day)
    if not ids:
        return 0
    cur.execute(RETRACT_BARS_ABSENT_FROM_RAW, (ids, firsts, lasts, pair_ids, pair_dates))
    return len(cur.fetchall())


def provider_rows_by_symbol(
    rows: Sequence[Mapping[str, Any]], sole_symbol: str
) -> dict[str, list[Mapping[str, Any]]]:
    """Group the provider's rows by symbol WITHOUT touching them.

    The mirror of `bars_by_symbol`: that one parses into `Bar` so the window filter and the
    counters have something to decide on, this one keeps the vendor's row whole so raw can store
    it. A row the provider did not label is attributed to `sole_symbol`, the same rule — the
    provider adds a `symbol` column only when several were requested.
    """
    out: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        # A ROW WHOSE DATE WILL NOT PARSE IS STILL WHAT THE PROVIDER SENT. It used to be dropped
        # here, which made stage 1 the judge of whether a row is usable — and a row deleted at
        # fetch is one no re-parse can recover. `normalise` refuses it instead.
        symbol = str(row.get("symbol") or sole_symbol).upper()
        out.setdefault(symbol, []).append(row)
    return out


#: EVERY column this pipeline adds to a provider row, and nothing computed from the vendor's own
#: fields. A test holds `raw_rows` to exactly this set, so a derived column creeping back into
#: stage 1 — `trade_date` was one, parsed from the vendor's `date` — fails rather than ships.
CONTEXT_COLUMNS = frozenset(
    {"security_id", "asked_symbol", "observed_symbol", "provider", "run_id", "provider_warnings"}
)


def raw_rows(
    subject: Subject,
    provider_rows: Sequence[Mapping[str, Any]],
    *,
    provider: str,
    run_id: str,
    observed: str,
    warnings: Sequence[str] = (),
) -> list[dict[str, Any]]:
    """The provider's rows, WHOLE, plus what we asked and who we asked it for.

    RAW IS THE VENDOR'S ANSWER WITH NOTHING DROPPED. This function used to take `Bar` — six
    fields — and the vendor sends ten: `open`, `high`, `low` and `vwap` were discarded at stage 1
    and unrecoverable without re-fetching every bar we hold. That is precisely what the two-stage
    split exists to prevent: adopting a field we do not use today must cost a re-parse of files
    already on disk, never a re-fetch. Measured 2026-09-12 against the captured payload —
    provider keys `close date dividend high low open split_ratio symbol volume vwap`, raw kept
    six of ten.

    CONTEXT IS ADDED, NEVER SUBTRACTED. `security_id` is recorded even though it is OURS: it is a
    fact about the REQUEST, exactly as `identifier_probe.asked_with` is. Without it stage 2 would
    re-resolve the symbol with TODAY's mapping, so a symbol repaired between the fetch and the
    transform would silently re-attribute a whole series. `asked_symbol` beside `observed_symbol`
    is what makes "what did we actually request" answerable when a value turns out wrong.

    NOTHING IS DERIVED INTO THE ROW. An earlier version added a parsed `trade_date` here because
    the I/O manager needs a partition key — but a key is needed for PLACEMENT, not for storage,
    so the asset passes `row_date` as the key function and the row itself stays the provider's.
    The rule is that stage 1 adds context and subtracts nothing; a normalised date is neither.
    """
    out: list[dict[str, Any]] = []
    for row in provider_rows:
        enriched = dict(row)
        enriched.update(
            {
                "security_id": subject.security_id,
                "asked_symbol": subject.symbol,
                "observed_symbol": observed,
                "provider": provider,
                "run_id": run_id,
                # WHAT THE PROVIDER SAID ABOUT ITSELF. `OBBject.warnings` is a provider
                # declaring itself degraded while still returning 200 — it was read for
                # classification and then discarded, so the text that explains an odd value was
                # never on disk beside it. Stored per row: Parquet dictionary-encodes one
                # repeated string per call to nothing.
                "provider_warnings": "\n".join(warnings) if warnings else None,
            }
        )
        out.append(enriched)
    return out


def normalise(
    rows: Sequence[Mapping[str, Any]],
    currencies: Mapping[str, str],
    *,
    source_code: str,
    window: tuple[date, date] | None = None,
) -> list[dict[str, Any]]:
    """Raw rows to `market.price_bar` rows. No provider call, so a fix here is free to re-run.

    DEDUPED ON THE CONFLICT KEY BY THE WRITER, not here — but a security appearing twice in one
    partition (asked under two symbols after a repair) would otherwise fail the whole statement with
    SQLSTATE 21000, which has happened four times in this pipeline and reads as a size problem.
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        # `close <= 0` USED TO BE THE CHECK HERE, and it is false for NaN — see `close_of`.
        close = close_of(row)
        security_id = row.get("security_id")
        # THE DATE IS PARSED HERE, FROM THE PROVIDER'S OWN FIELD. Raw carries the vendor's `date`
        # untouched; turning it into a `trade_date` is an interpretation and belongs downstream.
        parsed = row_date(row)
        if security_id is None or parsed is None or close is None:
            continue
        # AND THE WINDOW IS APPLIED HERE TOO. The provider widens a degenerate range and can
        # return a session still in progress; both used to be filtered at fetch, which threw the
        # rows away. They are stored and refused here, so the rule can change for free.
        if window is not None and not (window[0] <= parsed < window[1]):
            continue
        trade_date = parsed.isoformat()
        out.append(
            {
                "security_id": security_id,
                "trade_date": trade_date,
                "close": close,
                "volume": row.get("volume"),
                "currency_code": currencies.get(str(security_id)),
                "source_code": source_code,
            }
        )
    return out


#: WHICH SECURITIES HAVE A BAR THE CALLER WILL READ — one index probe per security, never a pass
#: over the bars. A `group by` over `market.price_bar` is O(ROWS), and this table's rows grow with
#: every night of the per-security history lane: it answered inside the role's 120 s
#: `statement_timeout` through 2026-09-20 and died at 126 s on 09-23 and again on 09-24, both runs
#: of `security_return` cancelled on this one statement before a single return was computed.
#:
#: THE SEMI-JOIN IS NOT THE FIX, and that was measured rather than assumed: written as `where exists
#: (...)` the planner turns it straight back into a Parallel Hash Semi Join that reads every
#: partition, and it hit a 110 s bound over 57.5M rows. `LATERAL ... LIMIT 1` cannot be flattened
#: that way, so it stays one parameterised probe per security into the `(security_id, trade_date)`
#: primary key. And the `trade_date` bound PRUNES — six of 61 yearly partitions — so the cost is
#: index descents per security, flat however deep history gets. 11,760 securities in 26 s.
#:
#: The weights are aggregated ONCE and joined, not looked up per security: a lateral over
#: `fund_holding_current` would evaluate that view 27,000 times.
SECURITIES_WITH_BARS = """
with weight as materialized (
  select security_id, max(coalesce(weight, 0)) as w
    from market.fund_holding_current
   group by security_id
)
select s.security_id::text
  from market.security s
 cross join lateral (
   select 1 from market.price_bar pb
    where pb.security_id = s.security_id and pb.trade_date >= %s
    limit 1
 ) has_bar
  left join weight on weight.security_id = s.security_id
 order by coalesce(weight.w, 0) desc, s.security_id
"""


def securities_with_bars(
    conn: Any, *, since: date, page: int = 500, limit: int | None = None
) -> Iterator[list[str]]:
    """Securities with a bar on or after `since`, heaviest holdings first, in pages.

    PAGED BECAUSE THE BARS ARE, NOT THE SECURITIES. One page's worth of daily history over the
    longest lookback is the thing held in memory — 500 securities is ~650k rows — so the page is a
    memory budget wearing a row count's clothes.

    `since` MUST BE THE WINDOW THE CALLER READS, AND THAT IS WHAT MAKES THIS EXACT RATHER THAN AN
    APPROXIMATION. `bars_for` returns nothing older than it, so a security whose bars all predate
    it contributes no series and writes no row whichever way it is enumerated — leaving it out
    changes the pages and nothing else. Proven on production 2026-09-24 over a sixteenth of the
    universe: the old enumeration restricted to that window and this one gave the same 733
    securities in the same positions, 0 differing. It is REQUIRED, not defaulted, because a
    default here is a second copy of the lookback free to drift from the one the caller reads.
    """
    sql = SECURITIES_WITH_BARS
    params: list[Any] = [since]
    if limit is not None:
        sql += " limit %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
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
