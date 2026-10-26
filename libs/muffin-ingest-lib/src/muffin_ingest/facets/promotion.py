"""Stage 2 of the promotion lane: does Yahoo price a candidate line? Bytes in, a verdict out.

A promotion wave mints a security for a directory line the universe does not hold. OpenFIGI's
directory says a line exists, not that anything trades on it: measured 2026-10-26, the candidates
included delisted lines kept under Bloomberg codes (`7785752Z`), temporary lines
(`ABB LTD-BUY BACK`, `ACCELLERON N 2 LINIE`), Czech certificates on London's international order
book, and Activision on SIX two years after Microsoft bought it. Decided by the user 2026-10-26:
each candidate's line is asked of Yahoo's chart, and only a line Yahoo prices with a recent
completed bar is promoted.

NO NETWORK. `fetch` is stage 1's; a corrected rule here costs a re-parse of the stored bodies.
"""

from datetime import date, datetime, timedelta

from muffin_ingest.facets import price_chart, prices
from muffin_ingest.providers import yahoo_chart

#: What the probe asks: thirty days of daily bars, so a line that trades has several and a line
#: that stopped has none.
WINDOW = timedelta(days=30)

#: A COMPLETED BAR NEWER THAN THIS, counted back from when the body was fetched. Ten days spans a
#: week of holidays (Golden Week, Chuseok) without admitting a line that stopped trading.
FRESH_FOR = timedelta(days=10)

#: Every verdict, and the only ones `market.promotion_probe.outcome` accepts.
PRICED = "priced"
OUTCOMES = (
    PRICED,
    "absent",
    "empty",
    "stale",
    "quote_only",
    "not_equity",
    "not_daily",
    "unreadable",
)

#: A symbol whose chart answers every day. The probe asks it once a run: an absence is recorded only
#: when the control priced in the same run, so a throttled or broken run cannot record a line as
#: unpriceable. The price lane uses AAPL as its control for the same reason.
CONTROL_SYMBOL = "AAPL"


def verdict(body: bytes, fetched_at: datetime) -> str:
    """What one chart body says about the line it was asked for.

    THE ORDER IS THE ARGUMENT. Yahoo's own statement that it does not carry the symbol comes first,
    then what kind of thing it is, and only then whether it traded:
      `absent`      Yahoo said so (`chart.error`, as for BDMS-F.BK, 2026-10-10)
      `not_daily`   a downsampled series, which says nothing about recent trading
      `not_equity`  `meta.instrumentType` is not EQUITY: a fund, a currency, an index
      `quote_only`  a live quote and no completed bar, as Picton (PCTN.L) is answered every visit
      `empty`       Yahoo knows the symbol and nothing traded in the window
      `stale`       its newest completed bar is older than `FRESH_FOR`
      `priced`      a completed bar with a positive close within `FRESH_FOR`
    """
    try:
        series = yahoo_chart.parse(body)
    except yahoo_chart.YahooRefused:
        return "unreadable"
    if series.error is not None:
        return "absent"
    if series.granularity not in (None, price_chart.INTERVAL):
        return "not_daily"
    if series.meta.get("instrumentType") != "EQUITY":
        return "not_equity"
    completed = [
        p.as_of
        for p in series.points
        if not p.is_live and prices.close_of({"close": p.close}) is not None
    ]
    if not completed:
        return "quote_only" if any(p.is_live for p in series.points) else "empty"
    return PRICED if fresh(max(completed), fetched_at) else "stale"


def fresh(newest: date, fetched_at: datetime) -> bool:
    """Whether a bar dated `newest` is recent, seen from when the body was fetched."""
    return fetched_at.date() - newest <= FRESH_FOR
