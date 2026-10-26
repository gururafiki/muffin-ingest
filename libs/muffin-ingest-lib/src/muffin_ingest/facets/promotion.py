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

from collections.abc import Sequence
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


#: TICKERS YAHOO WRITES WITH A FIXED NUMBER OF DIGITS. OpenFIGI's Hong Kong ticker `883` is Yahoo's
#: `0883.HK`: measured 2026-10-26, 325 of the 1,584 Hong Kong candidates had 1-3 digits, and the
#: tracked ones already hold `0016.HK`.
PAD_NUMERIC = {"HK": 4}


def yahoo_ticker(ticker: str, country: str | None) -> str:
    """The directory's ticker as Yahoo writes it.

    Bloomberg's share-class `/` is Yahoo's `-` (`BEK/B` is `BEK-B.TO`; measured, `BRK/B` 400s and
    `BRK-B` answers). Over the 9,031 candidates of 2026-10-26, 293 Canadian and 6 Australian tickers
    carried one. A Hong Kong number is padded to four digits.
    """
    spelt = ticker.replace("/", "-")
    width = PAD_NUMERIC.get(country or "")
    if width and spelt.isdigit():
        spelt = spelt.zfill(width)
    return spelt


def spellings(
    ticker: str | None,
    provider_symbol: str | None,
    country: str | None,
    suffixes: Sequence[str],
) -> list[str]:
    """The symbols to ask Yahoo about one directory line, in the order asked, without repeats.

    Each suffix the country uses (`market.yahoo_spelling`), on the ticker as Yahoo writes it: Korea
    is `.KS` then `.KQ`, because the KS composite files KOSDAQ lines with KOSPI's and Yahoo spells
    them `.KQ`. A country with no suffix listed is asked under the directory's own spelling only.
    These are CANDIDATES: Yahoo verifies each, and nothing is adopted that it did not price.
    """
    if ticker and suffixes:
        base = yahoo_ticker(ticker, country)
        return list(dict.fromkeys(base + suffix for suffix in suffixes))
    return [provider_symbol] if provider_symbol else []


#: WHAT A WRONG SPELLING IS ANSWERED WITH. `absent` is Yahoo saying it does not know the name.
#: `not_equity` is the other shape, and was found by the first live wave (2026-10-26): Yahoo answers
#: a Korean line under the wrong suffix with a SHADOW of the right one, the same company and
#: exchange typed `MUTUALFUND`. Measured over 30 Korean candidates asked under both suffixes, the
#: wrong one was `absent` 12 times and a `MUTUALFUND` shadow on KOE 18 times, in both directions
#: (a KOSPI line asked `.KQ` too); the right one was `EQUITY` every time. The directory types every
#: candidate as an equity, so a non-equity answer is about the name, never about the line.
NOT_THIS_NAME = frozenset({"absent", "not_equity"})


def asks_the_next_spelling(outcome: str) -> bool:
    """Whether a verdict sends the probe on to the line's next spelling.

    Only an answer that is not about the line (`NOT_THIS_NAME`). A stale, empty or quote-only
    equity IS the line: the company not trading. Asking further would only find a copy elsewhere,
    and Canada's NEO carries thin copies of TSX Venture lines (measured 2026-10-26: one bar in
    thirty days), which would promote a halted company through its quietest venue. `unreadable` is
    no answer at all, so another spelling would learn nothing from it.
    """
    return outcome in NOT_THIS_NAME


def conclude(verdicts: Sequence[str], spellings: int) -> tuple[int, str]:
    """What one line's walk over its spellings says, and which answer says it: (index, outcome).

    `priced` if any spelling priced. Otherwise the walk's last answer, when it is about the line
    (`stale`, `quote_only`, `empty`, `not_daily`) or no answer (`unreadable`). A walk that stopped
    on a wrong-name answer before asking every spelling is `unfinished`: a refusal or a failed
    request cut it short, and it says nothing. A finished walk of wrong-name answers only is
    `not_equity` if any spelling named a non-equity, else `absent`, so a fund the directory
    miscalled an equity is recorded as one rather than as unknown.
    """
    for index, outcome in enumerate(verdicts):
        if outcome == PRICED:
            return index, PRICED
    last = len(verdicts) - 1
    if not asks_the_next_spelling(verdicts[last]):
        return last, verdicts[last]
    if len(verdicts) < spellings:
        return last, "unfinished"
    for index, outcome in enumerate(verdicts):
        if outcome != "absent":
            return index, outcome
    return last, "absent"
