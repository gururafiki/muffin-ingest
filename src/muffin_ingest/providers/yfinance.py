"""yfinance, reached through the openbb hub in-process.

THE THING TO KNOW BEFORE SIZING ANYTHING: a batched call is NOT a batched request. Read from
`openbb_yfinance/utils/helpers.py`:

    data = yf.download(tickers=symbol, ..., threads=False, **kwargs)   # symbol = "A,B,C,…"

`threads=False`, comma-joined tickers — so yfinance issues ONE YAHOO REQUEST PER SYMBOL, serially,
inside one openbb call. It matches the measurement exactly: twelve symbols at full history took
8.1 s, or ~0.67 s each.

Two consequences, and both were got wrong first:

  * Batching collapses OUR call count, not the vendor's. A full pass over 10,894 askable equities
    is 545 openbb calls and ~10,894 Yahoo requests either way. Any claim that batching "saves the
    provider budget" is false; what it saves is process and framing overhead.
  * THE BUDGET IS THEREFORE DENOMINATED IN SYMBOLS, NOT CALLS. A limiter set to "1 call per second"
    against a batch of 20 is really 20 requests per second — twenty times looser than it reads, and
    the old system's rate-limit incidents came from exactly that kind of accounting.
"""

from __future__ import annotations

from muffin_ingest.providers.base import SecurityRef
from muffin_ingest.providers.openbb import classify as classify_hub
from muffin_ingest.providers.outcome import Outcome


class Yfinance:
    """Prices, dividends and splits for the whole universe."""

    code = "yfinance"

    #: Symbols per openbb call for the DAILY cross-section, where a batch is ~one day of bars each.
    #: Measured on the predecessor at 20 with no failures.
    batch_size = 20

    #: And a SMALLER one for full history, which is a MEMORY budget wearing a time budget's clothes:
    #: twelve symbols at full history measured 11.6 MB of JSON in 8.1 s, so twenty is ~19 MB before
    #: it is parsed into objects. The old Deno worker had 256 MB and used six; this one has more
    #: room and still has no reason to hold five times the frame it needs.
    history_batch_size = 10

    #: A symbol known to answer, used to prove the provider is up before any other symbol is blamed
    #: for an empty response. Without one, marking is impossible — which is the safe direction.
    control_subject = "AAPL"

    def spell(self, security: SecurityRef) -> str | None:
        """The PROVIDER symbol, and never the US ticker when a provider symbol exists.

        This distinction has decided the behaviour of five resources. OpenFIGI's US lookup returns a
        thin OTC foreign-ordinary line for most foreign companies — `ASMLF`, `TSMWF`, `SAPGF` — and
        prices fetched under it are prices of a different, barely-traded instrument. Measured
        2026-08-12: 365 of 900 sampled non-US securities were LABELLED as such a line while being
        priced off the local one, because the fetch key was already `coalesce(provider_symbol,
        ticker)` and only the display symbol was wrong.

        Returning None is a real answer meaning "this provider cannot be asked about this security",
        and it must never reach the ledger as an absence: a security we could not ASK about has not
        been found to have no data.
        """
        return security.provider_symbol or security.us_ticker

    def classify(self, error: str) -> Outcome:
        """Shared vocabulary, plus nothing of its own.

        `YFRateLimitError` is already caught by the `ratelimit` term, and the adapter's
        `'NoneType' object has no attribute 'empty'` — which reads like a bug in our code and is
        actually the adapter dereferencing a frame it never received for a venue it does not cover
        — is already classified as a statement about the subject.

        This method exists to make that explicit rather than implicit: a provider that shares the
        vocabulary says so, so a future term added here cannot be mistaken for the shared one.
        """
        return classify_hub(error)
