"""What every provider must be able to answer, and the one question that keeps being got wrong.

THE SAME COMPANY HAS SEVERAL CORRECT NAMES, AND WHICH IS RIGHT DEPENDS ON WHO IS ASKING. This is
not a tidiness point — it has decided the behaviour of at least five resources:

    prices          the PROVIDER symbol      005930.KS, SAAB-B.ST, 0006.HK
    SEC             the US ticker            BRK-B, and never OpenFIGI's BRK/B
    OpenFIGI        the ISIN                 US0378331005
    DART            a corp_code              00126380
    display         the primary listing      — 365 of 900 non-US securities were LABELLED as a
                                               thin OTC line while being priced off the local one

So a provider declares the KIND of key it wants and the ledger stores what was actually asked with.
A mark made under the wrong spelling is a statement about our typo, not about the company: `BRK/B`,
`WALMEX*.MX`, `6.HK` and `ESSITYB.ST` all return nothing while `BRK-B`, `WALMEX.MX`, `0006.HK` and
`ESSITY-B.ST` return full histories.

AND SPELLING IS NEVER PATTERN-MATCHED AND REWRITTEN. `security-symbol-repair` generates candidates
and VERIFIES each against the provider before adopting it, because the obvious Nordic rule matches
`SAND.ST` (Sandvik), `ALFA.ST` (Alfa Laval) and `TELIA.ST` — complete company names ending in a
letter the rule reads as a share class. `SAND.ST` really does generate `SAN-D.ST`; the provider
refuses it and nothing is written.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from muffin_ingest.providers.outcome import Outcome


@dataclass(frozen=True)
class SecurityRef:
    """Every name a security has, so a provider can pick the one it needs rather than be handed
    "the symbol" and asked to cope."""

    security_id: str
    provider_symbol: str | None = None
    us_ticker: str | None = None
    isin: str | None = None
    figi: str | None = None
    cik: str | None = None
    corp_code: str | None = None
    country_iso2: str | None = None
    has_us_listing: bool = False


@runtime_checkable
class Provider(Protocol):
    """One external source, and everything the ledger needs to know about how to treat it."""

    code: str
    """Matches `ingest.provider_budget.provider_code` and the Dagster pool name."""

    batch_size: int
    """1 where the provider does not batch, and that is a MEASUREMENT rather than a default:
    `equity/fundamental/{income,balance,cash}` return ZERO rows for two symbols, and so do
    `management` and `insider_trading`."""

    control_subject: str | None
    """A subject known to answer, used to prove the provider is up before any other is blamed.
    `None` where no such subject exists, which makes marking impossible for that provider — the
    safe direction."""

    def spell(self, security: SecurityRef) -> str | None:
        """The name THIS provider knows the security by, or None if it cannot address it.

        Returning None is a real answer and must not be confused with an absence: `pending_*`
        populations exist precisely so a security the provider cannot be ASKED about is never
        recorded as one it has no data for.
        """
        ...

    def classify(self, error: str) -> Outcome:
        """What this provider's wording means. Shared vocabulary lives in `vocab.py`; a provider
        overrides only where it says something the others do not."""
        ...
