"""The provider seam.

The tests are about the ONE question this seam exists to answer: which of a security's several
correct names does this provider want. Getting it wrong has decided the behaviour of five resources
and cost 621 of 1,015 rows in one backlog against a 25-calls-a-day provider.
"""

from __future__ import annotations

from muffin_ingest.providers.base import SecurityRef
from muffin_ingest.providers.outcome import Outcome
from muffin_ingest.providers.vocab import no_data_for_subject, throttled

SAMSUNG = SecurityRef(
    security_id="s1",
    provider_symbol="005930.KS",
    us_ticker=None,
    isin="KR7005930003",
    country_iso2="KR",
)
BERKSHIRE = SecurityRef(
    security_id="s2",
    provider_symbol="BRK-B",
    us_ticker="BRK-B",
    isin="US0846707026",
    country_iso2="US",
    has_us_listing=True,
)
ASML_OTC = SecurityRef(
    security_id="s3",
    provider_symbol="ASML.AS",
    us_ticker="ASMLF",
    isin="NL0010273215",
    country_iso2="NL",
    has_us_listing=False,
)


class PriceProvider:
    """Prices are fetched by the PROVIDER symbol — the local line, not the US lookup."""

    # Annotated, not inferred: a Protocol attribute is INVARIANT, so a stub declaring
    # `control_subject: str` does not satisfy `str | None` and mypy says so.
    code: str = "yfinance"
    batch_size: int = 12
    control_subject: str | None = "AAPL"

    def spell(self, security: SecurityRef) -> str | None:
        return security.provider_symbol

    def classify(self, error: str) -> Outcome:
        if throttled(error):
            return Outcome.THROTTLED
        return Outcome.DEAD_SUBJECT if no_data_for_subject(error) else Outcome.TRANSPORT


class SecProvider:
    """SEC knows only US registrants and only by their own ticker."""

    code: str = "sec"
    batch_size: int = 1
    control_subject: str | None = "AAPL"

    def spell(self, security: SecurityRef) -> str | None:
        return security.us_ticker if security.has_us_listing else None

    def classify(self, error: str) -> Outcome:
        return Outcome.DEAD_SUBJECT if no_data_for_subject(error) else Outcome.TRANSPORT


def test_the_two_providers_want_different_names_for_the_same_company() -> None:
    """The whole reason this seam exists."""
    assert PriceProvider().spell(BERKSHIRE) == "BRK-B"
    assert SecProvider().spell(BERKSHIRE) == "BRK-B"
    assert PriceProvider().spell(SAMSUNG) == "005930.KS"
    assert SecProvider().spell(SAMSUNG) is None, "SEC has no CIK for a Korean line"


def test_an_unaddressable_security_returns_none_rather_than_a_wrong_name() -> None:
    """ASML's US lookup is the thin OTC foreign-ordinary line `ASMLF`, which alpha_vantage answers
    with an empty object. 621 of 1,015 rows in that backlog were exactly this, at three calls a run
    against a 25-a-DAY quota — twenty-six days of budget to learn that an OTC line is an OTC
    line.
    """
    assert SecProvider().spell(ASML_OTC) is None
    assert PriceProvider().spell(ASML_OTC) == "ASML.AS", "prices still work off the local line"
