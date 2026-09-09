from __future__ import annotations

import pytest

from muffin_ingest import settings


def test_database_url_refuses_to_guess(monkeypatch: pytest.MonkeyPatch) -> None:
    """No default. A writer silently pointed at the wrong database is worse than one that stops."""
    monkeypatch.delenv("INGEST_DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="directly"):
        settings.database_url()


def test_a_provider_defaults_to_its_real_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Which is what keeps the cache removable without an outage."""
    monkeypatch.delenv("SEC_BASE_URL", raising=False)
    assert settings.provider_base("sec", "https://data.sec.gov") == "https://data.sec.gov"


def test_an_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_BASE_URL", "http://http-cache:8080/sec-data")
    assert (
        settings.provider_base("sec", "https://data.sec.gov") == "http://http-cache:8080/sec-data"
    )


def test_a_hyphenated_provider_maps_to_an_underscored_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`alpha-vantage` reads `ALPHA_VANTAGE_BASE_URL`; a dot or a hyphen in an env name is not
    portable, and getting this wrong would silently fall back to the origin and bypass the cache."""
    monkeypatch.setenv("ALPHA_VANTAGE_BASE_URL", "http://http-cache:8080/alphavantage")
    got = settings.provider_base("alpha-vantage", "https://www.alphavantage.co")
    assert got == "http://http-cache:8080/alphavantage"


def test_settings_are_read_at_call_time_not_import(monkeypatch: pytest.MonkeyPatch) -> None:
    """Read at import, a missing variable kills the process before argv is parsed — which is how a
    guard lost its offline mode and stopped running in CI entirely."""
    monkeypatch.setenv("MUFFIN_USER_AGENT", "first")
    assert settings.user_agent() == "first"
    monkeypatch.setenv("MUFFIN_USER_AGENT", "second")
    assert settings.user_agent() == "second"
