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


def test_the_cache_url_the_stack_actually_sets_is_the_one_that_works(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`HTTP_CACHE_URL` HAD BEEN SET SINCE THE SERVICE WAS CREATED AND READ BY NOTHING.

    The compose file sets it with a comment saying providers are reached "through the cache,
    exactly as the edge function reaches them" — and `provider_base` looked only for a per-provider
    `<NAME>_BASE_URL`, none of which the stack sets for the ingest services. So the first
    direct-HTTP provider would have gone straight to the origin while every piece of configuration
    said otherwise, and nothing in production can report a cache that is being bypassed.
    """
    monkeypatch.delenv("YAHOO_BASE_URL", raising=False)
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")
    assert settings.provider_base("yahoo", "https://real") == "http://http-cache:8080/yahoo"

    # A trailing slash is the obvious way to configure it and must not produce a double slash.
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080/")
    assert settings.provider_base("yahoo", "https://real") == "http://http-cache:8080/yahoo"


def test_a_per_provider_override_still_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """That is how the edge functions are configured, and an operator pointing ONE provider
    somewhere else — a local mock, a second cache — must be able to."""
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")
    monkeypatch.setenv("YAHOO_BASE_URL", "http://elsewhere")
    assert settings.provider_base("yahoo", "https://real") == "http://elsewhere"


def test_with_no_cache_configured_the_default_is_the_REAL_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """What makes the cache removable without an outage — the property the whole scheme rests on."""
    monkeypatch.delenv("HTTP_CACHE_URL", raising=False)
    monkeypatch.delenv("YAHOO_BASE_URL", raising=False)
    assert settings.provider_base("yahoo", "https://real") == "https://real"


def test_a_provider_the_proxy_does_not_serve_is_REFUSED_rather_than_404d(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A name that is not one of the proxy's own locations produces a URL the cache answers 404 to
    — a failure that reads as the provider being down. The proxy's path is the contract, so a typo
    fails at the point it is made."""
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")
    monkeypatch.delenv("SEC_DATA_BASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="not one of http-cache's locations"):
        settings.provider_base("yahooo", "https://real")

    # And the real ones are accepted, or the guard would refuse everything.
    assert settings.provider_base("sec-data", "https://real").endswith("/sec-data")
