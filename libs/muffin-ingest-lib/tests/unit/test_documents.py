"""The whole-file providers at their network boundary, with the network faked."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import pytest

from muffin_ingest import metrics
from muffin_ingest.providers import documents


def test_a_document_request_is_counted_under_its_provider_not_under_the_cache_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MEASURED IN PRODUCTION, on the first request driven after the roll:

        muffin_ingest_provider_requests_total{outcome="answered",provider="http-cache:8080"} 1.0

    `_get` labelled a request by its URL's host, and in production every base URL is the cache's —
    so SEC and NSE were one series named after a proxy. The fixture routes through the cache,
    which is what makes the two rules DISAGREE: reading the host gives `http-cache:8080`, naming
    the provider gives `nse`. Against the real origin they would agree, and this test would pass
    under either rule — which is exactly why none of the existing ones caught it.

    SEC's file left this module on 2026-11-11 (`sec_edgar.company_tickers`), where the same rule
    is asserted in `test_sec_edgar`.
    """
    for var in ("NSE_BASE_URL", "NSE_ARCHIVES_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")

    asked: list[str] = []
    counted: list[str] = []

    def fake_get(url: str, **kwargs: Any) -> httpx.Response:
        asked.append(url)
        return httpx.Response(200, content=b"{}")

    @contextmanager
    def recording(provider: str) -> Iterator[dict[str, str]]:
        counted.append(provider)
        yield {"outcome": "answered"}

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(metrics, "request", recording)

    documents.nse_equity_list()

    assert asked and all(url.startswith("http://http-cache:8080/") for url in asked), (
        f"the fixture must route through the cache, or the two rules cannot disagree: {asked}"
    )
    assert counted == ["nse"], f"counted under {counted}"
