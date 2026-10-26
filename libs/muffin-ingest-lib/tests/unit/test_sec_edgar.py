"""The SEC transport: edgartools configured before its first import, its cache off, three outcomes.

The configuration tests run in a SUBPROCESS, because what is under test is the order of a process's
first import of `edgar`, which no in-process test can undo or repeat.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest

from muffin_ingest.providers import sec_edgar

AGENT = "muffin-test/1.0 (ops@example.com)"


def _run(code: str, env: dict[str, str]) -> str:
    result = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip().splitlines()[-1]


def test_importing_the_provider_does_not_import_edgar() -> None:
    """The Dagster definitions load without edgartools installed, and importing it writes."""
    out = _run(
        "import sys, muffin_ingest.providers.sec_edgar; print('edgar' in sys.modules)",
        {},
    )
    assert out == "False"


def test_edgar_is_configured_before_its_first_import(tmp_path: Path) -> None:
    pytest.importorskip("edgar")
    scratch = tmp_path / "edgar"
    code = (
        "import json, os; from muffin_ingest.providers import sec_edgar as s; "
        "d = s.describe(); from edgar import config, httpclient; "
        "d['sec_base'] = config.SEC_BASE_URL; "
        # A BOOLEAN, never the agent itself: the header the manager will send, as it builds it.
        "d['sends_agent'] = httpclient.get_http_params()['headers']['User-Agent'] "
        "== os.environ['MUFFIN_USER_AGENT']; "
        "print(json.dumps(d))"
    )
    out = json.loads(
        _run(
            code,
            {
                "HTTP_CACHE_URL": "http://http-cache:8080",
                "MUFFIN_USER_AGENT": AGENT,
                "EDGAR_SCRATCH_DIR": str(scratch),
            },
        )
    )
    assert out["cache_mode"] == "Disabled"
    assert out["base_url"] == "http://http-cache:8080/sec"
    assert out["data_url"] == "http://http-cache:8080/sec-data"
    # edgartools read EDGAR_BASE_URL at import, so it was set first.
    assert out["sec_base"] == "http://http-cache:8080/sec"
    assert out["sends_agent"] is True
    assert out["rate_per_second"] == 5
    # Importing writes only an empty `_tcache/` and edgartools' zero-byte locale marker: nothing
    # that could serve a response.
    written = {
        str(p.relative_to(scratch)): p.stat().st_size for p in scratch.rglob("*") if p.is_file()
    }
    assert written == {"_tcache/.locale_fix_457_applied": 0}


@contextmanager
def _sec(answers: dict[str, Any], seen: list[httpx.Request]) -> Iterator[None]:
    """SEC answering each URL path with a status (or raising), recording every request."""
    pytest.importorskip("edgar")
    httpclient = sec_edgar.edgar().httpclient

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        answer = answers[request.url.path]
        if isinstance(answer, Exception):
            raise answer
        return httpx.Response(answer, content=b'{"ok": true}' if answer == 200 else b"<Error/>")

    @contextmanager
    def http_client(**kwargs: Any) -> Iterator[httpx.Client]:
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            yield client

    saved = httpclient.http_client
    httpclient.http_client = http_client
    try:
        yield
    finally:
        httpclient.http_client = saved


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MUFFIN_USER_AGENT", AGENT)
    monkeypatch.setenv("EDGAR_SCRATCH_DIR", str(tmp_path / "edgar"))
    monkeypatch.delenv("HTTP_CACHE_URL", raising=False)


def test_an_answer_and_an_absence_both_return_with_their_status() -> None:
    seen: list[httpx.Request] = []
    with _sec({"/a": 200, "/b": 404}, seen):
        answered = sec_edgar.fetch("https://data.sec.gov/a", provider="sec-data")
        absent = sec_edgar.fetch("https://data.sec.gov/b", provider="sec-data")
    assert (answered.status, answered.document.body) == (200, b'{"ok": true}')
    assert absent.status == 404


@pytest.mark.parametrize("status", [403, 429, 500, 503])
def test_a_refusal_raises_and_is_never_an_absence(status: int) -> None:
    with _sec({"/a": status}, []), pytest.raises(sec_edgar.SecRefused) as raised:
        sec_edgar.fetch("https://data.sec.gov/a", provider="sec-data")
    assert raised.value.status == status


def test_a_request_with_no_status_is_transport() -> None:
    with (
        _sec({"/a": httpx.ConnectError("refused")}, []),
        pytest.raises(sec_edgar.SecTransport),
    ):
        sec_edgar.fetch("https://data.sec.gov/a", provider="sec-data")


def test_every_request_goes_past_the_cache() -> None:
    """A filing-triggered refresh must not read last week's company facts from http-cache."""
    seen: list[httpx.Request] = []
    with _sec({"/a": 200}, seen):
        sec_edgar.fetch("https://data.sec.gov/a", provider="sec-data")
    assert seen[0].headers["X-Muffin-Cache-Bypass"] == "1"
    assert seen[0].headers["X-Muffin-Cache-No-Store"] == "1"


def test_urls_name_each_document_on_its_host() -> None:
    assert (
        sec_edgar.companyfacts_url(320193)
        == "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
    )
    assert (
        sec_edgar.submissions_url(1652044) == "https://data.sec.gov/submissions/CIK0001652044.json"
    )
    assert (
        sec_edgar.submissions_page_url("CIK0000043920-submissions-001.json")
        == "https://data.sec.gov/submissions/CIK0000043920-submissions-001.json"
    )
    assert (
        sec_edgar.daily_index_url(date(2026, 10, 23))
        == "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/form.20261023.idx"
    )
    assert (
        sec_edgar.quarter_listing_url(date(2026, 4, 1))
        == "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR2/index.json"
    )
    assert (
        sec_edgar.filing_text_url(320193, "0001140361-26-038022")
        == "https://www.sec.gov/Archives/edgar/data/320193/000114036126038022/"
        "0001140361-26-038022.txt"
    )
    assert sec_edgar.insider_dataset_url(2026, 1).endswith(
        "/files/structureddata/data/insider-transactions-data-sets/2026q1_form345.zip"
    )


def test_through_the_cache_each_document_uses_its_hosts_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`sec-data` proxies to data.sec.gov and `sec` to www.sec.gov; swapping them is a 404."""
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")
    assert sec_edgar.companyfacts_url(1).startswith("http://http-cache:8080/sec-data/")
    assert sec_edgar.daily_index_url(date(2026, 10, 23)).startswith(
        "http://http-cache:8080/sec/Archives/"
    )
