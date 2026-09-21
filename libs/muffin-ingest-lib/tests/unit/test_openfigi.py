"""The OpenFIGI `/v3/filter` parser, driven over a REAL captured page.

`openfigi_filter_au_page1.json` is what OpenFIGI served for `exchCode: AU, securityType2: Common
Stock` on 2026-09-12 — 100 rows, a `next` cursor and a `total` of 2,117. The error shapes below are
also real: `{"error": "Invalid key 'includeEntitlements'."}` is served with HTTP 200.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from muffin_ingest.facets import openfigi

FIX = Path(__file__).parent.parent / "fixtures"


def _page() -> bytes:
    return (FIX / "openfigi_filter_au_page1.json").read_bytes()


def test_the_captured_page_parses_to_100_rows_with_a_cursor_and_total() -> None:
    rows, next_, total = openfigi.parse_filter(_page(), exch_code="AU")
    assert len(rows) == 100
    assert total == 2117
    assert next_ and len(next_) > 1, "the page carries the cursor the sweep resumes from"


def test_the_columns_are_openfigi_s_fields_with_the_fine_type_beside_the_coarse() -> None:
    rows, _, _ = openfigi.parse_filter(_page(), exch_code="AU")
    row = rows[0]
    assert row["exch_code"] == "AU"
    assert row["figi"].startswith("BBG")
    assert row["composite_figi"] == row["figi"]  # a home-market row IS its composite
    assert row["security_type"] == "Common Stock"  # the coarse bucket we swept on
    # THE NAME IS THE ASSERTION, AND THE PREVIOUS VERSION HAD NEITHER HALF OF IT. It read
    # `row["security_type"] is not row.get("security_type_detail")`, which is true when the key is
    # ABSENT — `.get` returns None and a string is not None — so it passed while the parser emitted
    # a column no table has. The writer takes its columns from the row's keys, so the mismatch only
    # exists at the INSERT, and this lane had never run: the first real write died with
    # `column "security_type_detail" of relation "venue_listing" does not exist`.
    assert "security_type_detail" not in row, (
        "the database's column is `figi_security_type`; a second spelling here reaches no table"
    )
    assert row["figi_security_type"], row
    assert rows[0]["ticker"]


def test_the_fine_type_is_read_from_securityType_not_from_the_coarse_bucket() -> None:
    """THE TWO COINCIDE FOR A PLAIN COMMON STOCK, which is why the captured page cannot tell the
    rules apart — every row there reads `Common Stock` twice. An ETF is where they diverge, and it
    is the case the column exists for: `ETP` inside `Mutual Fund`, the only thing in this response
    that distinguishes an exchange-traded fund from an open-end one."""
    body = json.dumps(
        {
            "data": [
                {
                    "figi": "BBG000BDTBL9",
                    "ticker": "STW",
                    "name": "SPDR S&P/ASX 200 FUND",
                    "securityType": "ETP",
                    "securityType2": "Mutual Fund",
                }
            ],
            "next": None,
            "total": 1,
        }
    ).encode()
    rows, _, _ = openfigi.parse_filter(body, exch_code="AU")
    assert rows[0]["security_type"] == "Mutual Fund"
    assert rows[0]["figi_security_type"] == "ETP", (
        "reading securityType2 into both columns loses the only field that names a fund"
    )


def test_a_row_without_a_figi_or_ticker_is_dropped_not_stored() -> None:
    body = json.dumps(
        {
            "data": [
                {
                    "figi": "BBG000B9XBU4",
                    "ticker": "SWP",
                    "name": "Swoop",
                    "securityType2": "Common Stock",
                },
                {"figi": "BBG000B9XBU5", "ticker": "", "name": "No ticker"},
                {"ticker": "NOTICK", "name": "No figi"},
            ]
        }
    ).encode()
    rows, _, _ = openfigi.parse_filter(body, exch_code="AU")
    assert len(rows) == 1 and rows[0]["ticker"] == "SWP"


def test_a_200_carrying_an_error_is_a_refusal_not_an_empty_venue() -> None:
    """OpenFIGI answers `{"error": "Invalid key '…'."}` with HTTP 200. That is a shape problem in
    OUR request, and it must never read as a venue with no listings."""
    with pytest.raises(openfigi.OpenFigiUnreadable, match="Invalid key"):
        openfigi.parse_filter(b'{"error": "Invalid key \'includeEntitlements\'."}', exch_code="AU")


def test_an_exhausted_venue_has_no_next_cursor() -> None:
    rows, next_, total = openfigi.parse_filter(
        json.dumps(
            {"data": [{"figi": "BBG000B9XBU4", "ticker": "SWP"}], "next": None, "total": 1}
        ).encode(),
        exch_code="AU",
    )
    assert rows and next_ is None and total == 1


def test_an_unparseable_body_is_refused() -> None:
    with pytest.raises(openfigi.OpenFigiUnreadable, match="not JSON"):
        openfigi.parse_filter(b"<html>banana</html>", exch_code="AU")


# --- THE KEY -----------------------------------------------------------------------------------
#
# `OPENFIGI_API_KEY` was rendered into this service's environment from the day it existed, and the
# provider sent only a `Content-Type` behind a docstring calling the API "keyless". Every OpenFIGI
# budget this repo measured was therefore the anonymous one, measured while holding a key.


def _captured_headers(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Drive the real `_post` over a mock transport and return the headers it actually sent."""
    import httpx

    from muffin_ingest.providers import openfigi as provider

    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update({k.lower(): v for k, v in request.headers.items()})
        return httpx.Response(200, json={"data": [], "total": 0})

    transport = httpx.MockTransport(handler)
    real_post = httpx.post

    def posting(url: str, **kw: object) -> httpx.Response:
        with httpx.Client(transport=transport) as client:
            return client.post(url, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx, "post", posting)
    try:
        provider.filter_exchange("AU")
    finally:
        monkeypatch.setattr(httpx, "post", real_post)
    return seen


def test_the_api_key_is_sent_when_one_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENFIGI_API_KEY", "a-test-key")
    assert _captured_headers(monkeypatch).get("x-openfigi-apikey") == "a-test-key"


def test_no_header_is_sent_when_no_key_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """THE OTHER HALF, and it is not symmetry for its own sake: sending an EMPTY credential is not
    the same as sending none — OpenFIGI answers that with a 401, which would read as an outage on
    a deployment that has deliberately not configured a key."""
    monkeypatch.delenv("OPENFIGI_API_KEY", raising=False)
    assert "x-openfigi-apikey" not in _captured_headers(monkeypatch)


def test_both_budgets_follow_the_key_rather_than_being_assumed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both ceilings are the provider's OWN 413 text (`Request may only contain N mapping jobs`),
    measured 2026-09-21 at 11 jobs unkeyed and 101 keyed. A single constant would be wrong in one
    of the two configurations, and wrong upward is a 413 that fails the whole run."""
    from muffin_ingest.providers import openfigi as provider

    monkeypatch.delenv("OPENFIGI_API_KEY", raising=False)
    assert provider.has_api_key() is False
    assert provider.mapping_jobs_per_request() == 10
    assert provider.sweep_pacing_s() == 12.0

    monkeypatch.setenv("OPENFIGI_API_KEY", "a-test-key")
    assert provider.has_api_key() is True
    assert provider.mapping_jobs_per_request() == 100
    assert provider.sweep_pacing_s() == 0.3

    # WHITESPACE IS NOT A KEY. A secret rendered from an empty Jinja default arrives as "" or "\n",
    # and treating that as configured sends an empty credential — the 401 above.
    monkeypatch.setenv("OPENFIGI_API_KEY", "   \n ")
    assert provider.has_api_key() is False
