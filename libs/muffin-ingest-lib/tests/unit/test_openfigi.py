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
    assert row["security_type"] is not row.get("security_type_detail")
    assert rows[0]["ticker"]


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
