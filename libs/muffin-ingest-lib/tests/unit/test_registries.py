"""The two whole-file registry parsers, driven over the shapes the providers really serve.

Both headers and both row shapes below were copied from a live response on 2026-09-12 — SEC
10,426 entries, NSE 2,568 rows — rather than invented, because every rule here exists to survive
a specific upstream shape and a fixture that does not have that shape proves nothing.
"""

from __future__ import annotations

import json

import pytest

from muffin_ingest.facets import registries

#: VERBATIM FROM THE LIVE FILE, leading spaces included. Every column after the first has one, and
#: that is the whole reason the parser matches on a trimmed name.
NSE_HEADER = (
    "SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, "
    "ISIN NUMBER, FACE VALUE"
)
NSE_ROW = "20MICRONS,20 Microns Limited,EQ,06-OCT-2008,5,1,INE144J01027,5"


def test_the_cik_map_is_an_object_keyed_by_row_index_not_an_array() -> None:
    """READ AS A LIST IT YIELDS NOTHING, WITH NO ERROR.

    SEC serves `{"0": {"cik_str": 1045810, "ticker": "NVDA", ...}, "1": {...}}` — 10,426 entries
    keyed by position as a STRING. A parser expecting an array gets an empty result and reports
    success, which is indistinguishable from SEC having delisted every registrant.
    """
    body = json.dumps(
        {
            "0": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
            "1": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        }
    ).encode()
    assert registries.cik_map(body) == [
        {"ticker": "NVDA", "cik": 1045810},
        {"ticker": "AAPL", "cik": 320193},
    ]


def test_an_array_is_refused_rather_than_silently_empty() -> None:
    """The failure mode the shape above invites, made loud."""
    with pytest.raises(registries.RegistryUnreadable, match="object keyed by row index"):
        registries.cik_map(b'[{"cik_str": 1, "ticker": "A"}]')


def test_a_cik_map_that_parses_to_nothing_is_a_shape_change_not_an_empty_registry() -> None:
    """~10,400 filers is the steady state, so zero is never the news it appears to be."""
    with pytest.raises(registries.RegistryUnreadable, match="zero filers"):
        registries.cik_map(b"{}")


def test_the_nse_isin_column_is_found_by_name_not_by_position() -> None:
    """A COLUMN INSERTED UPSTREAM SHIFTS EVERY POSITIONAL READ, AND THE FAILURE IS SILENT.

    Each company would get another company's symbol, which then maps its filings onto the wrong
    security. The fixture inserts a column BEFORE the ISIN so a positional parser returns the
    wrong value rather than crashing — if it merely crashed, the rules would be indistinguishable.
    """
    shifted_header = NSE_HEADER.replace(" ISIN NUMBER", " NEW COLUMN, ISIN NUMBER")
    shifted_row = NSE_ROW.replace(",INE144J01027", ",something,INE144J01027")

    got = registries.nse_equities(f"{shifted_header}\n{shifted_row}\n".encode())
    assert got == [{"symbol": "20MICRONS", "isin": "INE144J01027"}], (
        "the ISIN was read by position: a column inserted upstream now maps this company onto "
        "whatever sits at the old index"
    )


def test_a_changed_nse_header_throws_rather_than_returning_nothing() -> None:
    """An empty list is a provider event; a header this parser cannot read is OUR problem, and
    the two demand opposite responses. Silence would look like India delisting."""
    with pytest.raises(registries.RegistryUnreadable, match="header changed"):
        registries.nse_equities(b"SYMBOL,NAME OF COMPANY\nFOO,Foo Ltd\n")


def test_a_row_whose_isin_is_not_twelve_characters_is_dropped() -> None:
    """A bad key here maps one company's filings onto another, so a malformed row is discarded
    rather than stored. The good row beside it is what proves the parser did not simply fail."""
    rows = "\n".join(
        [
            NSE_HEADER,
            NSE_ROW,
            "TRUNC,Truncated Ltd,EQ,01-JAN-2020,5,1,INE144,5",
            "EMPTY,Empty Ltd,EQ,01-JAN-2020,5,1,,5",
        ]
    )
    got = registries.nse_equities(f"{rows}\n".encode())
    assert got == [{"symbol": "20MICRONS", "isin": "INE144J01027"}]


def test_an_empty_nse_list_is_a_provider_event() -> None:
    with pytest.raises(registries.RegistryUnreadable, match="zero equities"):
        registries.nse_equities(f"{NSE_HEADER}\n".encode())
