"""The symbology facet — OpenFIGI positional mapping and Yahoo's ISIN search — over REAL captures.

`openfigi_mapping.json` is OpenFIGI's answer to two real ISINs plus a rejected idValue (entries:
AAPL hit, ACN hit, `{"error": "Invalid idValue format."}`). `openfigi_mapping_otc_lines.json` is six
Japanese ISINs that ALL resolve to thin US OTC lines on `exchCode: US` — the exact trap this family
exists to keep. The four `yahoo_search_*.json` files include Walmex resolving to a Frankfurt line
ONLY (`4GNB.F`), which is why `pick_home_listing` refuses to take the first hit.
"""

from __future__ import annotations

import json
from pathlib import Path

from muffin_ingest.facets import symbology

FIX = Path(__file__).parent.parent / "fixtures"

#: country → [(OpenFIGI exchCode, yfinance suffix)]. Load-bearing values, not a fake catalog: this
#: is the shape `market.exchange` seeds.
VENUES: dict[str, list[tuple[str, str]]] = {
    "US": [("US", "")],
    "KR": [("KS", ".KS")],
    "MX": [("MX", ".MX")],
    "JP": [("TKS", ".T")],
    "HK": [("HKS", ".HK")],
}


def _mapping() -> list[symbology.MappingEntry]:
    return symbology.parse_mapping(
        (FIX / "openfigi_mapping.json").read_bytes(),
        asked=["US0378331005", "IE00B4BNMY34", "ZZ0000000000"],
    )


def test_the_captured_mapping_is_positional_and_carries_hits_and_a_refusal() -> None:
    entries = _mapping()
    assert [e.asked for e in entries] == ["US0378331005", "IE00B4BNMY34", "ZZ0000000000"]
    assert entries[0].hits[0]["ticker"] == "AAPL"
    assert entries[1].hits[0]["ticker"] == "ACN"
    # The rejected idValue is a REFUSAL about that job, not data and not nothing.
    assert entries[2].error is not None
    assert entries[2].matched is False


def test_a_reordered_body_attaches_the_wrong_ticker_and_is_impossible_to_miss() -> None:
    """POSITIONAL IS LOAD-BEARING: entry j answers job j. If the parse ever read body[i] for
    asked[i-1], AAPL's ticker would land on the ACN job — the exact silence this family is built
    against — so the fixture makes the two entries disagree (AAPL first, asked ACN first)."""
    body = json.dumps(
        [
            {"data": [{"ticker": "ACN", "exchCode": "US", "name": "ACCENTURE"}]},
            {"data": [{"ticker": "AAPL", "exchCode": "US", "name": "APPLE"}]},
        ]
    ).encode()
    entries = symbology.parse_mapping(body, asked=["IE00B4BNMY34", "US0378331005"])
    assert entries[0].hits[0]["ticker"] == "ACN", "ACN is the answer to the ACN job"
    assert entries[1].hits[0]["ticker"] == "AAPL"


def test_every_otc_line_capture_resolves_nowhere_on_the_US_listing() -> None:
    """The `ASMLF` lesson, captured: all six Japanese ISINs resolve to thin US OTC foreign-ordinary
    lines (`ALNPF`, `ASCCF`, `CAJFF`…). For a Japanese security's LOCAL symbol this is the trap —
    they are US listings, not the `.T` line the price provider addresses."""
    body = (FIX / "openfigi_mapping_otc_lines.json").read_bytes()
    asked = [
        "JP3429800000",
        "JP3118000003",
        "JP3242800005",
        "JP3519400000",
        "JP3481800005",
        "JP3188200004",
    ]
    entries = symbology.parse_mapping(body, asked=asked)
    assert all(e.matched for e in entries)
    for e in entries:
        pick = symbology.pick_local_symbol("JP", e.hits, VENUES)
        assert pick is None, (
            f"a US OTC line must not be chosen as Japan's local symbol: {e.hits[0]}"
        )


def test_pick_local_symbol_chooses_the_home_board_not_the_first_hit() -> None:
    """Samsung lists on KS, US, PQ and more; the home board is what prices the Korean security."""
    matches = [
        {"ticker": "055550", "exch_code": "KP"},  # Korea OTC, not the primary board
        {"ticker": "SSNLF", "exch_code": "US"},
        {"ticker": "005930", "exch_code": "KS (KSE)"},  # label-stripped to KS
    ]
    pick = symbology.pick_local_symbol("KR", matches, VENUES)
    assert pick == {"symbol": "005930.KS", "composite_figi": None}


def test_the_us_case_has_no_suffix_at_all() -> None:
    pick = symbology.pick_local_symbol("US", [{"ticker": "BRK-B", "exch_code": "US"}], VENUES)
    assert pick == {"symbol": "BRK-B", "composite_figi": None}


def test_the_captured_yahoo_searches_parse_to_their_quotes() -> None:
    assert [
        q["symbol"]
        for q in symbology.parse_yahoo_search((FIX / "yahoo_search_aapl.json").read_bytes())
    ] == ["AAPL"]
    assert [
        q["symbol"]
        for q in symbology.parse_yahoo_search((FIX / "yahoo_search_samsung.json").read_bytes())
    ] == ["005930.KS"]
    assert symbology.parse_yahoo_search((FIX / "yahoo_search_nothing.json").read_bytes()) == []


def test_pick_home_listing_accepts_the_local_line_and_refuses_the_foreign_one() -> None:
    samsung = symbology.parse_yahoo_search((FIX / "yahoo_search_samsung.json").read_bytes())
    assert symbology.pick_home_listing("KR", samsung, VENUES) == "005930.KS"

    walmex = symbology.parse_yahoo_search((FIX / "yahoo_search_walmex.json").read_bytes())
    assert walmex[0]["symbol"] == "4GNB.F"  # the ONLY quote: Frankfurt
    assert symbology.pick_home_listing("MX", walmex, VENUES) is None, (
        "taking the first hit would price a Mexican retailer off a thin German line"
    )


def test_an_offshore_incorporation_falls_back_to_any_known_suffix() -> None:
    """N-PORT reports the INCORPORATION jurisdiction — Alibaba is `KY`, a Cayman incorporation with
    no Cayman venue. The home-market rule would refuse even the correct `9988.HK`, so where the
    country names no market, any KNOWN suffix is accepted (`.SG` Stuttgart is refused because the
    venue table has no Singapore/Stuttgart exchange)."""
    no_ky = {k: v for k, v in VENUES.items() if k != "KY"}
    hits = [
        {"symbol": "9988.HK", "quote_type": "EQUITY"},
        {"symbol": "KYG017191142.SG", "quote_type": "EQUITY"},
    ]
    assert symbology.pick_home_listing("KY", hits, no_ky) == "9988.HK"


def test_a_non_equity_quote_is_skipped() -> None:
    hits = [
        {"symbol": "AAPL250117C00200000", "quote_type": "OPTION"},
        {"symbol": "AAPL", "quote_type": "EQUITY"},
    ]
    assert symbology.pick_home_listing("US", hits, VENUES) == "AAPL"


def test_plan_symbols_records_a_hit_and_a_miss_as_observations() -> None:
    sec = "11111111-1111-1111-1111-111111111111"
    entries = _mapping()
    identifiers, symbols, probes = symbology.plan_symbols(
        sec,
        isin="US0378331005",
        country_iso2="US",
        mapping_entry=entries[0],
        yahoo_hits=symbology.parse_yahoo_search((FIX / "yahoo_search_aapl.json").read_bytes()),
        venues=VENUES,
        source="openfigi",
    )
    assert identifiers == [
        {"kind_code": "ticker", "value": "AAPL", "security_id": sec, "source_code": "openfigi"}
    ]
    assert symbols == [{"security_id": sec, "provider_code": "yfinance", "symbol": "AAPL"}]
    assert [(p["scheme"], p["outcome"]) for p in probes] == [("ticker", "hit"), ("symbol", "hit")]

    # And the security the providers have NOTHING for still records the misses.
    identifiers, symbols, probes = symbology.plan_symbols(
        sec,
        isin="ZZ0000000000",
        country_iso2="US",
        mapping_entry=entries[2],
        yahoo_hits=[],
        venues=VENUES,
        source="openfigi",
    )
    assert identifiers == [] and symbols == []
    assert [(p["scheme"], p["outcome"]) for p in probes] == [("ticker", "miss"), ("symbol", "miss")]
