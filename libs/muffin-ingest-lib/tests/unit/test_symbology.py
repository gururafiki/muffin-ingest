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
from typing import ClassVar

import pytest

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


def test_a_scheme_nobody_asked_about_earns_no_observation_either_way() -> None:
    """A REQUEST NEVER MADE AND A REQUEST THAT ANSWERED NOTHING ARE DIFFERENT FACTS — and on this
    ladder "not asked" is the COMMON case, since a rung skips any subject whose evidence is already
    held. Measured live 2026-09-22 on the first three subjects ever run: all three needed a ticker
    and already had a symbol, their local and Yahoo raw files were correctly EMPTY, and
    `identifier_probe` still gained three `scheme=symbol, outcome=miss` rows — a recorded answer to
    a question nobody put, which `stale_misses` would then pay a provider to re-ask in 30 days.

    THE FIXTURE MAKES THE TWO CANDIDATE RULES DISAGREE IN BOTH DIRECTIONS, because a rule of
    "record whatever we can see" and a rule of "record what we asked" give the same answer whenever
    a subject was asked — which is every case above this one.

    * a value with no question: the TICKER rung's own hits name the local line, so `value` is real
      while both symbol rungs were skipped. The symbol row is still written — it is a finding — and
      the probe is not, because nothing observed it.
    * no value and no question: the shape that was live. Nothing at all.
    * the control, same inputs, asked: a miss. So the flag is the only thing separating them.
    """
    sec = "11111111-1111-1111-1111-111111111111"
    entries = _mapping()

    # A VALUE WITH NO QUESTION. AAPL's own mapping hit names the US line, so the ladder can see a
    # symbol without either symbol rung having spent a request.
    identifiers, symbols, probes = symbology.plan_symbols(
        sec,
        isin="US0378331005",
        country_iso2="US",
        mapping_entry=entries[0],
        yahoo_hits=[],
        venues=VENUES,
        source="openfigi",
        asked_symbol=False,
    )
    assert symbols == [{"security_id": sec, "provider_code": "yfinance", "symbol": "AAPL"}]
    assert [(p["scheme"], p["outcome"]) for p in probes] == [("ticker", "hit")]

    # NO VALUE AND NO QUESTION — the shape that was live in production.
    identifiers, symbols, probes = symbology.plan_symbols(
        sec,
        isin="ZZ0000000000",
        country_iso2="US",
        mapping_entry=entries[2],
        yahoo_hits=[],
        venues=VENUES,
        source="openfigi",
        asked_symbol=False,
    )
    assert identifiers == [] and symbols == []
    assert [(p["scheme"], p["outcome"]) for p in probes] == [("ticker", "miss")]

    # THE CONTROL: the very same inputs, asked. A miss is an observation and is recorded.
    _, _, probes = symbology.plan_symbols(
        sec,
        isin="ZZ0000000000",
        country_iso2="US",
        mapping_entry=entries[2],
        yahoo_hits=[],
        venues=VENUES,
        source="openfigi",
        asked_symbol=True,
    )
    assert [(p["scheme"], p["outcome"]) for p in probes] == [("ticker", "miss"), ("symbol", "miss")]


def test_the_ticker_rung_carries_its_own_question_and_needs_no_flag() -> None:
    """`mapping_entry` IS the question: the caller reconstructs it from that rung's raw row, and
    the rung appends a row for every subject it asks about. So `None` means "not asked" and there
    is nothing a second flag could add — a guard that cannot fire reads as protection without
    being it, which is why this one is absent rather than defaulted.

    The symbol side is asked about here too, so the run is not silent: it is one rung skipped, not
    a subject skipped."""
    sec = "11111111-1111-1111-1111-111111111111"
    _, symbols, probes = symbology.plan_symbols(
        sec,
        isin="US0378331005",
        country_iso2="US",
        mapping_entry=None,
        yahoo_hits=symbology.parse_yahoo_search((FIX / "yahoo_search_aapl.json").read_bytes()),
        venues=VENUES,
        source="openfigi",
    )
    assert [(p["scheme"], p["outcome"]) for p in probes] == [("symbol", "hit")]
    assert symbols == [{"security_id": sec, "provider_code": "yfinance", "symbol": "AAPL"}]


def test_one_listing_is_never_given_to_two_securities() -> None:
    """`(provider_code, symbol)` IS UNIQUE, AND THE UPSERT'S `on conflict` DOES NOT NAME IT — so a
    listing already held elsewhere raised `UniqueViolation` and failed a whole batch of up to 200
    subjects. Measured 2026-09-24: Worldline holds two securities, and OpenFIGI names `WLN.PA` for
    both ISINs.

    THE FIXTURE MAKES EVERY CANDIDATE RULE DISAGREE: A wants a listing B holds (withheld), C and D
    claim one listing in the same batch (both refused — never broken with `min()`), E wants a free
    one (kept), and F re-resolves the listing F ALREADY holds (kept — a rule that withheld every
    held symbol would drop it, and a rule comparing only the symbol could not tell F from A).
    """

    def row(sid: str, symbol: str) -> dict[str, str]:
        return {"security_id": sid, "provider_code": "yfinance", "symbol": symbol}

    rows = [
        row("A", "X.PA"),
        row("C", "Y.PA"),
        row("D", "Y.PA"),
        row("E", "Z.PA"),
        row("F", "W.PA"),
        row("F", "W.PA"),
    ]
    holders = {("yfinance", "X.PA"): "B", ("yfinance", "W.PA"): "F"}

    adoption = symbology.adoptable_symbols(rows, holders)

    assert [(r["security_id"], r["symbol"]) for r in adoption.kept] == [
        ("E", "Z.PA"),
        ("F", "W.PA"),
        ("F", "W.PA"),
    ]
    assert adoption.held_elsewhere == [("A", "X.PA", "B")]
    assert adoption.ambiguous == {"Y.PA": ["C", "D"]}


def test_the_holders_are_asked_about_exactly_the_symbols_in_the_batch() -> None:
    """Narrowed to the batch — the table holds a symbol for every priced security, and a run
    covers 200."""

    class Conn:
        calls: ClassVar[list[tuple[str, tuple[object, ...]]]] = []

        def cursor(self) -> Conn:
            return self

        def __enter__(self) -> Conn:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...]) -> None:
            Conn.calls.append((sql, params))

        def fetchall(self) -> list[tuple[str, str, str]]:
            return [("yfinance", "X.PA", "B")]

    rows = [
        {"security_id": "A", "provider_code": "yfinance", "symbol": "X.PA"},
        {"security_id": "E", "provider_code": "yfinance", "symbol": "Z.PA"},
    ]
    assert symbology.symbol_holders(Conn(), rows) == {("yfinance", "X.PA"): "B"}
    assert Conn.calls[0][1] == (["yfinance"], ["X.PA", "Z.PA"])
    assert symbology.symbol_holders(Conn(), []) == {}, "no rows must not reach the database"
    assert len(Conn.calls) == 1


# --- who the ladder asks about -------------------------------------------------------------------


def test_a_population_query_anti_joins_over_the_entity_not_over_rows() -> None:
    """THE SHAPE, NOT THE TEXT. A backlog written as "join the evidence table and filter where it
    is null" keeps every security whose OWN ISIN row survives the join, so the queue never drains —
    that defect ran for months on `pending_industry`, reporting progress the whole time, and this
    schema has recorded it as its most expensive recurring shape.

    Asserted structurally because the alternative needs a database: `not exists` scoped to the
    security is the only form that asks the question about the ENTITY.
    """
    for sql in (symbology.SUBJECTS_NEEDING_TICKER, symbology.SUBJECTS_NEEDING_SYMBOL):
        flat = " ".join(sql.split())
        assert "not exists" in flat, flat
        assert "left join" not in flat, f"a left-join-and-filter cannot drain: {flat}"
        # The subquery must be tied to the security under test, or it asks "does ANY security have
        # one", which is true from the first row onward and excludes the whole universe.
        assert ".security_id = s.security_id" in flat, flat


def test_a_rung_must_name_the_evidence_it_supplies() -> None:
    """An unknown evidence name is refused rather than silently answering the other rung's
    question — two rungs sharing one grid makes a typo look like a working filter."""
    with pytest.raises(ValueError, match="unknown evidence"):
        symbology.subjects_needing(object(), "figi")


def test_the_populations_are_scoped_to_equities() -> None:
    """A BOND IS NOT A SYMBOLOGY SUBJECT. The population these rungs shipped with was
    `security.is_tradeable = false` — 23,341 securities of which 15,159 were bonds, aimed at
    OpenFIGI's US *equity* lookup and at Yahoo's search, neither of which can serve one."""
    for sql in (symbology.SUBJECTS_NEEDING_TICKER, symbology.SUBJECTS_NEEDING_SYMBOL):
        flat = " ".join(sql.split())
        assert "s.security_type_code = 'equity'" in flat, flat
        assert "is_tradeable" not in flat, (
            "is_tradeable is set by promotion, not by symbol resolution — it is false by default, "
            "so it selects almost the whole universe and says nothing about needing a symbol"
        )
