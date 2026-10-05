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
from typing import Any, ClassVar

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


def observed(probes: list[dict[str, object]]) -> list[tuple[object, object, object]]:
    """Each probe row as (scheme, provider, outcome) — the provider is part of the probe's key."""
    return [(p["scheme"], p["provider"], p["outcome"]) for p in probes]


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
        asked_local=True,
        asked_yahoo=True,
        dead_symbol=None,
    )
    assert identifiers == [
        {"kind_code": "ticker", "value": "AAPL", "security_id": sec, "source_code": "openfigi"}
    ]
    assert symbols == [{"security_id": sec, "provider_code": "yfinance", "symbol": "AAPL"}]
    assert observed(probes) == [
        ("ticker", "openfigi", "hit"),
        ("symbol", "openfigi", "hit"),
        ("symbol", "yahoo", "hit"),
    ]

    # And the security the providers have NOTHING for still records the misses.
    identifiers, symbols, probes = symbology.plan_symbols(
        sec,
        isin="ZZ0000000000",
        country_iso2="US",
        mapping_entry=entries[2],
        yahoo_hits=[],
        venues=VENUES,
        source="openfigi",
        asked_local=True,
        asked_yahoo=True,
        dead_symbol=None,
    )
    assert identifiers == [] and symbols == []
    assert observed(probes) == [
        ("ticker", "openfigi", "miss"),
        ("symbol", "openfigi", "miss"),
        ("symbol", "yahoo", "miss"),
    ]


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
        asked_local=False,
        asked_yahoo=False,
        dead_symbol=None,
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
        asked_local=False,
        asked_yahoo=False,
        dead_symbol=None,
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
        asked_local=True,
        asked_yahoo=False,
        dead_symbol=None,
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
        asked_local=True,
        asked_yahoo=True,
        dead_symbol=None,
    )
    assert observed(probes) == [("symbol", "openfigi", "miss"), ("symbol", "yahoo", "hit")]
    assert symbols == [{"security_id": sec, "provider_code": "yfinance", "symbol": "AAPL"}]


def test_each_symbol_rung_is_observed_under_its_own_provider() -> None:
    """TWO PROVIDERS, TWO OBSERVATIONS. The local rung is OpenFIGI and the fallback is Yahoo's
    search, and `identifier_probe` is keyed `(security_id, scheme, provider)`.

    The ladder used to record ONE symbol probe labelled `openfigi` whoever supplied the value, so
    this exact case — OpenFIGI found no local line, Yahoo did — would have stored Yahoo's answer as
    OpenFIGI's hit, and a Yahoo miss would have overwritten OpenFIGI's own miss under one key. The
    fixture makes the providers DISAGREE, which is the only shape where the label decides anything:
    while both hit or both miss, one row labelled either way reads the same.
    """
    sec = "11111111-1111-1111-1111-111111111111"
    _, symbols, probes = symbology.plan_symbols(
        sec,
        isin="US0378331005",
        country_iso2="US",
        mapping_entry=symbology.MappingEntry(asked="US0378331005"),
        yahoo_hits=symbology.parse_yahoo_search((FIX / "yahoo_search_aapl.json").read_bytes()),
        venues=VENUES,
        source="openfigi",
        asked_local=True,
        asked_yahoo=True,
        dead_symbol=None,
    )
    assert [(p["scheme"], p["provider"], p["outcome"], p["value"]) for p in probes] == [
        ("ticker", "openfigi", "miss", None),
        ("symbol", "openfigi", "miss", None),
        ("symbol", "yahoo", "hit", "AAPL"),
    ]
    assert symbols == [{"security_id": sec, "provider_code": "yfinance", "symbol": "AAPL"}], (
        "Yahoo's home-market line is adopted when OpenFIGI names none"
    )


def test_when_both_rungs_name_a_line_openfigis_local_line_is_adopted() -> None:
    """THE PREFERENCE IS A RULE, AND IT HAD NO TEST until the rungs were split. Yahoo's index is
    the fallback because it is the less reliable of the two — it resolved Walmex to a Frankfurt
    line only — so where OpenFIGI names the local line, that is the one adopted. The fixture makes
    the two rungs name DIFFERENT home-market lines; while they agree, either order passes."""
    sec = "11111111-1111-1111-1111-111111111111"
    entries = _mapping()
    _, symbols, probes = symbology.plan_symbols(
        sec,
        isin="US0378331005",
        country_iso2="US",
        mapping_entry=entries[0],
        yahoo_hits=[{"symbol": "APC", "quote_type": "EQUITY"}],
        venues=VENUES,
        source="openfigi",
        asked_local=True,
        asked_yahoo=True,
        dead_symbol=None,
    )
    assert symbols == [{"security_id": sec, "provider_code": "yfinance", "symbol": "AAPL"}]
    assert [(p["provider"], p["value"]) for p in probes if p["scheme"] == "symbol"] == [
        ("openfigi", "AAPL"),
        ("yahoo", "APC"),
    ], "each rung's own answer is still recorded as that rung's observation"


def test_yahoo_hits_for_a_subject_yahoo_was_not_asked_about_are_refused() -> None:
    """Hits can only come from the Yahoo rung's own raw file. Passing them with `asked_yahoo=False`
    is a wiring mistake at the call site, and either reading of it would record a false fact."""
    with pytest.raises(ValueError, match="Yahoo was not asked"):
        symbology.plan_symbols(
            "11111111-1111-1111-1111-111111111111",
            isin="US0378331005",
            country_iso2="US",
            mapping_entry=None,
            yahoo_hits=symbology.parse_yahoo_search((FIX / "yahoo_search_aapl.json").read_bytes()),
            venues=VENUES,
            source="openfigi",
            asked_local=True,
            asked_yahoo=False,
            dead_symbol=None,
        )


def test_a_local_line_named_by_the_unfiltered_rung_is_a_hit_not_a_miss() -> None:
    """THE LIVE DEFECT, 2026-09-24: every symbol the local rung resolved was adopted AND recorded
    as a miss. `identifier_probe` held 0 `symbol/hit` rows beside 759 `symbol/miss` rows for
    securities that did hold a yfinance symbol — National Healthcare Properties among them, `NHP`
    adopted from the local rung while its probe read `miss`.

    THE FIXTURE MAKES THE TWO SOURCES DISAGREE, which is the only shape that shows it. The ticker
    rung — restricted to `exchCode: US` — asked and got nothing, while the unfiltered local rung
    names the line. A ladder that picks the local line from the ticker rung's hits alone sees no
    value and, having asked, records a miss; one that reads the local rung's own matches records
    the hit it is. Whenever both rungs name the same line (every other test here) the two rules
    agree and nothing can tell them apart.
    """
    sec = "11111111-1111-1111-1111-111111111111"
    entries = _mapping()
    _, symbols, probes = symbology.plan_symbols(
        sec,
        isin="US0378331005",
        country_iso2="US",
        # The ticker rung asked and OpenFIGI answered with nothing for it.
        mapping_entry=symbology.MappingEntry(asked="US0378331005"),
        yahoo_hits=[],
        venues=VENUES,
        source="openfigi",
        asked_local=True,
        asked_yahoo=False,
        dead_symbol=None,
        # The local rung names AAPL on the US board — the captured answer.
        local_hits=entries[0].hits,
    )
    assert [(p["scheme"], p["outcome"], p["value"]) for p in probes] == [
        ("ticker", "miss", None),
        ("symbol", "hit", "AAPL"),
    ], probes
    assert symbols == [{"security_id": sec, "provider_code": "yfinance", "symbol": "AAPL"}]


def test_a_stale_miss_is_re_asked_only_while_its_evidence_is_still_missing() -> None:
    """A RUNG SKIPS A SUBJECT WHOSE EVIDENCE IS HELD, SO RE-ASKING ONE WOULD NEVER END.

    Re-requested, the rung skips it, the ladder records nothing because nothing asked, and the miss
    is exactly as stale on the next tick — a daily loop over every subject whose symbol arrived
    from somewhere other than the probe that missed. The anti-join is per scheme, because a
    subject can lack its ticker and hold its symbol.

    STRUCTURAL, because no fixture here has a database to be wrong against; the query was run on
    production before shipping (see the PR). The parameters are asserted too: the kinds are named
    once, in this module, and a literal copy in the SQL would be free to drift from them.
    """
    sql = " ".join(symbology.STALE_MISSES.split())
    assert (
        "p.scheme = %s and not exists (select 1 from market.security_identifier t "
        "where t.security_id = p.security_id and t.kind_code = %s)"
    ) in sql, sql
    assert (
        "p.scheme = 'symbol' and not exists (select 1 from market.security_provider_symbol s "
        "where s.security_id = p.security_id and s.provider_code = %s)"
    ) in sql, sql

    class Recording:
        params: ClassVar[list[tuple[object, ...]]] = []

        def cursor(self) -> Recording:
            return self

        def __enter__(self) -> Recording:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def execute(self, _sql: str, params: tuple[object, ...]) -> None:
            Recording.params.append(params)

        def fetchall(self) -> list[tuple[str]]:
            return []

    # AND THE SHARE CLASS IS RE-ASKED ON THE SAME TERMS, anti-joined on its own kind: a subject
    # can hold its ticker and still lack its class, and one that gained a class from any source
    # must leave the re-ask or it loops.
    assert (
        "p.scheme = %s and not exists (select 1 from market.security_identifier c "
        "where c.security_id = p.security_id and c.kind_code = %s)"
    ) in sql, sql

    symbology.stale_misses(Recording(), older_than_days=30)
    assert Recording.params == [
        (
            30,
            symbology.TICKER_KIND,
            symbology.TICKER_KIND,
            symbology.SYMBOL_PROVIDER,
            symbology.SHARE_CLASS_KIND,
            symbology.SHARE_CLASS_KIND,
        )
    ]


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
    for sql in (
        symbology.SUBJECTS_NEEDING_TICKER,
        symbology.SUBJECTS_NEEDING_SYMBOL,
        symbology.SUBJECTS_NEEDING_SHARE_CLASS,
    ):
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
    for sql in (
        symbology.SUBJECTS_NEEDING_TICKER,
        symbology.SUBJECTS_NEEDING_SYMBOL,
        symbology.SUBJECTS_NEEDING_SHARE_CLASS,
    ):
        flat = " ".join(sql.split())
        assert "s.security_type_code = 'equity'" in flat, flat
        assert "is_tradeable" not in flat, (
            "is_tradeable is set by promotion, not by symbol resolution — it is false by default, "
            "so it selects almost the whole universe and says nothing about needing a symbol"
        )


def test_the_share_class_population_asks_about_its_own_kind() -> None:
    """ONE QUERY, TWO KINDS — so the parameter is the whole rule. Passing the ticker's kind would
    ask "missing a ticker" and call it the share class, and every subject holding a ticker would
    never be keyed."""

    class Recording:
        params: ClassVar[list[tuple[object, ...]]] = []

        def cursor(self) -> Recording:
            return self

        def __enter__(self) -> Recording:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def execute(self, _sql: str, params: tuple[object, ...]) -> None:
            Recording.params.append(params)

        def fetchall(self) -> list[tuple[str]]:
            return []

    symbology.subjects_needing(Recording(), symbology.NEEDS_SHARE_CLASS)
    assert Recording.params == [(symbology.SHARE_CLASS_KIND,)]


# --- the share class ----------------------------------------------------------------------------

#: A REAL every-venue answer, captured from production's `raw_figi_local_symbol` 2026-09-26: UMS
#: Holdings (SG1J94892465), 26 lines across 25 venues, one share class on 25 of them and NONE on
#: the 26th (`UMSHSGD X1`). The null line is the ordinary case the planner must ignore — 2,340 such
#: lines sat in the stored answers, nearly all of them common stock.
UMS = FIX / "openfigi_mapping_local_ums.json"
UMS_CLASS = "BBG001SGJVC9"


def _ums_hits() -> tuple[dict[str, object], ...]:
    return symbology.parse_mapping(UMS.read_bytes(), asked=["SG1J94892465"])[0].hits


def test_the_mapping_hits_carry_the_share_class_of_every_line() -> None:
    hits = _ums_hits()
    assert len(hits) == 26
    assert sum(1 for h in hits if h["share_class_figi"] == UMS_CLASS) == 25
    assert [h["ticker"] for h in hits if h["share_class_figi"] is None] == ["UMSHSGD"]
    # The US-restricted capture carries it too, one line each.
    entries = _mapping()
    assert entries[0].hits[0]["share_class_figi"] == "BBG001S5N8V8"
    assert entries[1].hits[0]["share_class_figi"] == "BBG001SCXK90"


def test_one_isin_answer_names_one_class_and_the_null_lines_say_nothing() -> None:
    sec = "11111111-1111-1111-1111-111111111111"
    plan = symbology.plan_share_class(
        sec, isin="SG1J94892465", hits=_ums_hits(), source="openfigi", asked=True
    )
    assert plan.identifiers == [
        {
            "kind_code": "share_class_figi",
            "value": UMS_CLASS,
            "security_id": sec,
            "source_code": "openfigi",
        }
    ]
    assert [(p["scheme"], p["provider"], p["outcome"], p["value"]) for p in plan.probes] == [
        ("share_class_figi", "openfigi", "hit", UMS_CLASS)
    ]
    assert plan.conflicting == ()


def test_a_class_nobody_asked_for_is_adopted_but_not_observed() -> None:
    """A FINDING, NOT AN OBSERVATION — the rule the local line already keeps. Answers stored
    before the local rung asked for classes name one on 1,393 of 1,518 subjects; adopting those
    costs no request, and recording them as asked would put a question in the ledger that nobody
    put to the provider."""
    sec = "11111111-1111-1111-1111-111111111111"
    plan = symbology.plan_share_class(
        sec, isin="SG1J94892465", hits=_ums_hits(), source="openfigi", asked=False
    )
    assert [r["value"] for r in plan.identifiers] == [UMS_CLASS]
    assert plan.probes == []


def test_an_answer_with_no_class_is_a_miss_only_when_the_class_was_asked() -> None:
    sec = "11111111-1111-1111-1111-111111111111"
    asked = symbology.plan_share_class(
        sec, isin="ZZ0000000000", hits=(), source="openfigi", asked=True
    )
    assert asked.identifiers == []
    assert [(p["scheme"], p["outcome"], p["value"]) for p in asked.probes] == [
        ("share_class_figi", "miss", None)
    ]
    skipped = symbology.plan_share_class(
        sec, isin="ZZ0000000000", hits=(), source="openfigi", asked=False
    )
    assert skipped.identifiers == [] and skipped.probes == []


def test_two_classes_in_one_answer_are_refused_never_chosen() -> None:
    """NEVER MEASURED — 0 of 1,518 stored answers — which is exactly why the rule is written
    down: taking the first would attach a company's identity by row order, and a miss would tell
    the re-ask the provider had nothing. The fixture is the UMS answer with ONE line re-classed,
    so a planner reading only the first line, or the most common class, adopts something."""
    hits = [dict(h) for h in _ums_hits()]
    hits[3]["share_class_figi"] = "BBG00OTHER00"
    plan = symbology.plan_share_class(
        "s", isin="SG1J94892465", hits=hits, source="openfigi", asked=True
    )
    assert plan.identifiers == [] and plan.probes == []
    assert plan.conflicting == (UMS_CLASS, "BBG00OTHER00")  # sorted, so stable


def test_one_class_is_never_given_to_two_securities() -> None:
    """THE KEY REFUSES A SECOND HOLDER SILENTLY; THIS DECIDES IT FIRST SO THE REFUSAL IS COUNTED.

    Every candidate rule disagrees on this fixture: A names a class B holds (withheld), C and D
    name one class in the same batch (both refused, never broken by row order), E names a free one
    (kept), and F restates the class F already holds (kept — a rule withholding every held class
    would drop it, and one comparing only the value could not tell F from A).
    """

    def row(sid: str, value: str) -> dict[str, str]:
        return {
            "kind_code": "share_class_figi",
            "value": value,
            "security_id": sid,
            "source_code": "openfigi",
        }

    rows = [row("A", "X"), row("C", "Y"), row("D", "Y"), row("E", "Z"), row("F", "W")]
    adoption = symbology.adoptable_identifiers(rows, {"X": "B", "W": "F"})
    assert [(r["security_id"], r["value"]) for r in adoption.kept] == [("E", "Z"), ("F", "W")]
    assert adoption.held_elsewhere == [("A", "X", "B")]
    assert adoption.ambiguous == {"Y": ["C", "D"]}


def test_the_class_holders_are_asked_about_exactly_the_classes_in_the_batch() -> None:
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

        def fetchall(self) -> list[tuple[str, str]]:
            return [("X", "B")]

    rows = [
        {"kind_code": "share_class_figi", "value": "X", "security_id": "A"},
        {"kind_code": "ticker", "value": "AAPL", "security_id": "A"},
        {"kind_code": "share_class_figi", "value": "Z", "security_id": "E"},
    ]
    assert symbology.identifier_holders(Conn(), "share_class_figi", rows) == {"X": "B"}
    # Only the kind asked about, never the ticker riding in the same batch.
    assert Conn.calls[0][1] == ("share_class_figi", ["X", "Z"])
    assert symbology.identifier_holders(Conn(), "share_class_figi", []) == {}
    assert len(Conn.calls) == 1, "no rows must not reach the database"


def test_a_live_symbol_is_never_replaced_and_a_dead_one_may_be() -> None:
    """ADOPTION FILLS A GAP; IT DOES NOT OVERRULE A SYMBOL THAT WORKS. The held symbol may have been
    verified against the provider (`BRK-B`), and the ladder's pick is OpenFIGI's spelling plus a
    suffix (`BRK/B`) — the shape the edge's repair existed to undo. A DEAD held symbol is the one
    exception: the price lane rejected it alone, with a healthy control, so replacing it breaks
    nothing that works (Stage 3b).

    FIVE CASES, EACH A DIFFERENT RULE'S FAILURE: a gap is filled; a restatement is kept (a rule
    refusing every held security would drop it); a dead held symbol is repaired; a live one is
    refused with both names; and a death recorded against an OLDER spelling says nothing about
    the held one, so that pick is refused too — a rule asking only "is this security dead?" would
    replace a symbol nobody has rejected.
    """

    def row(sid: str, symbol: str) -> dict[str, str]:
        return {"security_id": sid, "provider_code": "yfinance", "symbol": symbol}

    split = symbology.repairable_symbols(
        [
            row("gap", "UMSH.SI"),
            row("same", "BRK-B"),
            row("dead", "0006.HK"),
            row("live", "BRK/B"),
            row("old-death", "6.HK"),
        ],
        {"same": "BRK-B", "dead": "6.HK", "live": "BRK-B", "old-death": "0006.HK"},
        {"dead": "6.HK", "old-death": "0006.HK.OLD"},
    )
    assert [r["security_id"] for r in split.kept] == ["gap", "same"]
    assert [(r["security_id"], r["symbol"]) for r in split.repairs] == [("dead", "0006.HK")]
    assert split.refused == [
        ("live", "BRK-B", "BRK/B"),
        ("old-death", "0006.HK", "6.HK"),
    ]


#: Hong Kong's real repair, recorded 2026-08-12: `6.HK` returns nothing because Yahoo pads Hong Kong
#: tickers to four digits, and `0006.HK` returns the series. OpenFIGI names `6` on `HKS`, so its
#: pick is the unpadded spelling for ever; only Yahoo's answer can repair it.
HK_LOCAL = [{"ticker": "6", "exch_code": "HKS", "composite_figi": "BBG000BBXRX1"}]
HK_YAHOO = [
    {"symbol": "0006.HK", "exchange": "HKG", "quote_type": "EQUITY", "name": "Power Assets"}
]


def _hk(
    *, dead: str | None, yahoo: bool = True
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    _, symbols, probes = symbology.plan_symbols(
        "hk",
        isin="HK0006000050",
        country_iso2="HK",
        mapping_entry=None,
        yahoo_hits=HK_YAHOO if yahoo else [],
        venues=VENUES,
        source="openfigi",
        asked_local=True,
        asked_yahoo=yahoo,
        dead_symbol=dead,
        local_hits=HK_LOCAL,
    )
    return symbols, probes


def test_a_symbol_the_provider_rejected_is_never_proposed_again() -> None:
    """OpenFIGI's pick comes first, so re-proposing a dead symbol is not merely useless: it also
    stops Yahoo's answer from ever being adopted. The control makes the two rules disagree — with
    nothing known dead, the local line wins as it always has."""
    symbols, _ = _hk(dead=None)
    assert [s["symbol"] for s in symbols] == ["6.HK"], "the control: the local line comes first"

    symbols, probes = _hk(dead="6.HK")
    assert [s["symbol"] for s in symbols] == ["0006.HK"]
    # THE OBSERVATIONS ARE UNTOUCHED: OpenFIGI did name `6.HK`, and that stays recorded.
    assert [(p["provider"], p["outcome"], p["value"]) for p in probes] == [
        ("openfigi", "hit", "6.HK"),
        ("yahoo", "hit", "0006.HK"),
    ]

    # The same spelling in another case is the same symbol.
    symbols, _ = _hk(dead="6.hk")
    assert [s["symbol"] for s in symbols] == ["0006.HK"]

    # No other candidate: nothing is proposed, and the dead symbol is not re-adopted.
    symbols, _ = _hk(dead="6.HK", yahoo=False)
    assert symbols == []


class _Recording:
    """A cursor that records what it was asked and answers with `rows`."""

    def __init__(self, rows: list[tuple[str, ...]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def cursor(self) -> _Recording:
        return self

    def __enter__(self) -> _Recording:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((" ".join(sql.split()), params))

    def fetchall(self) -> list[tuple[str, ...]]:
        return self.rows


def test_the_repair_checks_the_death_in_its_own_statement() -> None:
    """THE GUARD IS THE STATEMENT'S, NOT THE CALLER'S: a symbol that answered between the read and
    the write must not be replaced, and only SQL evaluated against the row being updated can know.
    The behaviour, and the mutations that break it, are proven against Postgres; this pins the
    clauses so a tidy-up cannot drop one silently."""
    flat = " ".join(symbology.REPAIR_DEAD_SYMBOLS.split())
    for clause in (
        "p.security_id = r.security_id",
        "p.provider_code = %s",
        "d.security_id = p.security_id",
        "d.provider = p.provider_code",
        "d.outcome = 'miss'",
        "d.asked_with = p.symbol",
        "returning p.security_id::text",
    ):
        assert clause in flat, clause

    assert symbology.repair_dead_symbols(_Recording(), []) == []
    cur = _Recording([("dead",)])
    repaired = symbology.repair_dead_symbols(
        cur,
        [
            {"security_id": "dead", "provider_code": "yfinance", "symbol": "0006.HK"},
            {"security_id": "alive-again", "provider_code": "yfinance", "symbol": "X.HK"},
        ],
    )
    # WHAT THE STATEMENT RETURNED, NOT WHAT WAS OFFERED: the second answered in between.
    assert repaired == ["dead"]
    assert cur.calls[0][1] == (["dead", "alive-again"], ["0006.HK", "X.HK"], "yfinance")


def test_a_dead_held_symbol_needs_a_symbol_and_is_re_asked_once_per_death() -> None:
    """The population gains the dead held symbol, tied to the security under test and to the
    spelling it holds; the re-ask takes it only until OpenFIGI answers after the death, or a symbol
    OpenFIGI names again would be re-asked every day for ever."""
    flat = " ".join(symbology.SUBJECTS_NEEDING_SYMBOL.split())
    assert "d.asked_with = p.symbol" in flat and "p.security_id = s.security_id" in flat, flat
    cur = _Recording()
    symbology.subjects_needing(cur, symbology.NEEDS_SYMBOL)
    assert cur.calls[0][1] == (symbology.SYMBOL_PROVIDER, symbology.SYMBOL_PROVIDER)

    flat = " ".join(symbology.DEAD_UNASKED.split())
    for clause in (
        "d.asked_with = p.symbol",
        "d.outcome = 'miss'",
        "q.observed_at > d.observed_at",
        "s.security_type_code = 'equity'",
        "i.kind_code = 'isin'",
    ):
        assert clause in flat, clause
    cur = _Recording([("a",), ("b",)])
    assert symbology.dead_unasked(cur) == {"a", "b"}
    assert cur.calls[0][1] == (symbology.SYMBOL_PROVIDER, "openfigi")


def test_dead_symbols_asks_only_about_the_securities_named() -> None:
    assert symbology.dead_symbols(_Recording(), []) == {}
    cur = _Recording([("b", "6.HK")])
    assert symbology.dead_symbols(cur, ["b", "a", "b"]) == {"b": "6.HK"}
    assert cur.calls[0][1] == (symbology.SYMBOL_PROVIDER, ["a", "b"])
