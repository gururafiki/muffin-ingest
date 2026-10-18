"""The company lane's quoteSummary rules, driven over Yahoo's own bytes.

EVERY `yahoo_summary_*.json.gz` HERE IS A RESPONSE BODY, gzipped for size and byte for byte once
decompressed (`capture.md`). Thirteen were captured from the node on 2026-10-10 with
`formatted=false`, the shape the lane asks for; five (NPN.JO, AZRG.TA, NBK.KW, SBK.JO, BARC.L) on
2026-10-11 without it, so every number arrives wrapped as `{"raw": ..., "fmt": ...}` and the rules
are held to read both. The listings are chosen so the plausible wrong rules give different answers:

  BHP.AX   quotes AUD, reports USD, and Yahoo's P/B is RIGHT (book is converted to AUD)
  0992.HK  quotes HKD, reports USD, likewise right; a fiscal Q4 missing from its quarters
  VOD.L    quotes GBp, reports EUR: P/B out by 100, `defaultKeyStatistics.forwardPE` 1,001
  BARC.L   quotes GBp, reports GBP: the same major unit, spelled differently
  NPN.JO   quotes ZAc (cents), reports USD
  AZRG.TA  quotes ILA (agorot); NBK.KW quotes KWF (fils, a thousandth)
  005930.KS  no `summaryDetail.forwardPE`, and the same currency both ways
  BDMS-F.BK  the 404 that names an absence
"""

from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from muffin_ingest import metrics
from muffin_ingest.facets import company
from muffin_ingest.facets.prices import Subject
from muffin_ingest.providers import documents, vocab, yahoo_chart, yahoo_session, yahoo_summary
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parents[1] / "fixtures"
KNOWN = frozenset(
    {"USD", "EUR", "GBP", "GBX", "AUD", "HKD", "CAD", "JPY", "KRW", "CHF"}
    | {"ZAR", "ZAC", "ILS", "ILA", "KWD", "KWF"}
)
FETCHED = "2026-10-10T12:00:00+00:00"


def summary_body(symbol: str) -> bytes:
    return gzip.decompress((FIXTURES / f"yahoo_summary_{symbol}.json.gz").read_bytes())


def edited(symbol: str, edit: Any) -> bytes:
    """A captured body with one change, for a case no listing happened to show."""
    parsed = json.loads(summary_body(symbol))
    edit(parsed["quoteSummary"]["result"][0])
    return json.dumps(parsed).encode()


def stored(symbol: str, *, body: bytes | None = None, fetched: str = FETCHED) -> dict[str, Any]:
    return {
        "security_id": f"sec-{symbol}",
        "asked_symbol": symbol,
        "provider": "yahoo",
        "url": f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
        "body": summary_body(symbol) if body is None else body,
        "fetched_at": fetched,
    }


def facts(symbol: str, *, body: bytes | None = None, known: frozenset[str] = KNOWN) -> Any:
    out = company.normalise([stored(symbol, body=body)], known_currencies=known)
    assert len(out.facts) == 1, out.stats
    return out.facts[0]


# --- units ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("symbol", "yahoo", "expected"),
    [
        ("VOD.L", 62.873737, 0.62873737),
        ("BARC.L", 88.1876, 0.881876),
        ("NPN.JO", 144.36395, 1.4436395),
        ("AZRG.TA", 167.2328, 1.672328),
        ("NBK.KW", 1663.9677, 1.6639677),
        ("BHP.AX", 4.231877, 4.231877),
        ("0992.HK", 7.7930484, 7.7930484),
        ("CSU.TO", None, None),
        ("AAPL", 45.739132, 45.739132),
    ],
)
def test_price_to_book_is_divided_only_by_a_subunit_factor(
    symbol: str, yahoo: float | None, expected: float | None
) -> None:
    """YAHOO'S BOOK VALUE IS IN THE QUOTE'S MAJOR UNIT, so only a subunit price is out of step.

    BHP.AX and 0992.HK are where a plausible rule goes wrong: they quote in one currency and
    report in another, and "convert whenever the currencies differ" would change a figure that is
    already right (BHP's book of 14.40 is AUD, against 9.73 USD of equity per share).
    """
    got = facts(symbol).fundamentals
    if symbol == "CSU.TO":
        # Captured with P/B; read here only to keep the listing in the table above honest.
        assert got["price_to_book"] == pytest.approx(11.0, rel=0.01)
        return
    assert got["price_to_book"] == pytest.approx(expected)
    raw = json.loads(got["raw"])["defaultKeyStatistics"]["priceToBook"]
    assert raw == pytest.approx(yahoo), "the raw jsonb keeps Yahoo's own figure"


def test_forward_pe_falls_back_only_where_the_currencies_are_spelled_the_same() -> None:
    """`defaultKeyStatistics.forwardPE` divides the price AS QUOTED by EPS in the reporting
    currency: 1,001 for VOD.L, 665.7 for BARC.L. So it may stand in only when the two spellings
    are identical, and BARC.L is the case that decides it: GBp and GBP share a major unit, and a
    rule comparing major units would take the 100-times figure."""
    assert facts("VOD.L").fundamentals["forward_pe"] == pytest.approx(11.812015)
    assert facts("005930.KS").fundamentals["forward_pe"] == pytest.approx(3.6383338)

    def drop_summary_forward(result: dict[str, Any]) -> None:
        result["summaryDetail"].pop("forwardPE", None)

    for symbol in ("VOD.L", "BARC.L", "BHP.AX"):
        got = facts(symbol, body=edited(symbol, drop_summary_forward)).fundamentals
        assert got["forward_pe"] is None, f"{symbol} took the unconverted figure"


def test_trailing_pe_is_taken_as_sent() -> None:
    """Right everywhere measured: SHEL.L's 11.22 is the price in pounds over 3.38, its trailing
    EPS of 4.53 USD converted to pounds by Yahoo."""
    assert facts("SHEL.L").fundamentals["pe_ratio"] == pytest.approx(11.223372)
    assert facts("NPN.JO").fundamentals["pe_ratio"] == pytest.approx(6.7866282)
    assert facts("VOD.L").fundamentals["pe_ratio"] is None, "a loss leaves no P/E"


@pytest.mark.parametrize(
    ("symbol", "cap", "currency"),
    [
        ("VOD.L", 27418447872, "GBP"),
        ("NPN.JO", 539912273920, "ZAR"),
        ("AZRG.TA", 43157536768, "ILS"),
        ("NBK.KW", 7545880064, "KWD"),
        ("BHP.AX", 309755183104, "AUD"),
        ("0992.HK", 407369023488, "HKD"),
        ("AAPL", 4912984883200, "USD"),
    ],
)
def test_a_market_cap_is_in_the_major_unit_of_the_quote(
    symbol: str, cap: int, currency: str
) -> None:
    """cap / (price x shares) is exactly 0.01 for GBp, ZAc and ILA, 0.001 for KWF, 1 otherwise —
    and never the reporting currency (BHP.AX reports in USD and is capitalised in AUD)."""
    got = facts(symbol).fundamentals
    assert got["market_cap"] == pytest.approx(cap)
    assert got["market_cap_currency"] == currency


def test_a_cap_whose_currency_is_unknown_is_withheld_with_it() -> None:
    """The table holds the pair together with a check; a cap is never labelled by a guess."""
    out = company.normalise([stored("VOD.L")], known_currencies=KNOWN - {"GBP"})
    got = out.facts[0].fundamentals
    assert got is not None
    assert got["market_cap"] is None and got["market_cap_currency"] is None
    assert out.stats["cap_withheld"] == 1


def test_dividend_yield_is_stored_as_a_percent_and_the_rest_as_sent() -> None:
    sap = facts("SAP.DE").fundamentals
    assert sap["dividend_yield"] == pytest.approx(1.33, abs=0.01)
    assert sap["debt_to_equity"] == pytest.approx(21.966, rel=0.01)
    barc = facts("BARC.L").fundamentals
    assert barc["dividend_yield"] == pytest.approx(2.7)
    assert 0 < barc["return_on_equity"] < 1, "a fraction on both sides"


# --- the other tables ----------------------------------------------------------------------------


def test_share_statistics_take_the_provider_s_date_when_it_states_one() -> None:
    aapl = facts("AAPL").share_stats
    assert aapl["as_of"] == datetime.fromtimestamp(1789430400, UTC).date().isoformat()
    assert aapl["outstanding_shares"] == 14594180000
    assert aapl["institutions_count"] == 7687
    vod = facts("VOD.L").share_stats
    assert vod["as_of"] == "2026-10-10", "no short-interest date outside the US: the fetch day"
    assert vod["short_interest"] is None


def test_no_share_count_means_no_share_statistics() -> None:
    """openbb's rule, kept: a row needed `sharesOutstanding`."""

    def drop(result: dict[str, Any]) -> None:
        result["defaultKeyStatistics"].pop("sharesOutstanding", None)

    assert facts("AAPL", body=edited("AAPL", drop)).share_stats is None


def test_an_estimate_is_labelled_with_the_quote_currency_as_quoted() -> None:
    """VOD.L's targets are pence like its price (mean 121.88 against 118.35), so the label is
    `GBX`. The edge upper-cased `GBp` and filed London targets as pounds."""
    assert facts("VOD.L").estimate["currency_code"] == "GBX"
    assert facts("NPN.JO").estimate["currency_code"] == "ZAC"
    assert facts("AAPL").estimate["number_of_analysts"] == 39
    out = company.normalise([stored("VOD.L")], known_currencies=KNOWN - {"GBX"})
    estimate = out.facts[0].estimate
    assert estimate is not None and estimate["currency_code"] is None
    assert out.stats["estimate_currency_withheld"] == 1


def test_officers_lose_placeholder_figures_and_doubled_spaces() -> None:
    aapl = {o["name"]: o for o in facts("AAPL").officers}
    assert "Mr. Kevan Parekh" in aapl, sorted(aapl)
    assert aapl["Mr. Timothy D. Cook"]["pay"] == 16759518
    nbk = facts("NBK.KW").officers
    assert nbk and all(o["pay"] is None for o in nbk), "NBK states no pay for anyone"
    assert {o["name"]: o["year_born"] for o in nbk}["Mr. Omar Ben Amor Bouhadiba"] == 1953

    def zero(result: dict[str, Any]) -> None:
        officer = result["assetProfile"]["companyOfficers"][0]
        officer["totalPay"] = {"raw": 0, "fmt": None}
        officer["yearBorn"] = 0

    first = facts("AAPL", body=edited("AAPL", zero)).officers[0]
    assert first["pay"] is None, "a pay of zero is not a salary"
    assert first["year_born"] is None


def test_the_profile_states_the_headquarters_the_edge_never_filled() -> None:
    """The edge read `hq_address_state` and `hq_address_country`, which openbb never sends."""
    got = facts("AAPL").profile
    assert (got["hq_city"], got["hq_state"], got["hq_country"]) == (
        "Cupertino",
        "CA",
        "United States",
    )
    assert got["employees"] == 150000
    assert facts("NBK.KW").profile["employees"] is None


def test_a_module_yahoo_omits_is_not_a_statement_that_there_is_nothing() -> None:
    """No `assetProfile` means no profile and NO officer list, which is not an empty one: only an
    empty one may retract the officers stored."""

    def drop(result: dict[str, Any]) -> None:
        result.pop("assetProfile")

    got = facts("AAPL", body=edited("AAPL", drop))
    assert got.profile is None and got.officers is None and got.sector_label is None
    assert got.fundamentals is not None


def test_classification_and_reporting_currency() -> None:
    got = facts("BHP.AX")
    assert got.reporting_currency == "USD"
    assert company.SECTOR_CODES[got.sector_label] == "materials"
    assert company.industry_code("financials", "Banks - Regional") == "financials--banks-regional"
    assert company.slug("Software—Infrastructure") == "software-infrastructure"


#: Yahoo's two wordings for a symbol it does not carry, each a body captured from the node. The
#: second fixture's name is the symbol as it is sent: a `/` is the path segment's `%2F`.
NAMED_ABSENCES = ("BDMS-F.BK", "3BBIF%2FF.BK")


@pytest.mark.parametrize("fixture", NAMED_ABSENCES)
def test_a_named_absence_is_counted_and_yields_nothing(fixture: str) -> None:
    """Never a negative cache: a `(symbol, yfinance)` miss would hide the security from the price
    sweep for thirty days, on the strength of one endpoint."""
    out = company.normalise([stored("X", body=summary_body(fixture))], known_currencies=KNOWN)
    assert out.facts == [] and out.stats["absent"] == 1


def test_the_newest_document_is_the_company_s_answer() -> None:
    older = stored("AAPL", fetched="2026-10-01T12:00:00+00:00")
    newer = stored("AAPL", body=summary_body("SAP.DE"), fetched="2026-10-08T12:00:00+00:00")
    out = company.normalise([newer, older], known_currencies=KNOWN)
    assert out.facts[0].reporting_currency == "EUR"
    assert out.facts[0].fetched_at == "2026-10-08T12:00:00+00:00"


def test_both_value_shapes_read_the_same() -> None:
    assert yahoo_summary.value({"x": {"raw": 1.5, "fmt": "1.50"}}, "x") == 1.5
    assert yahoo_summary.value({"x": 1.5}, "x") == 1.5
    assert yahoo_summary.value({"x": {}}, "x") is None, "an empty wrapper is Yahoo's no-value"
    bare = json.loads(summary_body("AAPL"))["quoteSummary"]["result"][0]["price"]
    wrapped = json.loads(summary_body("BARC.L"))["quoteSummary"]["result"][0]["price"]
    assert isinstance(bare["regularMarketPrice"], float)
    assert isinstance(wrapped["regularMarketPrice"], dict)
    assert facts("BARC.L").fundamentals["pe_ratio"] == pytest.approx(8.951041)


def test_a_summary_is_due_after_a_week_or_when_forced() -> None:
    today = date(2026, 10, 10)
    assert company.summary_due([], today)
    assert not company.summary_due([stored("AAPL", fetched="2026-10-04T00:00:00+00:00")], today)
    assert company.summary_due([stored("AAPL", fetched="2026-10-03T00:00:00+00:00")], today)
    assert company.summary_due([stored("AAPL")], today, force=True)


def test_raw_is_the_body_plus_who_asked() -> None:
    content = summary_body("AAPL")
    document = Document(
        url="u", body=content, content_type="application/json", fetched_at=datetime.now(UTC)
    )
    row = company.raw_rows(Subject("s", "AAPL", 1.0), document, run_id="r")[0]
    assert set(row) == set(document.as_row("r")) | company.CONTEXT_COLUMNS
    assert hashlib.sha256(row["body"]).hexdigest() == hashlib.sha256(content).hexdigest()


# --- the session and the request -----------------------------------------------------------------


@pytest.fixture
def outcomes(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """What each request was counted as, by a recorder that behaves as `metrics.request` does: an
    exception escaping the block is `transport`."""
    seen: list[tuple[str, str]] = []

    @contextmanager
    def recording(provider: str) -> Iterator[dict[str, str]]:
        state = {"outcome": "answered"}
        try:
            yield state
        except BaseException:
            state["outcome"] = "transport"
            raise
        finally:
            seen.append((provider, state["outcome"]))

    monkeypatch.setattr(metrics, "request", recording)
    return seen


@pytest.fixture
def through_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YAHOO_BASE_URL", raising=False)
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")
    monkeypatch.setenv("MUFFIN_USER_AGENT", "muffin-market/1.0 (ops@example.com)")


def test_a_session_sends_its_cookie_as_a_header_and_never_shows_either_value(
    monkeypatch: pytest.MonkeyPatch, through_cache: None, outcomes: list[tuple[str, str]]
) -> None:
    """A cookie JAR scopes `A3` to `.yahoo.com`, so it would send nothing to `http-cache:8080` and
    getcrumb would answer 401 through the cache while working direct (measured 2026-10-10)."""
    asked: list[tuple[str, dict[str, str]]] = []

    def get(url: str, **kwargs: Any) -> httpx.Response:
        headers = dict(kwargs.get("headers") or {})
        asked.append((url, headers))
        if url == yahoo_session.COOKIE_ORIGIN:
            return httpx.Response(
                404,
                headers={"set-cookie": "A3=cookie-value; Domain=.yahoo.com; Path=/"},
                request=httpx.Request("GET", url),
            )
        if url.endswith("/v1/test/getcrumb"):
            return httpx.Response(200, text="crumb-value")
        params = kwargs.get("params") or {}
        assert params["crumb"] == "crumb-value" and params["formatted"] == "false"
        return httpx.Response(200, content=summary_body("AAPL"))

    monkeypatch.setattr(httpx, "get", get)
    session = yahoo_session.open_session()
    assert asked[1][0] == "http://http-cache:8080/yahoo/v1/test/getcrumb"
    assert asked[1][1]["Cookie"] == "A3=cookie-value"
    assert "crumb-value" not in repr(session) and "cookie-value" not in repr(session)
    assert session.scrub("x crumb-value y A3=cookie-value z cookie-value") == (
        "x redacted y redacted z redacted"
    )

    document = yahoo_summary.fetch("BRK/B", session)
    assert asked[2][0].endswith("/quoteSummary/BRK%2FB"), "a slash stays inside the symbol"
    assert asked[2][1]["Cookie"] == "A3=cookie-value"
    assert "crumb-value" not in document.url and "crumb=redacted" in document.url
    assert outcomes == [("yahoo-session", "answered")] * 2 + [("yahoo-summary", "answered")]


@pytest.mark.parametrize(
    ("cookie", "crumb_status", "crumb_text"),
    [(None, 200, "crumb"), ("A3=x", 200, "<html>blocked</html>"), ("A3=x", 401, "Invalid Cookie")],
)
def test_a_session_that_cannot_open_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    outcomes: list[tuple[str, str]],
    cookie: str | None,
    crumb_status: int,
    crumb_text: str,
) -> None:
    def get(url: str, **kwargs: Any) -> httpx.Response:
        if url == yahoo_session.COOKIE_ORIGIN:
            headers = {"set-cookie": f"{cookie}; Domain=.yahoo.com"} if cookie else {}
            return httpx.Response(404, headers=headers, request=httpx.Request("GET", url))
        return httpx.Response(crumb_status, text=crumb_text)

    monkeypatch.setattr(httpx, "get", get)
    with pytest.raises(yahoo_session.SessionRefused):
        yahoo_session.open_session()
    assert outcomes[-1][1] == "refused", "a refusal is not a request that never answered"


def _session() -> yahoo_session.Session:
    return yahoo_session.Session(cookie="A3=c", crumb="k", opened_at=datetime.now(UTC))


@pytest.mark.parametrize(
    ("symbol", "fixture"), [("BDMS-F.BK", "BDMS-F.BK"), ("3BBIF/F.BK", "3BBIF%2FF.BK")]
)
def test_a_summary_answer_that_names_the_absence_is_returned_not_raised(
    monkeypatch: pytest.MonkeyPatch, outcomes: list[tuple[str, str]], symbol: str, fixture: str
) -> None:
    """Both wordings: `3BBIF/F.BK` read as a transport fault on the first scheduled day."""
    monkeypatch.setattr(
        httpx, "get", lambda url, **kw: httpx.Response(404, content=summary_body(fixture))
    )
    document = yahoo_summary.fetch(symbol, _session())
    assert yahoo_summary.parse(document.body).error is not None
    assert outcomes == [("yahoo-summary", "answered")]


@pytest.mark.parametrize(
    ("status", "content", "error", "throttle"),
    [
        (
            401,
            b'{"finance":{"error":{"code":"Unauthorized","description":"Invalid Crumb"}}}',
            yahoo_summary.CrumbRejected,
            False,
        ),
        (429, b"Too Many Requests", yahoo_chart.YahooRefused, True),
        (
            404,
            b'{"finance":{"error":{"code":"Not Found","description":"HTTP 404 Not Found"}}}',
            yahoo_chart.YahooRefused,
            False,
        ),
        (503, b"", yahoo_chart.YahooRefused, False),
    ],
)
def test_a_summary_refusal_is_counted_as_refused_and_never_names_the_symbol(
    monkeypatch: pytest.MonkeyPatch,
    outcomes: list[tuple[str, str]],
    status: int,
    content: bytes,
    error: type[Exception],
    throttle: bool,
) -> None:
    """`4290.T` contains `429`: a message naming it would read as a throttle and stop a run."""
    monkeypatch.setattr(httpx, "get", lambda url, **kw: httpx.Response(status, content=content))
    with pytest.raises(error) as raised:
        yahoo_summary.fetch("4290.T", _session())
    assert "4290" not in str(raised.value)
    assert vocab.throttled(str(raised.value)) is throttle
    assert outcomes == [("yahoo-summary", "refused")]


def test_the_chart_and_document_refusals_are_counted_as_refused(
    monkeypatch: pytest.MonkeyPatch, through_cache: None, outcomes: list[tuple[str, str]]
) -> None:
    """Both raised INSIDE the metrics block until now, which records an escaping exception as
    `transport`: every refusal they met was counted as a request that never answered."""
    monkeypatch.setattr(httpx, "get", lambda url, **kw: httpx.Response(503, content=b"down"))
    with pytest.raises(yahoo_chart.YahooRefused) as raised:
        yahoo_chart.fetch("4290.T", range_="5d", interval="1d")
    assert "4290" not in str(raised.value) and not vocab.throttled(str(raised.value))
    with pytest.raises(documents.DocumentRefused):
        documents.sec_company_tickers()
    assert outcomes == [("yahoo", "refused"), ("sec", "refused")]


# --- stage 2: the writes that need SQL -----------------------------------------------------------


#: The sector nodes the recording cursor answers with.
SECTORS = {
    "information-technology": "node-it",
    "communication-services": "node-comm",
    "financials": "node-fin",
}


class RecordingCursor:
    """Answers the classification's two reads and records every statement it is sent."""

    def __init__(self, existing: dict[str, str] | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.nodes = dict(existing or {})
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def execute(self, sql: str, params: Any = ()) -> None:
        text = " ".join(sql.split())
        self.calls.append((text, tuple(params)))
        self.rows, self.rowcount = [], 0
        if "and level = 1" in text:
            self.rows = list(SECTORS.items())
        elif text.startswith("insert into market.taxonomy_node"):
            _, codes, _names, _parents = params
            new = [c for c in codes if c not in self.nodes]
            self.nodes.update({c: f"node-{c}" for c in new})
            self.rowcount = len(new)
        elif "and code = any" in text:
            self.rows = [(c, self.nodes[c]) for c in params[1] if c in self.nodes]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


def test_a_headquarters_country_resolves_through_the_aliases_and_the_rest_are_named() -> None:
    known = {"united states": "US", "united kingdom": "GB", "korea, republic of": "KR"}
    aliases = {**known, "south korea": "KR"}
    us, uk = facts("AAPL"), facts("VOD.L")
    korean = facts("005930.KS")
    korean.country_name = "South Korea"
    nowhere = facts("NBK.KW")
    nowhere.country_name = "Atlantis"
    rows, unresolved = company.country_rows([us, uk, korean, nowhere], aliases)
    assert sorted((r["security_id"], r["provider_country_iso2"]) for r in rows) == [
        ("sec-005930.KS", "KR"),
        ("sec-AAPL", "US"),
        ("sec-VOD.L", "GB"),
    ]
    assert unresolved == {"Atlantis": 1}, "a name we cannot place is named, never dropped"


def test_the_aliases_win_over_the_formal_names() -> None:
    """`country_codes` reads the formal names first and the aliases after, so a provider spelling
    stored as an alias overrides a formal name that happens to collide with it."""

    class Names:
        def __init__(self) -> None:
            self.rows: list[tuple[Any, ...]] = []

        def execute(self, sql: str, params: Any = ()) -> None:
            assert "order by rank" in " ".join(sql.split())
            self.rows = [("russia", "XX", 0), ("russia", "RU", 1)]

        def fetchall(self) -> list[tuple[Any, ...]]:
            return self.rows

    assert company.country_codes(Names()) == {"russia": "RU"}


def test_officers_are_retracted_only_where_the_profile_lists_them() -> None:
    """A profile listing officers states the whole board; one listing none states there are none;
    a document with no profile module states nothing, so its company keeps what it has."""
    listed = facts("AAPL")
    empty = facts("SPY")
    silent = facts("AAPL", body=edited("AAPL", lambda r: r.pop("assetProfile")))
    silent.security_id = "sec-silent"
    assert empty.officers == [] and silent.officers is None

    cur = RecordingCursor()
    assert company.retract_officers(cur, [listed, empty, silent]) == 0
    ((sql, params),) = cur.calls
    assert sql.startswith("delete from market.security_officer")
    ids, source, keep_ids, keep_names = params
    assert ids == ["sec-AAPL", "sec-SPY"], "only companies whose document lists its officers"
    assert source == company.OFFICER_SOURCE
    assert set(keep_ids) == {"sec-AAPL"} and len(keep_names) == len(listed.officers or [])
    assert company.retract_officers(RecordingCursor(), [silent]) == 0


def test_an_industry_hangs_under_yahoo_s_own_sector_and_is_created_once() -> None:
    apple, vodafone = facts("AAPL"), facts("VOD.L")
    cur = RecordingCursor(existing={"communication-services--telecom-services": "node-telecom"})
    out = company.classify(cur, [apple, vodafone])

    inserted = next(params for sql, params in cur.calls if sql.startswith("insert into"))
    taxonomy, codes, names, parents = inserted
    assert taxonomy == company.TAXONOMY
    assert dict(zip(codes, parents, strict=True)) == {
        "communication-services--telecom-services": "node-comm",
        "information-technology--consumer-electronics": "node-it",
    }
    assert dict(zip(codes, names, strict=True))["information-technology--consumer-electronics"] == (
        "Consumer Electronics"
    )
    assert out.nodes_created == 1, "the telecom node already existed"
    assert sorted((r["security_id"], r["node_id"]) for r in out.rows) == [
        ("sec-AAPL", "node-information-technology--consumer-electronics"),
        ("sec-AAPL", "node-it"),
        ("sec-VOD.L", "node-comm"),
        ("sec-VOD.L", "node-telecom"),
    ]
    assert {r["source_code"] for r in out.rows} == {company.SOURCE}
    assert sorted(out.replaced) == ["sec-AAPL", "sec-VOD.L"]


def test_a_sector_we_cannot_map_keeps_the_stored_rows_and_no_sector_replaces_them() -> None:
    """An unmapped label is a vocabulary change upstream, not Yahoo withdrawing a sector, so the
    company keeps what it has. A profile with NO sector (SPY's) is a statement, and replaces."""
    renamed = facts("AAPL", body=edited("AAPL", lambda r: r["assetProfile"].update(sector="Tech")))
    fund = facts("SPY")
    out = company.classify(RecordingCursor(), [renamed, fund])
    assert out.unmapped == {"Tech": 1}
    assert out.replaced == ["sec-SPY"] and out.rows == []

    cur = RecordingCursor()
    assert company.retract_classification(cur, out) == 0
    ((sql, params),) = cur.calls
    assert sql.startswith("delete from market.security_taxonomy")
    assert params == (["sec-SPY"], company.SOURCE, company.TAXONOMY, [], [])
    assert company.retract_classification(RecordingCursor(), company.Classification()) == 0
