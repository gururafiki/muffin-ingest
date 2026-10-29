"""SEC company facts to statement rows.

Two kinds of fixture. `fixtures/sec/companyfacts_trimmed.json` holds five filers' real company facts
(captured 2026-10-26, cut to a few concepts and years) for the cases that decided a rule, with each
filer's calendar built from `fiscal_inputs.json` exactly as `test_fiscal` builds it. The precise
rules use small synthetic bodies, where the right rule and the wrong one give different answers.
`fixtures/sec/xbrl_concept.json` is production's `market.xbrl_concept` and metric categories,
exported 2026-10-27.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from functools import cache
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest.facets import fiscal, sec_facts
from muffin_ingest.facets.fiscal import Calendar, Period
from muffin_ingest.facets.sec_facts import Concept
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parents[1] / "fixtures" / "sec"
TRIMMED = json.loads((FIXTURES / "companyfacts_trimmed.json").read_bytes())
INPUTS = json.loads((FIXTURES / "fiscal_inputs.json").read_bytes())
XBRL = json.loads((FIXTURES / "xbrl_concept.json").read_bytes())

CONCEPTS = [Concept(**c) for c in XBRL["concepts"]]
#: The mapping muffin-deployment#475 seeded into `metric_source_field` (`sec_facts.lines`).
STATEMENT_OF_CATEGORY = {
    "income_statement": "income",
    "balance_sheet": "balance",
    "cash_flow": "cash",
    "share": "income",
}
LINES = {m: STATEMENT_OF_CATEGORY[c] for m, c in XBRL["categories"].items()}
KNOWN = {"USD", "EUR", "DKK", "TWD"}


@cache
def calendar(filer: str) -> Calendar:
    x = INPUTS[filer]
    return fiscal.calendar_from_sec(
        [fiscal.Report(a, f, date.fromisoformat(r), role) for a, f, r, role in x["reports"]],
        {a: fiscal.FilingLabel(fy, fp) for a, (fy, fp) in x["labels"].items()},
        [
            fiscal.Span(a, f, date.fromisoformat(s), date.fromisoformat(e), n)
            for a, f, s, e, n in x["spans"]
        ],
    )


def rows_of(filer: str) -> sec_facts.Normalised:
    facts = sec_facts.read(json.dumps(TRIMMED[filer]).encode(), CONCEPTS).facts
    return sec_facts.statement_rows(
        facts, calendar(filer), CONCEPTS, LINES,
        security_ids=["s"], known_currencies=KNOWN, as_of="2026-10-26T12:00:00+00:00",
    )  # fmt: skip


def row(out: sec_facts.Normalised, statement: str, period_type: str, end: str) -> dict[str, Any]:
    [found] = [
        r
        for r in out.rows
        if (r["statement"], r["period_type"], r["period_ending"]) == (statement, period_type, end)
    ]
    data: dict[str, Any] = json.loads(found["data"])
    return {**found, "data": data}


def has(out: sec_facts.Normalised, statement: str, period_type: str, end: str) -> bool:
    return any(
        (r["statement"], r["period_type"], r["period_ending"]) == (statement, period_type, end)
        for r in out.rows
    )


# --- synthetic bodies ----------------------------------------------------------------------------


def fact(
    end: str, val: float | bool | str, *, start: str | None = None,
    accn: str = "0000000000-24-000001",
    filed: str = "2025-02-01", fy: int = 2024, fp: str = "FY", form: str = "10-K",
) -> dict[str, Any]:  # fmt: skip
    point: dict[str, Any] = {"end": end, "val": val, "accn": accn, "fy": fy, "fp": fp, "form": form}
    point["filed"] = filed
    if start is not None:
        point["start"] = start
    return point


def body(spec: dict[tuple[str, str], dict[str, list[dict[str, Any]]]]) -> bytes:
    facts: dict[str, dict[str, Any]] = {}
    for (taxonomy, concept), units in spec.items():
        facts.setdefault(taxonomy, {})[concept] = {"label": concept, "units": units}
    return json.dumps({"cik": 1, "entityName": "Synthetic", "facts": facts}).encode()


def period(label: str, start: str, end: str, **kw: Any) -> Period:
    return Period(
        regime=kw.get("regime", 1),
        fiscal_year=kw.get("fiscal_year", 2024),
        fiscal_period=label,  # type: ignore[arg-type]
        period_start=date.fromisoformat(start),
        period_end=date.fromisoformat(end),
        is_canonical=kw.get("is_canonical", True),
        is_observed=kw.get("is_observed", True),
    )


#: A calendar-year quarterly filer: 2024 and its four quarters.
QUARTERLY = Calendar(
    regimes=[],
    periods=[
        period("FY", "2024-01-01", "2024-12-31"),
        period("Q1", "2024-01-01", "2024-03-31"),
        period("Q2", "2024-04-01", "2024-06-30"),
        period("Q3", "2024-07-01", "2024-09-30"),
        period("Q4", "2024-10-01", "2024-12-31"),
    ],
)

#: The same year for a filer with no 10-Q: the calendar computes its halves and quarters.
SEMI_ANNUAL = Calendar(
    regimes=[],
    periods=[
        period("FY", "2024-01-01", "2024-12-31"),
        period("H1", "2024-01-01", "2024-06-30", is_observed=False),
        period("H2", "2024-07-01", "2024-12-31", is_observed=False),
        period("Q1", "2024-01-01", "2024-03-31", is_observed=False),
        period("Q2", "2024-04-01", "2024-06-30", is_observed=False),
        period("Q3", "2024-07-01", "2024-09-30", is_observed=False),
        period("Q4", "2024-10-01", "2024-12-31", is_observed=False),
    ],
)

REVENUE_HIGH = Concept(
    "revenue", "us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax", 100, "USD"
)
REVENUE_LOW = Concept("revenue", "us-gaap", "Revenues", 80, "USD")
REVENUE_IFRS = Concept("revenue", "ifrs-full", "Revenue", 100, "USD")
ASSETS = Concept("total_assets", "ifrs-full", "Assets", 100, "USD")
SHARES = Concept(
    "shares_diluted", "us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding", 100, "shares"
)
EPS = Concept("eps_diluted", "us-gaap", "EarningsPerShareDiluted", 100, "USD/shares")
SYNTHETIC = [REVENUE_HIGH, REVENUE_LOW, REVENUE_IFRS, ASSETS, SHARES, EPS]
SYNTHETIC_LINES = {"revenue": "income", "total_assets": "balance", "shares_diluted": "income",
                   "eps_diluted": "income"}  # fmt: skip


def run(
    raw: bytes, cal: Calendar = QUARTERLY, *, concepts: list[Concept] = SYNTHETIC,
    lines: dict[str, str] = SYNTHETIC_LINES, ids: list[str] | None = None,
    known: set[str] = KNOWN,
) -> sec_facts.Normalised:  # fmt: skip
    facts = sec_facts.read(raw, concepts).facts
    return sec_facts.statement_rows(
        facts, cal, concepts, lines,
        security_ids=ids or ["s"], known_currencies=known, as_of="2026-10-26T12:00:00+00:00",
    )  # fmt: skip


# --- the cases that decided a rule ---------------------------------------------------------------


def test_a_year_to_date_figure_is_not_a_quarter() -> None:
    """Apple's Q2 is 111.2bn. Its 10-Q also states the six months to the same date (254.9bn under
    the edge's first rule, which read `fp` and admitted anything under 200 days)."""
    out = rows_of("AAPL")
    assert row(out, "income", "quarter", "2026-03-28")["data"]["revenue"] == 111_184_000_000
    assert out.stats["ytd"] > 0
    six_months = 254_940_000_000
    assert all(json.loads(r["data"]).get("revenue") != six_months for r in out.rows)


def test_costco_s_sixteen_week_fourth_quarter_is_a_quarter() -> None:
    """112 days. The edge's 80-100 day band refused it; the four quarters then sum to the year."""
    out = rows_of("COST")
    quarters = [
        row(out, "income", "quarter", end)["data"]["revenue"]
        for end in ("2019-11-24", "2020-02-16", "2020-05-10", "2020-08-30")
    ]
    assert quarters[-1] == 53_383_000_000
    assert sum(quarters) == row(out, "income", "annual", "2020-08-30")["data"]["revenue"]


def test_an_instant_at_a_year_end_is_also_the_fourth_quarter_s_balance_sheet() -> None:
    out = rows_of("AAPL")
    annual = row(out, "balance", "annual", "2025-09-27")["data"]["cash_and_equivalents"]
    assert annual == row(out, "balance", "quarter", "2025-09-27")["data"]["cash_and_equivalents"]
    assert has(out, "balance", "quarter", "2025-12-27")
    assert not has(out, "balance", "annual", "2025-12-27")


def test_a_filer_reporting_in_kroner_is_labelled_in_kroner() -> None:
    """Novo Nordisk files under `ifrs-full` only, in DKK."""
    r = row(rows_of("NVO"), "income", "annual", "2025-12-31")
    assert (r["currency"], r["data"]["revenue"]) == ("DKK", 309_064_000_000)


def test_the_reporting_currency_is_the_unit_with_the_most_points() -> None:
    """TSMC states TWD and a USD convenience translation; picking USD would relabel its figures."""
    node = TRIMMED["TSM"]["facts"]["ifrs-full"]["Revenue"]["units"]
    assert set(node) == {"TWD", "USD"} and len(node["TWD"]) > len(node["USD"])
    r = row(rows_of("TSM"), "income", "annual", "2024-12-31")
    assert (r["currency"], r["data"]["revenue"]) == ("TWD", 2_894_307_700_000)


def test_superseded_and_transition_periods_are_counted_not_written() -> None:
    """Bristow (VTOL): Era's December years are superseded by Bristow's March years, and the 2022
    change to December is a transition. Only canonical years become annual rows."""
    out = rows_of("VTOL")
    assert out.stats["superseded"] > 0 and out.stats["transition"] > 0
    canonical = {
        p.period_end.isoformat()
        for p in calendar("VTOL").periods
        if p.is_canonical and p.fiscal_period == "FY"
    }
    annual = {r["period_ending"] for r in out.rows if r["period_type"] == "annual"}
    assert annual and annual <= canonical
    assert "2019-12-31" not in annual


# --- resolution ----------------------------------------------------------------------------------


def test_priority_beats_a_later_filing() -> None:
    raw = body({
        ("us-gaap", REVENUE_HIGH.concept): {"USD": [
            fact("2024-12-31", 100, start="2024-01-01", filed="2025-02-01")]},
        ("us-gaap", REVENUE_LOW.concept): {"USD": [
            fact("2024-12-31", 999, start="2024-01-01", filed="2026-02-01")]},
    })  # fmt: skip
    assert row(run(raw), "income", "annual", "2024-12-31")["data"]["revenue"] == 100


def test_within_a_concept_the_later_filing_is_the_answer() -> None:
    """A restatement: the later filing's figure for the same period wins, whatever the order."""
    points = [
        fact(
            "2024-12-31", 200, start="2024-01-01", filed="2026-02-01", accn="0000000000-26-000001"
        ),
        fact("2024-12-31", 100, start="2024-01-01", filed="2025-02-01"),
    ]
    for ordered in (points, points[::-1]):
        raw = body({("us-gaap", REVENUE_HIGH.concept): {"USD": ordered}})
        assert row(run(raw), "income", "annual", "2024-12-31")["data"]["revenue"] == 200


def test_a_tie_between_two_concepts_does_not_depend_on_their_order() -> None:
    raw = body({
        ("us-gaap", REVENUE_HIGH.concept): {"USD": [fact("2024-12-31", 100, start="2024-01-01")]},
        ("ifrs-full", REVENUE_IFRS.concept): {"USD": [fact("2024-12-31", 101, start="2024-01-01")]},
    })  # fmt: skip
    a = run(raw, concepts=SYNTHETIC)
    b = run(raw, concepts=SYNTHETIC[::-1])
    assert row(a, "income", "annual", "2024-12-31") == row(b, "income", "annual", "2024-12-31")
    assert a.stats["tie_disagrees"] == 1


def test_both_taxonomies_are_read() -> None:
    raw = body({("ifrs-full", ASSETS.concept): {"EUR": [fact("2024-12-31", 7)]}})
    r = row(run(raw), "balance", "annual", "2024-12-31")
    assert (r["currency"], r["data"]["total_assets"]) == ("EUR", 7)


# --- matching the calendar -----------------------------------------------------------------------


def test_a_start_a_few_days_off_still_matches_its_period() -> None:
    """52/53-week calendars move a start by up to a week between a report and a comparative."""
    raw = body({("us-gaap", REVENUE_HIGH.concept): {"USD": [
        fact("2024-06-30", 5, start="2024-03-26", fp="Q2", form="10-Q"),
        fact("2024-09-30", 6, start="2024-06-15", fp="Q3", form="10-Q"),
    ]}})  # fmt: skip
    out = run(raw)
    assert row(out, "income", "quarter", "2024-06-30")["data"]["revenue"] == 5
    assert not has(out, "income", "quarter", "2024-09-30")
    assert out.stats["ytd"] == 1


def test_a_period_with_no_canonical_match_is_counted_by_its_reason() -> None:
    cal = Calendar(
        regimes=[],
        periods=[
            *QUARTERLY.periods,
            period("FY", "2023-03-01", "2024-02-29", is_canonical=False),
            period("T", "2025-01-01", "2025-03-31"),
        ],
    )
    raw = body({("us-gaap", REVENUE_HIGH.concept): {"USD": [
        fact("2024-02-29", 1, start="2023-03-01"),
        fact("2025-03-31", 2, start="2025-01-01"),
        fact("2023-12-31", 3, start="2023-01-01"),
    ]}})  # fmt: skip
    out = run(raw, cal)
    assert (out.stats["superseded"], out.stats["transition"], out.stats["off_calendar"]) == (
        1,
        1,
        1,
    )
    assert out.rows == []


def test_an_instant_is_written_for_the_periods_that_end_on_it() -> None:
    raw = body({("ifrs-full", ASSETS.concept): {"USD": [
        fact("2024-12-31", 10), fact("2024-06-30", 6, fp="Q2", form="10-Q"),
        fact("2024-05-15", 5, fp="Q2", form="10-Q"),
    ]}})  # fmt: skip
    out = run(raw)
    assert {(r["period_type"], r["period_ending"]) for r in out.rows} == {
        ("annual", "2024-12-31"),
        ("quarter", "2024-12-31"),
        ("quarter", "2024-06-30"),
    }
    assert out.stats["instant_off_calendar"] == 1


def test_a_quarterly_filer_has_no_half_years_and_a_semi_annual_filer_does() -> None:
    """The calendar computes H1/H2 for years before a filer's first 10-Q. For a quarterly filer
    those are predecessor years (Bristow's 2012 from Era's 10-Q), and a lone half row is noise."""
    raw = body({
        ("us-gaap", REVENUE_HIGH.concept): {"USD": [
            fact("2024-06-30", 50, start="2024-01-01", fp="Q2", form="10-Q")]},
        ("ifrs-full", ASSETS.concept): {"USD": [fact("2024-06-30", 9, fp="Q2", form="10-Q")]},
    })  # fmt: skip
    quarterly = Calendar(
        regimes=[],
        periods=[*QUARTERLY.periods, period("H1", "2024-01-01", "2024-06-30", is_observed=False)],
    )  # fmt: skip
    q = run(raw, quarterly)
    assert {r["period_type"] for r in q.rows} == {"quarter"}
    assert q.stats["half_of_a_quarterly_filer"] == 1
    semi = run(raw, SEMI_ANNUAL)
    assert {(r["statement"], r["period_type"]) for r in semi.rows} == {
        ("income", "half"),
        ("balance", "half"),
    }
    assert sec_facts.is_quarterly(QUARTERLY) and not sec_facts.is_quarterly(SEMI_ANNUAL)


# --- the rows ------------------------------------------------------------------------------------


def test_rows_fan_out_to_every_security_and_carry_their_metric_codes() -> None:
    """GOOG and GOOGL share a CIK, so one filer's rows are written for each of its equities."""
    raw = body({
        ("us-gaap", REVENUE_HIGH.concept): {"USD": [fact("2024-12-31", 100, start="2024-01-01")]},
        ("us-gaap", SHARES.concept): {"shares": [fact("2024-12-31", 3, start="2024-01-01")]},
        ("us-gaap", EPS.concept): {"USD/shares": [fact("2024-12-31", 1.5, start="2024-01-01")]},
    })  # fmt: skip
    out = run(raw, ids=["a", "b"])
    assert [r["security_id"] for r in out.rows] == ["a", "b"]
    first = {**out.rows[0], "data": json.loads(out.rows[0]["data"])}
    assert first == {
        "security_id": "a",
        "statement": "income",
        "period_ending": "2024-12-31",
        "period_type": "annual",
        "currency": "USD",
        "data": {
            "period_ending": "2024-12-31",
            "revenue": 100,
            "shares_diluted": 3,
            "eps_diluted": 1.5,
        },
        "source_code": "sec-xbrl",
        "as_of": "2026-10-26T12:00:00+00:00",
    }


def test_an_unknown_currency_is_withheld_and_counted() -> None:
    raw = body({("ifrs-full", ASSETS.concept): {"XTS": [fact("2024-12-31", 1)]}})
    out = run(raw)
    assert {r["currency"] for r in out.rows} == {None}
    assert out.stats["currency_withheld"] == 2


def test_a_row_takes_its_most_common_currency_and_counts_a_mix() -> None:
    concepts = [
        *SYNTHETIC,
        Concept("net_income", "us-gaap", "NetIncomeLoss", 100, "USD"),
        Concept("operating_income", "us-gaap", "OperatingIncomeLoss", 100, "USD"),
    ]
    lines = {**SYNTHETIC_LINES, "net_income": "income", "operating_income": "income"}
    raw = body({
        ("us-gaap", REVENUE_HIGH.concept): {"EUR": [fact("2024-12-31", 1, start="2024-01-01")]},
        ("us-gaap", "NetIncomeLoss"): {"EUR": [fact("2024-12-31", 2, start="2024-01-01")]},
        ("us-gaap", "OperatingIncomeLoss"): {"USD": [fact("2024-12-31", 3, start="2024-01-01")]},
    })  # fmt: skip
    out = run(raw, concepts=concepts, lines=lines)
    assert [r["currency"] for r in out.rows] == ["EUR"]
    assert out.stats["mixed_currency"] == 1


def test_a_metric_no_statement_is_mapped_for_is_counted() -> None:
    raw = body(
        {("us-gaap", REVENUE_HIGH.concept): {"USD": [fact("2024-12-31", 1, start="2024-01-01")]}}
    )
    out = run(raw, lines={"total_assets": "balance"})
    assert out.rows == [] and out.stats["unmapped_metric"] == 1


def test_every_category_production_holds_names_a_statement() -> None:
    assert set(XBRL["categories"].values()) <= set(STATEMENT_OF_CATEGORY)


def test_share_counts_are_read_beside_eps() -> None:
    """Production files share counts under category `share`; the income row carries them."""
    data = row(rows_of("AAPL"), "income", "annual", "2025-09-27")["data"]
    assert (data["eps_diluted"], data["shares_diluted"]) == (7.46, 15_004_697_000)


# --- reading -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("units", "kind", "expected"),
    [
        ({"TWD": [1, 2, 3], "USD": [1, 2]}, "USD", "TWD"),
        ({"EUR": [1, 2], "DKK": [1, 2]}, "USD", "DKK"),
        ({"USD": [1], "USD/shares": [1, 2]}, "USD", "USD"),
        ({"USD": [1], "EUR/shares": [1, 2]}, "USD/shares", "EUR/shares"),
        ({"shares": [1], "USD": [1, 2]}, "shares", "shares"),
        ({"USD": [1]}, "shares", None),
        ({"pure": [1]}, "USD", None),
    ],
)
def test_pick_unit_key(units: dict[str, list[int]], kind: str, expected: str | None) -> None:
    assert sec_facts.pick_unit_key(units, kind) == expected


@pytest.mark.parametrize(
    "raw",
    [
        b'<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>',
        b"[]",
        b'{"cik": 1}',
        b'{"facts": []}',
    ],
    ids=["s3-error", "not-an-object", "no-facts", "facts-not-an-object"],
)
def test_a_body_that_is_not_company_facts_raises(raw: bytes) -> None:
    with pytest.raises(fiscal.FactsUnreadable):
        sec_facts.read(raw, SYNTHETIC)


def test_a_value_that_is_not_a_finite_number_is_skipped() -> None:
    raw = body({("us-gaap", REVENUE_HIGH.concept): {"USD": [
        fact("2024-12-31", True, start="2024-01-01"), fact("2024-12-31", "7", start="2024-01-01"),
        fact("2024-12-31", 4, start="2024-01-01"),
    ]}})  # fmt: skip
    read = sec_facts.read(raw, SYNTHETIC)
    assert [f.value for f in read.facts] == [4] and read.stats["not_a_number"] == 2


def test_a_malformed_date_raises_rather_than_dropping_the_fact() -> None:
    raw = body({("us-gaap", REVENUE_HIGH.concept): {"USD": [fact("2024-13-31", 1)]}})
    with pytest.raises(fiscal.FactsUnreadable):
        sec_facts.read(raw, SYNTHETIC)


def test_stage_1_adds_only_its_context_columns() -> None:
    document = Document(
        url="https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
        body=b"{}",
        content_type="application/json",
        fetched_at=datetime(2026, 10, 26, 12, tzinfo=UTC),
    )
    [stored] = sec_facts.raw_rows(document, http_status=404, run_id="r")
    assert set(stored) - set(document.as_row("r")) == sec_facts.CONTEXT_COLUMNS
    assert stored["http_status"] == 404


class _Conn:
    """A connection answering one query with fixed rows."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.sql: list[tuple[str, Any]] = []

    def cursor(self) -> _Conn:
        return self

    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = ()) -> None:
        self.sql.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


def test_concepts_read_both_tables_and_refuse_a_concept_two_metrics_name() -> None:
    """`statement_rows` ranks facts by concept, so a concept two metrics named would be read for
    whichever came last. The loader fails rather than choose."""
    conn = _Conn([("total_assets", "us-gaap", "Assets", 100, "USD")])
    [spec] = sec_facts.concepts(conn)
    assert spec == Concept("total_assets", "us-gaap", "Assets", 100, "USD")
    assert "xbrl_fact_concept" in conn.sql[0][0] and "xbrl_concept" in conn.sql[0][0]
    clash = _Conn(
        [
            ("total_assets", "us-gaap", "Assets", 100, "USD"),
            ("other", "us-gaap", "Assets", 90, "USD"),
        ]
    )
    with pytest.raises(ValueError, match="us-gaap:Assets names both"):
        sec_facts.concepts(clash)


def test_lines_are_this_source_s_rows_of_metric_source_field() -> None:
    conn = _Conn([("revenue", "income"), ("total_assets", "balance")])
    assert sec_facts.lines(conn) == {"revenue": "income", "total_assets": "balance"}
    assert conn.sql[0][1] == ("sec-xbrl",)
