"""The fiscal calendar, over twelve filers' inputs as the library summarises them.

`fixtures/sec/fiscal_inputs.json` holds, per filer, its annual, transition and quarterly reports
from the submissions captured 2026-10-26 (plus one older page for AAPL, GEF and WMT), the label of
each of those accessions, and the spans the calendar reads, all produced by
`fiscal.summarise_facts` over the full company facts. Each test names the measured case where the
wrong rule gives another answer.
"""

from __future__ import annotations

import itertools
import json
from collections import Counter
from datetime import date
from functools import cache
from pathlib import Path

import pytest

from muffin_ingest.facets import fiscal

INPUTS = json.loads(
    (Path(__file__).parents[1] / "fixtures" / "sec" / "fiscal_inputs.json").read_bytes()
)
FILERS = sorted(INPUTS)


@cache
def calendar(filer: str) -> fiscal.Calendar:
    x = INPUTS[filer]
    return fiscal.calendar_from_sec(
        [fiscal.Report(a, f, date.fromisoformat(r), role) for a, f, r, role in x["reports"]],
        {a: fiscal.FilingLabel(fy, fp) for a, (fy, fp) in x["labels"].items()},
        [
            fiscal.Span(a, f, date.fromisoformat(s), date.fromisoformat(e), n)
            for a, f, s, e, n in x["spans"]
        ],
    )


def canonical(filer: str, fiscal_year: int, period: str) -> fiscal.Period:
    [found] = [
        p
        for p in calendar(filer).periods
        if p.is_canonical and p.fiscal_year == fiscal_year and p.fiscal_period == period
    ]
    return found


def ending(filer: str, end: str) -> list[fiscal.Period]:
    return [p for p in calendar(filer).periods if p.period_end == date.fromisoformat(end)]


# --- what holds for every filer ------------------------------------------------------------------


@pytest.mark.parametrize("filer", FILERS)
def test_one_canonical_period_per_label_and_one_row_per_key(filer: str) -> None:
    """`fiscal_period_canonical` and the primary key, which refuse a calendar breaking them."""
    periods = calendar(filer).periods
    labels = Counter((p.fiscal_year, p.fiscal_period) for p in periods if p.is_canonical)
    keys = Counter((p.regime, p.fiscal_year, p.fiscal_period) for p in periods)
    assert [k for k, n in labels.items() if n > 1] == []
    assert [k for k, n in keys.items() if n > 1] == []
    assert all(p.period_start <= p.period_end for p in periods)


@pytest.mark.parametrize("filer", FILERS)
def test_a_regime_s_labels_advance_with_its_years(filer: str) -> None:
    """One year later is one label later, and a year never filed skips its label too."""
    for regime in calendar(filer).regimes:
        years = sorted(
            (p.period_end, p.fiscal_year)
            for p in calendar(filer).periods
            if p.regime == regime.regime and p.fiscal_period == "FY"
        )
        for (end_a, year_a), (end_b, year_b) in itertools.pairwise(years):
            assert year_b - year_a == round((end_b - end_a).days / 365.25), (filer, end_b)


# --- labels --------------------------------------------------------------------------------------


def test_a_year_is_labelled_by_its_own_filing_not_a_later_one() -> None:
    """Apple's year ending 2022-09-24 is fy 2022 in its 10-K and 2023 and 2024 in the next two."""
    year = canonical("AAPL", 2022, "FY")
    assert (year.period_start, year.period_end) == (date(2021, 9, 26), date(2022, 9, 24))


def test_january_year_ends_follow_each_company_s_convention() -> None:
    """No date rule gets both: Walmart's year ending 2025-01-31 is fiscal 2025, Target's 2024."""
    assert canonical("WMT", 2025, "FY").period_end == date(2025, 1, 31)
    assert canonical("TGT", 2024, "FY").period_end == date(2025, 2, 1)


def test_a_regime_is_labelled_from_its_newest_year_and_early_mistags_are_counted() -> None:
    """Walmart tagged the years ending January 2010-2012 as fy 2009, 2010 and 2011."""
    assert canonical("WMT", 2010, "FY").period_end == date(2010, 1, 31)
    assert canonical("WMT", 2012, "FY").period_end == date(2012, 1, 31)
    assert calendar("WMT").stats["year_label_disagrees"] == 3


def test_a_lone_report_date_off_its_neighbours_anchor_is_dropped() -> None:
    """Walmart's 10-K for the year ending 2009-01-31 says 2009-04-01, the day it was filed."""
    assert len(calendar("WMT").regimes) == 1
    assert calendar("WMT").stats["report_off_anchor"] == 1
    assert canonical("WMT", 2009, "FY").period_end == date(2009, 1, 31)


# --- years only facts state ----------------------------------------------------------------------


def test_a_year_on_another_anchor_is_not_a_year_of_this_calendar() -> None:
    """Walmart's 10-Ks carry December years (6 facts each); ESCO's one fact ending 2017-12-31."""
    assert calendar("WMT").stats["year_off_anchor"] == 3
    assert calendar("ESE").stats["year_off_anchor"] == 1
    assert ending("WMT", "2011-12-31") == []


def test_a_fragment_on_the_new_anchor_is_not_a_year() -> None:
    """Greif's 304-day span ending 2024-09-30 sits on its new September anchor and is not a year."""
    assert ending("GEF", "2024-09-30") == []


def test_a_year_only_an_8k_states_is_not_derived() -> None:
    """Bristow's March 2020 year appears only in an 8-K (6 facts)."""
    assert ending("VTOL", "2020-03-31") == []


# --- changes of fiscal year ----------------------------------------------------------------------


def test_a_transition_period_is_t_and_starts_the_new_calendar() -> None:
    """Greif: October years to 2024, then a 10-KT from November 2024 to September 2025."""
    t = canonical("GEF", 2025, "T")
    assert (t.period_start, t.period_end) == (date(2024, 11, 1), date(2025, 9, 30))
    old, new = calendar("GEF").regimes
    assert (old.year_end_month, new.year_end_month, new.change_kind) == (10, 9, "year_end_change")
    assert canonical("GEF", 2024, "FY").period_end == date(2024, 10, 31)


def test_a_transition_and_the_year_it_follows_keep_their_labels() -> None:
    """Mistras: a May fiscal 2016, then a 10-KT for June to December 2016, also fy 2016."""
    assert canonical("MG", 2016, "FY").period_end == date(2016, 5, 31)
    assert canonical("MG", 2016, "T").period_end == date(2016, 12, 31)


def test_a_quarter_inside_a_transition_has_no_label() -> None:
    """Greif's three 2025 10-Qs fall inside its 10-KT; a quarter of no year is not invented."""
    assert calendar("GEF").stats["interim_off_calendar"] == 3
    assert ending("GEF", "2025-01-31") == []


def test_the_successor_s_calendar_wins() -> None:
    """Bristow (decision 11): its recast April-March year beats Era's overlapping December years."""
    era, successor, current = calendar("VTOL").regimes
    assert (successor.change_kind, current.change_kind) == ("successor", "year_end_change")
    recast = canonical("VTOL", 2019, "FY")
    assert (recast.regime, recast.period_end, recast.is_recast) == (2, date(2019, 3, 31), True)
    # Era's 2018 overlaps it by nine months; Era's 2019 loses the label 2019 to it.
    era_years = {
        p.fiscal_year: p.is_canonical
        for p in calendar("VTOL").periods
        if p.regime == era.regime and p.fiscal_period == "FY"
    }
    assert (era_years[2017], era_years[2018], era_years[2019]) == (True, False, False)
    # And Era's quarters go with their years.
    assert not any(
        p.is_canonical
        for p in calendar("VTOL").periods
        if p.regime == era.regime and p.fiscal_year in (2018, 2019)
    )


def test_a_recast_year_hosts_no_quarter_of_the_predecessor() -> None:
    """Era's 10-Qs of 2018 are Era's, not quarters of Bristow's April 2018 - March 2019 year."""
    assert not any(
        p.regime == 2 and p.fiscal_year == 2019 and p.fiscal_period != "FY"
        for p in calendar("VTOL").periods
    )


# --- quarters ------------------------------------------------------------------------------------


def test_quarters_are_numbered_by_position_and_a_wrong_tag_is_counted() -> None:
    """Greif's 10-Q ending 2020-07-31 is tagged Q2; it is the third quarter of its year."""
    q3 = canonical("GEF", 2020, "Q3")
    assert q3.period_end == date(2020, 7, 31)
    assert calendar("GEF").stats["quarter_label_disagrees"] == 1


def test_a_52_53_week_year_of_uneven_quarters() -> None:
    """Costco's 12/12/12/16 weeks: Q4 is the 112 days after Q3."""
    lengths = [
        (canonical("COST", 2026, q).period_end - canonical("COST", 2026, q).period_start).days + 1
        for q in ("Q1", "Q2", "Q3", "Q4")
    ]
    assert lengths == [84, 84, 84, 112]


def test_the_current_year_is_projected_to_hold_its_quarters() -> None:
    """Apple's FY2026 has no 10-K yet; its 10-Qs need a year ending September's last Saturday."""
    year = canonical("AAPL", 2026, "FY")
    assert (year.period_end, year.is_observed) == (date(2026, 9, 26), False)
    assert canonical("AAPL", 2026, "Q1").period_end == date(2025, 12, 27)
    assert not any(
        p.fiscal_year == 2026 and p.fiscal_period == "Q4" for p in calendar("AAPL").periods
    )
    regime = calendar("AAPL").regimes[-1]
    assert (regime.is_52_53_week, regime.week_end_isodow) == (True, 6)


def test_a_filer_with_no_10q_gets_computed_quarters_and_halves() -> None:
    """BHP and Vodafone file 20-F only; a provider's half-year rows still need a label."""
    h1 = canonical("BHP", 2026, "H1")
    assert (h1.period_start, h1.period_end, h1.is_observed) == (
        date(2025, 7, 1),
        date(2025, 12, 31),
        False,
    )
    assert canonical("VOD", 2026, "H1").period_end == date(2025, 9, 30)
    assert canonical("TSM", 2026, "Q4").period_end == date(2026, 12, 31)


# --- company facts -------------------------------------------------------------------------------


def _facts(*rows: dict[str, object]) -> bytes:
    return json.dumps({"facts": {"us-gaap": {"Revenues": {"units": {"USD": list(rows)}}}}}).encode()


def test_facts_give_each_filing_s_label_and_every_span_of_two_months_or_more() -> None:
    body = _facts(
        {
            "accn": "a",
            "fy": 2025,
            "fp": "FY",
            "form": "10-K",
            "start": "2024-10-01",
            "end": "2025-09-30",
        },
        {
            "accn": "a",
            "fy": 2025,
            "fp": "FY",
            "form": "10-K",
            "start": "2025-07-01",
            "end": "2025-09-30",
        },
        {
            "accn": "a",
            "fy": 2025,
            "fp": "FY",
            "form": "10-K",
            "start": "2025-09-01",
            "end": "2025-09-30",
        },
        {"accn": "a", "fy": 2025, "fp": "FY", "form": "10-K", "end": "2025-09-30"},
        {
            "accn": "b",
            "fy": 2024,
            "fp": "Q1",
            "form": "10-Q",
            "start": "2024-10-01",
            "end": "2024-12-31",
        },
    )
    labels, spans = fiscal.summarise_facts(body)
    assert labels == {"a": fiscal.FilingLabel(2025, "FY"), "b": fiscal.FilingLabel(2024, "Q1")}
    assert sorted((s.accession, s.days) for s in spans) == [("a", 91), ("a", 364), ("b", 91)]


@pytest.mark.parametrize(
    "body", [b"<Error/>", b"[]", b'{"cik": 1}'], ids=["xml", "list", "no-facts"]
)
def test_a_body_that_is_not_company_facts_raises(body: bytes) -> None:
    with pytest.raises(fiscal.FactsUnreadable):
        fiscal.summarise_facts(body)


# --- without SEC ---------------------------------------------------------------------------------


def test_a_provider_calendar_numbers_interim_ends_by_months_elapsed() -> None:
    """A March year with its second quarter missing: the third is still Q3, not Q2."""
    years = [date(2024, 3, 31), date(2025, 3, 31)]
    quarters = [date(2024, 6, 30), date(2024, 12, 31), date(2025, 3, 31), date(2025, 6, 30)]
    c = fiscal.calendar_from_periods(years, quarter_ends=quarters)
    labels = {(p.fiscal_year, p.fiscal_period): p.period_end for p in c.periods if p.is_canonical}
    assert labels[(2025, "Q1")] == date(2024, 6, 30)
    assert labels[(2025, "Q3")] == date(2024, 12, 31)
    assert labels[(2025, "Q4")] == date(2025, 3, 31)
    # The year after the last annual end holds the newest quarter.
    assert labels[(2026, "Q1")] == date(2025, 6, 30)
    assert c.regimes[0].change_kind == "observed"


def test_a_provider_calendar_labels_halves_and_a_december_year_by_its_end() -> None:
    c = fiscal.calendar_from_periods(
        [date(2024, 12, 31), date(2025, 12, 31)], half_ends=[date(2025, 6, 30), date(2026, 6, 30)]
    )
    labels = {(p.fiscal_year, p.fiscal_period): p.period_end for p in c.periods if p.is_canonical}
    assert labels[(2025, "FY")] == date(2025, 12, 31)
    assert labels[(2025, "H1")] == date(2025, 6, 30)
    assert labels[(2026, "H1")] == date(2026, 6, 30)


def test_an_end_in_the_first_week_of_january_belongs_to_the_year_before() -> None:
    assert fiscal.calendar_year_label(date(2027, 1, 2)) == 2026
    assert fiscal.calendar_year_label(date(2027, 1, 31)) == 2027


def test_no_annual_report_is_no_calendar() -> None:
    assert fiscal.calendar_from_sec([], {}, []).periods == []
    assert fiscal.calendar_from_periods([]).periods == []


def test_two_ends_for_one_half_keep_one_row() -> None:
    """The table's key is (regime, year, period): two H1 ends days apart must not both be stored."""
    c = fiscal.calendar_from_periods(
        [date(2024, 12, 31), date(2025, 12, 31)], half_ends=[date(2025, 6, 28), date(2025, 6, 30)]
    )
    [h1] = [p for p in c.periods if (p.fiscal_year, p.fiscal_period) == (2025, "H1")]
    assert h1.period_end == date(2025, 6, 30)
    assert c.stats["duplicate_label"] == 1


def test_rows_write_one_calendar_per_security_and_hold_the_tables_keys() -> None:
    """GOOG and GOOGL share one CIK, so the calendar is written under each; every row carries the
    keys `fiscal_calendar` and `fiscal_period` are declared with, and a period's regime exists."""
    cal = calendar("AAPL")
    calendars, periods = fiscal.rows(cal, security_ids=["a", "b"], source="sec-xbrl", as_of="t")
    assert len(calendars) == 2 * len(cal.regimes) and len(periods) == 2 * len(cal.periods)
    regimes = {(c["security_id"], c["regime"]) for c in calendars}
    assert len(regimes) == len(calendars)
    assert {(p["security_id"], p["regime"]) for p in periods} <= regimes
    keys = Counter(
        (p["security_id"], p["regime"], p["fiscal_year"], p["fiscal_period"]) for p in periods
    )
    assert max(keys.values()) == 1
    assert (
        {c["source_code"] for c in calendars} == {"sec-xbrl"} == {p["source_code"] for p in periods}
    )
