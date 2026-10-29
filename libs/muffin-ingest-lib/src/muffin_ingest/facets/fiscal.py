"""The fiscal calendar: each company's years and quarters, named the way the company names them.

DECIDED 2026-10-24 (Phase 5 decisions 7 and 11). A statement row is labelled with the canonical
fiscal period it matches (`market.label_fiscal_periods`), so two providers' rows for one period,
dated days apart, are served once, and a fiscal-year change no longer reads as a repeated year.

WHY THE LABEL COMES FROM THE FILING, measured 2026-10-26: company facts' `fy` and `fp` label the
FILING, not the period. Every accession's facts carry exactly one `(fy, fp)`, so Apple's year ending
2022-09-24 is fy 2022 in its own 10-K and 2023 and 2024 in the next two. A year's label is the
`fy` of any fact in its ORIGINAL annual report. That is also what tells Walmart's year ending
2025-01-31 (fiscal 2025) from Target's ending 2025-02-01 (fiscal 2024): no rule on the date can.

THE SPINE IS THE COMPANY'S OWN REPORTS. Every annual report (10-K, 20-F, 40-F) and transition
report (10-KT) gives a year end, and every 10-Q a quarter end: all 858 measured carry a
`reportDate`. Facts add the years a registrant reports but never filed itself, which is the
successor case: Bristow's March years before 2021 exist under this CIK only as comparatives.

  * A REGIME is one calendar: a run of year ends on one anchor month (the month end nearest the
    year end, so Apple's 52/53-week Saturdays all anchor on September). A change of anchor starts
    a new regime. A period that is not a year (a 10-KT, or 7 months of Mistras) is a transition `T`.
  * THE NEWEST CALENDAR WINS (decision 11). A year overlapping a newer regime's year by more than
    half is superseded (`is_canonical = false`), and so is a period whose label a newer regime also
    uses. The second rule is what keeps the partial unique index on canonical labels satisfiable.
  * The CURRENT year is projected one year past the last year end, `is_observed = false`, so the
    quarters filed before its annual report have a year to belong to.

A year derived from facts must be a year (355-375 days: 363, 364 and 370 measured), stated in an
annual report, and on an existing regime's anchor. Each guard has a measured case it refuses:
Bristow's March 2020 appears only in an 8-K, Greif's 304-day fragment ends on its new anchor, and
Walmart's 10-Ks carry December years inside a January calendar.

PURE. `summarise_facts` reads a company facts body; everything else takes plain values.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import Literal

Role = Literal["annual", "transition", "interim"]
Label = Literal["FY", "H1", "H2", "Q1", "Q2", "Q3", "Q4", "T"]
ChangeKind = Literal["initial", "year_end_change", "successor", "observed"]

#: A fiscal year, in days. 52/53-week years run 363 to 371; Apple's measured 363, 364 and 370.
YEAR_DAYS = (355, 375)
#: A quarter, in days. Costco's 16-week fourth quarter is 112, its 12-week quarters 84.
QUARTER_DAYS = (75, 125)
#: Two ends this close are one end.
SAME_END = timedelta(days=20)
#: The shortest span read as a period at all; shorter facts are months or weeks.
MIN_SPAN_DAYS = 60


class FactsUnreadable(RuntimeError):
    """The body is not a company facts document. Never read as a filer with no facts."""


@dataclass(frozen=True)
class Report:
    """One of the registrant's own accounts filings, never an amendment.

    `interim` is a QUARTERLY report (10-Q, 10-QT). A 6-K is not one: it carries monthly revenue as
    often as results, and no quarterly XBRL.
    """

    accession: str
    form: str
    report_date: date
    role: Role


@dataclass(frozen=True)
class FilingLabel:
    """What a filing's own facts call it: SEC's `fy` and `fp`."""

    fiscal_year: int | None
    fiscal_period: str | None


@dataclass(frozen=True)
class Span:
    """A duration a filing states facts for, and how many."""

    accession: str
    form: str | None
    start: date
    end: date
    facts: int

    @property
    def days(self) -> int:
        return (self.end - self.start).days


@dataclass(frozen=True)
class Regime:
    regime: int
    year_end_month: int
    year_end_day: int | None
    is_52_53_week: bool
    week_end_isodow: int | None
    first_year_end: date
    last_year_end: date | None
    change_kind: ChangeKind


@dataclass(frozen=True)
class Period:
    regime: int
    fiscal_year: int
    fiscal_period: Label
    period_start: date
    period_end: date
    is_canonical: bool
    is_observed: bool
    is_recast: bool = False


@dataclass
class Calendar:
    regimes: list[Regime]
    periods: list[Period]
    stats: Counter[str] = field(default_factory=Counter)


# --- company facts -------------------------------------------------------------------------------


def summarise_facts(body: bytes) -> tuple[dict[str, FilingLabel], list[Span]]:
    """Each filing's label, and every duration of `MIN_SPAN_DAYS` or more its facts state.

    A filing whose facts disagree about `(fy, fp)` takes the most common: none did over 213
    accessions measured, so this is a tie-break and not a rule.
    """
    try:
        payload = json.loads(body)
        taxonomies = payload["facts"]
    except (ValueError, KeyError, TypeError) as exc:
        raise FactsUnreadable(f"not a company facts document: {exc}") from exc
    if not isinstance(taxonomies, dict):
        raise FactsUnreadable("`facts` is not an object")
    labels: dict[str, Counter[tuple[int | None, str | None]]] = {}
    spans: Counter[tuple[str, str | None, date, date]] = Counter()
    for concepts in taxonomies.values():
        for concept in concepts.values():
            for facts in (concept.get("units") or {}).values():
                for fact in facts:
                    accession = fact.get("accn")
                    if not accession:
                        continue
                    fy = fact.get("fy")
                    labels.setdefault(accession, Counter())[
                        (int(fy) if fy is not None else None, fact.get("fp"))
                    ] += 1
                    if "start" not in fact:
                        continue
                    start, end = date.fromisoformat(fact["start"]), date.fromisoformat(fact["end"])
                    if (end - start).days >= MIN_SPAN_DAYS:
                        spans[(accession, fact.get("form"), start, end)] += 1
    return (
        {a: FilingLabel(*c.most_common(1)[0][0]) for a, c in labels.items()},
        [Span(a, f, s, e, n) for (a, f, s, e), n in spans.items()],
    )


# --- dates ---------------------------------------------------------------------------------------


def _month_end(year: int, month: int) -> date:
    return date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)


def anchor(end: date) -> int:
    """The month of the month end nearest `end`, so a 52/53-week Saturday anchors on its month."""
    previous = date(end.year, end.month, 1) - timedelta(days=1)
    return (
        previous.month if (end - previous) < (_month_end(end.year, end.month) - end) else end.month
    )


def _add_months(day: date, months: int) -> date:
    """`day` moved by whole months, clamped to the month's last day."""
    total = day.month - 1 + months
    year, month = day.year + total // 12, total % 12 + 1
    return min(date(year, month, 1) + timedelta(days=day.day - 1), _month_end(year, month))


def _years_between(a: date, b: date) -> int:
    return round((b - a).days / 365.25)


def calendar_year_label(end: date) -> int:
    """The label when no filing names the year: its calendar year, and the year before for an end
    on 1-7 January (a December 52/53-week year spilling over)."""
    return end.year - 1 if end.month == 1 and end.day <= 7 else end.year


def _next_year_end(group: Sequence[_Year]) -> date:
    """The current year's end, projected: 52 weeks on, or the anchor's month end a year on."""
    last = group[-1].end
    return (
        last + timedelta(days=364) if _is_52_53(group) else _month_end(last.year + 1, anchor(last))
    )


# --- building ------------------------------------------------------------------------------------


@dataclass
class _Year:
    """A year or transition while the calendar is built."""

    end: date
    start: date | None
    label: Label
    fiscal_year: int | None
    #: The registrant's own annual or transition report states it.
    own: bool
    observed: bool = True
    regime: int = 0
    recast: bool = False


def _best_start(spans: Iterable[Span], end: date, low: int, high: int) -> date | None:
    """The start most facts state for a span ending at `end` whose length is within [low, high]."""
    counts: Counter[date] = Counter()
    for span in spans:
        if span.end == end and low <= span.days <= high:
            counts[span.start] += span.facts
    if not counts:
        return None
    return max(counts, key=lambda s: (counts[s], -s.toordinal()))


def _is_year(start: date | None, end: date) -> bool:
    return start is None or YEAR_DAYS[0] <= (end - start).days <= YEAR_DAYS[1]


#: How many of a regime's latest year ends decide whether it is 52/53-week. Its CURRENT rule is what
#: matters: Apple's years ended on Fridays in the 1990s and on Saturdays since.
WEEKDAY_EVIDENCE = 4


def _is_52_53(group: Sequence[_Year]) -> bool:
    ends = [y.end for y in group if y.label == "FY" and y.observed][-WEEKDAY_EVIDENCE:]
    if not ends or all(e == _month_end(e.year, e.month) for e in ends):
        return False
    return len({e.isoweekday() for e in ends}) == 1


def _stray(before: _Year | None, year: _Year, after: _Year | None, stats: Counter[str]) -> bool:
    if year.label == "T" or before is None or after is None:
        return False
    if anchor(before.end) == anchor(after.end) != anchor(year.end):
        stats["report_off_anchor"] += 1
        return True
    return False


def _regimes(years: list[_Year]) -> list[list[_Year]]:
    """Years in order, split where the anchor month changes."""
    groups: list[list[_Year]] = []
    for year in sorted(years, key=lambda y: y.end):
        if groups and anchor(groups[-1][-1].end) == anchor(year.end):
            groups[-1].append(year)
        else:
            groups.append([year])
    return groups


def _distance(day: date, group: Sequence[_Year]) -> int:
    first, last = group[0].end, group[-1].end
    if first <= day <= last:
        return 0
    return min(abs((day - first).days), abs((day - last).days))


def _finish(groups: list[list[_Year]], stats: Counter[str], *, labelled: bool) -> list[_Year]:
    """Starts, regime numbers, recasts and labels, in place; the years in order."""
    for number, group in enumerate(groups, start=1):
        group.sort(key=lambda y: y.end)
        first_own = min((y.end for y in group if y.own), default=None)
        previous: _Year | None = None
        for year in group:
            year.regime = number
            if year.start is None:
                if previous is not None and (year.end - previous.end).days <= 400:
                    year.start = previous.end + timedelta(days=1)
                else:
                    year.start = _add_months(year.end, -12) + timedelta(days=1)
            year.recast = (
                number > 1 and not year.own and first_own is not None and year.end < first_own
            )
            previous = year
        if labelled:
            _label(group, stats)
        else:
            for year in group:
                year.fiscal_year = calendar_year_label(year.end)
    return [y for g in groups for y in g]


def _label(group: Sequence[_Year], stats: Counter[str]) -> None:
    """Every year in a regime counted from its NEWEST labelled year, by years elapsed.

    NOT EACH FILING'S OWN `fy`, because a filer's early tags can be wrong. Measured 2026-10-27,
    Walmart's 10-Ks label the years ending January 2010-2014 as 2009, 2010, 2011, 2012 and 2013
    (offsets -1, -2, -2, -1, -1 against the end year) and every year from 2015 on by its end year,
    which is how Walmart names them. Target is -1 throughout; Costco, Apple, Microsoft and JPMorgan
    are 0 throughout. One reference per regime keeps the years consecutive; a filing that disagrees
    with it is counted.
    """
    labelled = [y for y in group if y.own and y.fiscal_year is not None]
    reference = max(labelled, key=lambda y: (y.label == "FY", y.end), default=None)
    for year in group:
        if reference is None or reference.fiscal_year is None:
            year.fiscal_year = calendar_year_label(year.end)
            continue
        counted = reference.fiscal_year + _years_between(reference.end, year.end)
        if year.own and year.fiscal_year is not None and year.fiscal_year != counted:
            stats["year_label_disagrees"] += 1
        year.fiscal_year = counted


def _year_period(year: _Year) -> Period:
    assert year.start is not None and year.fiscal_year is not None
    return Period(
        regime=year.regime,
        fiscal_year=year.fiscal_year,
        fiscal_period=year.label,
        period_start=year.start,
        period_end=year.end,
        is_canonical=True,
        is_observed=year.observed,
        is_recast=year.recast,
    )


def calendar_from_sec(
    reports: Sequence[Report],
    labels: Mapping[str, FilingLabel],
    spans: Sequence[Span],
) -> Calendar:
    """A SEC registrant's calendar from its own reports and its company facts."""
    stats: Counter[str] = Counter()
    by_accession: dict[str, list[Span]] = {}
    for span in spans:
        by_accession.setdefault(span.accession, []).append(span)

    # 1. Years from the registrant's own annual and transition reports.
    own: list[_Year] = []
    for report in sorted(reports, key=lambda r: r.report_date):
        if report.role == "interim":
            continue
        if any(abs(y.end - report.report_date) <= SAME_END for y in own):
            stats["duplicate_report"] += 1
            continue
        start = _best_start(
            by_accession.get(report.accession, ()), report.report_date, MIN_SPAN_DAYS, 400
        )
        is_transition = report.role == "transition" or not _is_year(start, report.report_date)
        label = labels.get(report.accession)
        own.append(
            _Year(
                end=report.report_date,
                start=start,
                label="T" if is_transition else "FY",
                fiscal_year=label.fiscal_year if label else None,
                own=True,
            )
        )
    if not own:
        return Calendar([], [], stats)
    # A LONE ANNUAL REPORT OFF ITS NEIGHBOURS' ANCHOR IS A WRONG DATE, NOT A NEW CALENDAR: Walmart's
    # 10-K for the year ending 2009-01-31 carries a `reportDate` of 2009-04-01, the day it was
    # filed.
    own = [
        year
        for before, year, after in zip([None, *own[:-1]], own, [*own[1:], None], strict=True)
        if not _stray(before, year, after, stats)
    ]
    groups = _regimes(own)

    # 2. Years only facts state: a year, in an annual report, on an existing regime's anchor.
    annual_forms = {r.form for r in reports if r.role != "interim"}
    derived: dict[date, list[Span]] = {}
    for span in spans:
        if span.form in annual_forms and YEAR_DAYS[0] <= span.days <= YEAR_DAYS[1]:
            derived.setdefault(span.end, []).append(span)
    every_end = [y.end for g in groups for y in g]
    for end in sorted(derived):
        if any(abs(end - e) <= SAME_END for e in every_end):
            continue
        homes = [i for i, g in enumerate(groups) if anchor(g[0].end) == anchor(end)]
        if not homes:
            stats["year_off_anchor"] += 1
            continue
        home = min(homes, key=lambda i: (_distance(end, groups[i]), -i))
        groups[home].append(
            _Year(
                end=end,
                start=_best_start(derived[end], end, *YEAR_DAYS),
                label="FY",
                fiscal_year=None,
                own=False,
            )
        )
        groups[home].sort(key=lambda y: y.end)
        every_end.append(end)
        stats["derived_years"] += 1

    # 3. The current year, projected, so the quarters filed before its report have a year.
    groups[-1].append(
        _Year(
            end=_next_year_end(groups[-1]),
            start=None,
            label="FY",
            fiscal_year=None,
            own=False,
            observed=False,
        )
    )
    years = _finish(groups, stats, labelled=True)
    stats["years"] = sum(1 for y in years if y.label == "FY" and y.observed)
    stats["transitions"] = sum(1 for y in years if y.label == "T")

    # 4. Quarters: each 10-Q in the year that contains it, numbered by its position in the year.
    #
    # NOT a recast year: a successor's recast years overlap the predecessor's own quarters, and
    # Era's 2018 10-Qs would be relabelled into Bristow's April-March year. And NOT a transition: a
    # 10-Q inside a 10-KT's months (Greif's three in 2025) has no quarter of any year to be.
    #
    # BY POSITION, NOT BY THE FILING'S `fp`, which can be wrong: Greif's 10-Q ending 2020-07-31 is
    # tagged Q2 and is its third quarter. Position holds for Costco's 12/12/12/16 weeks (84, 168 and
    # 252 days in) and a 16-week first quarter alike. A disagreement is counted, never acted on.
    periods = [_year_period(y) for y in years]
    hosts = [y for y in years if y.label == "FY" and not y.recast]
    interim = sorted((r for r in reports if r.role == "interim"), key=lambda r: r.report_date)
    first_interim = interim[0].report_date if interim else None
    held: dict[int, dict[int, tuple[date, date | None]]] = {}
    for report in interim:
        candidates = [
            y for y in hosts if y.start is not None and y.start <= report.report_date <= y.end
        ]
        if not candidates:
            stats["interim_off_calendar"] += 1
            continue
        host = max(candidates, key=lambda y: (y.regime, y.end))
        assert host.start is not None
        number = _nth(host.start, report.report_date, 91.3)
        filed = labels.get(report.accession)
        if filed is not None and filed.fiscal_period in ("Q1", "Q2", "Q3", "Q4"):
            stats["quarter_label_disagrees"] += filed.fiscal_period != f"Q{number}"
        quarters = held.setdefault(id(host), {})
        if number not in (1, 2, 3) or number in quarters:
            stats["interim_off_calendar"] += 1
            continue
        quarters[number] = (
            report.report_date,
            _best_start(by_accession.get(report.accession, ()), report.report_date, *QUARTER_DAYS),
        )
        stats["quarters_observed"] += 1
    for year in years:
        if year.label != "FY":
            continue
        assert year.start is not None and year.fiscal_year is not None
        quarters = held.get(id(year), {})
        if quarters:
            if 3 in quarters and year.observed:
                quarters[4] = (year.end, quarters[3][0] + timedelta(days=1))
            previous: date | None = None
            for number in sorted(quarters):
                end, start = quarters[number]
                if start is None:
                    start = (
                        previous + timedelta(days=1)
                        if previous is not None and number - 1 in quarters
                        else _add_months(year.start, 3 * (number - 1))
                    )
                periods.append(
                    Period(year.regime, year.fiscal_year, f"Q{number}", start, end, True, True)  # type: ignore[arg-type]
                )
                previous = end
        elif not year.recast and (first_interim is None or year.end < first_interim):
            # A FILER WITH NO 10-Q (20-F, 40-F) gets computed quarters and halves, so a provider's
            # quarterly or half-yearly rows can still be labelled (BHP, VOD, TSM).
            computed = _computed(year)
            periods.extend(computed)
            stats["quarters_computed"] += sum(1 for p in computed if p.fiscal_period[0] == "Q")

    regimes = [_regime(n, g, groups, observed_kind=None) for n, g in enumerate(groups, start=1)]
    return Calendar(regimes, _canonical(periods, stats), stats)


def _nth(start: date, end: date, length: float) -> int:
    """Which quarter (`length` 91.3) or half (182.6) of a year starting `start` ends at `end`."""
    return max(1, round(((end - start).days + 1) / length))


def _computed(year: _Year) -> list[Period]:
    """Quarters and halves at 3, 6 and 9 months into the year: never observed."""
    assert year.start is not None and year.fiscal_year is not None
    out: list[Period] = []
    previous = year.start
    for number in (1, 2, 3, 4):
        end = year.end if number == 4 else _add_months(year.start, 3 * number) - timedelta(days=1)
        out.append(
            Period(year.regime, year.fiscal_year, f"Q{number}", previous, end, True, False)  # type: ignore[arg-type]
        )
        previous = end + timedelta(days=1)
    middle = _add_months(year.start, 6) - timedelta(days=1)
    out.append(Period(year.regime, year.fiscal_year, "H1", year.start, middle, True, False))
    out.append(
        Period(
            year.regime, year.fiscal_year, "H2", middle + timedelta(days=1), year.end, True, False
        )
    )
    return out


def _overlap(a: Period, b: Period) -> int:
    return max(0, (min(a.period_end, b.period_end) - max(a.period_start, b.period_start)).days + 1)


def _canonical(periods: list[Period], stats: Counter[str]) -> list[Period]:
    """One row per (regime, year, period), then the newest calendar wins (decision 11)."""
    # The table's key is (regime, fiscal year, fiscal period): of two rows sharing it, the later end
    # is kept and the other is not stored at all.
    by_key: dict[tuple[int, int, str], Period] = {}
    for period in periods:
        key = (period.regime, period.fiscal_year, period.fiscal_period)
        held = by_key.get(key)
        if held is not None:
            stats["duplicate_label"] += 1
            if held.period_end >= period.period_end:
                continue
        by_key[key] = period
    kept = sorted(by_key.values(), key=lambda p: (p.regime, p.period_end, p.fiscal_period))

    # A year overlapping a newer regime's year by more than half the shorter is superseded.
    years = [p for p in kept if p.fiscal_period in ("FY", "T")]
    superseded: set[tuple[int, int, str]] = set()
    for older in years:
        for newer in years:
            if newer.regime <= older.regime:
                continue
            if _overlap(older, newer) * 2 > min(_length(older), _length(newer)):
                superseded.add((older.regime, older.fiscal_year, older.fiscal_period))
    # Then one canonical year per label, the newest regime's: a year that loses either way takes its
    # quarters and halves with it, which belong to that year's calendar and no other.
    year_winners = _winners(
        p for p in years if (p.regime, p.fiscal_year, p.fiscal_period) not in superseded
    )
    canonical_years = {
        (p.regime, p.fiscal_year) for p in year_winners.values() if p.fiscal_period == "FY"
    }
    part_winners = _winners(
        p
        for p in kept
        if p.fiscal_period not in ("FY", "T") and (p.regime, p.fiscal_year) in canonical_years
    )
    out: list[Period] = []
    for period in kept:
        label = (period.fiscal_year, period.fiscal_period)
        canonical = (year_winners.get(label) or part_winners.get(label)) is period
        stats["superseded"] += not canonical
        out.append(replace(period, is_canonical=canonical))
    return out


def _winners(periods: Iterable[Period]) -> dict[tuple[int, str], Period]:
    """Per label, the period in the newest regime."""
    winners: dict[tuple[int, str], Period] = {}
    for period in periods:
        label = (period.fiscal_year, period.fiscal_period)
        if label not in winners or period.regime > winners[label].regime:
            winners[label] = period
    return winners


def _length(period: Period) -> int:
    return (period.period_end - period.period_start).days + 1


def _regime(
    number: int,
    group: Sequence[_Year],
    groups: Sequence[Sequence[_Year]],
    *,
    observed_kind: ChangeKind | None,
) -> Regime:
    observed = [y for y in group if y.observed]
    ends = [y.end for y in observed if y.label == "FY"] or [y.end for y in observed]
    weeks = _is_52_53(group)
    if observed_kind is not None:
        kind = observed_kind
    elif number == 1:
        kind = "initial"
    else:
        earlier = [y for g in groups[: number - 1] for y in g if y.observed]
        overlaps = any(
            y.start is not None and e.start is not None and y.start <= e.end and e.start <= y.end
            for y in observed
            for e in earlier
        )
        kind = "successor" if overlaps else "year_end_change"
    return Regime(
        regime=number,
        year_end_month=anchor(group[-1].end),
        year_end_day=None if weeks else Counter(e.day for e in ends).most_common(1)[0][0],
        is_52_53_week=weeks,
        week_end_isodow=ends[-1].isoweekday() if weeks else None,
        first_year_end=min(y.end for y in observed),
        last_year_end=None if number == len(groups) else max(y.end for y in observed),
        change_kind=kind,
    )


# --- without SEC ---------------------------------------------------------------------------------


def calendar_from_periods(
    annual_ends: Collection[date],
    quarter_ends: Collection[date] = (),
    half_ends: Collection[date] = (),
) -> Calendar:
    """A calendar observed from a provider's stored period ends, for a company SEC does not know.

    Each year is labelled by the calendar year of its end (`calendar_year_label`): no filing names
    it. An interim end is numbered by the months elapsed since its year began, so a missing quarter
    does not shift the ones after it, and the year after the last annual end is projected so the
    newest interim rows have a year.
    """
    stats: Counter[str] = Counter()
    ends = sorted(set(annual_ends))
    if not ends:
        return Calendar([], [], stats)
    groups = _regimes(
        [_Year(end=e, start=None, label="FY", fiscal_year=None, own=True) for e in ends]
    )
    groups[-1].append(
        _Year(
            end=_next_year_end(groups[-1]),
            start=None,
            label="FY",
            fiscal_year=None,
            own=False,
            observed=False,
        )
    )
    years = _finish(groups, stats, labelled=False)
    for year in years:
        if not _is_year(year.start, year.end):
            year.label = "T"
    periods = [_year_period(y) for y in years]
    for prefix, interim, length, count in (
        ("Q", sorted(set(quarter_ends)), 91.3, 4),
        ("H", sorted(set(half_ends)), 182.6, 2),
    ):
        for end in interim:
            hosts = [
                y
                for y in years
                if y.label == "FY" and y.start is not None and y.start <= end <= y.end
            ]
            if not hosts:
                stats["interim_off_calendar"] += 1
                continue
            host = hosts[-1]
            assert host.start is not None and host.fiscal_year is not None
            number = min(count, _nth(host.start, end, length))
            months = 12 // count
            periods.append(
                Period(
                    host.regime,
                    host.fiscal_year,
                    f"{prefix}{number}",  # type: ignore[arg-type]
                    _add_months(host.start, months * (number - 1)),
                    end,
                    True,
                    True,
                )
            )
    regimes = [
        _regime(n, g, groups, observed_kind="observed") for n, g in enumerate(groups, start=1)
    ]
    return Calendar(regimes, _canonical(periods, stats), stats)


# --- rows ----------------------------------------------------------------------------------------


def rows(
    calendar: Calendar, *, security_ids: Collection[str], source: str, as_of: str
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """`market.fiscal_calendar` and `market.fiscal_period` rows, for every security given.

    One calendar per COMPANY, written under each of its securities: GOOG and GOOGL share Alphabet's
    years, and `label_fiscal_periods` labels each security's statements from its own rows.
    """
    calendars: list[dict[str, object]] = [
        {
            "security_id": sid,
            "regime": r.regime,
            "source_code": source,
            "year_end_month": r.year_end_month,
            "year_end_day": r.year_end_day,
            "is_52_53_week": r.is_52_53_week,
            "week_end_isodow": r.week_end_isodow,
            "first_year_end": r.first_year_end.isoformat(),
            "last_year_end": r.last_year_end.isoformat() if r.last_year_end else None,
            "change_kind": r.change_kind,
            "as_of": as_of,
        }
        for sid in security_ids
        for r in calendar.regimes
    ]
    periods: list[dict[str, object]] = [
        {
            "security_id": sid,
            "regime": p.regime,
            "fiscal_year": p.fiscal_year,
            "fiscal_period": p.fiscal_period,
            "period_start": p.period_start.isoformat(),
            "period_end": p.period_end.isoformat(),
            "is_canonical": p.is_canonical,
            "is_observed": p.is_observed,
            "is_recast": p.is_recast,
            "source_code": source,
            "as_of": as_of,
        }
        for sid in security_ids
        for p in calendar.periods
    ]
    return calendars, periods
