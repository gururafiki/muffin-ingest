"""Stage 2 of SEC's company facts: a filer's statement rows, one per statement and fiscal period.

DECIDED 2026-10-24 (Phase 5 decisions 5 and 9). Company facts feed SEC's statement rows, quarterly
and annual, and our own reader does it: edgartools' standard map has no `ifrs-full` concept, and its
facts parser returns `None` on any exception, which would make a failure look like a filer with no
facts. A line is a `market.metric` code, resolved per period through `market.xbrl_concept`, in both
taxonomies. A new line is a `metric` row plus an `xbrl_concept` row.

PORTED FROM THE EDGE (`market-refresh/xbrl.ts::factsFromCompanyFacts`, which wrote `security_metric`
directly), with the same resolution rules:
  * PER PERIOD, NOT PER COMPANY. Caterpillar tags `NetIncomeLoss` for 2009-2011 and another concept
    since, so taking the first concept with any data gives three years of seventeen.
  * Within a period the highest-priority concept wins, then the LATEST filing: a period is restated
    by amendments and repeated by later filings, and the newest is the company's current answer.
  * BOTH TAXONOMIES. AB InBev's company facts have no `us-gaap` node at all, only `ifrs-full`.
  * The unit key IS the reporting currency (`pick_unit_key`).

ONE RULE IS NEW, and it replaces the edge's day bands. A fact is written only for a CANONICAL period
of the filer's fiscal calendar (`facets.fiscal`): a duration whose end is the period's end and whose
start is within `START_DAYS` of its start, or an instant on a period end. The edge classified a
duration as a quarter at 80-100 days, which refuses Costco's 16-week fourth quarter (112 days) and
lets nothing tell a superseded year from the one that replaced it. Matching the calendar refuses a
year-to-date figure for the same reason the bands did (its start is a quarter or two too early), and
the reason a fact is refused is counted: `ytd`, `superseded`, `transition`, `off_calendar`,
`instant_off_calendar`, `half_of_a_quarterly_filer`. Measured 2026-10-27 over 19 filers with their
complete submissions history: 30,216 durations and 11,548 instants written, 10,654 year-to-date
facts refused, 666 durations off the calendar (Google Inc's 2015 quarters, filed under another
CIK; NVO's monthly 6-K facts; ESCO's off-cycle comparatives).

AN INSTANT AT A FISCAL-YEAR END IS WRITTEN TWICE, as `annual` and as `quarter` (the fourth quarter's
balance sheet), where the calendar has a fourth quarter ending there. The ratio views take a stock
metric from its latest `quarter` row, so an annual-only filer (20-F, 40-F) would otherwise have no
balance sheet for them at all; its calendar computes the quarters, so it has one.

A QUARTER STATED ONLY YEAR TO DATE IS COMPUTED, and its row says so (decided by the user
2026-11-04). A 10-Q states the cash-flow statement year to date, so SEC's quarterly cash flow was
mostly Q1: measured on production 2026-11-02, `cash` rows Q1 37,780, Q2 17,247, Q3 16,420, and the
Q2/Q3 rows that exist average 1.4 lines (operating cash flow in 956 of 17,247). So Q2 = H1 - Q1
and Q3 = 9M - H1, for a quarterly filer, a money line (never a per-share figure or a share count,
which do not add), and only where the filer states no three-month figure for that line. Both terms
come from ONE concept, each its latest filing, so a concept change between filings cannot be
subtracted from another. The rows carry `SOURCE_YTD`, ranked below a provider's stated figure as
`derived-q4` is, so a stated line always wins at the metric level. Where a stated figure does
exist the computed one is compared with it and counted (`ytd_agrees`, `ytd_disagrees`): the guard.
The fourth quarter is not computed here; `derive_ttm` already derives it from the year
(`derived-q4`).

THE LABEL IS NOT WRITTEN HERE. `market.label_fiscal_periods` is the one labeller (decision 7): two
lanes writing labels would be two rules free to disagree.

NO NETWORK. `providers.sec_edgar.fetch` is stage 1's.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Literal

from muffin_ingest.facets.fiscal import Calendar, FactsUnreadable, Period
from muffin_ingest.providers.documents import Document

#: What stage 1 adds to a stored body: how SEC answered. A 404 is an absence, filed beside the
#: answer, and the body cannot say which it is.
CONTEXT_COLUMNS = frozenset({"http_status"})

#: The source these rows are written under. The edge wrote the same code into `security_metric`.
SOURCE = "sec-xbrl"

#: A quarter computed from two year-to-date facts (the module docstring). Its own source, so the
#: row says it was computed and a stated figure outranks it.
SOURCE_YTD = "sec-xbrl-ytd"

#: How far a computed quarter may sit from a stated one before it counts as a disagreement: a
#: thousandth, or the ROUNDING UNIT the figures share, whichever is larger. A filer reporting in
#: thousands rounds each figure separately, so a difference of two can miss the third by one unit:
#: Bristow's 2021 nine months less its six is -57,000 against a stated -56,000 (captured facts).
YTD_TOLERANCE = 0.001

#: How far a duration's start may sit from its period's. 52/53-week calendars move a start by up to
#: a week between the year a period is first reported and a later filing's comparative.
START_DAYS = 7

PeriodType = Literal["annual", "quarter", "half"]

#: EVERY CONCEPT THE READER READS: `market.xbrl_concept` (shared with the edge's readers) and
#: `market.xbrl_fact_concept`, which only this reader reads (decision 14: xbrl_concept's key carries
#: no taxonomy, so us-gaap `Assets` cannot sit beside ifrs-full `Assets` for one metric there).
CONCEPTS_SQL = """
select metric_code, taxonomy, concept, priority, unit from market.xbrl_concept
union all
select metric_code, taxonomy, concept, priority, unit from market.xbrl_fact_concept
order by 1, 2, 3
"""

#: WHICH STATEMENT EACH METRIC IS A LINE OF: `market.metric_source_field` for this source, seeded
#: by muffin-deployment#475 from `metric.category` (share counts beside EPS). The field IS the
#: metric code, so the derivation reads these rows back with the same table.
LINES_SQL = """
select metric_code, statement from market.metric_source_field where source_code = %s
"""

_PERIOD_TYPE: dict[str, PeriodType] = {
    "FY": "annual",
    "Q1": "quarter",
    "Q2": "quarter",
    "Q3": "quarter",
    "Q4": "quarter",
    "H1": "half",
    "H2": "half",
}

_CURRENCY = re.compile(r"^[A-Z]{3}$")
_PER_SHARE = re.compile(r"^[A-Z]{3}/shares$")


@dataclass(frozen=True)
class Concept:
    """A `market.xbrl_concept` row: which concept states a metric, and how much to trust it."""

    metric_code: str
    taxonomy: str
    concept: str
    priority: int
    #: A KIND, not a literal: `USD` is "whichever currency this filer reports in", `USD/shares` a
    #: per-share figure in it, `shares` a count.
    unit: str


@dataclass(frozen=True)
class Fact:
    taxonomy: str
    concept: str
    #: The unit key the value sits under (`USD`, `DKK`, `USD/shares`, `shares`).
    unit: str
    start: date | None
    end: date
    value: float
    accession: str | None
    fiscal_year: int | None
    fiscal_period: str | None
    form: str | None
    filed: date | None
    #: SEC's own calendar frame (`CY2023Q3I`), kept for parity checks; nothing here decides on it.
    frame: str | None

    @property
    def currency(self) -> str | None:
        """The reporting currency the unit key names, or None for a count."""
        code = self.unit.split("/", 1)[0]
        return code if _CURRENCY.match(code) else None


@dataclass
class Read:
    facts: list[Fact]
    stats: Counter[str] = field(default_factory=Counter)


@dataclass
class Normalised:
    """`market.security_statement` rows, and what was decided on the way."""

    rows: list[dict[str, Any]]
    stats: Counter[str] = field(default_factory=Counter)


def concepts(conn: Any) -> list[Concept]:
    """Every concept read, refusing one that two metrics name.

    ONE METRIC PER CONCEPT is what `statement_rows` assumes when it ranks facts by concept: a
    concept two metrics named would count for whichever was read last. None does (62 rows measured
    2026-10-27), and a row that would must fail the run rather than be read for one of them.
    """
    with conn.cursor() as cur:
        cur.execute(CONCEPTS_SQL)
        found = [
            Concept(str(m), str(t), str(c), int(p), str(u)) for m, t, c, p, u in cur.fetchall()
        ]
    named: dict[tuple[str, str], str] = {}
    for spec in found:
        held = named.setdefault((spec.taxonomy, spec.concept), spec.metric_code)
        if held != spec.metric_code:
            raise ValueError(
                f"{spec.taxonomy}:{spec.concept} names both {held} and {spec.metric_code}"
            )
    return found


def lines(conn: Any) -> dict[str, str]:
    """Each metric this source writes, with the statement it is a line of."""
    with conn.cursor() as cur:
        cur.execute(LINES_SQL, (SOURCE,))
        return {str(metric): str(statement) for metric, statement in cur.fetchall()}


def raw_rows(document: Document, *, http_status: int, run_id: str) -> list[dict[str, Any]]:
    """The stored row: the body whole, and how SEC answered."""
    return [{**document.as_row(run_id), "http_status": http_status}]


def pick_unit_key(units: Mapping[str, Any], kind: str) -> str | None:
    """Which unit key inside a concept holds the filer's own figures.

    A filer may publish several: TSMC reports in TWD and USD, the second being a convenience
    translation. The reporting currency carries the full history, so the key with the most data
    points wins; choosing USD by habit would relabel Novo Nordisk's kroner as dollars. Ties break on
    the key, so the answer does not depend on the order SEC sends them in.
    """
    if kind == "shares":
        return "shares" if "shares" in units else None
    pattern = _PER_SHARE if "/shares" in kind else _CURRENCY
    best: str | None = None
    best_len = -1
    for key, points in units.items():
        if not pattern.match(key):
            continue
        n = len(points) if isinstance(points, list) else 0
        if n > best_len or (n == best_len and best is not None and key < best):
            best, best_len = key, n
    return best


def _date(value: Any, what: str) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise FactsUnreadable(f"{what} is not a date: {value!r}") from exc


def read(body: bytes, concepts: Iterable[Concept]) -> Read:
    """The facts of every concept named, each under the unit key `pick_unit_key` chooses.

    A body that is not a company facts document raises `FactsUnreadable`: a parse failure must never
    read as a filer with no facts. A fact whose value is not a finite number is skipped and counted.
    """
    try:
        payload = json.loads(body)
        taxonomies = payload["facts"]
    except (ValueError, KeyError, TypeError) as exc:
        raise FactsUnreadable(f"not a company facts document: {exc}") from exc
    if not isinstance(taxonomies, dict):
        raise FactsUnreadable("`facts` is not an object")
    out = Read(facts=[])
    seen: set[tuple[str, str]] = set()
    for spec in concepts:
        if (spec.taxonomy, spec.concept) in seen:
            continue
        seen.add((spec.taxonomy, spec.concept))
        node = (taxonomies.get(spec.taxonomy) or {}).get(spec.concept)
        if not isinstance(node, dict) or not isinstance(node.get("units"), dict):
            continue
        unit = pick_unit_key(node["units"], spec.unit)
        if unit is None:
            out.stats["no_unit"] += 1
            continue
        out.stats["concepts"] += 1
        for point in node["units"][unit]:
            value = point.get("val")
            if (
                isinstance(value, bool)
                or not isinstance(value, int | float)
                or not math.isfinite(value)
            ):
                out.stats["not_a_number"] += 1
                continue
            fy = point.get("fy")
            out.facts.append(
                Fact(
                    taxonomy=spec.taxonomy,
                    concept=spec.concept,
                    unit=unit,
                    start=_date(point["start"], "start") if point.get("start") else None,
                    end=_date(point.get("end"), "end"),
                    value=float(value),
                    accession=point.get("accn"),
                    fiscal_year=int(fy) if fy is not None else None,
                    fiscal_period=point.get("fp"),
                    form=point.get("form"),
                    filed=_date(point["filed"], "filed") if point.get("filed") else None,
                    frame=point.get("frame"),
                )
            )
    out.stats["facts"] = len(out.facts)
    return out


def is_quarterly(calendar: Calendar) -> bool:
    """Whether the filer reports quarterly: any 10-Q, i.e. an OBSERVED first-to-third quarter.

    A filer that does not (20-F, 40-F) gets half-years; one that does gets none. The calendar
    computes H1/H2 for every year before a filer's first quarterly report, so Yahoo's half rows can
    be labelled, and a quarterly filer's predecessor years fall there: measured 2026-10-27,
    Bristow's 2012 six months from Era's 10-Q would otherwise be a quarterly company's one half row.
    """
    return any(p.is_observed and p.fiscal_period in ("Q1", "Q2", "Q3") for p in calendar.periods)


def _targets(
    fact: Fact, by_end: Mapping[date, Sequence[Period]], *, quarterly: bool
) -> tuple[list[PeriodType], str]:
    """The period types a fact is written as, or the reason it is not written."""
    periods = by_end.get(fact.end, ())
    canonical = [p for p in periods if p.is_canonical]
    if fact.start is None:
        labels = {p.fiscal_period for p in canonical}
        if "FY" in labels:
            # THE FOURTH QUARTER'S BALANCE SHEET ONLY WHERE A FOURTH QUARTER ENDS: a successor's
            # older years (Bristow's March years, stated under Era's CIK) host no quarters, and a
            # quarter row there could never be labelled (measured in Phase 5 step 5).
            return (["annual", "quarter"] if "Q4" in labels else ["annual"]), "instant"
        if quarterly and labels & {"Q1", "Q2", "Q3"}:
            return ["quarter"], "instant"
        if not quarterly and "H1" in labels:
            return ["half"], "instant"
        if any(p.fiscal_period == "T" for p in periods):
            return [], "transition"
        return [], "instant_off_calendar"
    window = timedelta(days=START_DAYS)
    near = [p for p in periods if abs(p.period_start - fact.start) <= window]
    matched = {
        _PERIOD_TYPE[p.fiscal_period]
        for p in near
        if p.is_canonical and p.fiscal_period in _PERIOD_TYPE
    }
    if quarterly and "half" in matched:
        matched.discard("half")
        if not matched:
            return [], "half_of_a_quarterly_filer"
    if matched:
        return sorted(matched), "duration"
    if any(p.fiscal_period == "T" for p in near):
        return [], "transition"
    if near:
        return [], "superseded"
    if canonical:
        return [], "ytd"
    return [], "off_calendar"


def statement_rows(
    facts: Sequence[Fact],
    calendar: Calendar,
    concepts: Iterable[Concept],
    lines: Mapping[str, str],
    *,
    security_ids: Sequence[str],
    known_currencies: Collection[str],
    as_of: str,
) -> Normalised:
    """Statement rows for every security the CIK maps to.

    `lines` maps a metric code to its statement (`lines()`, from `metric_source_field`). A
    row's data holds `period_ending` and one key per metric code with a value; its currency is the
    most common one its lines are in (`mixed_currency` counted), withheld when `known_currencies`
    lacks it so a row can never carry a code the metrics cannot reference.
    """
    out = Normalised(rows=[])
    stats = out.stats
    rank = {(c.taxonomy, c.concept): c for c in concepts}
    by_end: dict[date, list[Period]] = {}
    for period in calendar.periods:
        by_end.setdefault(period.period_end, []).append(period)
    quarterly = is_quarterly(calendar)
    year_starts = _year_starts(calendar) if quarterly else {}

    # (metric, period type, end) -> (priority, filed, tie-break) and the fact
    best: dict[tuple[str, PeriodType, date], tuple[tuple[int, date, str], Fact]] = {}
    # (metric, taxonomy, concept, end) -> every year-to-date fact ending there, each filing's
    cumulative: dict[tuple[str, str, str, date], list[Fact]] = {}
    for fact in facts:
        spec = rank.get((fact.taxonomy, fact.concept))
        if spec is None:
            continue
        if spec.metric_code not in lines:
            stats["unmapped_metric"] += 1
            continue
        if _is_year_to_date(fact, year_starts):
            cumulative.setdefault(
                (spec.metric_code, fact.taxonomy, fact.concept, fact.end), []
            ).append(fact)
        types, why = _targets(fact, by_end, quarterly=quarterly)
        stats[why] += 1
        if not types:
            continue
        score = (spec.priority, fact.filed or date.min, f"{fact.taxonomy}:{fact.concept}")
        for period_type in types:
            key = (spec.metric_code, period_type, fact.end)
            held = best.get(key)
            if held is not None and held[0][:2] == score[:2] and held[1].value != fact.value:
                stats["tie_disagrees"] += 1
            # Higher priority, then the later filing; then the concept name, so a tie in one
            # filing between two concepts does not depend on the order SEC lists them in.
            if (
                held is None
                or score[:2] > held[0][:2]
                or (score[:2] == held[0][:2] and score[2] < held[0][2])
            ):
                best[key] = (score, fact)

    grouped: dict[tuple[str, PeriodType, date], dict[str, tuple[float, str | None]]] = {}
    for (metric, period_type, end), (_, fact) in best.items():
        grouped.setdefault((lines[metric], period_type, end), {})[metric] = (
            fact.value,
            fact.currency,
        )
    computed: dict[tuple[str, PeriodType, date], dict[str, tuple[float, str | None]]] = {}
    for (metric, end), (value, currency) in _computed_quarters(
        cumulative, year_starts, rank, best, stats
    ).items():
        computed.setdefault((lines[metric], "quarter", end), {})[metric] = (value, currency)

    rows = _rows(grouped, SOURCE, known_currencies, as_of, stats)
    rows += _rows(computed, SOURCE_YTD, known_currencies, as_of, stats)
    stats["statements"] = len(rows)
    out.rows = [{"security_id": sid, **row} for sid in security_ids for row in rows]
    return out


def _rows(
    grouped: Mapping[tuple[str, PeriodType, date], Mapping[str, tuple[float, str | None]]],
    source: str,
    known_currencies: Collection[str],
    as_of: str,
    stats: Counter[str],
) -> list[dict[str, Any]]:
    """One row per statement and period, labelled with its lines' most common currency."""
    rows: list[dict[str, Any]] = []
    for (statement, period_type, end), carried in sorted(grouped.items()):
        codes = Counter(code for _, code in carried.values() if code)
        currency: str | None = None
        if codes:
            if len(codes) > 1:
                stats["mixed_currency"] += 1
            chosen = sorted(codes.items(), key=lambda item: (-item[1], item[0]))[0][0]
            if chosen in known_currencies:
                currency = chosen
            else:
                stats["currency_withheld"] += 1
        data: dict[str, Any] = {"period_ending": end.isoformat()}
        for metric in sorted(carried):
            data[metric] = carried[metric][0]
        rows.append(
            {
                "statement": statement,
                "period_ending": end.isoformat(),
                "period_type": period_type,
                "currency": currency,
                "data": json.dumps(data, sort_keys=True),
                "source_code": source,
                "as_of": as_of,
            }
        )
    return rows


#: A canonical quarter's end -> (its year's start, the quarter before it ends on, or None for Q1).
YearStarts = Mapping[date, Sequence[tuple[date, date | None]]]


def _year_starts(calendar: Calendar) -> dict[date, list[tuple[date, date | None]]]:
    """For every canonical Q1-Q3 end: where its fiscal year starts, and the previous quarter's end.

    The year starts where its canonical Q1 starts. A year with no canonical Q1 has no year-to-date
    figure that can be split, so its quarters are absent here.
    """
    quarters: dict[tuple[int, int], dict[str, Period]] = {}
    for p in calendar.periods:
        if p.is_canonical and p.fiscal_period in ("Q1", "Q2", "Q3"):
            quarters.setdefault((p.regime, p.fiscal_year), {})[p.fiscal_period] = p
    out: dict[date, list[tuple[date, date | None]]] = {}
    for year in quarters.values():
        q1 = year.get("Q1")
        if q1 is None:
            continue
        previous: date | None = None
        for label in ("Q1", "Q2", "Q3"):
            quarter = year.get(label)
            if quarter is None:
                break
            out.setdefault(quarter.period_end, []).append((q1.period_start, previous))
            previous = quarter.period_end
    return out


def _is_year_to_date(fact: Fact, year_starts: YearStarts) -> bool:
    """A money duration from its fiscal year's start to a canonical Q1-Q3 end.

    Q1's own figure is one: three months, and the year to date. Per-share figures and share counts
    are not, because they do not add.
    """
    if fact.start is None or fact.currency is None or "/" in fact.unit:
        return False
    window = timedelta(days=START_DAYS)
    return any(abs(fact.start - start) <= window for start, _ in year_starts.get(fact.end, ()))


def _vintage(fact: Fact) -> tuple[date, str]:
    """When a figure was filed; within a day the accession breaks the tie, not the input order."""
    return (fact.filed or date.min, fact.accession or "")


def _rounding_unit(*values: float) -> float:
    """The largest power of ten up to a million that divides every value: what a filer rounds to."""
    for k in range(6, 0, -1):
        unit = 10.0**k
        if all(v == int(v) and int(v) % int(unit) == 0 for v in values):
            return unit
    return 1.0


def _computed_quarters(
    cumulative: Mapping[tuple[str, str, str, date], Sequence[Fact]],
    year_starts: YearStarts,
    rank: Mapping[tuple[str, str], Concept],
    best: Mapping[tuple[str, PeriodType, date], tuple[tuple[int, date, str], Fact]],
    stats: Counter[str],
) -> dict[tuple[str, date], tuple[float, str | None]]:
    """Q2 = H1 - Q1 and Q3 = 9M - H1, where the filer states no three-month figure for the line.

    The minuend and the subtrahend come from one concept: the highest-priority concept holding
    both, then the later filing of the minuend, then the concept name.

    AND FROM ONE VINTAGE. The minuend is the latest filing's figure; the subtrahend is the figure as
    it stood at that filing, the latest filed on or before it. Taking each term's latest filing
    pairs a restated term with one never restated: measured over 250 production filers, that put a
    2021 second quarter's net income at +7.0m against a restated -0.5m, Q1 from a 2023
    restatement less a half year from the original 2022 10-Q.
    """
    by_metric_end: dict[tuple[str, date], list[tuple[str, str]]] = {}
    for metric, taxonomy, concept, end in cumulative:
        by_metric_end.setdefault((metric, end), []).append((taxonomy, concept))
    out: dict[tuple[str, date], tuple[float, str | None]] = {}
    for (metric, end), held in sorted(by_metric_end.items()):
        previous_ends = {prev for _, prev in year_starts.get(end, ()) if prev is not None}
        candidates: list[tuple[tuple[int, date, str], float, str | None]] = []
        for prev in previous_ends:
            for taxonomy, concept in held:
                later = max(cumulative[(metric, taxonomy, concept, end)], key=_vintage)
                earlier_ones = [
                    f
                    for f in cumulative.get((metric, taxonomy, concept, prev), ())
                    if _vintage(f) <= _vintage(later)
                ]
                if not earlier_ones:
                    continue
                earlier = max(earlier_ones, key=_vintage)
                score = (rank[(taxonomy, concept)].priority, later.filed or date.min,
                         f"{taxonomy}:{concept}")  # fmt: skip
                candidates.append((score, later.value - earlier.value, later.currency))
        if not candidates:
            continue
        # Highest priority, then the later filing; then the SMALLER name, as the stated rule does.
        top = max(candidates, key=lambda c: c[0][:2])
        tied = [c for c in candidates if c[0][:2] == top[0][:2]]
        _, value, currency = min(tied, key=lambda c: c[0][2])
        stated = best.get((metric, "quarter", end))
        if stated is not None:
            reported = stated[1].value
            unit = _rounding_unit(reported, value)
            if abs(value - reported) <= max(unit, abs(reported) * YTD_TOLERANCE):
                stats["ytd_agrees"] += 1
            else:
                stats["ytd_disagrees"] += 1
            continue
        stats["ytd_computed"] += 1
        out[(metric, end)] = (value, currency)
    return out
