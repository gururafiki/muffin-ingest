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

    # (metric, period type, end) -> (priority, filed, tie-break) and the fact
    best: dict[tuple[str, PeriodType, date], tuple[tuple[int, date, str], Fact]] = {}
    for fact in facts:
        spec = rank.get((fact.taxonomy, fact.concept))
        if spec is None:
            continue
        if spec.metric_code not in lines:
            stats["unmapped_metric"] += 1
            continue
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

    grouped: dict[tuple[str, PeriodType, date], dict[str, Fact]] = {}
    for (metric, period_type, end), (_, fact) in best.items():
        grouped.setdefault((lines[metric], period_type, end), {})[metric] = fact

    rows: list[dict[str, Any]] = []
    for (statement, period_type, end), carried in sorted(grouped.items()):
        codes = Counter(f.currency for f in carried.values() if f.currency)
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
            data[metric] = carried[metric].value
        rows.append(
            {
                "statement": statement,
                "period_ending": end.isoformat(),
                "period_type": period_type,
                "currency": currency,
                "data": json.dumps(data, sort_keys=True),
                "source_code": SOURCE,
                "as_of": as_of,
            }
        )
    stats["statements"] = len(rows)
    out.rows = [{"security_id": sid, **row} for sid in security_ids for row in rows]
    return out
