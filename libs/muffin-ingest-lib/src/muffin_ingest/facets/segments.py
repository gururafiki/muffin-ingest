"""Revenue and profit per business line, from a filing's own XBRL instance.

PORTED AS-IS FROM THE EDGE (`market-refresh/segments.ts`, decided 2026-11-09, Phase 6 decision 2).
Same rules, same output, so the lanes that replace the edge's four segment resources can be checked
row for row against what `security_segment` already holds. Widening any rule comes after the
switch, measured.

WHY A FILING'S INSTANCE. SEC's company facts API strips dimensions: measured 2026-08-28,
`companyconcept` for AAPL revenue returns one value per period and no iPhone. Every filing
publishes its XBRL instance as a separate file (AAPL's 10-Q 0.74 MB, Diageo's 20-F 10.92 MB), and
its contexts carry the dimensions the API drops. DART and NSE publish the same kind of instance;
CNINFO's PDFs are read into the same `SegmentFact` rows by their own reader.

THE AXIS NAME IS DATA. Diageo's split is `ifrs-full:ProductsAndServicesAxis` where Apple's is
`srt:ProductOrServiceAxis`, so the allowlist is `market.segment_axis` (`AxisSpec`) and the concepts
are `market.xbrl_concept` (`ConceptSpec`). Adding a taxonomy is rows.

FAITHFUL TO JAVASCRIPT WHERE IT DECIDES AN ANSWER. Values are floats added in the edge's order, and
never with `sum()`, which compensates since Python 3.12 and can differ from JS in the last place.
The tag regexes are the edge's, ASCII-only, as `\\w` is in JS. Maps iterate in insertion order, as
JS `Map` does. Two axes of equal priority are ordered as ICU's root collation orders ASCII names
(`localeCompare`), approximated by `_collation_key`.

NO NETWORK. Stage 1 fetches; this reads text it is given. The readers at the end are the control
tables every regulator's lane parses with, and the rows every one of them writes.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

QUALIFIER = "qualifier"
SUBTOTAL = "subtotal"
RESIDUAL = "residual"


@dataclass(frozen=True, slots=True)
class AxisSpec:
    """One axis worth keeping, from `market.segment_axis`.

    `kind` is `product`, `business` or `geography` for an axis that segments the company, and
    `qualifier` for one that narrows a fact without sub-dividing it
    (`srt:ConsolidationItemsAxis = OperatingSegmentsMember`; Alphabet tags every segment figure with
    one). A fact carrying a qualifier beside its segment axis is still a segment fact; a fact
    carrying an unknown second axis is not.

    `required_member` is a member the axis MUST carry for the fact to be kept, or None for any.
    Korean filings tag every fact with `ConsolidatedAndSeparateFinancialStatementsAxis`, and
    `SeparateMember` is parent-company-only accounts (1,538 consolidated against 1,213 separate
    facts on one DART filing): an open qualifier would reconcile one set against the other.
    """

    axis: str
    kind: str
    priority: int
    required_member: str | None = None


@dataclass(frozen=True, slots=True)
class ConceptSpec:
    """Which concept expresses which metric, from `market.xbrl_concept`."""

    metric_code: str
    concept: str
    priority: int


@dataclass(frozen=True, slots=True)
class MemberRole:
    """A member that cannot be a business line, from `market.segment_member_class`.

    The two roles need OPPOSITE treatment. A `subtotal` is dropped outright: a split containing one
    double-counts by construction (Chevron's "aggregation before other operating segments"
    reconciles exactly with `AllOtherSegments` and was served as its business breakdown). A
    `residual` is KEPT, because Chevron's own `depreciation` split needs it to reconcile, but may
    never be the whole of a split (the residual-only rule in `segment_facts_from`).
    """

    member_code: str
    role: str


@dataclass(frozen=True, slots=True)
class SegmentFact:
    """One member's value for one metric and period, with the split it belongs to.

    `parent_axis` / `parent_member` are set for a cell of a cross-tab: Alphabet tags Search,
    YouTube, Network and Subscriptions with the product axis AND `StatementBusinessSegmentsAxis =
    GoogleServices`, and the four sum to Google Services (342,721,000,000), not to the company. A
    reader must never mix the two levels, as it must never mix partitions.

    `reconciled_to` is the figure the split was accepted against: the filing's consolidated value
    for a flat split, the parent member's for a nested one. Without it a guard can only compare
    against a second derivation of the total, and cannot tell a double count from two sources
    disagreeing.

    `partition_id` 1..n numbers the disclosed splits that reconcile; 0 means a subtotal or a member
    that could not be placed, which nothing may aggregate (`assign_partitions`).
    """

    axis: str
    member_code: str
    metric_code: str
    period_type: str
    period_start: str
    period_ending: str
    value: float
    currency: str | None
    parent_axis: str | None
    parent_member: str | None
    reconciled_to: float | None
    partition_id: int


@dataclass(frozen=True, slots=True)
class Context:
    """A `contextRef`'s explicit dimensions (axis -> member, in document order) and period."""

    dims: dict[str, str]
    start: str | None
    end: str | None
    instant: str | None


@dataclass(frozen=True, slots=True)
class RawFact:
    concept: str
    metric_code: str
    ctx_ref: str
    unit_ref: str | None
    value: float


# ── JavaScript semantics this port depends on ──────────────────────────────────────────────────

#: What JS `String.prototype.trim` removes. Python's `str.strip()` keeps U+FEFF and removes
#: U+001C..U+001F, which JS does not.
_JS_WHITESPACE = "".join(
    map(chr, [0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0xA0, 0x1680, *range(0x2000, 0x200B)])
) + "".join(map(chr, [0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF]))


def _trim(s: str) -> str:
    return s.strip(_JS_WHITESPACE)


def _add_up(values: Iterable[float]) -> float:
    """`values.reduce((a, v) => a + v, 0)`: left to right, uncompensated."""
    total = 0.0
    for v in values:
        total += v
    return total


#: ICU's root order for ASCII punctuation, which sorts before digits, which sort before letters.
_PUNCTUATION_ORDER = "_-,;:!?.'\"()[]{}@*/\\&#%`^+<=>|~$"


def _collation_key(s: str) -> tuple[list[tuple[int, int]], list[int]]:
    """`a.localeCompare(b)` for ASCII names: letters case-blind first, lower before upper after."""
    primary: list[tuple[int, int]] = []
    tertiary: list[int] = []
    for ch in s:
        if ch.isascii() and ch.isalpha():
            primary.append((2, ord(ch.lower())))
            tertiary.append(1 if ch.isupper() else 0)
        elif ch.isascii() and ch.isdigit():
            primary.append((1, ord(ch)))
            tertiary.append(0)
        else:
            at = _PUNCTUATION_ORDER.find(ch)
            primary.append((0, at if at >= 0 else len(_PUNCTUATION_ORDER) + ord(ch)))
            tertiary.append(0)
    return primary, tertiary


def text_of(body: bytes) -> str:
    """An instance's bytes as the edge read them (`Response.text()`: UTF-8, BOM dropped)."""
    return body.decode("utf-8-sig", errors="replace")


# ── Reading the instance ───────────────────────────────────────────────────────────────────────

_UNIT = re.compile(r'<[\w-]*:?unit\b[^>]*\bid="([^"]+)"[^>]*>([\s\S]*?)</[\w-]*:?unit>', re.A)
_DIVIDE = re.compile(r"<[\w-]*:?divide\b", re.A)
_MEASURE = re.compile(r"<[\w-]*:?measure\b[^>]*>([\s\S]*?)</[\w-]*:?measure>", re.A)
_ISO_CURRENCY = re.compile(r"iso4217:([A-Z]{3})", re.A)
_CONTEXT = re.compile(
    r'<[\w-]*:?context\b[^>]*\bid="([^"]+)"[^>]*>([\s\S]*?)</[\w-]*:?context>', re.A
)
_EXPLICIT_MEMBER = re.compile(
    r'<[\w-]*:?explicitMember\b[^>]*\bdimension="([^"]+)"[^>]*>([\s\S]*?)</[\w-]*:?explicitMember>',
    re.A,
)
_START = re.compile(r"<[\w-]*:?startDate\b[^>]*>([\s\S]*?)</", re.A)
_END = re.compile(r"<[\w-]*:?endDate\b[^>]*>([\s\S]*?)</", re.A)
_INSTANT = re.compile(r"<[\w-]*:?instant\b[^>]*>([\s\S]*?)</", re.A)
_FACT = re.compile(r"<([\w-]+:[\w.-]+)\b([^>]*)>([^<]*)</\1>", re.A)
_CONTEXT_REF = re.compile(r'\bcontextRef="([^"]+)"', re.A)
_UNIT_REF = re.compile(r'\bunitRef="([^"]+)"', re.A)
_NUMBER = re.compile(r"-?\d+(\.\d+)?", re.A)
_DATE_ONLY = re.compile(r"\d{4}-\d{2}-\d{2}", re.A)


def _local(tag: str) -> str:
    """Strip any namespace prefix: `xbrli:context` and `context` are the same element."""
    return tag.split(":", 1)[1] if ":" in tag else tag


def _first(pattern: re.Pattern[str], text: str) -> str | None:
    m = pattern.search(text)
    return None if m is None else _trim(m.group(1))


def parse_units(xml: str) -> dict[str, str | None]:
    """`unitRef` -> ISO currency, or None for anything that is not money.

    A filer reports in its own currency and the unit is where it arrives (Diageo in USD, Novo
    Nordisk in DKK), so it is never defaulted. A `divide` unit (a ratio) and `shares` are not money,
    so a per-share figure can never be stored as an amount.
    """
    out: dict[str, str | None] = {}
    for m in _UNIT.finditer(xml):
        unit_id, body = m.group(1), m.group(2)
        if _DIVIDE.search(body):
            out[unit_id] = None
            continue
        measure = _first(_MEASURE, body)
        iso = None if measure is None else _ISO_CURRENCY.fullmatch(measure)
        out[unit_id] = None if iso is None else iso.group(1)
    return out


def parse_contexts(xml: str) -> dict[str, Context]:
    """`contextRef` -> its explicit dimensions and period.

    A `typedMember` is ignored: its value is an arbitrary XML fragment, not a member this schema
    could ever map to a concept.
    """
    out: dict[str, Context] = {}
    for m in _CONTEXT.finditer(xml):
        ctx_id, body = m.group(1), m.group(2)
        dims: dict[str, str] = {}
        for d in _EXPLICIT_MEMBER.finditer(body):
            dims[_trim(d.group(1))] = _trim(d.group(2))
        out[ctx_id] = Context(
            dims=dims,
            start=_first(_START, body),
            end=_first(_END, body),
            instant=_first(_INSTANT, body),
        )
    return out


_EPOCH_ORDINAL = date(1970, 1, 1).toordinal()


def _days_since_epoch(value: str) -> float | None:
    """`Date.parse(value) / 86_400_000`. A date is UTC midnight; so is a time with no offset,
    because the edge's container ran in UTC."""
    try:
        if _DATE_ONLY.fullmatch(value):
            return float(date.fromisoformat(value).toordinal() - _EPOCH_ORDINAL)
        moment = datetime.fromisoformat(value)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.timestamp() / 86_400


def period_type_for(start: str, end: str) -> str | None:
    """A duration -> `quarter` (84-98 days) or `annual` (358-372), else None.

    A window, because a 52/53-week calendar makes a quarter 84-98 days and a year 358-372. Anything
    else, a nine-month year-to-date figure above all, is REJECTED rather than rounded: storing nine
    months as a quarter is the 4x error `security_statement`'s key exists to prevent.
    """
    a, b = _days_since_epoch(start), _days_since_epoch(end)
    if a is None or b is None:
        return None
    days = b - a
    if 84 <= days <= 98:
        return "quarter"
    if 358 <= days <= 372:
        return "annual"
    return None


def parse_facts(xml: str, concepts: Iterable[ConceptSpec]) -> list[RawFact]:
    """Every numeric fact whose concept maps to a metric.

    A fact with no value, or a non-numeric one, is DROPPED rather than coerced. That is what handles
    `xsi:nil`, which asserts an absence and would put a fabricated zero beside real revenue if read
    as 0.
    """
    by_concept: dict[str, ConceptSpec] = {}
    for c in concepts:
        prev = by_concept.get(c.concept)
        if prev is None or c.priority > prev.priority:
            by_concept[c.concept] = c
    out: list[RawFact] = []
    for m in _FACT.finditer(xml):
        tag, attrs, text = m.group(1), m.group(2), m.group(3)
        spec = by_concept.get(_local(tag))
        if spec is None:
            continue
        ctx_ref = _CONTEXT_REF.search(attrs)
        if ctx_ref is None:
            continue
        raw = _trim(text)
        if raw == "" or not _NUMBER.fullmatch(raw):
            continue
        unit_ref = _UNIT_REF.search(attrs)
        out.append(
            RawFact(
                concept=_local(tag),
                metric_code=spec.metric_code,
                ctx_ref=ctx_ref.group(1),
                unit_ref=None if unit_ref is None else unit_ref.group(1),
                value=float(raw),
            )
        )
    return out


# ── Which members form one disclosed split ─────────────────────────────────────────────────────

#: Beyond this many members the subset search is abandoned and every member is left unplaced. That
#: is the honest failure: the facts are still stored, only not claimed to reconcile.
MAX_MEMBERS = 18
#: How close a split must come to its total, RELATIVELY. Alphabet's FY2025 segments sum to
#: 402,963,000,000 against a consolidated 402,836,000,000 (a 0.032% reconciling item), while the
#: errors guarded against are integer multiples: a doubled split is 100% out, a subtotal beside its
#: children ~70%. Half a percent separates them by two orders of magnitude.
TOLERANCE_FRACTION = 0.005
#: A floor, so a tiny total is not matched by anything.
TOLERANCE_FLOOR = 1.0


def _tolerance(total: float) -> float:
    """`Math.max(1, Math.abs(total) * 0.005)`, which is NaN for a NaN total, as in JS."""
    if math.isnan(total):
        return math.nan
    return max(TOLERANCE_FLOOR, abs(total) * TOLERANCE_FRACTION)


def assign_partitions(members: Sequence[tuple[str, float]], total: float) -> dict[str, int]:
    """(member code, value) pairs -> partition per member: 1..n for each maximal split that sums to
    `total`, 0 for whatever is left.

    MEASURED, Amazon H1-2025: the product axis carries a seven-member split AND a Product/Service
    split each summing to 323,369, and the business axis a three-member one. Summing an axis doubles
    the company's revenue and summing every member triples it, silently. Apple's product members
    decompose `ProductMember` only, so the split that reconciles mixes extensions with a standard
    member ({iPhone, iPad, Mac, Wearables, Service}): neither "prefer extension members" nor "take
    the deepest" survives it. The filing states its own consolidated figure, so the answer is local:
    a partition is a maximal set of members summing to it, numbered in the order found.
    """
    out = {code: 0 for code, _ in members}
    if not members or not math.isfinite(total):
        return out
    # The common case by far: one clean split and no subtotals.
    tol = _tolerance(total)
    sum_all = _add_up(value for _, value in members)
    if abs(sum_all - total) <= tol:
        for code, _ in members:
            out[code] = 1
        return out
    if len(members) > MAX_MEMBERS:
        return out

    remaining = list(members)
    partition = 0
    while remaining:
        best = _max_subset_summing_to(remaining, total)
        if best is None:
            break
        partition += 1
        for i in best:
            out[remaining[i][0]] = partition
        chosen = set(best)
        remaining = [m for i, m in enumerate(remaining) if i not in chosen]
    return out


_GENERATED = re.compile(r":udf_", re.I)


def _is_generated(member_code: str) -> bool:
    """A member code a filing generated rather than named.

    Korean filers emit `entity00144164:udf_NOTE_20231114182510391Member` beside
    `entity00144164:LpgSalesMemberOfProductsAndServices` for the same line and value: two complete,
    equally valid splits. Both are stored; this only decides which tied split is partition 1, and
    "LPG sales" is worth more than a timestamp. A preference among equals, never a rewrite.
    """
    return _GENERATED.search(member_code) is not None


def _max_subset_summing_to(members: Sequence[tuple[str, float]], target: float) -> list[int] | None:
    """Indices of the largest subset summing to `target`, or None. Bounded by `MAX_MEMBERS`.

    The edge enumerated every mask from 1 upwards and kept the first with the most members, then the
    fewest generated codes. The sums are built per mask in one pass, `sums[m] = sums[m without its
    highest bit] + that member's value`, which adds the members in ascending index order exactly as
    the edge's inner loop did, so a float sum lands on the same bits.
    """
    n = len(members)
    sums = [0.0]
    generated = [0]
    for code, value in members:
        g = 1 if _is_generated(code) else 0
        sums += [s + value for s in sums]
        generated += [x + g for x in generated]
    tol = _tolerance(target)
    qualifying = [mask for mask in range(1, 1 << n) if not abs(sums[mask] - target) > tol]
    if not qualifying:
        return None
    # More members wins; among equals the split a human can read wins; among those, the first found.
    best = min(qualifying, key=lambda mask: (-mask.bit_count(), generated[mask], mask))
    return [i for i in range(n) if best >> i & 1]


# ── The whole pipeline for one filing ──────────────────────────────────────────────────────────


@dataclass(slots=True, eq=False)
class _Candidate:
    """A segment fact before it is placed. Identity matters: a marginal is recognised by object."""

    axis: str
    member_code: str
    parent_axis: str | None
    parent_member: str | None
    concept: str
    metric_code: str
    period_type: str
    period_start: str
    period_ending: str
    value: float
    currency: str | None
    #: The qualifier axes this fact carries, sorted and joined (`qualifier_key`).
    qualifier: str


def _key(metric: str, period_type: str, start: str, end: str) -> str:
    return f"{metric}|{period_type}|{start}|{end}"


def _first_defined(*values: float | None) -> float | None:
    """JS `a ?? b ?? c`: the first that is not None, zero included."""
    for v in values:
        if v is not None:
            return v
    return None


def segment_facts_from(
    xml: str,
    axes: Iterable[AxisSpec],
    concepts: Iterable[ConceptSpec],
    member_roles: Iterable[MemberRole] = (),
) -> list[SegmentFact]:
    """Instance text -> segment facts, partitioned.

    A fact is kept only when, after removing qualifier axes, one or two allowlisted segment axes
    remain and no unknown axis does. That last clause keeps `FairValueByFairValueHierarchyLevelAxis`
    and `StatementEquityComponentsAxis` out: Apple's instance carries 206 dimensioned facts of which
    44 are segmentations.
    """
    axes = list(axes)
    concepts = list(concepts)
    roles = list(member_roles)
    subtotals = {r.member_code for r in roles if r.role == SUBTOTAL}
    residuals = {r.member_code for r in roles if r.role == RESIDUAL}
    units = parse_units(xml)
    contexts = parse_contexts(xml)
    facts = parse_facts(xml, concepts)

    kind_of = {a.axis: a.kind for a in axes}
    priority_of_axis = {a.axis: a.priority for a in axes}
    required_member_of = {a.axis: a.required_member for a in axes}
    priority_of: dict[str, int] = {}
    for spec in concepts:
        prev = priority_of.get(spec.concept)
        if prev is None or spec.priority > prev:
            priority_of[spec.concept] = spec.priority

    def is_segment(axis: str) -> bool:
        kind = kind_of.get(axis)
        return kind is not None and kind != QUALIFIER

    def qualifier_key(dims: Iterable[tuple[str, str]]) -> str:
        """A fact's qualifier context, sorted (XBRL promises no dimension order) and joined.

        Ignored when deciding WHETHER a fact is a segment fact, and essential to what it reconciles
        TO. Samsung's FY2024 filing states four Revenue facts with no segment axis: Consolidated
        300.87T, Consolidated + OperatingSegments 329.39T (what the divisions sum to), Consolidated
        + MaterialReconcilingItems -28.52T and Separate 209.05T. Keyed on (metric, period) alone
        they share a slot, and the winner was the elimination.
        """
        return "|".join(sorted(f"{a}={m}" for a, m in dims if kind_of.get(a) == QUALIFIER))

    # key -> (the undimensioned figure, the priority of its concept). Priority, not document order:
    # `Revenues` and `RevenueFromContractWithCustomerExcludingAssessedTax` can both be stated and
    # differ, and `security-xbrl` resolves that by `xbrl_concept.priority`.
    totals: dict[str, tuple[float, int]] = {}
    # The same totals by the concept that stated them, so a split reconciles against its OWN concept
    # first. SK Gas states `Revenue` 7,095,902,060,317 and `RevenueFromContractsWithCustomers`
    # 7,050,068,258,000 for FY2024 and splits the second; against the first it is 0.65% short.
    totals_by_concept: dict[str, float] = {}
    # (concept, metric, period) keys stated more than once with different values, so the concept
    # alone cannot name a target. Refusing beats handing a split whichever fact was parsed last.
    ambiguous_concept: set[str] = set()
    totals_by_qualifier: dict[str, tuple[float, int]] = {}
    totals_by_concept_qualifier: dict[str, float] = {}
    # `axis|metric|periodType|start|end` -> (qualifier member -> value), for facts with no segment
    # dimension and exactly one qualifier: the columns a segment column's total is derived from.
    qualifier_member_totals: dict[str, dict[str, float]] = {}
    candidates: list[_Candidate] = []

    for f in facts:
        ctx = contexts.get(f.ctx_ref)
        if ctx is None:
            continue
        # AN INSTANT IS A STOCK and belongs here too: rejecting instants drops every balance-sheet
        # segmentation.
        is_instant = ctx.instant is not None
        start = ctx.instant if is_instant else ctx.start
        end = ctx.instant if is_instant else ctx.end
        if not start or not end:
            continue
        period_type = "instant" if is_instant else period_type_for(start, end)
        if period_type is None:
            continue
        currency = None if f.unit_ref is None else units.get(f.unit_ref)
        if currency is None:
            continue

        dims = list(ctx.dims.items())
        segs = [(a, m) for a, m in dims if is_segment(a)]
        # A SUBTOTAL IS NOT A SEGMENT, dropped before partitioning: a subtotal plus a residual can
        # reconcile perfectly and would then BE the chosen partition.
        if any(m in subtotals for _, m in segs):
            continue
        unknown = [a for a, _ in dims if a not in kind_of]
        wrong_member = any(
            (req := required_member_of.get(a)) is not None and req != m for a, m in dims
        )
        # RECORDED BEFORE THE PIN REJECTS IT: an elimination must never be a segment MEMBER, and
        # must stay available as arithmetic. Equinor states the consolidated 106,462m and an
        # elimination of -42,421m, and 106,462 - (-42,421) = 148,883m is what its segments sum to.
        if not segs and len(dims) == 1 and kind_of.get(dims[0][0]) == QUALIFIER:
            axis, member = dims[0]
            bucket_key = f"{axis}|{_key(f.metric_code, period_type, start, end)}"
            qualifier_member_totals.setdefault(bucket_key, {})[member] = f.value

        if wrong_member:
            continue
        # AN UNKNOWN AXIS DISQUALIFIES THE FACT, before it can be mistaken for a total below.
        if unknown:
            continue

        if not segs:
            # The filing's own figure: "no SEGMENT dimension", not "no dimension". A Korean filing
            # tags every fact with a qualifier, so nothing in it is literally undimensioned.
            k = _key(f.metric_code, period_type, start, end)
            priority = priority_of.get(f.concept, 0)
            held = totals.get(k)
            if held is None or priority > held[1]:
                totals[k] = (f.value, priority)
            ck = f"{f.concept}|{k}"
            prior = totals_by_concept.get(ck)
            if prior is not None and prior != f.value:
                ambiguous_concept.add(ck)
            totals_by_concept[ck] = f.value
            qual = qualifier_key(dims)
            qk = f"{qual}|{k}"
            held_q = totals_by_qualifier.get(qk)
            if held_q is None or priority > held_q[1]:
                totals_by_qualifier[qk] = (f.value, priority)
            totals_by_concept_qualifier[f"{f.concept}|{qual}|{k}"] = f.value
            continue
        if len(segs) < 1 or len(segs) > 2:
            continue

        # TWO SEGMENT AXES IS A CROSS-TAB CELL. The finer axis (higher `segment_axis.priority`) is
        # the member and the coarser its parent: Alphabet lists product lines WITHIN Google
        # Services.
        ordered = (
            sorted(segs, key=lambda am: (-priority_of_axis.get(am[0], 0), _collation_key(am[0])))
            if len(segs) == 2
            else segs
        )
        candidates.append(
            _Candidate(
                axis=ordered[0][0],
                member_code=ordered[0][1],
                parent_axis=ordered[1][0] if len(ordered) > 1 else None,
                parent_member=ordered[1][1] if len(ordered) > 1 else None,
                concept=f.concept,
                metric_code=f.metric_code,
                period_type=period_type,
                period_start=start,
                period_ending=end,
                value=f.value,
                currency=currency,
                qualifier=qualifier_key(dims),
            )
        )

    # A filing can state one fact twice at different `decimals`; keep the first, or the member is
    # counted twice inside its own partition.
    seen: set[str] = set()
    unique: list[_Candidate] = []
    for c in candidates:
        k = (
            f"{c.axis}|{c.member_code}|{c.parent_member or ''}|{c.metric_code}|"
            f"{c.period_type}|{c.period_ending}"
        )
        if k in seen:
            continue
        seen.add(k)
        unique.append(c)

    marginals = _marginals(unique)
    synthetic = set(marginals)
    unique.extend(marginals)

    # THE SPLIT IS A PROPERTY OF (AXIS, PERIOD END): learned from whichever bucket reconciles, and
    # applied to every bucket on that axis and date, which is what makes segment ASSETS usable at
    # all (they never sum to consolidated assets). A cross-tab cell is grouped within its parent.
    groups: dict[str, list[_Candidate]] = {}
    for c in unique:
        g = (
            f"{c.axis}|{c.period_ending}"
            if c.parent_member is None
            else f"{c.parent_axis}={c.parent_member}|{c.axis}|{c.period_ending}"
        )
        groups.setdefault(g, []).append(c)

    # What each flat member is worth, so a cross-tab group knows the total it reconciles to.
    flat_value: dict[str, float] = {}
    for c in unique:
        if c.parent_member is not None:
            continue
        flat_value[
            f"{c.axis}|{c.member_code}|{c.metric_code}|{c.period_type}|"
            f"{c.period_start}|{c.period_ending}"
        ] = c.value

    out: list[SegmentFact] = []
    for group in groups.values():
        # Learned per (metric, period type, span): a quarter is never reconciled against a year.
        # Segment PROFIT never reconciles (ASC 280 and IFRS 8 require a reconciliation, not an
        # identity), so the split is inferred from the metric that does and applied to the rest.
        by_bucket: dict[str, list[_Candidate]] = {}
        for c in group:
            by_bucket.setdefault(
                _key(c.metric_code, c.period_type, c.period_start, c.period_ending), []
            ).append(c)

        best_map: dict[str, int] | None = None
        best_placed = 0
        # THE SPLIT IS SHARED ACROSS THE GROUP; THE TARGET IS NOT. A fiscal-year end is also Q4's
        # end, and stamping one bucket's target on the group handed quarters the year's revenue
        # (3,021 of 11,211 flat revenue splits, measured 2026-09-04).
        target_by_bucket: dict[str, float] = {}
        for bucket_key, cands in by_bucket.items():
            target = _target(
                cands,
                totals=totals,
                totals_by_concept=totals_by_concept,
                ambiguous_concept=ambiguous_concept,
                totals_by_qualifier=totals_by_qualifier,
                totals_by_concept_qualifier=totals_by_concept_qualifier,
                qualifier_member_totals=qualifier_member_totals,
                flat_value=flat_value,
            )
            if target is None:
                continue
            placement = assign_partitions([(c.member_code, c.value) for c in cands], target)
            target_by_bucket[bucket_key] = target
            placed = sum(1 for p in placement.values() if p > 0)
            if placed > best_placed:
                best_placed = placed
                best_map = placement

        # A RESIDUAL ALONE IS NOT A SPLIT. A bucket holding only some of `best_map`'s members
        # inherits the partition anyway, and when those are all residual the leftovers wear the
        # company's name: Chevron served `AllOtherSegments 581m` as its FY2025 revenue breakdown.
        # Completeness cannot be the rule (Chevron's assets reconcile without the residual), nor can
        # reconciliation (segment profit never does): a partition needs one non-residual member.
        real_members: set[str] = set()
        for c in group:
            p = 0 if best_map is None else best_map.get(c.member_code, 0)
            if p > 0 and c.member_code not in residuals:
                real_members.add(
                    f"{_key(c.metric_code, c.period_type, c.period_start, c.period_ending)}|{p}"
                )

        for c in group:
            inherited = 0 if best_map is None else best_map.get(c.member_code, 0)
            bucket_key = _key(c.metric_code, c.period_type, c.period_start, c.period_ending)
            partition_id = (
                0
                if inherited > 0 and f"{bucket_key}|{inherited}" not in real_members
                else inherited
            )
            # A MARGINAL THAT EARNS NO PARTITION IS DROPPED, NOT STORED AT 0: it is ours, and
            # unplaced it duplicates the cells it was summed from.
            if c in synthetic and partition_id == 0:
                continue
            # Its own bucket's target, or nothing: a borrowed target is how 3,021 false
            # disagreements arose.
            own = target_by_bucket.get(bucket_key)
            out.append(
                SegmentFact(
                    axis=c.axis,
                    member_code=c.member_code,
                    metric_code=c.metric_code,
                    period_type=c.period_type,
                    period_start=c.period_start,
                    period_ending=c.period_ending,
                    value=c.value,
                    currency=c.currency,
                    parent_axis=c.parent_axis,
                    parent_member=c.parent_member,
                    reconciled_to=own if partition_id > 0 else None,
                    partition_id=partition_id,
                )
            )
    return out


def _marginals(unique: Sequence[_Candidate]) -> list[_Candidate]:
    """A cross-tab summed both ways, as flat candidates the partition search must still accept.

    A cell reconciles to its parent, so a filer publishing the grid without the parent's total
    leaves every cell unplaceable: Chevron FY2025 discloses US/non-US x upstream/downstream revenue
    and no flat business-line revenue (19,990 such cells in 153 securities, measured 2026-09-05).
    Summing across the parent axis gives the cell dimension and summing by parent the parent
    dimension.

    Three guards keep it honest. A member the filer states flatly is never replaced by our sum. The
    marginal is an ordinary candidate, so it survives only if it reconciles: Novo's members NEST
    (Ozempic inside TotalGLP1), and flat its cells reach 4.2x revenue. One that earns no partition
    is dropped by the caller. The QUALIFIER is kept: Chevron tags its grid with
    `ConsolidationItemsAxis = OperatingSegmentsMember`, whose total (231,370m) differs from the bare
    one (184,432m).
    """
    flat_seen = {
        f"{c.axis}|{c.member_code}|{c.metric_code}|{c.period_type}|{c.period_ending}"
        for c in unique
        if c.parent_member is None
    }
    rolled: dict[str, _Candidate] = {}

    def roll(c: _Candidate, axis: str, member_code: str) -> None:
        flat_key = f"{axis}|{member_code}|{c.metric_code}|{c.period_type}|{c.period_ending}"
        if flat_key in flat_seen:
            return
        existing = rolled.get(flat_key)
        if existing is not None:
            existing.value += c.value
            return
        rolled[flat_key] = replace(
            c, axis=axis, member_code=member_code, parent_axis=None, parent_member=None
        )

    for c in unique:
        if c.parent_member is None or c.parent_axis is None:
            continue
        roll(c, c.axis, c.member_code)
        roll(c, c.parent_axis, c.parent_member)
    return list(rolled.values())


def _target(
    cands: Sequence[_Candidate],
    *,
    totals: dict[str, tuple[float, int]],
    totals_by_concept: dict[str, float],
    ambiguous_concept: set[str],
    totals_by_qualifier: dict[str, tuple[float, int]],
    totals_by_concept_qualifier: dict[str, float],
    qualifier_member_totals: dict[str, dict[str, float]],
    flat_value: dict[str, float],
) -> float | None:
    """The figure one bucket's members are reconciled against, or None.

    For a cross-tab it is the parent member's own value. For a flat split it is the stated total the
    members actually add up to, tried most specific first (concept and qualifier, concept,
    qualifier, the derived segment-column totals, then the plain total); if none reconciles,
    precedence decides unchanged, which is what keeps a split that reconciles to nothing described
    as before.
    """
    g0 = cands[0]
    flat_key = _key(g0.metric_code, g0.period_type, g0.period_start, g0.period_ending)
    if g0.parent_member is not None:
        return flat_value.get(
            f"{g0.parent_axis}|{g0.parent_member}|{g0.metric_code}|{g0.period_type}|"
            f"{g0.period_start}|{g0.period_ending}"
        )

    own_concept = list(dict.fromkeys(c.concept for c in cands))
    same_concept = (
        None
        if len(own_concept) != 1 or f"{own_concept[0]}|{flat_key}" in ambiguous_concept
        else totals_by_concept.get(f"{own_concept[0]}|{flat_key}")
    )
    own_qualifier = list(dict.fromkeys(c.qualifier for c in cands))
    one_qualifier = own_qualifier[0] if len(own_qualifier) == 1 else None
    concept_qualified = (
        totals_by_concept_qualifier.get(f"{own_concept[0]}|{one_qualifier}|{flat_key}")
        if len(own_concept) == 1 and one_qualifier is not None
        else None
    )
    held_q = (
        None if one_qualifier is None else totals_by_qualifier.get(f"{one_qualifier}|{flat_key}")
    )
    qualified = None if held_q is None else held_q[0]
    plain_held = totals.get(flat_key)
    plain = None if plain_held is None else plain_held[0]

    candidate_sum = _add_up(c.value for c in cands)
    offered = [
        concept_qualified,
        same_concept,
        qualified,
        *_derived_targets(plain, one_qualifier, flat_key, qualifier_member_totals),
        plain,
    ]
    reconciling = next(
        (t for t in offered if t is not None and abs(candidate_sum - t) <= _tolerance(t)), None
    )
    return _first_defined(
        reconciling,
        concept_qualified,
        same_concept,
        qualified,
        _derived_fallback(plain, one_qualifier, flat_key, qualifier_member_totals),
        plain,
    )


def _derived_targets(
    plain: float | None,
    one_qualifier: str | None,
    flat_key: str,
    qualifier_member_totals: dict[str, dict[str, float]],
) -> list[float]:
    """A segment column's total, derived from the other columns, offered and never imposed.

    Segment revenue includes intersegment sales that consolidation removes, so a correct split
    exceeds the consolidated figure by the elimination. Southern Copper's FY2018 10-K states
    7,096,700,000 consolidated and -79,300,000 eliminated, under TWO member names (once in the
    segment table, once in the geography table), and its segments carry no qualifier at all. So
    every qualifier axis with totals for this metric and period is walked, and both the every-member
    and the distinct-VALUE sums are offered. A candidate is accepted only if the members already sum
    to it, so an extra one can rescue a split and never select a wrong target.
    """
    if plain is None:
        return []
    own_axis: str | None = None
    own_member: str | None = None
    if one_qualifier and "|" not in one_qualifier and "=" in one_qualifier:
        own_axis, own_member = one_qualifier.split("=", 1)
    out: list[float] = []
    suffix = f"|{flat_key}"
    for k, bucket in qualifier_member_totals.items():
        if not k.endswith(suffix):
            continue
        axis = k[: len(k) - len(suffix)]
        others = [v for m, v in bucket.items() if not (axis == own_axis and m == own_member)]
        if not others:
            continue
        every = _add_up(others)
        distinct = _add_up(dict.fromkeys(others))
        out.append(plain - every)
        if distinct != every:
            out.append(plain - distinct)
    return out


def _derived_fallback(
    plain: float | None,
    one_qualifier: str | None,
    flat_key: str,
    qualifier_member_totals: dict[str, dict[str, float]],
) -> float | None:
    """The narrower derivation the edge used before `_derived_targets`, kept as the fallback so a
    split that reconciles to nothing is described exactly as it was."""
    if one_qualifier is None or plain is None:
        return None
    if "=" not in one_qualifier or "|" in one_qualifier:
        return None
    axis, member = one_qualifier.split("=", 1)
    bucket = qualifier_member_totals.get(f"{axis}|{flat_key}")
    if bucket is None:
        return None
    others = [v for m, v in bucket.items() if m != member]
    return plain - _add_up(others) if others else None


# ── Finding the instance in a filing ───────────────────────────────────────────────────────────

#: The largest instance read. AEP's 2015 10-K (`aep-20151231.xml`, 127.72 MB) killed the edge
#: worker on every firing for two days, and a killed worker stamps nothing, so the same filing
#: headed the queue for ever. 32 MB is ~3x the largest document known to parse (Diageo's 20-F,
#: 10.92 MB).
MAX_INSTANCE_BYTES = 32 * 1024 * 1024

_LINKBASE = re.compile(r"_(cal|def|lab|pre|ref)\.xml\Z", re.A)
_FILING_SUMMARY = re.compile(r"FilingSummary\.xml\Z", re.A | re.I)
_INSTANCE_NAME = re.compile(r"-\d{8}(_htm)?\.xml\Z", re.A)
_INDEX_HREF = re.compile(r'href="[^"]*/([A-Za-z0-9_-]+\.xml)"', re.A)


def is_instance_name(name: str) -> bool:
    """The instance, not a linkbase or `FilingSummary.xml`, in both eras: `aapl-20250628_htm.xml`
    (inline XBRL, 2019+) and `aapl-20150926.xml` (the standalone instance before it). The edge's
    rule, unchanged: a name it accepts is always chosen first (`_pick_instance`)."""
    if not name.endswith(".xml"):
        return False
    if _LINKBASE.search(name):
        return False
    if _FILING_SUMMARY.match(name):
        return False
    return _INSTANCE_NAME.search(name) is not None


def is_extracted_instance_name(name: str) -> bool:
    """Any instance SEC extracted from an inline XBRL document: `<primary document>_htm.xml`.

    WIDENED 2026-11-14 (the user's decision; umbrella docs/deferred/2026-11-14-an-agent-named-
    instance-is-read-as-no-instance.md). Filing agents name the primary document their own way, so
    the edge's dated rule rejected `scco-20260930x10q_htm.xml`, `form10-q_htm.xml` and
    `desp20260930_10q_htm.xml`, and recorded each filing as having no instance for good. Measured on
    the lane's first wave: 10 of 29 filings, 7 of the 11 read carrying segments; since 2019, 64.7%
    of agent-filed filings had no segment rows against 24.2% self-filed.
    """
    return name.endswith("_htm.xml") and not _LINKBASE.search(name)


def _pick_instance(names: Sequence[str]) -> str | None:
    """The instance among a filing's file names, or None.

    THE EDGE'S RULE FIRST, so every filing it read picks the same file and parity holds: shortest
    of the names it accepts. Only when it accepts none, the one `*_htm.xml` SEC extracted; two or
    more with no dated name are ambiguous and read as no instance, as before.
    """
    dated = [n for n in names if is_instance_name(n)]
    if dated:
        return sorted(dated, key=len)[0]
    extracted = [n for n in names if is_extracted_instance_name(n)]
    return extracted[0] if len(set(extracted)) == 1 else None


def _js_number(value: object) -> float:
    """`Number(value)` for what `index.json` puts in `size`: a numeric string, '' or a number."""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, int | float):
        return float(value)
    text = _trim(str(value))
    if text == "":
        return 0.0
    try:
        return float(text)
    except ValueError:
        return math.nan


def instance_from_index_json(body: str) -> tuple[str, float | None] | None:
    """(instance file name, its size) from a filing's `index.json`, or None to try the HTML index.

    None covers both an unreadable listing and one naming no instance: `index.json` is INCOMPLETE
    for some filings (Diageo's and Infosys's FY2025 20-Fs list 4 items where the HTML index lists
    the instance), so "no instance here" is not trusted from it. The size it carries is what gates
    `MAX_INSTANCE_BYTES` before the body is fetched. Shortest name wins: a filing occasionally
    carries an amended companion.
    """
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    directory = parsed.get("directory") if isinstance(parsed, dict) else None
    items = directory.get("item") if isinstance(directory, dict) else None
    if items is None:
        return None
    if not isinstance(items, list):
        return None
    sizes: dict[str, float | None] = {}
    for item in items:
        if item is None:
            return None  # `i.name` of null throws in the edge, which fell through to the HTML
        if not isinstance(item, dict):
            continue
        raw_name = item.get("name")
        name = "" if raw_name is None else str(raw_name)
        if not (is_instance_name(name) or is_extracted_instance_name(name)):
            continue
        size = item.get("size")
        n = _js_number(0 if size is None else size)
        sizes.setdefault(name, None if n == 0 or math.isnan(n) else n)
    picked = _pick_instance(list(sizes))
    return None if picked is None else (picked, sizes[picked])


def instance_from_index_html(html: str) -> str | None:
    """The instance file name from a filing's `<accession>-index.htm`, which carries no size."""
    return _pick_instance([str(m.group(1)) for m in _INDEX_HREF.finditer(html)])


def instance_is_too_large(size: float | None) -> bool:
    """An unknown size is NOT too large: the HTML fallback has none, and refusing it would drop
    every foreign private issuer whose `index.json` is incomplete."""
    return size is not None and size > MAX_INSTANCE_BYTES


# ── The control tables, and the rows a parsed filing becomes ───────────────────────────────────

#: The metrics a segment is read for, as `market.metric` codes (the edge's `SEGMENT_METRICS`).
#: `total_assets`, `long_lived_assets` and `goodwill` are instants.
SEGMENT_METRICS = (
    "revenue",
    "operating_income",
    "capital_expenditure",
    "total_assets",
    "depreciation",
    "cost_of_revenue",
    "long_lived_assets",
    "goodwill",
)


@dataclass(frozen=True)
class Vocabulary:
    """The control tables one parse reads, for every regulator: a new axis, concept or member role
    is a row, never a release.

    `parser_version` is `market.segment_parser.version`. Every filing read is stamped with it
    (`security_filing.segments_parser_version`), and raising it makes every filing due again. It
    is a row and not a `code_version` because the unit of work is the FILING, and a filer's
    partition holds many: 226,000 SEC accounts filings cannot be a partition each.
    """

    axes: tuple[AxisSpec, ...]
    concepts: tuple[ConceptSpec, ...]
    roles: tuple[MemberRole, ...]
    parser_version: int


class VocabularyEmpty(RuntimeError):
    """A control table has no rows. Parsing would read every filing as disclosing nothing, and
    stamp it as read."""


def vocabulary(conn: Any) -> Vocabulary:
    """`segment_axis`, the segment metrics' `xbrl_concept` rows, `segment_member_class` and the
    parser version. Ordered, so two runs read them identically."""
    with conn.cursor() as cur:
        cur.execute(
            "select axis, kind, priority, required_member from market.segment_axis order by axis"
        )
        axes = tuple(AxisSpec(str(a), str(k), int(p), r) for a, k, p, r in cur.fetchall())
        cur.execute(
            "select metric_code, concept, priority from market.xbrl_concept "
            "where metric_code = any(%s) order by metric_code, concept",
            (list(SEGMENT_METRICS),),
        )
        concepts = tuple(ConceptSpec(str(m), str(c), int(p)) for m, c, p in cur.fetchall())
        cur.execute(
            "select member_code, class from market.segment_member_class order by member_code"
        )
        roles = tuple(MemberRole(str(m), str(c)) for m, c in cur.fetchall())
        cur.execute("select max(version) from market.segment_parser")
        found = cur.fetchone()
    if not axes:
        raise VocabularyEmpty("market.segment_axis is empty: every filing would yield nothing")
    if not concepts:
        raise VocabularyEmpty("market.xbrl_concept has no segment metric: nothing would be read")
    version = 1 if found is None or found[0] is None else int(found[0])
    return Vocabulary(axes, concepts, roles, version)


#: `market.security_segment`'s key. `parent_key` is generated (`coalesce(parent_member, '')`): it
#: is carried by every row for the dedupe and never inserted (`writers.upsert(computed=...)`).
#: THE ACCESSION IS NOT IN IT, on purpose: a newer filing restating a period takes the row over.
SEGMENT_KEY = (
    "security_id",
    "axis",
    "member_code",
    "parent_key",
    "metric_code",
    "period_type",
    "period_ending",
)
#: What a parse may change. `as_of` is last and is left out of the comparison: it says when the
#: document was read, and a re-read that changes nothing must not rewrite the row.
SEGMENT_UPDATE = (
    "period_start",
    "value",
    "currency_code",
    "partition_id",
    "parent_axis",
    "parent_member",
    "reconciled_to",
    "accession_number",
    "source_code",
    "as_of",
)
SEGMENT_COMPARE = SEGMENT_UPDATE[:-1]
#: The key a filing's own rows are retracted on, within (security, accession).
SEGMENT_RETRACT_KEY = SEGMENT_KEY[1:]
SEGMENT_RETRACT_TYPES = {
    "axis": "text",
    "member_code": "text",
    "parent_key": "text",
    "metric_code": "text",
    "period_type": "text",
    "period_ending": "date",
}

#: A filing read is stamped whether or not it had segments: "this filing discloses none" is a
#: permanent fact about an immutable document. Never stamped for a document that was not read.
STAMP_PARSED = """
update market.security_filing
   set segments_parsed_at = %s, segments_parser_version = %s
 where accession_number = %s and security_id = any(%s::uuid[])
"""


def _numeric(value: float) -> int | Decimal:
    """A parsed figure as the number the edge stored: JS serialised a float by its shortest
    round-trip digits, and Postgres read that text exactly. Passing the float itself would have
    Postgres round it to 15 significant digits on the way into `numeric`."""
    if value.is_integer():
        return int(value)
    return Decimal(repr(value))


def segment_rows(
    facts: Iterable[SegmentFact],
    *,
    security_id: str,
    accession: str,
    source: str,
    as_of: str,
    known_currencies: Collection[str],
) -> tuple[list[dict[str, Any]], Counter[str]]:
    """One filing's facts as `market.security_segment` rows for one security.

    A ROW IN A CURRENCY `market.currency` DOES NOT HOLD IS WITHHELD AND COUNTED: the column
    references that table, so one such row would fail the filing's whole write. The edge inserted
    the code instead; a lane does not mint reference data.
    """
    rows: list[dict[str, Any]] = []
    stats: Counter[str] = Counter()
    for f in facts:
        if f.currency not in known_currencies:
            stats["currency_unknown"] += 1
            continue
        rows.append(
            {
                "security_id": security_id,
                "axis": f.axis,
                "member_code": f.member_code,
                "parent_key": f.parent_member or "",
                "metric_code": f.metric_code,
                "period_type": f.period_type,
                "period_ending": f.period_ending,
                "period_start": f.period_start,
                "value": _numeric(f.value),
                "currency_code": f.currency,
                "partition_id": f.partition_id,
                "parent_axis": f.parent_axis,
                "parent_member": f.parent_member,
                "reconciled_to": None if f.reconciled_to is None else _numeric(f.reconciled_to),
                "accession_number": accession,
                "source_code": source,
                "as_of": as_of,
            }
        )
        stats["placed" if f.partition_id > 0 else "unplaced"] += 1
    return rows, stats
