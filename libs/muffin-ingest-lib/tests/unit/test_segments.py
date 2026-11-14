"""The segment partitioner, ported from the edge with its checks (`segments-check.ts`).

Every defect this parser can have produces a plausible number rather than an error: a doubled
revenue, a subtotal counted beside its own children, nine months filed as a quarter, pounds labelled
dollars. So each fixture is built so that the CANDIDATE RULES DISAGREE, and the comments say which
wrong rule each one rules out. The figures are the real ones where the edge's check used them.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import pytest

from muffin_ingest.facets import segments
from muffin_ingest.facets.segments import (
    AxisSpec,
    ConceptSpec,
    MemberRole,
    SegmentFact,
    assign_partitions,
    instance_is_too_large,
    period_type_for,
    segment_facts_from,
)

PROD = "srt:ProductOrServiceAxis"
BIZ = "us-gaap:StatementBusinessSegmentsAxis"
IPROD = "ifrs-full:ProductsAndServicesAxis"
REV = "RevenueFromContractWithCustomerExcludingAssessedTax"
FAIR_VALUE_AXIS = "us-gaap:FairValueByFairValueHierarchyLevelAxis"

AXES = [
    AxisSpec(PROD, "product", 100),
    AxisSpec(BIZ, "business", 90),
    AxisSpec(IPROD, "product", 100),
    AxisSpec("srt:ConsolidationItemsAxis", "qualifier", 0),
]
CONCEPTS = [
    ConceptSpec("revenue", REV, 100),
    ConceptSpec("revenue", "Revenue", 100),
    ConceptSpec("operating_income", "OperatingIncomeLoss", 100),
]
Q_START = "2025-04-01"
Q_END = "2025-06-30"
YEAR = {"start": "2025-01-01", "end": "2025-12-31"}


def fact(
    concept: str, value: float | None, dims: dict[str, str] | None = None, /, **kw: Any
) -> dict[str, Any]:
    return {"concept": concept, "value": value, "dims": dims or {}, **kw}


def prod(member: str) -> dict[str, str]:
    return {PROD: member}


def instance(facts: list[dict[str, Any]], units: dict[str, str] | None = None) -> str:
    """An instance document in the shape SEC serves, one context per distinct period and dims."""
    units = units or {"usd": "iso4217:USD"}
    ctx_ids: dict[str, str] = {}
    contexts: list[str] = []
    rows: list[str] = []
    for f in facts:
        start, end, instant = f.get("start", Q_START), f.get("end", Q_END), f.get("instant")
        key = json.dumps([instant or start, instant or end, instant or "", f["dims"]])
        ctx = ctx_ids.get(key)
        if ctx is None:
            ctx = f"c{len(ctx_ids)}"
            ctx_ids[key] = ctx
            members = "".join(
                f'<xbrldi:explicitMember dimension="{a}">{m}</xbrldi:explicitMember>'
                for a, m in f["dims"].items()
            )
            period = (
                f"<xbrli:instant>{instant}</xbrli:instant>"
                if instant
                else f"<xbrli:startDate>{start}</xbrli:startDate>"
                f"<xbrli:endDate>{end}</xbrli:endDate>"
            )
            contexts.append(
                f'<xbrli:context id="{ctx}"><xbrli:entity>'
                '<xbrli:identifier scheme="s">1</xbrli:identifier>'
                + (f"<xbrli:segment>{members}</xbrli:segment>" if members else "")
                + f"</xbrli:entity><xbrli:period>{period}</xbrli:period></xbrli:context>"
            )
        value = "" if f["value"] is None else _js(f["value"])
        rows.append(
            f'<us-gaap:{f["concept"]} contextRef="{ctx}" unitRef="{f.get("unit", "usd")}"'
            f"{f.get('attrs', '')}>{value}</us-gaap:{f['concept']}>"
        )
    unit_xml = "".join(
        f'<xbrli:unit id="{u}"><xbrli:measure>{m}</xbrli:measure></xbrli:unit>'
        for u, m in units.items()
    )
    body = f"{unit_xml}{''.join(contexts)}{''.join(rows)}"
    return f'<?xml version="1.0"?><xbrli:xbrl>{body}</xbrli:xbrl>'


def _js(value: float) -> str:
    """How JS interpolates a number into the fixture: no trailing `.0`."""
    return str(int(value)) if float(value).is_integer() else str(value)


def parts(out: list[SegmentFact], partition: int) -> list[SegmentFact]:
    return [f for f in out if f.partition_id == partition]


def total(rows: list[SegmentFact]) -> float:
    return sum(f.value for f in rows)


# 1. THE HEADLINE CASE. Amazon Q2-2025 discloses THREE splits each summing to 167,702: seven product
#    lines, Product+Service, and (elsewhere) three business segments. Summing the product axis gives
#    335,404; summing every member more. Both plausible, both wrong.
def test_overlapping_splits_on_one_axis_are_separate_partitions() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 167702),
                fact(REV, 61485, prod("amzn:OnlineStoresMember")),
                fact(REV, 40348, prod("amzn:ThirdPartySellerServicesMember")),
                fact(REV, 30873, prod("amzn:AmazonWebServicesMember")),
                fact(REV, 15694, prod("amzn:AdvertisingServicesMember")),
                fact(REV, 12208, prod("amzn:SubscriptionServicesMember")),
                fact(REV, 5595, prod("amzn:PhysicalStoresMember")),
                fact(REV, 1499, prod("amzn:OtherServicesMember")),
                fact(REV, 68246, prod("us-gaap:ProductMember")),
                fact(REV, 99456, prod("us-gaap:ServiceMember")),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    assert len(out) == 9
    assert len(parts(out, 1)) == 7 and total(parts(out, 1)) == 167702
    assert len(parts(out, 2)) == 2 and total(parts(out, 2)) == 167702
    assert parts(out, 0) == []
    # The mutation this defends against: aggregating the AXIS rather than the PARTITION.
    assert total(out) == 335404


# 1b. PARTITION 1 IS THE FINEST SPLIT WHATEVER ORDER THE FILING USED. With the coarse pair stated
#     first, "first subset found" gives {Product, Service} and "maximal" the seven lines.
def test_partition_one_is_the_finest_split_even_when_a_coarser_one_comes_first() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 167702),
                fact(REV, 68246, prod("us-gaap:ProductMember")),
                fact(REV, 99456, prod("us-gaap:ServiceMember")),
                fact(REV, 61485, prod("amzn:OnlineStoresMember")),
                fact(REV, 40348, prod("amzn:ThirdPartySellerServicesMember")),
                fact(REV, 30873, prod("amzn:AmazonWebServicesMember")),
                fact(REV, 15694, prod("amzn:AdvertisingServicesMember")),
                fact(REV, 12208, prod("amzn:SubscriptionServicesMember")),
                fact(REV, 5595, prod("amzn:PhysicalStoresMember")),
                fact(REV, 1499, prod("amzn:OtherServicesMember")),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    p1 = parts(out, 1)
    assert len(p1) == 7 and total(p1) == 167702
    assert any(f.member_code == "amzn:AmazonWebServicesMember" for f in p1)


# 2. A SUBTOTAL IS NOT A MEMBER OF THE SPLIT. Apple's reconciling split mixes extensions with a
#    standard member: "prefer extension members" drops Service, "every member" gives 160,649.
def test_a_subtotal_is_excluded_and_the_split_mixes_extensions_with_a_standard_member() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 94036),
                fact(REV, 44582, prod("aapl:IPhoneMember")),
                fact(REV, 8046, prod("aapl:MacMember")),
                fact(REV, 6581, prod("aapl:IPadMember")),
                fact(REV, 7404, prod("aapl:WearablesHomeandAccessoriesMember")),
                fact(REV, 27423, prod("us-gaap:ServiceMember")),
                fact(REV, 66613, prod("us-gaap:ProductMember")),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    p1 = parts(out, 1)
    assert len(p1) == 5 and total(p1) == 94036
    assert next(f for f in out if f.member_code == "us-gaap:ProductMember").partition_id == 0
    assert any(f.member_code == "us-gaap:ServiceMember" for f in p1)


# 3. SEGMENT PROFIT DOES NOT RECONCILE, BY DESIGN, AND MUST STILL BE PLACED. The profit members sum
#    to 38,949 against a consolidated 28,202, so a self-reconciling rule places none of them.
def test_segment_profit_inherits_the_split_from_revenue() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 94036),
                fact(REV, 41198, {BIZ: "aapl:AmericasSegmentMember"}),
                fact(REV, 52838, {BIZ: "aapl:EuropeSegmentMember"}),
                fact("OperatingIncomeLoss", 28202),
                fact("OperatingIncomeLoss", 16511, {BIZ: "aapl:AmericasSegmentMember"}),
                fact("OperatingIncomeLoss", 22438, {BIZ: "aapl:EuropeSegmentMember"}),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    profit = [f for f in out if f.metric_code == "operating_income"]
    assert len(profit) == 2 and all(f.partition_id == 1 for f in profit)
    assert total(profit) != 28202


# 4. A QUALIFIER AXIS NARROWS A FACT; AN UNKNOWN AXIS DISQUALIFIES IT. The rejected fact carries a
#    DISTINCT member, so only the unknown-axis rule can be what drops it.
def test_a_qualifier_is_kept_and_an_unknown_second_axis_rejects_the_fact() -> None:
    ops = {"srt:ConsolidationItemsAxis": "us-gaap:OperatingSegmentsMember"}
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100),
                fact(REV, 60, {BIZ: "goog:GoogleCloudMember", **ops}),
                fact(REV, 40, {BIZ: "goog:GoogleServicesMember", **ops}),
                fact(
                    REV,
                    999,
                    {
                        BIZ: "goog:OtherBetsMember",
                        FAIR_VALUE_AXIS: "us-gaap:FairValueInputsLevel2Member",
                    },
                ),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    assert len(out) == 2 and all(f.partition_id == 1 for f in out)
    assert not any(f.value == 999 for f in out)


# 5. A YEAR-TO-DATE FIGURE IS NOT A QUARTER.
def test_period_types_are_windows_and_anything_else_is_rejected() -> None:
    assert period_type_for("2025-03-30", "2025-06-28") == "quarter"
    assert period_type_for("2024-09-29", "2025-09-27") == "annual"
    assert period_type_for("2025-01-01", "2025-06-30") is None
    assert period_type_for("2024-10-01", "2025-06-28") is None
    assert period_type_for("not a date", "2025-06-28") is None


# 6. THE CURRENCY COMES FROM THE UNIT, never defaulted (Diageo files in USD despite being British).
def test_the_reporting_currency_is_read_from_the_unit() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100, unit="gbp"),
                fact(REV, 60, prod("x:AMember"), unit="gbp"),
                fact(REV, 40, prod("x:BMember"), unit="gbp"),
            ],
            {"gbp": "iso4217:GBP"},
        ),
        AXES,
        CONCEPTS,
    )
    assert len(out) == 2 and all(f.currency == "GBP" for f in out)


def test_a_ratio_or_share_unit_is_not_money() -> None:
    units = segments.parse_units(
        '<xbrli:unit id="u1"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD'
        "</xbrli:measure></xbrli:unitNumerator></xbrli:divide></xbrli:unit>"
        '<xbrli:unit id="u2"><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unit>'
        '<xbrli:unit id="u3"><xbrli:measure> iso4217:EUR </xbrli:measure></xbrli:unit>'
    )
    assert units == {"u1": None, "u2": None, "u3": "EUR"}


# 7. AN EMPTY VALUE IS AN ABSENCE: `xsi:nil` read as 0 puts a fabricated zero beside real revenue.
def test_a_nil_fact_is_an_absence_not_a_zero() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100),
                fact(REV, 60, prod("x:AMember")),
                fact(REV, 40, prod("x:BMember")),
                fact(REV, None, prod("x:CMember"), attrs=' xsi:nil="true"'),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    assert len(out) == 2 and not any(f.member_code == "x:CMember" for f in out)


# 8. THE SAME FACT TWICE MUST COUNT ONCE, or the member's own partition stops reconciling.
def test_a_duplicated_fact_is_stored_once() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100),
                fact(REV, 60, prod("x:AMember")),
                fact(REV, 60, prod("x:AMember"), attrs=' decimals="-3"'),
                fact(REV, 40, prod("x:BMember")),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    assert len(out) == 2 and all(f.partition_id == 1 for f in out)


# 9. NO CONSOLIDATED FIGURE MEANS NO CLAIM: the members are stored, unplaced.
def test_without_a_consolidated_figure_members_are_stored_but_unplaced() -> None:
    out = segment_facts_from(
        instance([fact(REV, 60, prod("x:AMember")), fact(REV, 40, prod("x:BMember"))]),
        AXES,
        CONCEPTS,
    )
    assert len(out) == 2 and all(f.partition_id == 0 for f in out)


# 10. THE SUBSET SEARCH IS BOUNDED, and the common case still works at any width.
def test_the_subset_search_is_bounded() -> None:
    many = [(f"m{i}", 1.0) for i in range(24)]
    assert set(assign_partitions(many, 999).values()) == {0}
    assert set(assign_partitions(many, 24).values()) == {1}


# 11. THE TARGET IS CHOSEN BY CONCEPT PRIORITY, NOT DOCUMENT ORDER. The members' own concept has no
#     undimensioned fact, and the LOW-priority total is stated last, so "highest priority" and
#     "last seen" disagree.
def test_the_highest_priority_total_wins_not_the_last_seen() -> None:
    out = segment_facts_from(
        instance(
            [
                fact("Revenues", 100),
                fact(REV, 60, prod("x:AMember")),
                fact(REV, 40, prod("x:BMember")),
                fact("SalesRevenueNet", 137),
            ]
        ),
        AXES,
        [
            ConceptSpec("revenue", REV, 100),
            ConceptSpec("revenue", "Revenues", 120),
            ConceptSpec("revenue", "SalesRevenueNet", 70),
            ConceptSpec("operating_income", "OperatingIncomeLoss", 100),
        ],
    )
    members = [f for f in out if f.member_code.startswith("x:")]
    assert len(members) == 2 and all(f.partition_id == 1 for f in members)


# 12. A SPLIT NEED NOT SUM EXACTLY. Alphabet's real FY2025 segments are 0.03% over consolidated
#     revenue (hedging gains); under an exact rule Google Cloud disappears.
def test_a_small_reconciling_item_does_not_sink_the_split() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 402836000000),
                fact(REV, 342721000000, {BIZ: "goog:GoogleServicesMember"}),
                fact(REV, 58705000000, {BIZ: "goog:GoogleCloudMember"}),
                fact(REV, 1537000000, {BIZ: "goog:AllOtherSegmentsMember"}),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    assert len(out) == 3 and all(f.partition_id == 1 for f in out)


# 13. …BUT THE TOLERANCE STILL REJECTS A SUBTOTAL COUNTED BESIDE ITS CHILDREN (160%).
def test_the_tolerance_still_rejects_a_double_count() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100000000000),
                fact(REV, 60000000000, prod("x:AMember")),
                fact(REV, 40000000000, prod("x:BMember")),
                fact(REV, 60000000000, prod("x:SubtotalMember")),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    p1 = parts(out, 1)
    assert len(p1) == 2 and total(p1) == 100000000000


ASSET_CONCEPTS = [*CONCEPTS, ConceptSpec("total_assets", "Assets", 100)]


# 14. AN INSTANT INHERITS THE SPLIT, BECAUSE IT CAN NEVER EARN ONE: the assets sum to 900 against
#     1000, so a self-reconciling rule places neither.
def test_segment_assets_are_instants_and_inherit_the_split() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100),
                fact(REV, 60, {BIZ: "x:CloudMember"}),
                fact(REV, 40, {BIZ: "x:RetailMember"}),
                fact("Assets", 1000, instant=Q_END),
                fact("Assets", 500, {BIZ: "x:CloudMember"}, instant=Q_END),
                fact("Assets", 400, {BIZ: "x:RetailMember"}, instant=Q_END),
            ]
        ),
        AXES,
        ASSET_CONCEPTS,
    )
    assets = [f for f in out if f.metric_code == "total_assets"]
    assert len(assets) == 2 and all(f.period_type == "instant" for f in assets)
    assert all(f.partition_id == 1 for f in assets)


# 15. …BUT AN INSTANT WITH NO DURATION TO LEARN FROM STAYS UNPLACED (a balance-sheet comparative).
def test_an_instant_with_no_duration_bucket_is_unplaced() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100),
                fact(REV, 60, {BIZ: "x:CloudMember"}),
                fact(REV, 40, {BIZ: "x:RetailMember"}),
                fact("Assets", 500, {BIZ: "x:CloudMember"}, instant="2024-12-31"),
                fact("Assets", 400, {BIZ: "x:RetailMember"}, instant="2024-12-31"),
            ]
        ),
        AXES,
        ASSET_CONCEPTS,
    )
    assets = [f for f in out if f.metric_code == "total_assets"]
    assert len(assets) == 2 and all(f.partition_id == 0 for f in assets)


# 16. A CROSS-TAB CELL RECONCILES TO ITS PARENT, NOT TO THE COMPANY. Alphabet's real figures, with
#     `GoogleAdvertising` a subtotal of three cells that must land in partition 0.
def test_a_cross_tab_cell_reconciles_to_its_parent() -> None:
    gs = {BIZ: "goog:GoogleServicesMember"}
    out = segment_facts_from(
        instance(
            [
                fact(REV, 402836000000),
                fact(REV, 342721000000, gs),
                fact(REV, 58705000000, {BIZ: "goog:GoogleCloudMember"}),
                fact(REV, 1537000000, {BIZ: "goog:AllOtherSegmentsMember"}),
                fact(REV, 224532000000, {**gs, PROD: "goog:GoogleSearchOtherMember"}),
                fact(
                    REV,
                    48030000000,
                    {**gs, PROD: "goog:SubscriptionsPlatformsAndDevicesRevenueMember"},
                ),
                fact(REV, 40367000000, {**gs, PROD: "goog:YouTubeAdvertisingRevenueMember"}),
                fact(REV, 29792000000, {**gs, PROD: "goog:GoogleNetworkMember"}),
                fact(REV, 294691000000, {**gs, PROD: "goog:GoogleAdvertisingRevenueMember"}),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    flat = [f for f in out if f.parent_member is None]
    nested = [f for f in out if f.parent_member is not None]
    assert len(flat) == 3 and all(f.partition_id == 1 for f in flat)
    assert len(nested) == 5
    p1 = parts(nested, 1)
    assert len(p1) == 4 and total(p1) == 342721000000
    assert next(f for f in nested if "GoogleAdvertising" in f.member_code).partition_id == 0
    assert any("YouTube" in f.member_code for f in p1)
    assert all(f.axis == PROD and f.parent_axis == BIZ for f in nested)
    assert all(f.reconciled_to == 342721000000 for f in p1)


# ── The four rules the Korean spike forced out, each measured on a real DART filing ────────────

CS = "ifrs-full:ConsolidatedAndSeparateFinancialStatementsAxis"
CONSOLIDATED = "ifrs-full:ConsolidatedMember"
KR_AXES = [*AXES, AxisSpec(IPROD, "product", 100), AxisSpec(CS, "qualifier", 0, CONSOLIDATED)]


# 17. A PINNED QUALIFIER REJECTS THE WRONG MEMBER. The parent-only fact is on a member the
#     consolidated split does not have, so the duplicate rule cannot be what discards it.
def test_a_pinned_qualifier_keeps_only_the_consolidated_split() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100, {CS: CONSOLIDATED}),
                fact(REV, 60, {CS: CONSOLIDATED, IPROD: "x:AMember"}),
                fact(REV, 40, {CS: CONSOLIDATED, IPROD: "x:BMember"}),
                fact(REV, 30, {CS: "ifrs-full:SeparateMember", IPROD: "x:ParentOnlyMember"}),
            ]
        ),
        KR_AXES,
        CONCEPTS,
    )
    assert len(out) == 2 and all(f.partition_id == 1 for f in out)
    assert not any(f.value == 30 for f in out)


# 18. "NO SEGMENT DIMENSIONS" IS THE TOTAL, NOT "NO DIMENSIONS AT ALL".
def test_a_fact_carrying_only_qualifiers_is_the_total() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100, {CS: CONSOLIDATED}),
                fact(REV, 60, {CS: CONSOLIDATED, IPROD: "x:AMember"}),
                fact(REV, 40, {CS: CONSOLIDATED, IPROD: "x:BMember"}),
            ]
        ),
        KR_AXES,
        CONCEPTS,
    )
    assert len(out) == 2 and all(f.partition_id == 1 for f in out)


# 19. THE TARGET IS THE MEMBERS' OWN CONCEPT FIRST. SK Gas's real FY2024 figures, with `Revenues`
#     at the higher priority, which is what makes the two rules disagree.
def test_a_split_reconciles_against_its_own_concept_first() -> None:
    out = segment_facts_from(
        instance(
            [
                fact("Revenues", 7095902060317),
                fact(REV, 7050068258000),
                fact(REV, 6517517172000, prod("x:LpgMember")),
                fact(REV, 347127843000, prod("x:OtherMember")),
                fact(REV, 185423243000, prod("x:ElectricityMember")),
            ]
        ),
        AXES,
        [*CONCEPTS, ConceptSpec("revenue", "Revenues", 120)],
    )
    p1 = parts(out, 1)
    assert len(p1) == 3 and total(p1) == 7050068258000


# 20. AMONG TIED SPLITS, THE ONE A HUMAN CAN READ WINS; the generated one is still stored.
def test_a_readable_split_beats_a_generated_one_of_equal_size() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100),
                fact(REV, 60, prod("x:udf_NOTE_20231114182510391Member")),
                fact(REV, 40, prod("x:udf_NOTE_20231114182516257Member")),
                fact(REV, 60, prod("x:LpgSalesMember")),
                fact(REV, 40, prod("x:OtherSalesMember")),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    p1 = parts(out, 1)
    assert len(p1) == 2 and all("udf_" not in f.member_code for f in p1)
    assert len(parts(out, 2)) == 2


# The size gate that unwedged the queue: both ends asserted, because a gate rejecting Diageo would
# lose the foreign private issuers this feature exists to reach.
def test_the_instance_size_gate() -> None:
    mb = 1024 * 1024
    assert instance_is_too_large(127.72 * mb)  # AEP 2015 10-K killed the worker
    assert not instance_is_too_large(10.92 * mb)  # Diageo 20-F, the largest known to parse
    assert not instance_is_too_large(6.53 * mb)
    assert not instance_is_too_large(0.74 * mb)
    assert not instance_is_too_large(None)  # the HTML fallback carries no size


# A TARGET BELONGS TO ITS OWN BUCKET. The annual and quarterly splits share a period end and have
# different totals, so a target shared across the group is visibly the wrong one.
def test_each_bucket_carries_its_own_target() -> None:
    y = {"start": "2024-07-01", "end": Q_END}
    out = segment_facts_from(
        instance(
            [
                fact(REV, 1000, **y),
                fact(REV, 600, prod("x:AlphaMember"), **y),
                fact(REV, 400, prod("x:BetaMember"), **y),
                fact(REV, 250),
                fact(REV, 150, prod("x:AlphaMember")),
                fact(REV, 100, prod("x:BetaMember")),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    placed = [f for f in out if f.partition_id > 0]
    annual = [f for f in placed if f.period_type == "annual"]
    quarter = [f for f in placed if f.period_type == "quarter"]
    assert len(annual) == 2 and len(quarter) == 2
    assert all(f.reconciled_to == 1000 for f in annual)
    assert all(f.reconciled_to == 250 for f in quarter)
    for rows in (annual, quarter):
        assert total(rows) == rows[0].reconciled_to


# A TOTAL BELONGS TO A QUALIFIER CONTEXT. Samsung's FY2024 proportions: two splits in ONE filing
# must reach DIFFERENT targets, which no rule picking one "best" total per period can satisfy.
CONS_ITEMS = "ifrs-full:SegmentConsolidationItemsAxis"
SEGS = "ifrs-full:SegmentsAxis"
GEO = "ifrs-full:GeographicalAreasAxis"
OPS = "ifrs-full:OperatingSegmentsMember"
SAMSUNG_AXES = [
    *AXES,
    AxisSpec(CS, "qualifier", 0, CONSOLIDATED),
    AxisSpec(CONS_ITEMS, "qualifier", 0),
    AxisSpec(SEGS, "business", 90),
    AxisSpec(GEO, "geography", 80),
]


def test_each_split_reconciles_to_the_total_of_its_own_qualifier_context() -> None:
    c = CONSOLIDATED
    out = segment_facts_from(
        instance(
            [
                fact("Revenue", 300870, {CS: c}),
                # Dims in the OPPOSITE order to the divisions: the qualifier key has to be sorted.
                fact("Revenue", 329390, {CONS_ITEMS: OPS, CS: c}),
                fact(
                    "Revenue",
                    -28520,
                    {CS: c, CONS_ITEMS: "ifrs-full:MaterialReconcilingItemsMember"},
                ),
                fact("Revenue", 209050, {CS: "ifrs-full:SeparateMember"}),
                fact("Revenue", 174890, {CS: c, CONS_ITEMS: OPS, SEGS: "x:DxMember"}),
                fact("Revenue", 111070, {CS: c, CONS_ITEMS: OPS, SEGS: "x:DsMember"}),
                fact("Revenue", 29160, {CS: c, CONS_ITEMS: OPS, SEGS: "x:SdcMember"}),
                fact("Revenue", 14270, {CS: c, CONS_ITEMS: OPS, SEGS: "x:HarmanMember"}),
                fact("Revenue", 46640, {CS: c, GEO: "x:KoreaMember"}),
                fact("Revenue", 133270, {CS: c, GEO: "x:UsMember"}),
                fact("Revenue", 120960, {CS: c, GEO: "x:OtherMember"}),
            ]
        ),
        SAMSUNG_AXES,
        CONCEPTS,
    )
    placed = [f for f in out if f.partition_id > 0]
    seg = [f for f in placed if f.axis == SEGS]
    geo = [f for f in placed if f.axis == GEO]
    assert len(seg) == 4
    assert all(f.reconciled_to == 329390 for f in seg) and total(seg) == 329390
    assert len(geo) == 3
    assert all(f.reconciled_to == 300870 for f in geo) and total(geo) == 300870
    assert all(f.reconciled_to not in (209050, -28520) for f in placed)


def test_a_split_whose_members_disagree_about_their_qualifier_borrows_no_total() -> None:
    c = CONSOLIDATED
    out = segment_facts_from(
        instance(
            [
                fact("Revenue", 300870, {CS: c}),
                fact("Revenue", 329390, {CS: c, CONS_ITEMS: OPS}),
                fact("Revenue", 200000, {CS: c, CONS_ITEMS: OPS, SEGS: "x:AMember"}),
                fact("Revenue", 129390, {CS: c, SEGS: "x:BMember"}),
            ]
        ),
        SAMSUNG_AXES,
        CONCEPTS,
    )
    assert all(f.reconciled_to != 329390 for f in out if f.partition_id > 0)


# AN ELIMINATION COLUMN IS NOT A SEGMENT SPLIT. Hyundai Mobis's real figures, in millions, with
# production's pin (migration 173). The edge's check records that this fixture cannot reproduce the
# open-qualifier defect; the real document was verified both ways instead.
PINNED = [
    *AXES,
    AxisSpec(CS, "qualifier", 0, CONSOLIDATED),
    AxisSpec(CONS_ITEMS, "qualifier", 0, OPS),
    AxisSpec(SEGS, "business", 90),
]


def test_the_operating_column_is_the_split_and_the_elimination_column_is_rejected() -> None:
    c, elim = CONSOLIDATED, "ifrs-full:EliminationOfIntersegmentAmountsMember"
    placed = [
        f
        for f in segment_facts_from(
            instance(
                [
                    fact("Revenue", 61118127, {CS: c}),
                    fact("Revenue", 76544947, {CS: c, CONS_ITEMS: OPS}),
                    fact("Revenue", -15426820, {CS: c, CONS_ITEMS: elim}),
                    fact("Revenue", 58702105, {CS: c, CONS_ITEMS: OPS, SEGS: "x:AutoPartsMember"}),
                    fact("Revenue", 17842842, {CS: c, CONS_ITEMS: OPS, SEGS: "x:AfterSalesMember"}),
                    fact(
                        "Revenue", -10901986, {CS: c, CONS_ITEMS: elim, SEGS: "x:AutoPartsMember"}
                    ),
                    fact(
                        "Revenue", -4524834, {CS: c, CONS_ITEMS: elim, SEGS: "x:AfterSalesMember"}
                    ),
                    fact("Revenue", 0, {CS: c, CONS_ITEMS: OPS, SEGS: "x:CommonMember"}),
                    fact("Revenue", 0, {CS: c, CONS_ITEMS: elim, SEGS: "x:CommonMember"}),
                ]
            ),
            PINNED,
            CONCEPTS,
        )
        if f.partition_id > 0
    ]
    assert len(placed) == 3 and total(placed) == 76544947
    assert all(f.value >= 0 for f in placed)


def test_a_filer_that_omits_the_pinned_axis_is_untouched() -> None:
    c = CONSOLIDATED
    placed = [
        f
        for f in segment_facts_from(
            instance(
                [
                    fact("Revenue", 1000, {CS: c}),
                    fact("Revenue", 600, {CS: c, SEGS: "x:OneMember"}),
                    fact("Revenue", 400, {CS: c, SEGS: "x:TwoMember"}),
                ]
            ),
            PINNED,
            CONCEPTS,
        )
        if f.partition_id > 0
    ]
    assert len(placed) == 2 and total(placed) == 1000


# A RESIDUAL ALONE IS NOT A SPLIT (Chevron FY2025). Three plausible repairs each destroy correct
# data on this filing: dropping residuals breaks depreciation, demanding completeness breaks assets,
# demanding reconciliation breaks operating income. The shipped rule drops only revenue.
def test_a_residual_alone_is_not_served_and_beside_real_segments_it_is() -> None:
    seg, up, down, other = (
        BIZ,
        "cvx:UpstreamSegmentMember",
        "cvx:DownstreamSegmentMember",
        ("us-gaap:AllOtherSegmentsMember"),
    )
    d = "DepreciationDepletionAndAmortization"
    at = {"instant": "2025-12-31"}
    served = [
        f
        for f in segment_facts_from(
            instance(
                [
                    fact(d, 20132, **YEAR),
                    fact(d, 18445, {seg: up}, **YEAR),
                    fact(d, 1404, {seg: down}, **YEAR),
                    fact(d, 283, {seg: other}, **YEAR),
                    fact(REV, 231370, **YEAR),
                    fact(REV, 581, {seg: other}, **YEAR),
                    fact("Assets", 312218, **at),
                    fact("Assets", 256975, {seg: up}, **at),
                    fact("Assets", 55243, {seg: down}, **at),
                    fact("OperatingIncomeLoss", 120, **YEAR),
                    fact("OperatingIncomeLoss", 100, {seg: up}, **YEAR),
                    fact("OperatingIncomeLoss", 50, {seg: down}, **YEAR),
                ]
            ),
            AXES,
            [
                *CONCEPTS,
                ConceptSpec("depreciation", d, 100),
                ConceptSpec("total_assets", "Assets", 100),
            ],
            [MemberRole(other, "residual")],
        )
        if f.partition_id > 0
    ]

    def of(metric: str) -> list[SegmentFact]:
        return [f for f in served if f.metric_code == metric]

    assert of("revenue") == []
    assert len(of("depreciation")) == 3 and total(of("depreciation")) == 20132
    assert len(of("total_assets")) == 2 and total(of("total_assets")) == 312218
    assert len(of("operating_income")) == 2 and total(of("operating_income")) == 150


def test_a_subtotal_member_is_dropped_before_partitioning() -> None:
    # Chevron's "aggregation before other operating segments" reconciles exactly with AllOther and
    # would otherwise BE the chosen split.
    agg = "us-gaap:ReportableSegmentAggregationBeforeOtherOperatingSegmentMember"
    out = segment_facts_from(
        instance(
            [
                fact(REV, 231370, **YEAR),
                fact(REV, 230789, {BIZ: agg}, **YEAR),
                fact(REV, 581, {BIZ: "us-gaap:AllOtherSegmentsMember"}, **YEAR),
            ]
        ),
        AXES,
        CONCEPTS,
        [MemberRole(agg, "subtotal")],
    )
    assert [f.member_code for f in out] == ["us-gaap:AllOtherSegmentsMember"]
    assert out[0].partition_id == 0


# A SEGMENT COLUMN'S TOTAL, DERIVED FROM THE OTHER COLUMNS (Southern Copper FY2018): the segments
# carry no qualifier, and one elimination is stated under two member names, so only the
# distinct-VALUE sum matches.
def test_a_split_including_intersegment_sales_reconciles_to_consolidated_less_the_elimination() -> (
    None
):
    cons = "srt:ConsolidationItemsAxis"
    out = [
        f
        for f in segment_facts_from(
            instance(
                [
                    fact(REV, 7000, **YEAR),
                    fact(REV, -100, {cons: "us-gaap:IntersegmentEliminationMember"}, **YEAR),
                    fact(REV, -100, {cons: "x:CorporateAndEliminationsMember"}, **YEAR),
                    fact(REV, 4000, {BIZ: "x:NorthMember"}, **YEAR),
                    fact(REV, 3100, {BIZ: "x:SouthMember"}, **YEAR),
                ]
            ),
            AXES,
            CONCEPTS,
        )
        if f.partition_id > 0
    ]
    assert len(out) == 2 and total(out) == 7100
    assert out[0].reconciled_to == 7100


def test_an_offered_target_never_moves_a_split_that_already_reconciles() -> None:
    cons = "srt:ConsolidationItemsAxis"
    kept = [
        f
        for f in segment_facts_from(
            instance(
                [
                    fact(REV, 1000, **YEAR),
                    fact(REV, -50, {cons: "us-gaap:IntersegmentEliminationMember"}, **YEAR),
                    fact(REV, 600, {BIZ: "x:NorthMember"}, **YEAR),
                    fact(REV, 400, {BIZ: "x:SouthMember"}, **YEAR),
                ]
            ),
            AXES,
            CONCEPTS,
        )
        if f.partition_id > 0
    ]
    assert len(kept) == 2 and kept[0].reconciled_to == 1000


# A CROSS-TAB IS ALSO A FLAT SPLIT, SUMMED THE OTHER WAY. The cell axis is the product axis (100)
# and the parent the business axis (90), as in the Alphabet fixture.
GRID_PARENT = BIZ
GRID_CELL = PROD


def test_a_grid_with_no_stated_parent_total_is_summed_into_flat_splits() -> None:
    # Chevron: the grid carries a qualifier whose total (231,370) differs from the bare one
    # (184,432), so a marginal that dropped it would reconcile to nothing.
    opseg = {"srt:ConsolidationItemsAxis": "us-gaap:ReportableSegmentsMember"}
    out = segment_facts_from(
        instance(
            [
                fact(REV, 184432, **YEAR),
                fact(REV, 231370, opseg, **YEAR),
                fact(REV, 581, {**opseg, GRID_CELL: "us-gaap:AllOtherSegmentsMember"}, **YEAR),
                fact(
                    REV,
                    45518,
                    {**opseg, GRID_PARENT: "country:US", GRID_CELL: "x:UpstreamMember"},
                    **YEAR,
                ),
                fact(
                    REV,
                    72485,
                    {**opseg, GRID_PARENT: "country:US", GRID_CELL: "x:DownstreamMember"},
                    **YEAR,
                ),
                fact(
                    REV,
                    42861,
                    {**opseg, GRID_PARENT: "srt:NonUsMember", GRID_CELL: "x:UpstreamMember"},
                    **YEAR,
                ),
                fact(
                    REV,
                    69925,
                    {**opseg, GRID_PARENT: "srt:NonUsMember", GRID_CELL: "x:DownstreamMember"},
                    **YEAR,
                ),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    flat = [f for f in out if f.parent_member is None and f.partition_id > 0]

    def on(axis: str) -> list[SegmentFact]:
        return [f for f in flat if f.axis == axis]

    assert len(on(GRID_CELL)) == 3 and total(on(GRID_CELL)) == 231370
    assert next(f for f in flat if f.member_code == "x:UpstreamMember").value == 88379
    # Summed BY parent: 0.25% short of the total because AllOther has no geography, inside
    # tolerance.
    assert len(on(GRID_PARENT)) == 2 and total(on(GRID_PARENT)) == 230789
    assert next(f for f in flat if f.member_code == "country:US").value == 118003
    assert any(f.parent_member is not None for f in out)


def test_a_nested_hierarchy_is_never_served_at_its_inflated_flat_sum() -> None:
    # Novo: International Operations contains the regions beneath it.
    out = segment_facts_from(
        instance(
            [
                fact(REV, 310, **YEAR),
                fact(REV, 180, {GRID_PARENT: "x:USMember", GRID_CELL: "x:CareMember"}, **YEAR),
                fact(REV, 130, {GRID_PARENT: "x:IntlMember", GRID_CELL: "x:CareMember"}, **YEAR),
                fact(REV, 80, {GRID_PARENT: "x:EucanMember", GRID_CELL: "x:CareMember"}, **YEAR),
                fact(REV, 50, {GRID_PARENT: "x:ApacMember", GRID_CELL: "x:CareMember"}, **YEAR),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    served = total([f for f in out if f.parent_member is None and f.partition_id > 0])
    assert served != 620
    assert served in (0, 310)


def test_a_marginal_that_reconciles_only_to_its_parent_is_dropped() -> None:
    # Alphabet: the cells sum to Google Services, not to the company.
    out = segment_facts_from(
        instance(
            [
                fact(REV, 402836, **YEAR),
                fact(REV, 342721, {GRID_CELL: "x:ServicesMember"}, **YEAR),
                fact(REV, 58578, {GRID_CELL: "x:CloudMember"}, **YEAR),
                fact(REV, 1537, {GRID_CELL: "x:BetsMember"}, **YEAR),
                fact(
                    REV,
                    224532,
                    {GRID_CELL: "x:ServicesMember", GRID_PARENT: "x:SearchMember"},
                    **YEAR,
                ),
                fact(
                    REV,
                    118189,
                    {GRID_CELL: "x:ServicesMember", GRID_PARENT: "x:YouTubeMember"},
                    **YEAR,
                ),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    assert len([f for f in out if f.parent_member is None]) == 3
    assert len([f for f in out if f.parent_member is not None]) == 2


def test_a_filed_flat_member_keeps_its_filed_value() -> None:
    out = segment_facts_from(
        instance(
            [
                fact(REV, 150, **YEAR),
                fact(REV, 100, {GRID_CELL: "x:UpstreamMember"}, **YEAR),
                fact(REV, 50, {GRID_CELL: "x:DownstreamMember"}, **YEAR),
                fact(REV, 40, {GRID_PARENT: "country:US", GRID_CELL: "x:UpstreamMember"}, **YEAR),
                fact(
                    REV, 50, {GRID_PARENT: "srt:NonUsMember", GRID_CELL: "x:UpstreamMember"}, **YEAR
                ),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    up = [f for f in out if f.parent_member is None and f.member_code == "x:UpstreamMember"]
    assert len(up) == 1 and up[0].value == 100


# ── Port fidelity: the places where Python and JavaScript differ by default ────────────────────


def test_sums_add_left_to_right_like_javascript() -> None:
    # `sum()` compensates since Python 3.12; JS does not. 0.1 + 0.2 + 0.3 is 0.6000000000000001 in
    # JS and plain left-to-right addition, 0.6 under compensation.
    assert segments._add_up([0.1, 0.2, 0.3]) == 0.1 + 0.2 + 0.3 != 0.6


def test_the_subset_search_matches_brute_force_mask_order() -> None:
    # The edge enumerated masks from 1 upward and kept the first with the most members, then the
    # fewest generated codes. Compare against that enumeration on a case with ties.
    members = [("x:udf_a", 30.0), ("x:A", 30.0), ("x:B", 70.0), ("x:udf_b", 70.0), ("x:C", 100.0)]

    def brute(ms: list[tuple[str, float]], target: float) -> list[int] | None:
        best: list[int] | None = None
        best_gen = 0
        for mask in range(1, 1 << len(ms)):
            idx = [i for i in range(len(ms)) if mask >> i & 1]
            s = 0.0
            for i in idx:
                s += ms[i][1]
            if abs(s - target) > segments._tolerance(target):
                continue
            gen = sum(1 for i in idx if ":udf_" in ms[i][0])
            if best is None or len(idx) > len(best) or (len(idx) == len(best) and gen < best_gen):
                best, best_gen = idx, gen
        return best

    for target in (100.0, 130.0, 200.0, 300.0, 1.0):
        assert segments._max_subset_summing_to(members, target) == brute(members, target)


def test_the_parser_strips_like_javascript_trim() -> None:
    contexts = segments.parse_contexts(
        '<context id="c1"><entity><segment><xbrldi:explicitMember dimension="\ufeffa:Axis">'
        "\u00a0a:M\u00a0</xbrldi:explicitMember></segment></entity><period>"
        "<instant>\ufeff2025-06-30 </instant></period></context>"
    )
    assert contexts["c1"].dims == {"a:Axis": "a:M"} and contexts["c1"].instant == "2025-06-30"


def test_equal_priority_axes_are_ordered_as_locale_compare_orders_them() -> None:
    # Codepoint order puts "B" before "a"; ICU's root collation compares letters case-blind first
    # and punctuation before letters. The expected order is Node 20's `localeCompare`, checked
    # 2026-11-09.
    keys = sorted(
        ["x:bAxis", "x:Aaxis", "x:aAxis", "x:B-Axis", "x:1Axis"], key=segments._collation_key
    )
    assert keys == ["x:1Axis", "x:aAxis", "x:Aaxis", "x:B-Axis", "x:bAxis"]
    names = [
        "a_b",
        "a-b",
        "a:b",
        "a.b",
        "a0",
        "ab",
        "aB",
        "Ab",
        "AB",
        "ifrs_full:X",
        "ifrs-full:SegmentsAxis",
    ]
    assert sorted(reversed(names), key=segments._collation_key) == [
        *names[:9],
        "ifrs_full:X",
        names[10],
    ]


# ── Finding the instance ───────────────────────────────────────────────────────────────────────


def test_instance_names_cover_both_eras_and_no_linkbase() -> None:
    assert segments.is_instance_name("aapl-20250628_htm.xml")
    assert segments.is_instance_name("aapl-20150926.xml")
    for name in (
        "aapl-20150926_cal.xml",
        "aapl-20150926_lab.xml",
        "FilingSummary.xml",
        "x.xsd",
        "x.xml",
    ):
        assert not segments.is_instance_name(name)


def test_index_json_gives_the_shortest_instance_and_its_size() -> None:
    body = json.dumps(
        {
            "directory": {
                "item": [
                    {"name": "aep-20151231_cal.xml", "size": "100"},
                    {"name": "aep-20151231a-20151231.xml", "size": "5"},
                    {"name": "aep-20151231.xml", "size": "133922045"},
                    {"name": "FilingSummary.xml", "size": ""},
                ]
            }
        }
    )
    name, size = segments.instance_from_index_json(body) or ("", None)
    assert name == "aep-20151231.xml" and size == 133922045
    assert instance_is_too_large(size)


def test_an_incomplete_or_unreadable_index_json_falls_through_to_the_html() -> None:
    # Diageo's FY2025 20-F: `index.json` lists the zip and three empty index files, not the
    # instance.
    diageo = json.dumps(
        {"directory": {"item": [{"name": "0000835403-25-000026-xbrl.zip", "size": "9"}]}}
    )
    assert segments.instance_from_index_json(diageo) is None
    assert segments.instance_from_index_json("<html>") is None
    assert segments.instance_from_index_json(json.dumps({"directory": {"item": [None]}})) is None
    html = (
        '<a href="/Archives/edgar/data/835403/000083540325000026/deo-20250630_htm.xml">x</a>'
        '<a href="/Archives/edgar/data/835403/000083540325000026/deo-20250630_lab.xml">x</a>'
    )
    assert segments.instance_from_index_html(html) == "deo-20250630_htm.xml"
    assert segments.instance_from_index_html("<html></html>") is None


#: Instance names filing agents gave, each from a filing the edge recorded as having no instance
#: (read on the Dagster lane's first wave, 2026-11-14).
AGENT_NAMED = [
    "scco-20260930x10q_htm.xml",  # Toppan Merrill
    "form10-q_htm.xml",  # M2 Compliance
    "bomn20260930_10q_htm.xml",  # RDG
    "desp20260930_10q_htm.xml",
    "lwg_10q-093026_htm.xml",
    "actuate_i10q-093026_htm.xml",
]


@pytest.mark.parametrize("name", AGENT_NAMED)
def test_an_instance_a_filing_agent_named_is_found_in_both_listings(name: str) -> None:
    """The edge's dated rule rejects every one of these, so the filing read as having no
    instance, permanently. 7 of the first 11 such filings read carried segments."""
    assert not segments.is_instance_name(name), "the edge's rule must stay as it was"
    stem = name.removesuffix("_htm.xml")
    body = json.dumps(
        {
            "directory": {
                "item": [
                    {"name": "0001104659-26-120639-xbrl.zip", "size": "900"},
                    {"name": "FilingSummary.xml", "size": "40"},
                    {"name": "scco-20260930.xsd", "size": "50"},
                    {"name": "scco-20260930_cal.xml", "size": "60"},
                    {"name": "scco-20260930_lab.xml", "size": "70"},
                    {"name": name, "size": "5010000"},
                    {"name": f"{stem}.htm", "size": "2000000"},
                ]
            }
        }
    )
    assert segments.instance_from_index_json(body) == (name, 5010000)
    html = (
        f'<a href="/Archives/edgar/data/1001838/000110465926120639/{name}">x</a>'
        '<a href="/Archives/edgar/data/1001838/000110465926120639/scco-20260930_def.xml">x</a>'
        '<a href="/Archives/edgar/data/1001838/000110465926120639/FilingSummary.xml">x</a>'
    )
    assert segments.instance_from_index_html(html) == name


def test_the_edge_s_dated_name_still_wins_when_both_kinds_are_listed() -> None:
    """Every filing the edge read must pick the same file. The agent-style name here is SHORTER
    than the dated one, so a rule ranking all candidates by length together would pick it."""
    body = json.dumps(
        {
            "directory": {
                "item": [
                    {"name": "q_htm.xml", "size": "10"},
                    {"name": "aapl-20250628_htm.xml", "size": "960000"},
                ]
            }
        }
    )
    assert segments.instance_from_index_json(body) == ("aapl-20250628_htm.xml", 960000)
    html = '<a href="/x/q_htm.xml">x</a><a href="/x/aapl-20250628_htm.xml">x</a>'
    assert segments.instance_from_index_html(html) == "aapl-20250628_htm.xml"


def test_two_extracted_instances_and_no_dated_one_are_no_instance() -> None:
    """SEC extracts one instance per inline XBRL filing; two would mean we cannot tell which is
    the company's, so the filing reads as it did before rather than as a guess."""
    body = json.dumps(
        {
            "directory": {
                "item": [
                    {"name": "a10q_htm.xml", "size": "1"},
                    {"name": "ex99_htm.xml", "size": "2"},
                ]
            }
        }
    )
    assert segments.instance_from_index_json(body) is None
    assert (
        segments.instance_from_index_html(
            '<a href="/x/a10q_htm.xml">x</a><a href="/x/ex99_htm.xml">x</a>'
        )
        is None
    )
    # A name listed twice is still one instance.
    twice = json.dumps(
        {
            "directory": {
                "item": [
                    {"name": "a10q_htm.xml", "size": "1"},
                    {"name": "a10q_htm.xml", "size": "9"},
                ]
            }
        }
    )
    assert segments.instance_from_index_json(twice) == ("a10q_htm.xml", 1)
    # A linkbase is never an instance, whatever it ends with.
    assert not segments.is_extracted_instance_name("form10-q_htm_cal.xml")


def test_an_unknown_size_is_not_a_zero_size() -> None:
    body = json.dumps({"directory": {"item": [{"name": "x-20250630_htm.xml", "size": ""}]}})
    assert segments.instance_from_index_json(body) == ("x-20250630_htm.xml", None)


# ── Rules the edge's checks never exercised, each found by mutating this port ──────────────────


def test_period_type_windows_are_inclusive_at_both_ends() -> None:
    def days(n: int) -> str | None:
        return period_type_for("2025-01-01", (date(2025, 1, 1) + timedelta(days=n)).isoformat())

    assert [days(n) for n in (83, 84, 98, 99)] == [None, "quarter", "quarter", None]
    assert [days(n) for n in (357, 358, 372, 373)] == [None, "annual", "annual", None]


def test_the_tolerance_is_half_a_percent_inclusive_with_a_floor_of_one() -> None:
    assert set(assign_partitions([("A", 600.0), ("B", 405.0)], 1000).values()) == {1}
    assert set(assign_partitions([("A", 600.0), ("B", 405.5)], 1000).values()) == {0}
    assert set(assign_partitions([("A", 6.0), ("B", 5.0)], 10).values()) == {1}
    assert set(assign_partitions([("A", 6.0), ("B", 5.5)], 10).values()) == {0}


def test_among_equal_splits_the_first_in_document_order_is_partition_one() -> None:
    # {A,B}, {B,C}, {A,D} and {C,D} all sum to 100; the edge's mask order finds {A,B} first.
    out = assign_partitions([("A", 60.0), ("B", 40.0), ("C", 60.0), ("D", 40.0)], 100)
    assert out == {"A": 1, "B": 1, "C": 2, "D": 2}


def test_when_two_buckets_place_as_many_members_the_first_one_s_split_is_shared() -> None:
    # Revenue places {A, B}, operating income {B, C}: two members each. The first bucket in document
    # order decides, so revenue's split is served and operating income inherits it.
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100),
                fact(REV, 60, {BIZ: "x:AMember"}),
                fact(REV, 40, {BIZ: "x:BMember"}),
                fact(REV, 50, {BIZ: "x:CMember"}),
                fact("OperatingIncomeLoss", 50),
                fact("OperatingIncomeLoss", 10, {BIZ: "x:AMember"}),
                fact("OperatingIncomeLoss", 20, {BIZ: "x:BMember"}),
                fact("OperatingIncomeLoss", 30, {BIZ: "x:CMember"}),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    served = {(f.metric_code, f.member_code) for f in out if f.partition_id == 1}
    assert served == {
        ("revenue", "x:AMember"),
        ("revenue", "x:BMember"),
        ("operating_income", "x:AMember"),
        ("operating_income", "x:BMember"),
    }


def test_equal_priority_axes_in_a_cross_tab_follow_locale_order() -> None:
    lower, upper = "x:aSegmentAxis", "x:BSegmentAxis"
    axes = [*AXES, AxisSpec(lower, "business", 95), AxisSpec(upper, "business", 95)]
    out = segment_facts_from(
        instance(
            [
                fact(REV, 100),
                fact(REV, 100, {upper: "x:PMember"}),
                fact(REV, 70, {upper: "x:PMember", lower: "x:CMember"}),
                fact(REV, 30, {upper: "x:PMember", lower: "x:DMember"}),
            ]
        ),
        axes,
        CONCEPTS,
    )
    nested = [f for f in out if f.parent_member is not None]
    # `localeCompare` puts "x:a..." before "x:B...", so `lower` is the enumerated axis.
    assert nested and all(f.axis == lower and f.parent_axis == upper for f in nested)


def test_the_member_s_own_concept_beats_a_higher_priority_concept_under_the_same_qualifier() -> (
    None
):
    ops = {"srt:ConsolidationItemsAxis": "us-gaap:OperatingSegmentsMember"}
    out = segment_facts_from(
        instance(
            [
                fact(REV, 1000, ops),
                fact("Revenues", 1004, ops),
                fact(REV, 600, {**ops, BIZ: "x:AMember"}),
                fact(REV, 400, {**ops, BIZ: "x:BMember"}),
            ]
        ),
        AXES,
        [*CONCEPTS, ConceptSpec("revenue", "Revenues", 120)],
    )
    assert len(out) == 2 and all(f.reconciled_to == 1000 for f in out)


EQUINOR_AXES = [
    AxisSpec(PROD, "product", 100),
    AxisSpec(BIZ, "business", 90),
    AxisSpec("srt:ConsolidationItemsAxis", "qualifier", 0, "us-gaap:OperatingSegmentsMember"),
]


def test_a_pinned_elimination_is_still_arithmetic_for_the_segment_column_total() -> None:
    # Equinor states the consolidated 106,462m and an elimination of -42,421m, both on the pinned
    # axis, and its segments sum to the difference, 148,883m. The elimination can never be a member,
    # and must still be recorded before the pin rejects it.
    qual = "srt:ConsolidationItemsAxis"
    ops = {qual: "us-gaap:OperatingSegmentsMember"}
    out = segment_facts_from(
        instance(
            [
                fact("Revenue", 106462),
                fact("Revenue", -42421, {qual: "us-gaap:IntersegmentEliminationMember"}),
                fact("Revenue", 100000, {**ops, BIZ: "x:ExplorationMember"}),
                fact("Revenue", 48883, {**ops, BIZ: "x:MarketingMember"}),
            ]
        ),
        EQUINOR_AXES,
        CONCEPTS,
    )
    assert len(out) == 2 and all(f.partition_id == 1 and f.reconciled_to == 148883 for f in out)


def test_when_nothing_reconciles_the_narrow_derived_total_is_the_target() -> None:
    # The members (700 + 500 + a 300 corporate line) sum to no stated or derived total, so
    # precedence decides: the derived 1,200 (1,000 less a -200 elimination) comes before the plain
    # 1,000, and the subset that sums to it is the split. Against 1,000 it would be {A, C}.
    qual = "srt:ConsolidationItemsAxis"
    ops = {qual: "us-gaap:OperatingSegmentsMember"}
    out = segment_facts_from(
        instance(
            [
                fact(REV, 1000),
                fact(REV, -200, {qual: "us-gaap:IntersegmentEliminationMember"}),
                fact(REV, 700, {**ops, BIZ: "x:AMember"}),
                fact(REV, 500, {**ops, BIZ: "x:BMember"}),
                fact(REV, 300, {**ops, BIZ: "x:CMember"}),
            ]
        ),
        AXES,
        CONCEPTS,
    )
    placed = {f.member_code: f.reconciled_to for f in out if f.partition_id == 1}
    assert placed == {"x:AMember": 1200, "x:BMember": 1200}
