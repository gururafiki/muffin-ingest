"""CNINFO's side of the China segment lane: the search answer, the stock lists, which report is
which, what raw holds, and what retention may drop. Over a captured answer where one exists."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from muffin_ingest.facets import cninfo as cn
from muffin_ingest.facets.segments import SegmentFact
from muffin_ingest.providers import cninfo

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_a_captured_search_answer_names_each_report_by_its_public_link() -> None:
    """China Yangtze Power's first page, captured 2026-11-15: 30 announcements of 61, so the page is
    the newest 30 (the rest is the deferred widening). Every link is on the STATIC host: a stored
    bare path would render as a dead link and nothing downstream could tell."""
    body = (FIXTURES / "cninfo_announcements_600900.json").read_bytes()
    found = cn.parse_announcements(body)
    assert len(found) == 30
    assert all(a.url.startswith("https://static.cninfo.com.cn/finalpage/") for a in found)
    # The category also lists six `主要财务指标` data files (`.js`), under a title `classify`
    # reads as a full report: they are filed, and never fetched.
    data_files = [a for a in found if not a.url.endswith(".PDF")]
    assert len(data_files) == 6 and all(a.url.endswith(".js") for a in data_files)
    assert all(a.kind == "full" and not a.is_report for a in data_files)
    assert sum(a.is_report for a in found) == sum(a.kind == "full" for a in found) - 6
    newest = next(a for a in found if a.title == "长江电力2025年年度报告")
    assert newest.kind == "full"
    # CNINFO STAMPS BEIJING MIDNIGHT, so 30 April arrives as 16:00 UTC on the 29th; the edge
    # stored the UTC date, the day before the one the report's own link is filed under.
    assert newest.date == "2026-04-30"
    assert f"/{newest.date}/" in newest.url
    assert newest.url == "https://static.cninfo.com.cn/finalpage/2026-04-30/1225262036.PDF"
    assert newest.size_kb == 1499
    kinds = {a.kind for a in found}
    assert kinds == {"full", "summary"}


@pytest.mark.parametrize(
    ("title", "kind"),
    [
        ("中国长江电力股份有限公司2025年年度报告", "full"),
        ("长江电力2025年年度报告摘要", "summary"),
        ("2024年年度报告（更正后）", "full"),
        ("2024年年度报告摘要（更正版）", "summary"),
        ("振华重工2025年年度报告（英文版）", "english"),
        ("2025 Annual Report", "english"),
    ],
)
def test_the_title_says_which_of_the_three_documents_it_is(title: str, kind: str) -> None:
    """A corrected SUMMARY stays a summary: `更正` is no branch, so it cannot displace `摘要`."""
    assert cn.classify(title) == kind


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1777478400000, "2026-04-30"),
        (1777478399999, "2026-04-29"),
        (0, None),
        (-5, None),
        ("1777478400000", None),
        (None, None),
        (True, None),
        (float("nan"), None),
        (float("inf"), None),
    ],
)
def test_a_date_is_epoch_milliseconds_and_anything_else_is_an_absence(
    value: Any, expected: str | None
) -> None:
    assert cn.iso_from_epoch_ms(value) == expected


def test_an_answer_that_is_not_a_search_yields_nothing_rather_than_raising() -> None:
    assert cn.parse_announcements(b"<html>error</html>") == []
    assert cn.parse_announcements(b'{"announcements": null}') == []
    assert cn.parse_announcements(b'{"announcements": [{"announcementTitle": "x"}]}') == []


def test_the_stock_lists_map_six_digit_codes_to_org_ids() -> None:
    """The shape measured 2026-11-15 (`category`, `code`, `orgId`, `pinyin`, `zwjc`). A Beijing
    org id need not carry its code (`9900037542`), so nothing may derive one from the other."""
    sz = json.dumps(
        {
            "stockList": [
                {"category": "A股", "code": "000001", "orgId": "gssz0000001", "zwjc": "平安银行"},
                {"category": "A股", "code": "600900", "orgId": "gssh0600900", "zwjc": "长江电力"},
                {"category": "A股", "code": "BAD", "orgId": "x"},
            ]
        }
    ).encode()
    bj = json.dumps(
        {"stockList": [{"category": "A股", "code": "920001", "orgId": "9900037542"}]}
    ).encode()
    assert cn.org_ids([sz, bj]) == {
        "000001": "gssz0000001",
        "600900": "gssh0600900",
        "920001": "9900037542",
    }


def test_a_filing_row_never_carries_the_read_marks() -> None:
    """A re-walk writes these columns only, so it can never make a report look unread; `filing_date`
    is the announcement date, which the ownership rule orders filings by."""
    found = cn.parse_announcements((FIXTURES / "cninfo_announcements_600900.json").read_bytes())
    rows = cn.filing_rows(["s1", "s2"], found[:2])
    assert len(rows) == 4
    for row in rows:
        assert "segments_parsed_at" not in row and "segments_parser_version" not in row
        assert row["filing_date"] == row["report_date"]
        assert row["is_xbrl"] is False
        assert row["source_code"] == "cninfo"
    assert {r["report_type"] for r in rows} <= {cn.FULL, cn.SUMMARY, cn.ENGLISH}


def _report(url: str, outcome: str, fetched: str, run: str, **extra: Any) -> dict[str, Any]:
    return {
        "document": "report",
        "accession": url,
        "outcome": outcome,
        "fetched_at": fetched,
        "run_id": run,
        "body": b"%PDF-1.7 ..." if outcome == "answered" else b"",
        **extra,
    }


def test_the_newest_fetch_decides_what_raw_holds_for_a_report() -> None:
    rows = [
        _report("u", "answered", "2026-11-01T00:00:00+00:00", "r1"),
        _report("u", "absent", "2026-11-10T00:00:00+00:00", "r2"),
    ]
    held = cn.held_report(rows, "u")
    assert held is not None and held.outcome == "absent"
    assert cn.report_body(rows, held) is None, "an older fetch's body is not this fetch's"
    assert cn.held_report(rows, "other") is None


def test_a_pruned_report_is_recorded_but_not_held() -> None:
    """Retention drops the body and stamps `pruned_at`; stage 1 must fetch it again when it is due,
    and stage 2 must not read a body that is gone."""
    rows = [_report("u", "answered", "2026-11-01T00:00:00+00:00", "r1", pruned_at="2026-12-01")]
    rows[0]["body"] = None
    held = cn.held_report(rows, "u")
    assert held is not None and held.pruned
    assert cn.report_body(rows, held) is None


def test_the_newest_answered_search_is_the_one_stage_2_reads() -> None:
    rows = [
        {
            "document": "announcements",
            "accession": "page-1",
            "http_status": 200,
            "body": b"old",
            "fetched_at": "2026-10-01",
        },
        {
            "document": "announcements",
            "accession": "page-1",
            "http_status": 200,
            "body": b"new",
            "fetched_at": "2026-11-01",
        },
        {
            "document": "announcements",
            "accession": "page-1",
            "http_status": 404,
            "body": b"",
            "fetched_at": "2026-12-01",
        },
    ]
    newest = cn.newest_list(rows)
    assert newest is not None and newest["body"] == b"new"


@pytest.mark.parametrize(
    ("status", "body", "too_large", "outcome"),
    [
        (200, b"%PDF-1.7", False, "answered"),
        (200, b"<html>", False, "not_pdf"),
        (404, b"", False, "absent"),
        (200, b"", True, "too_large"),
    ],
)
def test_a_report_fetch_ends_in_one_of_four_ways(
    status: int, body: bytes, too_large: bool, outcome: str
) -> None:
    from muffin_ingest.providers.documents import Document

    answer = cninfo.Answer(
        Document("u", body, "application/pdf", datetime.now(UTC)), status, too_large
    )
    assert cn.report_outcome(answer) == outcome


NOW = datetime(2026, 12, 1, tzinfo=UTC)
GB = 1024**3


def _held(url: str, size: int, read_days_ago: int | None) -> cn.HeldReport:
    read = None if read_days_ago is None else NOW - timedelta(days=read_days_ago)
    return cn.HeldReport("k", url, size, read)


def test_retention_drops_read_reports_past_thirty_days_and_never_an_unread_one() -> None:
    held = [_held("old", 1, 31), _held("recent", 1, 2), _held("unread", 1, None)]
    assert [h.url for h in cn.choose_prunes(held, now=NOW)] == ["old"]


def test_over_the_cap_retention_drops_read_reports_oldest_read_first_down_to_the_target() -> None:
    held = [
        _held("a", 2 * GB, 5),
        _held("b", 2 * GB, 3),
        _held("c", 2 * GB, 1),
        _held("unread", 1 * GB, None),
    ]
    # 7 GB held, cap 5, target 4: dropping `a` leaves 5 (still over the target), `b` leaves 3.
    assert [h.url for h in cn.choose_prunes(held, now=NOW)] == ["a", "b"]


def test_an_unread_backlog_over_the_cap_loses_nothing() -> None:
    held = [_held(f"u{n}", 2 * GB, None) for n in range(4)]
    assert cn.choose_prunes(held, now=NOW) == []


def test_a_report_link_is_fetched_through_the_cache_and_stored_as_the_public_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for var in ("CNINFO_STATIC_BASE_URL", "CNINFO_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")
    public = "https://static.cninfo.com.cn/finalpage/2026-04-30/1225262036.PDF"
    assert (
        cninfo.report_fetch_url(public)
        == "http://http-cache:8080/cninfo-static/finalpage/2026-04-30/1225262036.PDF"
    )
    assert cninfo.announcements_url() == "http://http-cache:8080/cninfo/new/hisAnnouncement/query"
    with pytest.raises(ValueError):
        cninfo.report_fetch_url("https://example.com/x.PDF")


def test_the_search_form_is_the_one_the_edge_sent() -> None:
    """`stock=<code>,<orgId>`: a bare code answers a 200 with no announcements."""
    assert cninfo.announcements_form("600900", "gssh0600900") == {
        "stock": "600900,gssh0600900",
        "tabName": "fulltext",
        "pageSize": 30,
        "pageNum": 1,
        "category": "category_ndbg_szsh",
    }


# --- a table whose stated unit is wrong -----------------------------------------------------------


def _fact(
    axis: str,
    member: str,
    value: float,
    *,
    metric: str = "revenue",
    partition: int = 1,
    ending: str = "2025-12-31",
) -> SegmentFact:
    return SegmentFact(
        axis=axis, member_code=member, metric_code=metric, period_type="annual",
        period_start=ending[:4] + "-01-01", period_ending=ending, value=value, currency="CNY",
        parent_axis=None, parent_member=None, reconciled_to=None if metric != "revenue" else 1.0,
        partition_id=partition,
    )  # fmt: skip


def test_a_split_ten_thousand_times_too_small_is_put_right_by_the_company_s_revenue() -> None:
    # 航天长峰 FY2025: `单位：元` over figures its own text states in 万元.
    facts = [
        _fact("cninfo:分行业", "cninfo:军工电子", 54_294.31),
        _fact("cninfo:分行业", "cninfo:其他", 68_000.00),
        _fact("cninfo:分行业", "cninfo:军工电子", 40_000.00, metric="cost_of_revenue"),
        # Another table of the same report, stated in the right unit: left alone.
        _fact("cninfo:分地区", "cninfo:境内", 1_222_943_100.00),
    ]
    revenue = {date(2025, 12, 31): 1_249_406_904.66}
    out, rescaled = cn.rescale_misstated_units(facts, revenue)
    assert rescaled == {("cninfo:分行业", "2025-12-31"): 10_000}
    assert [f.value for f in out] == [542_943_100.0, 680_000_000.0, 400_000_000.0, 1_222_943_100.0]
    assert out[0].reconciled_to == 10_000.0 and out[3].reconciled_to == 1.0


def test_a_split_a_thousand_times_too_small_is_put_right_too() -> None:
    facts = [_fact("cninfo:分行业", "cninfo:医药", 4_829_871.79)]
    out, rescaled = cn.rescale_misstated_units(facts, {date(2022, 12, 31): 4_829_871_790.0})
    assert rescaled == {} and out == facts  # wrong year: nothing to compare with
    facts = [_fact("cninfo:分行业", "cninfo:医药", 4_829_871.79, ending="2022-12-31")]
    out, rescaled = cn.rescale_misstated_units(facts, {date(2022, 12, 31): 4_829_871_790.0})
    assert rescaled == {("cninfo:分行业", "2022-12-31"): 1_000}
    assert out[0].value == 4_829_871_790.0


@pytest.mark.parametrize(
    ("revenue", "why"),
    [
        ({}, "no stated revenue: no evidence, no change"),
        ({date(2025, 12, 31): 122_294.31 * 1.01}, "a split that is right"),
        ({date(2025, 12, 31): 122_294.31 * 10_000 * 5}, "a fifth of revenue is not this table"),
        ({date(2026, 1, 31): 122_294.31 * 10_000}, "a revenue a month away is another year"),
    ],
)
def test_a_split_is_left_as_read_without_evidence_that_its_unit_is_wrong(
    revenue: dict[date, float], why: str
) -> None:
    facts = [
        _fact("cninfo:分行业", "cninfo:军工电子", 54_294.31),
        _fact("cninfo:分行业", "cninfo:其他", 68_000.00),
    ]
    out, rescaled = cn.rescale_misstated_units(facts, revenue)
    assert rescaled == {} and out == facts, why


def test_a_revenue_dated_within_ten_days_of_the_period_end_is_that_year() -> None:
    facts = [_fact("cninfo:分行业", "cninfo:军工电子", 122_294.31)]
    _, rescaled = cn.rescale_misstated_units(facts, {date(2025, 12, 28): 1_222_943_100.0})
    assert rescaled == {("cninfo:分行业", "2025-12-31"): 10_000}


def test_the_hong_kong_list_maps_an_h_share_onto_its_a_share_s_org_id() -> None:
    """Measured 2026-11-26: 215 of `hke_stock.json`'s 2,798 rows carry an A-share's org id. Only
    those map, keyed by the code as a number (`00939` -> 939, the form a listing symbol takes once
    its zeros are stripped). A Hong Kong company with no A-share has no CNINFO filer, and an A-share
    code never reaches this map."""
    a = json.dumps(
        {"stockList": [{"code": "601939", "orgId": "9900000062"},
                       {"code": "600900", "orgId": "gssh0600900"}]}
    ).encode()  # fmt: skip
    hk = json.dumps(
        {"stockList": [{"code": "00939", "orgId": "9900000062"},
                       {"code": "00001", "orgId": "gshk0000001"},
                       {"code": "BAD", "orgId": "9900000062"}]}
    ).encode()  # fmt: skip
    assert cn.hk_org_ids([a, hk]) == {939: "9900000062"}
    assert cn.org_ids([a, hk]) == {"601939": "9900000062", "600900": "gssh0600900"}


def test_a_filer_s_reports_reach_every_holder_even_one_with_no_six_digit_listing() -> None:
    """Joining each holder to its OWN six-digit listing dropped an H-share line or an issuer sibling
    from the filer's security list without a word, so a mapped holder never received a report. The
    code the search is asked with comes from ANY holder's listing; the holders come from
    `security_filer` alone."""
    text = " ".join(cn.FILERS.split())
    assert "join market.listing l on l.security_id = sf.security_id" not in text
    assert "array_agg(distinct sf.security_id::text) from market.security_filer sf where" in text
    assert "where o.source_code = 'cninfo' and o.filer_id = sf.filer_id" in text
