"""SEC's segment lane: the queue, the raw rows, and what raw holds for a filing."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from muffin_ingest.facets import sec_segments, segments
from muffin_ingest.facets.segments import SegmentFact
from muffin_ingest.providers.documents import Document
from muffin_ingest.providers.sec_edgar import InstanceFetch, SecAnswer

ACCESSION = "0000320193-26-000020"


def _answer(url: str, body: bytes, status: int = 200, at: str = "2026-11-10T12:00:00") -> SecAnswer:
    fetched = datetime.fromisoformat(at).replace(tzinfo=UTC)
    return SecAnswer(Document(url, body, "text/xml", fetched), status)


def _fetch(outcome: str, *documents: tuple[str, SecAnswer]) -> InstanceFetch:
    return InstanceFetch(tuple(documents), "x-20260627_htm.xml", outcome)  # type: ignore[arg-type]


class Cursor:
    def __init__(self, answers: list[list[tuple[Any, ...]]], calls: list[tuple[str, Any]]) -> None:
        self.answers, self.calls = answers, calls
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self.calls.append((" ".join(sql.split()), params))
        self.rows = self.answers.pop(0) if self.answers else []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def __init__(self, *answers: list[tuple[Any, ...]]) -> None:
        self.answers = list(answers)
        self.calls: list[tuple[str, Any]] = []

    def cursor(self) -> Cursor:
        return Cursor(self.answers, self.calls)


# ── the queue ──────────────────────────────────────────────────────────────────────────────────


def test_a_filing_is_due_only_as_an_xbrl_accounts_form_of_an_equity_unread_at_this_version() -> (
    None
):
    """The predicate is one piece of SQL shared by the sensor, both stages and the check, so the
    four cannot disagree about what is left."""
    for sql in (sec_segments.DUE_BY_FILER, sec_segments.FILINGS, sec_segments.UNREAD):
        text = " ".join(sql.split())
        assert "f.is_xbrl" in text
        assert "s.security_type_code = 'equity'" in text
        # THE REGULATOR IS ON THE FORM, never the filing's `source_code`, which names the lane that
        # wrote the row (`sec-submissions`) and would match nothing.
        assert "where source_code = 'sec' and carries_segments" in text
        assert "f.source_code" not in text
        assert "f.segments_parsed_at is null" in text
        assert "coalesce(f.segments_parser_version, 0) < (select max(version)" in text


def test_filings_come_oldest_first_with_the_securities_to_write_them_for() -> None:
    both = ["s-goog", "s-googl"]
    conn = Conn(
        [
            (320193, "0000320193-26-000020", date(2026, 7, 31), ["s-goog"], both),
            (320193, "0000320193-25-000079", date(2025, 8, 1), both, both),
            # Named and not due: every security holding it is written.
            (320193, "0000320193-24-000081", date(2024, 8, 2), None, both),
        ]
    )
    found = sec_segments.filings(conn, [320193], ["0000320193-24-000081"])[320193]

    assert [f.accession[11:13] for f in found] == ["24", "25", "26"]
    assert [f.due for f in found] == [False, True, True]
    assert found[0].security_ids == ("s-goog", "s-googl")
    assert found[2].security_ids == ("s-goog",), "a share class already read is not rewritten"
    sql, params = conn.calls[0]
    assert params == ([320193], ["0000320193-24-000081"])
    assert "having bool_or(" in sql and "or f.accession_number = any(%s)" in sql


def test_no_filers_asks_nothing() -> None:
    conn = Conn()
    assert sec_segments.filings(conn, []) == {}
    assert conn.calls == []


def test_due_by_filer_and_what_is_unread() -> None:
    assert sec_segments.due_by_filer(Conn([(320193, 2), (1652044, 1)])) == {320193: 2, 1652044: 1}
    conn = Conn([(3, 2, date(2026, 11, 3), 41)])
    assert sec_segments.unread(conn, 1) == sec_segments.Unread(3, 2, date(2026, 11, 3), 41)
    assert conn.calls[0][1] == {"days": 1}
    assert sec_segments.unread(Conn([]), 1) == sec_segments.Unread(0, 0, None, 0)


def test_a_filing_never_read_is_judged_from_when_we_held_it_not_from_its_filing_date() -> None:
    """Friday's filings are written on Monday, three days old by their own date. A clock on
    `filing_date` would call them late the moment they arrive, in every run of Monday's wave."""
    text = " ".join(sec_segments.UNREAD.split())
    never = "f.segments_parsed_at is null and f.as_of <= now() - make_interval(days => %(days)s)"
    assert text.count(f"filter (where {never})") == 3
    assert "filing_date <=" not in text and "current_date" not in text
    # A filing read by an older parser version is counted apart, whatever its age.
    assert (
        "count(distinct f.accession_number) filter (where f.segments_parsed_at is not null)" in text
    )


# ── raw ────────────────────────────────────────────────────────────────────────────────────────


def test_raw_keeps_every_document_whole_and_adds_only_the_declared_columns() -> None:
    index = _answer("https://www.sec.gov/x/index.json", b'{"directory": {}}')
    instance = _answer("https://www.sec.gov/x/x-20260627_htm.xml", b"<xbrl>\xff</xbrl>")
    rows = sec_segments.raw_rows(
        _fetch("answered", ("index_json", index), ("instance", instance)),
        accession=ACCESSION,
        run_id="run-1",
    )
    assert [r["document"] for r in rows] == ["index_json", "instance"]
    assert rows[1]["body"] == b"<xbrl>\xff</xbrl>" and rows[1]["sha256"] == instance.document.sha256
    assert {r["outcome"] for r in rows} == {"answered"}
    added = set(rows[0]) - set(index.document.as_row("run-1"))
    assert added == set(sec_segments.CONTEXT_COLUMNS)
    assert all(set(sec_segments.MERGE_ON) <= set(r) for r in rows)


def _stored(
    document: str, outcome: str | None, *, run: str, at: str, body: bytes = b""
) -> dict[str, Any]:
    return {
        "accession": ACCESSION,
        "document": document,
        "http_status": 200,
        "outcome": outcome,
        "run_id": run,
        "fetched_at": at,
        "body": body,
    }


def test_the_newest_finished_fetch_decides_and_its_own_instance_is_the_body() -> None:
    rows = [
        _stored("index_json", "answered", run="r1", at="2026-11-10T12:00:00"),
        _stored("instance", "answered", run="r1", at="2026-11-10T12:00:01", body=b"<xbrl/>"),
    ]
    got = sec_segments.finished(rows)
    assert got == sec_segments.Finished("answered", "2026-11-10T12:00:01", "r1")
    assert sec_segments.instance_body(rows, got) == b"<xbrl/>"
    assert sec_segments.held_accessions(rows) == {ACCESSION}
    # The outcome needs no body: stage 2 asks it of every filing before reading a megabyte.
    light = [{k: v for k, v in r.items() if k != "body"} for r in rows]
    assert sec_segments.finished(light) == got
    assert sec_segments.instance_body(light, got) is None


@pytest.mark.parametrize("outcome", ["absent", "too_large"])
def test_a_fetch_that_ended_without_an_instance_has_no_body(outcome: str) -> None:
    rows = [_stored("index_json", outcome, run="r1", at="2026-11-10T12:00:00")]
    got = sec_segments.finished(rows)
    assert got is not None and got.outcome == outcome
    assert sec_segments.instance_body(rows, got) is None


def test_a_later_fetch_that_found_no_instance_does_not_use_an_older_one() -> None:
    """A re-fetch replaces the same document of the same filing and cannot remove the instance row
    an earlier fetch wrote. Taking any stored instance would parse a document the newest fetch
    says is not there."""
    merged = [
        _stored("instance", "answered", run="r1", at="2026-11-01T12:00:01", body=b"<old/>"),
        # The merge keeps one row per (accession, document): r2's index_json replaced r1's.
        _stored("index_json", "absent", run="r2", at="2026-11-10T12:00:00"),
        _stored("index_html", "absent", run="r2", at="2026-11-10T12:00:01"),
    ]
    got = sec_segments.finished(merged)
    assert got is not None and got.outcome == "absent"
    assert sec_segments.instance_body(merged, got) is None


def test_an_answered_fetch_never_reads_another_run_s_instance() -> None:
    torn = [
        _stored("index_json", "answered", run="r2", at="2026-11-10T12:00:00"),
        _stored("instance", "answered", run="r1", at="2026-11-01T12:00:00", body=b"<old/>"),
    ]
    got = sec_segments.finished(torn)
    assert got is not None and got.run_id == "r2"
    assert sec_segments.instance_body(torn, got) is None


def test_rows_with_no_finished_fetch_are_not_held() -> None:
    assert sec_segments.finished([]) is None
    unfinished = [_stored("index_json", None, run="r1", at="2026-11-10T12:00:00")]
    assert sec_segments.finished(unfinished) is None
    assert sec_segments.held_accessions(unfinished) == set()


# ── the rows written, and the control tables ───────────────────────────────────────────────────


IPHONE = SegmentFact(
    axis="srt:ProductOrServiceAxis",
    member_code="aapl:IPhoneMember",
    metric_code="revenue",
    period_type="quarter",
    period_start="2026-03-29",
    period_ending="2026-06-27",
    value=44582000000.0,
    currency="USD",
    parent_axis=None,
    parent_member=None,
    reconciled_to=94036000000.0,
    partition_id=1,
)


def test_segment_rows_carry_the_key_the_filing_and_exact_numbers() -> None:
    cell = replace(
        IPHONE,
        member_code="goog:SearchMember",
        parent_axis="us-gaap:StatementBusinessSegmentsAxis",
        parent_member="goog:ServicesMember",
        value=0.1 + 0.2,
        reconciled_to=None,
        partition_id=0,
    )
    rows, stats = segments.segment_rows(
        [IPHONE, cell, replace(IPHONE, member_code="x:KronerMember", currency="DKK")],
        security_id="s1",
        accession=ACCESSION,
        source=sec_segments.SOURCE,
        as_of="2026-11-10T12:00:01",
        known_currencies={"USD"},
    )
    assert len(rows) == 2 and stats == {"placed": 1, "unplaced": 1, "currency_unknown": 1}
    flat, nested = rows
    assert set(flat) == {*segments.SEGMENT_KEY, *segments.SEGMENT_UPDATE}
    assert (flat["parent_key"], nested["parent_key"]) == ("", "goog:ServicesMember")
    assert flat["value"] == 44582000000 and isinstance(flat["value"], int)
    # 0.1 + 0.2 as JavaScript serialised it, not as float8 -> numeric rounds it (0.3).
    assert nested["value"] == Decimal("0.30000000000000004")
    assert flat["reconciled_to"] == 94036000000 and nested["reconciled_to"] is None
    assert flat["accession_number"] == ACCESSION and flat["source_code"] == "sec-segments"


def test_the_vocabulary_is_read_ordered_and_refuses_an_empty_control_table() -> None:
    axes = [("srt:ProductOrServiceAxis", "product", 100, None)]
    concepts = [("revenue", "Revenues", 100)]
    roles = [("us-gaap:AllOtherSegmentsMember", "residual")]
    conn = Conn(axes, concepts, roles, [(20,)])
    got = segments.vocabulary(conn)
    assert got.parser_version == 20
    assert got.axes[0].axis == "srt:ProductOrServiceAxis" and got.axes[0].required_member is None
    assert got.concepts[0].concept == "Revenues" and got.roles[0].role == "residual"
    assert conn.calls[1][1] == (list(segments.SEGMENT_METRICS),)
    assert all("order by" in sql for sql, _ in conn.calls[:3])

    with pytest.raises(segments.VocabularyEmpty, match="segment_axis"):
        segments.vocabulary(Conn([], concepts, roles, [(20,)]))
    with pytest.raises(segments.VocabularyEmpty, match="xbrl_concept"):
        segments.vocabulary(Conn(axes, [], roles, [(20,)]))
    assert segments.vocabulary(Conn(axes, concepts, [], [(None,)])).parser_version == 1
