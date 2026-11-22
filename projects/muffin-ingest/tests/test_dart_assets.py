"""The segments family's Korea lane, end to end through Dagster.

DART is faked at the provider (`dart.filings`, `dart.instance`), so the lane's own choice of what
to fetch runs and each test counts the requests a run made. The XBRL parser is faked at
`segments.segment_facts_from`: it is SEC's, with its own tests, and its parity on DART's archives is
measured on real filings. The database is a fake that HOLDS `security_filing` and its marks: a
report a run reads stops being due.
"""

from __future__ import annotations

import io
import json
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import segments
from muffin_ingest.providers import dart
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.segments import automation, core, raw
from muffin_ingest_dagster.defs.segments.partitions import DART_FILER_PARTITION, FLOOR_DAYS
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

VOCABULARY = json.loads((FIXTURES / "segments" / "vocabulary.json").read_bytes())

SAMSUNG, SKGAS = "00126380", "00144164"
FY25, FY24, Q3 = "20260310002820", "20250311001085", "20251114000111"
SKGAS_FY25 = "20260316001594"
NO_FILE = b"<?xml version='1.0'?><result><status>014</status><message>no file</message></result>"
LIMIT = b'{"status":"020","message":"limit"}'


def archive(instance: bytes = b"<xbrli:xbrl>an instance</xbrli:xbrl>") -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("entity00126380_2025-12-31.xbrl", instance)
        z.writestr("entity00126380_2025-12-31_lab-ko.xml", b"<labels/>")
    return out.getvalue()


def listing(*filed: tuple[str, str, str], pages: int = 1) -> bytes:
    return json.dumps(
        {
            "status": "000",
            "total_page": pages,
            "list": [{"rcept_no": r, "report_nm": n, "rcept_dt": d} for r, n, d in filed],
        }
    ).encode()


LISTS: dict[str, bytes] = {
    SAMSUNG: listing(
        (FY25, "사업보고서 (2025.12)", "20260310"),
        (Q3, "분기보고서 (2025.09)", "20251114"),
        (FY24, "사업보고서 (2024.12)", "20250311"),
    ),
    SKGAS: listing((SKGAS_FY25, "사업보고서 (2025.12)", "20260316")),
}


class Dart:
    """Answers by request. A request with no answer is a test asking for something it did not
    expect."""

    def __init__(self) -> None:
        #: By corp code, or by (corp code, page) for a page past the first.
        self.lists: dict[str | tuple[str, int], tuple[int, bytes] | Exception] = {
            k: (200, v) for k, v in LISTS.items()
        }
        self.archives: dict[str, tuple[int, bytes, bool] | Exception] = {
            r: (200, archive(), False) for r in (FY25, FY24, SKGAS_FY25)
        }
        self.calls: list[str] = []

    def filings(self, corp_code: str, page: int = 1) -> dart.Answer:
        self.calls.append(f"list {corp_code}" if page == 1 else f"list {corp_code} p{page}")
        answer = self.lists.get((corp_code, page)) if page > 1 else self.lists.get(corp_code)
        if answer is None:
            raise AssertionError(f"unexpected list: {corp_code}")
        if isinstance(answer, Exception):
            raise answer
        return dart.Answer(_document(f"list:{corp_code}", answer[1]), answer[0], False)

    def instance(self, rcept_no: str) -> dart.Answer:
        self.calls.append(rcept_no)
        answer = self.archives.get(rcept_no)
        if answer is None:
            raise AssertionError(f"unexpected archive: {rcept_no}")
        if isinstance(answer, Exception):
            raise answer
        status, body, too_large = answer
        return dart.Answer(_document(rcept_no, body), status, too_large)


#: When a request's answer was fetched, by the fake's URL, where a test needs it older than now.
FETCHED_AT: dict[str, datetime] = {}


def _document(url: str, body: bytes) -> Document:
    return Document(
        url=f"http://http-cache:8080/dart/{url}?crtfc_key=REDACTED",
        body=body,
        content_type="application/x-msdownload",
        fetched_at=FETCHED_AT.get(url, datetime.now(UTC)),
    )


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch) -> Iterator[Dart]:
    fake = Dart()
    monkeypatch.setattr(dart, "filings", fake.filings)
    monkeypatch.setattr(dart, "instance", fake.instance)
    yield fake


PARSED: list[bytes] = []


@pytest.fixture(autouse=True)
def parser(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    PARSED.clear()

    def parse(text: str, *_: Any) -> list[segments.SegmentFact]:
        PARSED.append(text.encode())
        return [
            segments.SegmentFact(
                axis="ifrs-full:SegmentsAxis",
                member_code="dart:LngMember",
                metric_code="revenue",
                period_type="annual",
                period_start="2025-01-01",
                period_ending="2025-12-31",
                value=7050068258000.0,
                currency="KRW",
                parent_axis=None,
                parent_member=None,
                reconciled_to=7050068258000.0,
                partition_id=1,
            )
        ]

    monkeypatch.setattr(segments, "segment_facts_from", parse)
    yield


# --- the database --------------------------------------------------------------------------------

#: Module level: a resource's class attribute becomes a config field.
FILERS: dict[str, list[str]] = {}
FILING_ROWS: dict[tuple[str, str], dict[str, Any]] = {}
PARSER = {"version": 20}
CALLS: list[tuple[str, Any]] = []


def _unread(row: dict[str, Any]) -> bool:
    mark = row.get("segments_parser_version")
    return row.get("segments_parsed_at") is None or (mark or 0) < PARSER["version"]


class Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, given: Sequence[Any] | Mapping[str, Any] = ()) -> None:
        text = " ".join(sql.split())
        params = [] if isinstance(given, Mapping) else list(given)
        CALLS.append((text, params))
        self.rows, self.rowcount = [], 0
        if "group by sf.filer_id" in text:
            self.rows = [(c, FILERS[c]) for c in params[0] if c in FILERS]
        elif text.startswith("select distinct f.accession_number"):
            wanted = set(params[0])
            self.rows = sorted(
                {(r["accession_number"],) for r in FILING_ROWS.values()
                 if r["accession_number"] in wanted and not _unread(r)}
            )  # fmt: skip
        elif "having bool_or" in text:
            sids, named = set(params[0]), set(params[1])
            by: dict[str, list[dict[str, Any]]] = {}
            for r in FILING_ROWS.values():
                if r["security_id"] in sids and r["report_type"] == "사업보고서":
                    by.setdefault(r["accession_number"], []).append(r)
            for acc, rows in by.items():
                unread = sorted(r["security_id"] for r in rows if _unread(r))
                if unread or acc in named:
                    filed = min(date.fromisoformat(r["filing_date"]) for r in rows)
                    self.rows.append((acc, filed, unread or None, [r["security_id"] for r in rows]))
        elif "from market.segment_axis" in text:
            self.rows = [
                (a["axis"], a["kind"], a["priority"], a["required_member"])
                for a in VOCABULARY["axes"]
            ]
        elif "from market.xbrl_concept" in text:
            self.rows = [
                (c["metric_code"], c["concept"], c["priority"]) for c in VOCABULARY["concepts"]
            ]
        elif "from market.segment_member_class" in text:
            self.rows = [(r["member_code"], r["class"]) for r in VOCABULARY["member_roles"]]
        elif "from market.segment_parser" in text:
            self.rows = [(PARSER["version"],)]
        elif text.startswith("select code from market.currency"):
            self.rows = [("USD",), ("KRW",)]
        elif text.startswith("insert into market.security_filing"):
            columns = text[text.index("(") + 1 : text.index(")")].split(", ")
            for n in range(0, len(params), len(columns)):
                row = dict(zip(columns, params[n : n + len(columns)], strict=True))
                held = FILING_ROWS.setdefault(
                    (row["security_id"], row["accession_number"]),
                    {"segments_parsed_at": None, "segments_parser_version": None},
                )
                held.update(row)
                self.rowcount += 1
        elif text.startswith("update market.security_filing"):
            at, version, accession, security_ids = params
            for sid in security_ids:
                row = FILING_ROWS[(sid, accession)]
                row["segments_parsed_at"], row["segments_parser_version"] = at, version

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        CALLS.append(("commit", []))

    def rollback(self) -> None:
        CALLS.append(("rollback", []))


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    FILERS.clear()
    FILERS.update({SAMSUNG: ["s-kr1"], SKGAS: ["s-kr2"]})
    FILING_ROWS.clear()
    PARSER["version"] = 20
    CALLS.clear()
    FETCHED_AT.clear()
    yield


def instance_with(*keys: str) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(DART_FILER_PARTITION, list(keys))
    return instance


def run(
    tmp_path: Path, instance: dg.DagsterInstance, keys: Sequence[str]
) -> dg.ExecuteInProcessResult:
    single = len(keys) == 1
    return dg.materialize(
        [raw.raw_dart_filings, core.dart_segment],
        instance=instance,
        partition_key=keys[0] if single else None,
        tags=None
        if single
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
        run_config={"ops": {"raw_dart_filings": {"config": {"pace_seconds": 0.0}}}},
        resources={
            "postgres": FakePostgres(),
            "parquet_io": ParquetIOManager(str(tmp_path)),
            "raw_store": RawStore(base_path=str(tmp_path)),
        },
        raise_on_error=False,
    )


def meta(result: dg.ExecuteInProcessResult, key: str) -> dict[str, Any]:
    for event in result.get_asset_materialization_events():
        materialization = event.step_materialization_data.materialization
        if materialization.asset_key == dg.AssetKey(key):
            return {k: v.value for k, v in materialization.metadata.items()}
    raise AssertionError(f"{key} did not materialize")


def segment_inserts() -> list[list[Any]]:
    return [p for sql, p in CALLS if sql.startswith("insert into market.security_segment")]


def marked() -> set[str]:
    return {acc for (_, acc), r in FILING_ROWS.items() if not _unread(r)}


def test_a_first_visit_lists_each_filer_and_reads_its_annual_reports(
    tmp_path: Path, provider: Dart
) -> None:
    """Each filer's list, then its annual reports newest first in rounds across filers (a quarterly
    report has no 11011 archive and is not filed); each archive's instance is parsed and marked."""
    result = run(tmp_path, instance_with(SAMSUNG, SKGAS), [SAMSUNG, SKGAS])
    assert result.success
    assert provider.calls == [f"list {SAMSUNG}", f"list {SKGAS}", FY25, SKGAS_FY25, FY24]
    fetched = meta(result, "raw_dart_filings")
    assert (fetched["requested"], fetched["list_outcomes"], fetched["list_answered"]) == (2, 2, 2)
    assert (fetched["archives_named"], fetched["archive_outcomes"]) == (3, 3)
    wrote = meta(result, "dart_segment")
    assert (wrote["filings"], wrote["outcomes"], wrote["read"], wrote["annual_reports_listed"]) == (
        3, 3, 3, 3,
    )  # fmt: skip
    assert {acc for _, acc in FILING_ROWS} == {FY25, FY24, SKGAS_FY25}
    assert marked() == {FY25, FY24, SKGAS_FY25}
    assert len(PARSED) == 3 and all(b"an instance" in p for p in PARSED)
    assert segment_inserts()
    assert all("REDACTED" in str(r.get("url")) for r in RawStore(base_path=str(tmp_path))
               .stored_rows_for(raw.DART_FILINGS, SAMSUNG))  # fmt: skip


def test_a_second_visit_inside_the_floor_asks_nothing_and_fetches_nothing_read(
    tmp_path: Path, provider: Dart
) -> None:
    instance = instance_with(SAMSUNG)
    assert run(tmp_path, instance, [SAMSUNG]).success
    provider.calls.clear()
    PARSED.clear()
    result = run(tmp_path, instance, [SAMSUNG])
    assert result.success
    assert provider.calls == [], "the list is fresh, and nothing is due"
    fetched = meta(result, "raw_dart_filings")
    assert (fetched["list_fresh"], fetched["list_outcomes"], fetched["archives_named"]) == (1, 1, 0)
    assert meta(result, "dart_segment")["filings"] == 0
    assert PARSED == []


@pytest.mark.parametrize(
    ("age_days", "listed_again"),
    # A list fetched on a filer's floor day is due on its next one, whatever the hour of the
    # tick: an hour short of a floor period is old enough.
    [(raw.LIST_MAX_AGE_DAYS - 1, False), (FLOOR_DAYS - 1 / 24, True)],
)
def test_a_list_younger_than_the_floor_is_not_asked_and_one_as_old_is(
    tmp_path: Path, provider: Dart, age_days: float, listed_again: bool
) -> None:
    instance = instance_with(SAMSUNG)
    FETCHED_AT[f"list:{SAMSUNG}"] = datetime.now(UTC) - timedelta(days=age_days)
    assert run(tmp_path, instance, [SAMSUNG]).success
    FETCHED_AT.clear()
    provider.calls.clear()

    assert run(tmp_path, instance, [SAMSUNG]).success
    assert provider.calls == ([f"list {SAMSUNG}"] if listed_again else [])


def test_a_fresh_list_still_names_its_archives_when_stage_2_never_filed_them(
    tmp_path: Path, provider: Dart
) -> None:
    """Stage 1 alone, its archives left unasked: `security_filing` holds nothing for the filer, so
    the next run can only know its annual reports from the stored list."""
    instance = instance_with(SAMSUNG)
    first = dg.materialize(
        [raw.raw_dart_filings],
        instance=instance,
        partition_key=SAMSUNG,
        run_config={
            "ops": {"raw_dart_filings": {"config": {"pace_seconds": 0.0, "budget_bytes": 0}}}
        },
        resources={
            "postgres": FakePostgres(),
            "parquet_io": ParquetIOManager(str(tmp_path)),
            "raw_store": RawStore(base_path=str(tmp_path)),
        },
    )
    assert first.success and provider.calls == [f"list {SAMSUNG}"]
    assert FILING_ROWS == {}

    again = run(tmp_path, instance, [SAMSUNG])
    assert again.success
    assert provider.calls[1:] == [FY25, FY24], "planned from the stored list, newest first"
    assert meta(again, "raw_dart_filings")["list_fresh"] == 1


def test_a_walk_a_transport_failure_cut_short_is_listed_again(
    tmp_path: Path, provider: Dart
) -> None:
    """Page 1 of two answered and page 2 did not: page 1 is filed, but the walk settled nothing."""
    provider.lists[SAMSUNG] = (200, listing((FY25, "사업보고서 (2025.12)", "20260310"), pages=2))
    provider.lists[(SAMSUNG, 2)] = dart.DartTransport("reset")
    instance = instance_with(SAMSUNG)
    first = run(tmp_path, instance, [SAMSUNG])
    assert first.success and meta(first, "raw_dart_filings")["list_transport"] == 1
    provider.calls.clear()

    provider.lists[(SAMSUNG, 2)] = (200, listing((FY24, "사업보고서 (2024.12)", "20250311")))
    again = run(tmp_path, instance, [SAMSUNG])
    assert again.success
    assert provider.calls[:2] == [f"list {SAMSUNG}", f"list {SAMSUNG} p2"]
    assert meta(again, "raw_dart_filings")["list_answered"] == 1


def test_dart_saying_the_file_does_not_exist_marks_the_report_and_writes_nothing(
    tmp_path: Path, provider: Dart
) -> None:
    """Status 014 with HTTP 200 is a settled fact about an immutable filing: marked, never asked
    again, and nothing retracted, because nothing was read."""
    provider.archives[FY24] = (200, NO_FILE, False)
    result = run(tmp_path, instance_with(SAMSUNG), [SAMSUNG])
    assert result.success
    wrote = meta(result, "dart_segment")
    assert (wrote["read"], wrote["no_instance"], wrote["outcomes"]) == (1, 1, 2)
    assert marked() == {FY25, FY24}
    assert len(PARSED) == 1


def test_a_refusal_in_the_body_stops_the_run_and_marks_nothing_unasked(
    tmp_path: Path, provider: Dart
) -> None:
    """`020` is DART's daily limit, sent with HTTP 200: a company asked after it is `unasked`, never
    a company with no filings."""
    provider.lists[SAMSUNG] = (200, LIMIT)
    result = run(tmp_path, instance_with(SAMSUNG, SKGAS), [SAMSUNG, SKGAS])
    assert result.success
    fetched = meta(result, "raw_dart_filings")
    assert (fetched["list_refused"], fetched["list_unasked"], fetched["list_outcomes"]) == (1, 1, 2)
    assert provider.calls == [f"list {SAMSUNG}"]
    assert marked() == set()


def test_a_body_that_is_neither_an_archive_nor_a_refusal_is_asked_again(
    tmp_path: Path, provider: Dart
) -> None:
    provider.archives[FY24] = (200, b"<html>gateway</html>", False)
    instance = instance_with(SAMSUNG)
    result = run(tmp_path, instance, [SAMSUNG])
    assert meta(result, "dart_segment")["not_zip"] == 1
    assert marked() == {FY25}
    provider.calls.clear()
    provider.archives[FY24] = (200, archive(), False)
    assert run(tmp_path, instance, [SAMSUNG]).success
    assert provider.calls == [FY24], "the list is fresh; the archive is due again"
    assert marked() == {FY25, FY24}


def test_the_registry_maps_each_korean_equity_with_a_listed_code(tmp_path: Path) -> None:
    xml = (
        b"<result><list><corp_code>00126380</corp_code><stock_code>005930</stock_code></list>"
        b"<list><corp_code>00144164</corp_code><stock_code>018670</stock_code></list></result>"
    )
    body = io.BytesIO()
    with zipfile.ZipFile(body, "w") as z:
        z.writestr("CORPCODE.xml", xml)
    calls: list[tuple[str, list[Any]]] = []

    class RegistryCursor(Cursor):
        def execute(self, sql: str, given: Sequence[Any] | Mapping[str, Any] = ()) -> None:
            text = " ".join(sql.split())
            calls.append((text, list(given) if not isinstance(given, Mapping) else []))
            self.rows = (
                [("s-kr1", "005930"), ("s-kr2", "018670"), ("s-kr3", "999999")]
                if text.startswith("select s.security_id::text, min(c.code)")
                else []
            )

    class RegistryConn(Conn):
        def cursor(self) -> Cursor:
            return RegistryCursor()

    class RegistryPostgres(Postgres):
        @contextmanager
        def connect(self) -> Iterator[Any]:
            yield RegistryConn()

    result = core.security_dart_filer(
        dg.build_asset_context(),
        postgres=RegistryPostgres(),
        raw_dart_corp_codes=[{"body": body.getvalue()}],
    )
    assert isinstance(result, dg.MaterializeResult) and result.metadata is not None
    assert {k: v for k, v in result.metadata.items() if k != "changed"} == {
        "listed": 2, "securities": 3, "mapped": 2, "unmapped": 1,
    }  # fmt: skip
    inserted = [(s, p) for s, p in calls if s.startswith("insert into market.security_filer")]
    assert len(inserted) == 1
    sql, params = inserted[0]
    columns = sql[sql.index("(") + 1 : sql.index(")")].split(", ")
    rows = [dict(zip(columns, params[n : n + 3], strict=True)) for n in range(0, len(params), 3)]
    assert rows == [
        {"security_id": "s-kr1", "source_code": "dart", "filer_id": "00126380"},
        {"security_id": "s-kr2", "source_code": "dart", "filer_id": "00144164"},
    ]


def test_the_floor_asks_every_filer_never_collected_and_the_day_s_hash() -> None:
    grid = [f"{n:08d}" for n in range(25)]
    chunks = automation.floor_chunks(grid, date(2026, 11, 16), set(grid[:10]), 10)
    assert [c[0] for c in chunks] == ["00000000", "00000010", "00000020"][-len(chunks) :]
    assert all(len(c) <= 10 for c in chunks)
    assert {k for c in chunks for k in c} >= set(grid[10:])
