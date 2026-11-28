"""Europe's segment lane, end to end through Dagster.

The index is faked at the provider (`xbrl_org.filings`, `xbrl_org.report`), so the lane's own choice
of what to fetch runs and each test counts the requests a run made. The note reader is faked at
`esef.read_report`: it has its own tests over nine captured reports. The database is a fake that
HOLDS `security_filing` and its marks: a report a run reads stops being due.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import esef, segments
from muffin_ingest.facets import segment_note as sn
from muffin_ingest.providers import xbrl_org
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.esef import automation, core, raw
from muffin_ingest_dagster.defs.esef.partitions import ESEF_FILER_PARTITION
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres

LOREAL, INDITEX = "529900JI1GG6F7RKVI53", "549300TTCXZOGZM2EY83"
LO25, LO24 = f"{LOREAL}-2025-12-31-ESEF-FR-0", f"{LOREAL}-2024-12-31-ESEF-FR-0"
LO_H1 = f"{LOREAL}-2025-06-30-ESEF-FR-0"
IN25, IN24, IN24_AGAIN = (
    f"{INDITEX}-2026-01-31-ESEF-ES-0",
    f"{INDITEX}-2025-01-31-ESEF-ES-0",
    f"{INDITEX}-2025-01-31-ESEF-ES-1",
)


def listing(*reports: tuple[str, str, str]) -> bytes:
    """A filer's list as the index serves it: (fxo_id, period end, date added)."""
    return json.dumps(
        {
            "data": [
                {
                    "type": "filing",
                    "attributes": {
                        "fxo_id": fxo,
                        "period_end": end,
                        "country": fxo.split("-")[-2],
                        "date_added": added,
                        "published": None,
                        "json_url": f"/{fxo}.json",
                    },
                }
                for fxo, end, added in reports
            ],
            "links": {},
            "meta": {"count": len(reports)},
        }
    ).encode()


LISTS: dict[str, bytes] = {
    # L'Oréal's half-year report is listed beside its annual ones: never fetched.
    LOREAL: listing(
        (LO25, "2025-12-31", "2026-03-24 16:22:30"),
        (LO_H1, "2025-06-30", "2025-08-01 10:00:00"),
        (LO24, "2024-12-31", "2025-04-15 12:09:28"),
    ),
    # Inditex closes on 31 January and re-filed FY2024: the re-filing is the one read.
    INDITEX: listing(
        (IN25, "2026-01-31", "2026-03-20 09:00:00"),
        (IN24, "2025-01-31", "2025-03-14 09:00:00"),
        (IN24_AGAIN, "2025-01-31", "2025-04-02 09:00:00"),
    ),
}
REPORT = b'{"facts": {}}'


class Index:
    """Answers by request. A request with no answer is a test asking for something it did not
    expect."""

    def __init__(self) -> None:
        self.lists: dict[str, tuple[int, bytes] | Exception] = {
            k: (200, v) for k, v in LISTS.items()
        }
        self.reports: dict[str, tuple[int, bytes, bool] | Exception] = {
            f"/{fxo}.json": (200, REPORT, False) for fxo in (LO25, LO24, IN25, IN24_AGAIN)
        }
        self.calls: list[str] = []

    def filings(self, lei: str) -> xbrl_org.Answer:
        self.calls.append(f"list {lei}")
        answer = self.lists.get(lei)
        if answer is None:
            raise AssertionError(f"unexpected list: {lei}")
        if isinstance(answer, Exception):
            raise answer
        return xbrl_org.Answer(_document(f"list:{lei}", answer[1]), answer[0], False)

    def report(self, json_url: str) -> xbrl_org.Answer:
        self.calls.append(json_url.strip("/").removesuffix(".json"))
        answer = self.reports.get(json_url)
        if answer is None:
            raise AssertionError(f"unexpected report: {json_url}")
        if isinstance(answer, Exception):
            raise answer
        status, body, too_large = answer
        return xbrl_org.Answer(_document(json_url, body), status, too_large)


def _document(url: str, body: bytes) -> Document:
    return Document(
        url=f"http://http-cache:8080/xbrl-org/{url}",
        body=body,
        content_type="application/json",
        fetched_at=datetime.now(UTC),
    )


@pytest.fixture
def index(monkeypatch: pytest.MonkeyPatch) -> Iterator[Index]:
    fake = Index()
    monkeypatch.setattr(xbrl_org, "filings", fake.filings)
    monkeypatch.setattr(xbrl_org, "report", fake.report)
    yield fake


READ: list[bytes] = []


@pytest.fixture(autouse=True)
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    READ.clear()

    def read(body: bytes, vocabulary: esef.Vocabulary) -> esef.ReportRead:
        READ.append(body)
        fact = segments.SegmentFact(
            axis="esef:segments",
            member_code="esef:Luxe",
            metric_code="revenue",
            period_type="annual",
            period_start="2025-01-01",
            period_ending="2025-12-31",
            value=15_595_100_000.0,
            currency="EUR",
            parent_axis=None,
            parent_member=None,
            reconciled_to=44_052_000_000.0,
            partition_id=1,
        )
        return esef.ReportRead("ifrs-full:x", sn.NoteRead(facts=[fact], tables=1, read=1))

    monkeypatch.setattr(esef, "read_report", read)
    yield


# --- the database --------------------------------------------------------------------------------

#: Module level: a resource's class attribute becomes a config field.
FILERS: dict[str, list[str]] = {}
YEAR_ENDS: dict[str, tuple[int, int]] = {}
FILING_ROWS: dict[tuple[str, str], dict[str, Any]] = {}
PARSER = {"version": 1}
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
        elif "from market.fiscal_calendar" in text:
            ends = [YEAR_ENDS[s] for s in params[0] if s in YEAR_ENDS]
            self.rows = list(ends[:1])
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
                if r["security_id"] in sids and r["report_type"] == esef.ANNUAL:
                    by.setdefault(r["accession_number"], []).append(r)
            for acc, rows in by.items():
                unread = sorted(r["security_id"] for r in rows if _unread(r))
                if unread or acc in named:
                    filed = min(date.fromisoformat(r["filing_date"]) for r in rows)
                    self.rows.append((acc, filed, unread or None, [r["security_id"] for r in rows]))
        elif "from market.segment_note_rule" in text:
            self.rows = [
                ("block", "^DisclosureOfEntitysReportableSegmentsExplanatory$", "consolidated"),
                ("heading", "(?i)segment", "business"),
                ("label", "(?i)^revenue$", "revenue"),
            ]
        elif "from market.segment_axis" in text:
            self.rows = [("business", "esef:segments")]
        elif "from market.segment_parser" in text:
            self.rows = [(PARSER["version"],)]
        elif text.startswith("select code from market.currency"):
            self.rows = [("USD",), ("EUR",)]
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
    FILERS.update({LOREAL: ["s-fr1"], INDITEX: ["s-es1"]})
    YEAR_ENDS.clear()
    YEAR_ENDS.update({"s-es1": (1, 31)})
    FILING_ROWS.clear()
    PARSER["version"] = 1
    CALLS.clear()
    yield


def instance_with(*keys: str) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(ESEF_FILER_PARTITION, list(keys))
    return instance


def run(
    tmp_path: Path, instance: dg.DagsterInstance, keys: Sequence[str]
) -> dg.ExecuteInProcessResult:
    single = len(keys) == 1
    return dg.materialize(
        [raw.raw_esef_filings, core.esef_segment],
        instance=instance,
        partition_key=keys[0] if single else None,
        tags=None
        if single
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
        run_config={"ops": {"raw_esef_filings": {"config": {"pace_seconds": 0.0}}}},
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


def marked() -> set[str]:
    return {acc for (_, acc), r in FILING_ROWS.items() if not _unread(r)}


def test_a_first_visit_lists_each_filer_and_reads_its_annual_reports(
    tmp_path: Path, index: Index
) -> None:
    """Each filer's list, then its annual reports newest first in rounds across filers. A half-year
    report is never fetched, and of a period filed twice only the newer filing is."""
    result = run(tmp_path, instance_with(LOREAL, INDITEX), [LOREAL, INDITEX])
    assert result.success
    assert index.calls == [f"list {LOREAL}", f"list {INDITEX}", LO25, IN25, LO24, IN24_AGAIN]
    fetched = meta(result, "raw_esef_filings")
    assert (fetched["requested"], fetched["list_outcomes"], fetched["list_answered"]) == (2, 2, 2)
    assert (fetched["reports_named"], fetched["report_outcomes"]) == (4, 4)
    wrote = meta(result, "esef_segment")
    assert (wrote["filings"], wrote["outcomes"], wrote["read"], wrote["annual_reports_listed"]) == (
        4, 4, 4, 4,
    )  # fmt: skip
    assert {acc for _, acc in FILING_ROWS} == {LO25, LO24, IN25, IN24_AGAIN}
    assert marked() == {LO25, LO24, IN25, IN24_AGAIN}
    assert len(READ) == 4
    assert any(sql.startswith("insert into market.security_segment") for sql, _ in CALLS)


def test_a_second_visit_inside_the_floor_asks_nothing_and_fetches_nothing_read(
    tmp_path: Path, index: Index
) -> None:
    instance = instance_with(LOREAL)
    assert run(tmp_path, instance, [LOREAL]).success
    index.calls.clear()
    READ.clear()
    result = run(tmp_path, instance, [LOREAL])
    assert result.success
    assert index.calls == [], "the list is fresh, and nothing is due"
    fetched = meta(result, "raw_esef_filings")
    assert (fetched["list_fresh"], fetched["list_outcomes"], fetched["reports_named"]) == (1, 1, 0)
    assert meta(result, "esef_segment")["filings"] == 0
    assert READ == []


def test_a_429_stops_the_run_and_marks_nothing_unasked(tmp_path: Path, index: Index) -> None:
    """A refusal is never an absence: the filer after it is `unasked`, and nothing is marked."""
    index.lists[LOREAL] = xbrl_org.XbrlOrgRefused(429, "https://filings.xbrl.org/api/filings")
    result = run(tmp_path, instance_with(LOREAL, INDITEX), [LOREAL, INDITEX])
    assert result.success
    fetched = meta(result, "raw_esef_filings")
    assert (fetched["list_refused"], fetched["list_unasked"], fetched["list_outcomes"]) == (1, 1, 2)
    assert index.calls == [f"list {LOREAL}"]
    assert marked() == set()


def test_a_body_that_is_not_json_is_asked_again(tmp_path: Path, index: Index) -> None:
    index.reports[f"/{LO24}.json"] = (200, b"<html>gateway</html>", False)
    instance = instance_with(LOREAL)
    result = run(tmp_path, instance, [LOREAL])
    assert meta(result, "esef_segment")["not_json"] == 1
    assert marked() == {LO25}
    index.calls.clear()
    index.reports[f"/{LO24}.json"] = (200, REPORT, False)
    assert run(tmp_path, instance, [LOREAL]).success
    assert index.calls == [LO24], "the list is fresh; the report is due again"
    assert marked() == {LO25, LO24}


def test_a_report_the_index_no_longer_serves_is_marked_and_writes_nothing(
    tmp_path: Path, index: Index
) -> None:
    index.reports[f"/{LO24}.json"] = (404, b"", False)
    result = run(tmp_path, instance_with(LOREAL), [LOREAL])
    assert result.success
    wrote = meta(result, "esef_segment")
    assert (wrote["read"], wrote["absent"], wrote["outcomes"]) == (1, 1, 2)
    assert marked() == {LO25, LO24}
    assert len(READ) == 1


def test_the_registry_maps_each_covered_equity_and_releases_the_sec_s() -> None:
    calls: list[tuple[str, list[Any]]] = []

    class RegistryCursor(Cursor):
        def execute(self, sql: str, given: Sequence[Any] | Mapping[str, Any] = ()) -> None:
            text = " ".join(sql.split())
            calls.append((text, list(given) if not isinstance(given, Mapping) else []))
            if text.startswith("select s.security_id::text, i.lei"):
                self.rows = [("s-fr1", LOREAL, False), ("s-es1", INDITEX, False),
                             ("s-nl1", "724500Y6DUVHQD6OXN27", True)]  # fmt: skip
            elif text.startswith("with released as"):
                self.rows = [(1, 4, 2)]
            else:
                self.rows = []

    class RegistryConn(Conn):
        def cursor(self) -> Cursor:
            return RegistryCursor()

    class RegistryPostgres(Postgres):
        @contextmanager
        def connect(self) -> Iterator[Any]:
            yield RegistryConn()

    result = core.security_esef_filer(postgres=RegistryPostgres())
    assert isinstance(result, dg.MaterializeResult) and result.metadata is not None
    assert {k: v for k, v in result.metadata.items() if k != "changed"} == {
        "securities": 3, "mapped": 2, "filers": 2, "the_sec_s": 1,
        "released": 1, "released_segments": 4, "released_filings": 2,
    }  # fmt: skip
    inserted = [(s, p) for s, p in calls if s.startswith("insert into market.security_filer")]
    sql, params = inserted[0]
    columns = sql[sql.index("(") + 1 : sql.index(")")].split(", ")
    width = len(columns)
    rows = [
        dict(zip(columns, params[n : n + width], strict=True)) for n in range(0, len(params), width)
    ]
    assert rows == [
        {"security_id": "s-fr1", "source_code": "esef", "filer_id": LOREAL, "via": "registry"},
        {"security_id": "s-es1", "source_code": "esef", "filer_id": INDITEX, "via": "registry"},
    ]
    released = [p for s, p in calls if s.startswith("with released as")]
    assert released == [[["s-nl1"]]]


def test_the_floor_asks_every_filer_never_collected() -> None:
    grid = [f"{n:020d}" for n in range(12)]
    runs = automation.floor_runs(grid, date(2026, 11, 28), set(grid[:5]))
    covered = {
        k
        for r in runs
        for k in grid[
            grid.index(r.tags[ASSET_PARTITION_RANGE_START_TAG]) : grid.index(
                r.tags[ASSET_PARTITION_RANGE_END_TAG]
            )
            + 1
        ]
    }
    assert covered >= set(grid[5:])
    assert all(r.run_key and r.run_key.startswith("2026-11-28:") for r in runs)
    assert all(r.tags["dagster/max_runtime"] == "1800" for r in runs)
