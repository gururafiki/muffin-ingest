"""Japan's segment lane, end to end through Dagster.

EDINET is faked at the provider (`edinet.daily_index`, `edinet.document`, `edinet.code_list`), so
the lane's own choice of what to ask runs and each test counts the requests a run made. The reader
is the real one, over a real report's instance and the vocabulary the deployment seeds (the
library's `edinet_rules`, loaded by path so there is one copy). The database is a fake that HOLDS
`security_filing` and its marks: a report a run reads stops being due.
"""

from __future__ import annotations

import gzip
import importlib.util
import io
import json
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import dagster as dg
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import edinet as fe
from muffin_ingest.providers import edinet
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.edinet import automation, core, raw
from muffin_ingest_dagster.defs.edinet.partitions import EDINET_FILER_PARTITION
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres

LIB_TESTS = Path(__file__).resolve().parents[3] / "libs" / "muffin-ingest-lib" / "tests"
FIXTURES = LIB_TESTS / "fixtures" / "edinet"


def _rules() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "edinet_rules", LIB_TESTS / "unit" / "edinet_rules.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RULES = _rules()
DAY = (FIXTURES / "edinet_day_2026-06-25.json").read_bytes()
YELLOW_HAT = gzip.decompress((FIXTURES / "edinet_yellowhat-S100YC2P.xbrl.gz").read_bytes())
EMPTY_DAY = json.dumps(
    {"metadata": {"status": "200", "resultset": {"count": 0}}, "results": []}
).encode()
OUTSIDE = b'{"metadata": {"status": "404", "message": "Not Found"}}'


def archive(instance: bytes) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("XBRL/PublicDoc/jpcrp030000-asr-001_E03196-000_2025-03-31_01.xbrl", instance)
        z.writestr("XBRL/AuditDoc/jpaud-aar-cn-001_E03196-000_2025-03-31_01.xbrl", b"<x/>")
    return out.getvalue()


def _document(url: str, body: bytes) -> Document:
    return Document(
        url=f"http://http-cache:8080/edinet/{url}?Subscription-Key=REDACTED",
        body=body,
        content_type="application/json",
        fetched_at=datetime.now(UTC),
    )


class Edinet:
    """Answers by request. A request with no answer is a test asking for something it did not
    expect."""

    def __init__(self) -> None:
        self.days: dict[str, tuple[int, bytes] | Exception] = {}
        self.archives: dict[str, tuple[int, bytes] | Exception] = {}
        self.calls: list[str] = []

    def daily_index(self, day: date | str, timeout_s: float = 120.0) -> edinet.Answer:
        key = str(day)
        self.calls.append(f"day {key}")
        answer = self.days.get(key)
        if answer is None:
            raise AssertionError(f"unexpected day: {key}")
        if isinstance(answer, Exception):
            raise answer
        return edinet.Answer(_document(f"documents.json?date={key}", answer[1]), answer[0])

    def document(self, doc_id: str, timeout_s: float = 240.0) -> edinet.Answer:
        self.calls.append(doc_id)
        answer = self.archives.get(doc_id)
        if answer is None:
            raise AssertionError(f"unexpected archive: {doc_id}")
        if isinstance(answer, Exception):
            raise answer
        return edinet.Answer(_document(f"documents/{doc_id}", answer[1]), answer[0])


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch) -> Iterator[Edinet]:
    fake = Edinet()
    monkeypatch.setattr(edinet, "daily_index", fake.daily_index)
    monkeypatch.setattr(edinet, "document", fake.document)
    yield fake


# --- the database --------------------------------------------------------------------------------

#: Module level: a resource's class attribute becomes a config field.
FILERS: dict[str, list[str]] = {}
FILING_ROWS: dict[tuple[str, str], dict[str, Any]] = {}
CATALOGUE: dict[str, dict[str, Any]] = {}
PARSER = {"version": 1}
CALLS: list[tuple[str, Any]] = []


def _unread(row: dict[str, Any]) -> bool:
    mark = row.get("segments_parser_version")
    return row.get("segments_parsed_at") is None or (mark or 0) < PARSER["version"]


def _columns(text: str) -> list[str]:
    return text[text.index("(") + 1 : text.index(")")].split(", ")


def _rows(text: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    columns = _columns(text)
    width = len(columns)
    return [
        dict(zip(columns, params[n : n + width], strict=True)) for n in range(0, len(params), width)
    ]


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
        if "group by sf.filer_id" in text and "array_agg" in text:
            self.rows = [(c, FILERS[c]) for c in params[0] if c in FILERS]
        elif "having bool_or" in text:
            sids, named = set(params[0]), set(params[1])
            by: dict[str, list[dict[str, Any]]] = {}
            for r in FILING_ROWS.values():
                if r["security_id"] in sids and r["report_type"] == fe.ANNUAL:
                    by.setdefault(r["accession_number"], []).append(r)
            for acc, rows in by.items():
                unread = sorted(r["security_id"] for r in rows if _unread(r))
                if unread or acc in named:
                    filed = min(date.fromisoformat(r["filing_date"]) for r in rows)
                    self.rows.append((acc, filed, unread or None, [r["security_id"] for r in rows]))
        elif "from market.segment_note_rule" in text:
            ordered = sorted(RULES.RULES, key=lambda r: (r[0], -r[3], r[1]))
            self.rows = [(rule, pattern, role) for rule, pattern, role, _ in ordered]
        elif "from market.segment_axis" in text:
            self.rows = list(RULES.AXES)
        elif "from market.segment_parser" in text:
            self.rows = [(PARSER["version"],)]
        elif text.startswith("select code from market.currency"):
            self.rows = [("USD",), ("JPY",)]
        elif text.startswith("insert into market.edinet_document"):
            for row in _rows(text, params):
                held = CATALOGUE.setdefault(row["doc_id"], {"withdrawn": row["withdrawn"]})
                held.update({k: v for k, v in row.items() if k != "withdrawn"})
                self.rowcount += 1
        elif text.startswith("update market.edinet_document"):
            for doc_id in params[0]:
                if doc_id in CATALOGUE and not CATALOGUE[doc_id]["withdrawn"]:
                    CATALOGUE[doc_id]["withdrawn"] = True
                    self.rows.append((CATALOGUE[doc_id]["edinet_code"],))
        elif "from market.file_edinet_filings" in text:
            codes = params[0]
            count = 0
            for doc in CATALOGUE.values():
                if (doc["ordinance_code"], doc["form_code"]) != ("010", "030000"):
                    continue
                if doc["withdrawn"] or (codes is not None and doc["edinet_code"] not in codes):
                    continue
                for sid in FILERS.get(doc["edinet_code"], []):
                    held = FILING_ROWS.setdefault(
                        (sid, doc["doc_id"]),
                        {"segments_parsed_at": None, "segments_parser_version": None},
                    )
                    held.update(
                        {
                            "security_id": sid,
                            "accession_number": doc["doc_id"],
                            "report_type": fe.ANNUAL,
                            "filing_date": doc["filed_on"],
                        }
                    )
                    count += 1
            self.rows = [(count, 0, 0)]
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
    FILING_ROWS.clear()
    CATALOGUE.clear()
    PARSER["version"] = 1
    CALLS.clear()
    yield


def resources(tmp_path: Path) -> dict[str, Any]:
    return {
        "postgres": FakePostgres(),
        "parquet_io": ParquetIOManager(str(tmp_path)),
        "raw_store": RawStore(base_path=str(tmp_path)),
    }


def meta(result: dg.ExecuteInProcessResult, key: str) -> dict[str, Any]:
    for event in result.get_asset_materialization_events():
        materialization = event.step_materialization_data.materialization
        if materialization.asset_key == dg.AssetKey(key):
            return {k: v.value for k, v in materialization.metadata.items()}
    raise AssertionError(f"{key} did not materialize")


def sql_sent(prefix: str) -> list[list[Any]]:
    return [p for sql, p in CALLS if sql.startswith(prefix)]


# --- the day lane -------------------------------------------------------------------------------


def run_days(
    tmp_path: Path, first: str, last: str, instance: dg.DagsterInstance | None = None
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [raw.raw_edinet_days, core.edinet_document],
        instance=instance or dg.DagsterInstance.ephemeral(),
        partition_key=first if first == last else None,
        tags=None
        if first == last
        else {ASSET_PARTITION_RANGE_START_TAG: first, ASSET_PARTITION_RANGE_END_TAG: last},
        run_config={"ops": {"raw_edinet_days": {"config": {"pace_seconds": 0.0}}}},
        resources=resources(tmp_path),
        raise_on_error=False,
    )


def test_a_day_s_reports_are_catalogued_and_a_mapped_company_s_report_is_filed(
    tmp_path: Path, provider: Edinet
) -> None:
    """Each day is listed once; its annual reports and amendments are catalogued, the withdrawal is
    marked, and `file_edinet_filings` files Nintendo's report for the security mapped to it."""
    FILERS["E02367"] = ["s-nintendo"]
    provider.days = {"2026-06-24": (200, EMPTY_DAY), "2026-06-25": (200, DAY)}
    result = run_days(tmp_path, "2026-06-24", "2026-06-25")
    assert result.success
    assert provider.calls == ["day 2026-06-24", "day 2026-06-25"]
    fetched = meta(result, "raw_edinet_days")
    assert (fetched["requested"], fetched["outcomes"], fetched["answered"]) == (2, 2, 2)
    wrote = meta(result, "edinet_document")
    assert (wrote["days"], wrote["listed"], wrote["catalogued"], wrote["annual_reports"]) == (
        2, 8, 4, 1,
    )  # fmt: skip
    assert (wrote["withdrawals_named"], wrote["count_mismatch"], wrote["filed"]) == (1, 0, 1)
    assert set(CATALOGUE) == {"S100Y9NX", "S100YK3Q", "S100YA3V", "S100Y564"}
    assert list(FILING_ROWS) == [("s-nintendo", "S100Y9NX")]
    # THE FILERS THE DAYS TOUCHED, NEVER NULL: a day is not a pass over the whole catalogue.
    [codes] = sql_sent("select filed, withdrawn_filings, withdrawn_segments")
    assert codes[0] is not None and "E02367" in codes[0]
    stored = RawStore(base_path=str(tmp_path)).stored_rows_for(raw.DAYS, "2026-06-25")
    assert all("REDACTED" in str(r.get("url")) for r in stored)


def test_a_settled_day_is_not_asked_again_and_a_recent_one_is(
    tmp_path: Path, provider: Edinet
) -> None:
    """A list fetched ten days after its day is settled; a recent day's list is asked every night
    until it is."""
    recent = (datetime.now(UTC).date() - timedelta(days=2)).isoformat()
    provider.days = {"2026-06-25": (200, DAY), recent: (200, EMPTY_DAY)}
    instance = dg.DagsterInstance.ephemeral()
    assert run_days(tmp_path, "2026-06-25", "2026-06-25", instance).success
    assert run_days(tmp_path, recent, recent, instance).success
    provider.calls.clear()
    again = run_days(tmp_path, "2026-06-25", "2026-06-25", instance)
    assert again.success and provider.calls == []
    assert meta(again, "raw_edinet_days")["settled"] == 1
    assert run_days(tmp_path, recent, recent, instance).success
    assert provider.calls == [f"day {recent}"]


def test_a_held_list_is_never_replaced_by_an_absence(tmp_path: Path, provider: Edinet) -> None:
    """EDINET's window moves, so a day it once listed answers 404 later: the list stands."""
    recent = (datetime.now(UTC).date() - timedelta(days=2)).isoformat()
    provider.days = {recent: (200, DAY)}
    instance = dg.DagsterInstance.ephemeral()
    assert run_days(tmp_path, recent, recent, instance).success
    provider.days = {recent: (404, OUTSIDE)}
    again = run_days(tmp_path, recent, recent, instance)
    assert again.success and meta(again, "raw_edinet_days")["kept"] == 1
    held = fe.held_day(RawStore(base_path=str(tmp_path)).stored_rows_for(raw.DAYS, recent))
    assert held is not None and bytes(held["body"]) == DAY


def test_a_day_outside_the_window_with_nothing_held_is_an_absence(
    tmp_path: Path, provider: Edinet
) -> None:
    provider.days = {"2016-11-28": (404, OUTSIDE)}
    result = run_days(tmp_path, "2016-11-28", "2016-11-28")
    assert result.success
    assert meta(result, "raw_edinet_days")["absent"] == 1
    assert meta(result, "edinet_document")["days_without_list"] == 1


def test_a_refusal_fails_the_run_and_claims_no_day(tmp_path: Path, provider: Edinet) -> None:
    """A day not heard must not read as a day EDINET listed nothing for: the run claims none."""
    provider.days = {
        "2026-06-24": (200, EMPTY_DAY),
        "2026-06-25": edinet.EdinetRefused(401, "documents.json", "Access denied"),
    }
    instance = dg.DagsterInstance.ephemeral()
    result = run_days(tmp_path, "2026-06-24", "2026-06-25", instance)
    assert not result.success
    assert instance.get_materialized_partitions(raw.DAYS) == set()
    assert CATALOGUE == {}


def test_a_withdrawal_listed_later_is_never_undone_by_re_reading_the_original_day(
    tmp_path: Path, provider: Edinet
) -> None:
    """A withdrawal is only ever set: the day that listed the original still says it stands."""
    FILERS["E02367"] = ["s-nintendo"]
    later = json.dumps(
        {
            "metadata": {"status": "200", "resultset": {"count": 1}},
            "results": [{"docID": "S100Y9NX", "withdrawalStatus": "2"}],
        }
    ).encode()
    provider.days = {"2026-06-25": (200, DAY), "2026-07-01": (200, later)}
    instance = dg.DagsterInstance.ephemeral()
    assert run_days(tmp_path, "2026-06-25", "2026-06-25", instance).success
    withdrew = run_days(tmp_path, "2026-07-01", "2026-07-01", instance)
    assert meta(withdrew, "edinet_document")["withdrawals_marked"] == 1
    assert CATALOGUE["S100Y9NX"]["withdrawn"] is True
    # Re-read the original day's stored list (stage 2 only): it must not restore the report.
    replay = dg.materialize(
        [raw.raw_edinet_days, core.edinet_document],
        selection=[core.edinet_document],
        instance=instance,
        partition_key="2026-06-25",
        resources=resources(tmp_path),
    )
    assert replay.success and CATALOGUE["S100Y9NX"]["withdrawn"] is True
    update_columns = [
        sql for sql, _ in CALLS if sql.startswith("insert into market.edinet_document")
    ]
    assert update_columns and all("withdrawn = excluded" not in s for s in update_columns)


# --- the filer lane -----------------------------------------------------------------------------


def run_filers(tmp_path: Path, keys: Sequence[str]) -> dg.ExecuteInProcessResult:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(EDINET_FILER_PARTITION, list(keys))
    single = len(keys) == 1
    return dg.materialize(
        [raw.raw_edinet_filings, core.edinet_segment],
        instance=instance,
        partition_key=keys[0] if single else None,
        tags=None
        if single
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
        run_config={"ops": {"raw_edinet_filings": {"config": {"pace_seconds": 0.0}}}},
        resources=resources(tmp_path),
        raise_on_error=False,
    )


def _filing(sid: str, doc_id: str, filed: str) -> None:
    FILING_ROWS[(sid, doc_id)] = {
        "security_id": sid,
        "accession_number": doc_id,
        "report_type": fe.ANNUAL,
        "filing_date": filed,
        "segments_parsed_at": None,
        "segments_parser_version": None,
    }


def test_a_filer_s_reports_are_fetched_newest_first_and_read_against_their_figures(
    tmp_path: Path, provider: Edinet
) -> None:
    """Rounds across filers, newest first; the newest report's note is read by the real reader into
    segment rows, and a report EDINET no longer holds is marked without rows."""
    FILERS.update({"E03196": ["s-yellowhat"], "E00001": ["s-other"]})
    _filing("s-yellowhat", "S100YC2P", "2025-06-25")
    _filing("s-yellowhat", "S100OLD1", "2024-06-25")
    _filing("s-other", "S100OTH1", "2025-06-26")
    provider.archives = {
        "S100YC2P": (200, archive(YELLOW_HAT)),
        "S100OLD1": (404, OUTSIDE),
        "S100OTH1": (200, archive(b"<xbrli:xbrl/>")),
    }
    result = run_filers(tmp_path, ["E00001", "E03196"])
    assert result.success, [e.message for e in result.all_events if e.is_failure]
    assert provider.calls == ["S100OTH1", "S100YC2P", "S100OLD1"]
    fetched = meta(result, "raw_edinet_filings")
    assert (fetched["requested"], fetched["named"], fetched["outcomes"]) == (2, 3, 3)
    wrote = meta(result, "edinet_segment")
    assert (wrote["filings"], wrote["outcomes"], wrote["read"], wrote["no_instance"]) == (
        3,
        3,
        2,
        1,
    )
    assert wrote["parser_version"] == 1 and wrote["no_note"] == 1
    assert wrote["rows"] > 0 and wrote["tables_read"] > 0
    assert all(not _unread(r) for r in FILING_ROWS.values())
    inserted = sql_sent("insert into market.security_segment")
    assert inserted and any("edinet:" in str(v) for p in inserted for v in p)


def test_a_second_visit_fetches_nothing_read(tmp_path: Path, provider: Edinet) -> None:
    FILERS["E03196"] = ["s-yellowhat"]
    _filing("s-yellowhat", "S100YC2P", "2025-06-25")
    provider.archives = {"S100YC2P": (200, archive(YELLOW_HAT))}
    assert run_filers(tmp_path, ["E03196"]).success
    provider.calls.clear()
    again = run_filers(tmp_path, ["E03196"])
    assert again.success and provider.calls == []
    assert meta(again, "edinet_segment")["filings"] == 0


def test_a_body_that_is_neither_an_archive_nor_an_absence_is_asked_again(
    tmp_path: Path, provider: Edinet
) -> None:
    FILERS["E03196"] = ["s-yellowhat"]
    _filing("s-yellowhat", "S100YC2P", "2025-06-25")
    provider.archives = {"S100YC2P": (200, b'{"metadata": {"status": "200"}}')}
    first = run_filers(tmp_path, ["E03196"])
    assert meta(first, "edinet_segment")["not_zip"] == 1
    assert _unread(FILING_ROWS[("s-yellowhat", "S100YC2P")])
    provider.archives = {"S100YC2P": (200, archive(YELLOW_HAT))}
    provider.calls.clear()
    assert run_filers(tmp_path, ["E03196"]).success
    assert provider.calls == ["S100YC2P"]


def test_a_refusal_stops_the_run_and_leaves_the_rest_unasked(
    tmp_path: Path, provider: Edinet
) -> None:
    FILERS["E03196"] = ["s-yellowhat"]
    _filing("s-yellowhat", "S100YC2P", "2025-06-25")
    _filing("s-yellowhat", "S100OLD1", "2024-06-25")
    provider.archives = {"S100YC2P": edinet.EdinetRefused(429, "documents", "throttled")}
    result = run_filers(tmp_path, ["E03196"])
    fetched = meta(result, "raw_edinet_filings")
    assert (fetched["refused"], fetched["unasked"], fetched["outcomes"]) == (1, 1, 2)
    assert all(_unread(r) for r in FILING_ROWS.values())


# --- the registry -------------------------------------------------------------------------------

CODE_LIST_HEADER = (
    "ＥＤＩＮＥＴコード,提出者種別,上場区分,連結の有無,資本金,決算日,提出者名,提出者名（英字）,"  # noqa: RUF001
    "提出者名（ヨミ）,所在地,提出者業種,証券コード,提出者法人番号"  # noqa: RUF001
)


def code_list_zip(*rows: str) -> bytes:
    text = "\r\n".join(["ダウンロード実行日,2026年11月27日現在,件数,3件", CODE_LIST_HEADER, *rows])
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("EdinetcodeDlInfo.csv", text.encode("cp932"))
    return buf.getvalue()


def test_the_registry_maps_japanese_equities_and_leaves_the_sec_s_companies_alone() -> None:
    """Decision 2: a company the SEC gives a split is the SEC's. Toyota is in the code list and is
    released, with what was written under it; Nintendo and a new listing (`409A`) are mapped, and
    their reports are filed for exactly the filers mapped."""
    body = code_list_zip(
        "E02144,内国法人・組合,上場,有,1,3月31日,トヨタ自動車株式会社,TOYOTA,ト,愛知,輸送用機器,72030,1",
        "E02367,内国法人・組合,上場,有,1,3月31日,任天堂株式会社,NINTENDO,ニ,京都,その他製品,79740,2",
        "E39999,内国法人・組合,上場,有,1,3月31日,新規上場株式会社,NEW,シ,東京,情報・通信業,409A0,3",
    )
    calls: list[tuple[str, list[Any]]] = []

    class RegistryCursor(Cursor):
        def execute(self, sql: str, given: Sequence[Any] | Mapping[str, Any] = ()) -> None:
            text = " ".join(sql.split())
            calls.append((text, list(given) if not isinstance(given, Mapping) else []))
            self.rows, self.rowcount = [], 0
            if text.startswith("select s.security_id::text, min(c.code)"):
                self.rows = [
                    ("s-toyota", "7203", True),
                    ("s-nintendo", "7974", False),
                    ("s-new", "409A", False),
                    ("s-unlisted", "9999", False),
                ]
            elif text.startswith("with released as"):
                self.rows = [(1, 12, 3)]
            elif "from market.file_edinet_filings" in text:
                self.rows = [(20, 0, 0)]

    class RegistryConn(Conn):
        def cursor(self) -> Cursor:
            return RegistryCursor()

    class RegistryPostgres(Postgres):
        @contextmanager
        def connect(self) -> Iterator[Any]:
            yield RegistryConn()

    result = core.security_edinet_filer(
        dg.build_asset_context(),
        postgres=RegistryPostgres(),
        raw_edinet_code_list=[{"body": body}],
    )
    assert isinstance(result, dg.MaterializeResult) and result.metadata is not None
    got = {k: v for k, v in result.metadata.items() if k != "changed"}
    assert {k: got[k] for k in ("listed", "securities", "mapped", "the_sec_s", "unmapped")} == {
        "listed": 3, "securities": 4, "mapped": 2, "the_sec_s": 1, "unmapped": 1,
    }  # fmt: skip
    assert (got["released"], got["released_segments"], got["released_filings"]) == (1, 12, 3)
    [(sql, params)] = [
        (s, p) for s, p in calls if s.startswith("insert into market.security_filer")
    ]
    assert _rows(sql, params) == [
        {
            "security_id": "s-nintendo",
            "source_code": "edinet",
            "filer_id": "E02367",
            "via": "registry",
        },
        {"security_id": "s-new", "source_code": "edinet", "filer_id": "E39999", "via": "registry"},
    ]
    assert "via = excluded.via" in sql
    [released] = [p for s, p in calls if s.startswith("with released as")]
    assert released == [["s-toyota"]]
    [filed] = [p for s, p in calls if "from market.file_edinet_filings" in s]
    assert filed == [["E02367", "E39999"]]


# --- automation ---------------------------------------------------------------------------------


def test_the_daily_tick_asks_the_last_ten_whole_days_in_tokyo() -> None:
    """15:40 UTC is 00:40 the next day in Tokyo, so yesterday there is the newest whole day."""
    at = datetime(2026, 11, 27, 15, 40, tzinfo=UTC)
    with dg.build_schedule_context(scheduled_execution_time=at) as context:
        request = automation.edinet_daily(context)
    assert isinstance(request, dg.RunRequest)
    assert request.tags[ASSET_PARTITION_RANGE_START_TAG] == "2026-11-18"
    assert request.tags[ASSET_PARTITION_RANGE_END_TAG] == "2026-11-27"
    assert request.run_key == "2026-11-28:2026-11-18"


def test_due_filers_are_asked_in_aligned_chunks() -> None:
    grid = [f"E{n:05d}" for n in range(25)]
    chunks = automation.chunks_due(grid, ["E00003", "E00004", "E00021", "E99999"])
    assert [c[0] for c in chunks] == ["E00000", "E00020"]
    assert all(len(c) <= 10 for c in chunks)
