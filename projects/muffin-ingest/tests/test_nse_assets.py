"""The segments family's India lane, end to end through Dagster.

NSE is faked at the provider (`nse.filings`, `nse.instance`), so the lane's own choice of what to
fetch runs and each test counts the requests a run made. The normaliser and parser are REAL, over
Reliance's and Axis Bank's real instances (`fixtures/nse/`). The database is a fake that HOLDS
`security_filing` and its marks: a result a run reads stops being due.
"""

from __future__ import annotations

import json
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
from muffin_ingest.providers import nse
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.segments import core, raw
from muffin_ingest_dagster.defs.segments.partitions import FLOOR_DAYS, NSE_FILER_PARTITION
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

VOCABULARY = json.loads((FIXTURES / "segments" / "vocabulary.json").read_bytes())
ARCHIVES = "https://nsearchives.nseindia.com/corporate/xbrl/"

#: Reliance FY2024 consolidated and standalone, Axis Bank FY2024 consolidated.
REL_CONS = ARCHIVES + "INDAS_104634_1106038_22042024074704.xml"
REL_STAND = ARCHIVES + "INDAS_104633_1106029_22042024074028.xml"
AXIS_CONS = ARCHIVES + "BANKING_104697_1107859_25042024023559.xml"
BODIES = {
    link: (FIXTURES / "nse" / link.rsplit("/", 1)[1]).read_bytes()
    for link in (REL_CONS, REL_STAND, AXIS_CONS)
}


def results(*rows: tuple[str, str, str]) -> bytes:
    """A results list as NSE now writes it: links on the `.in` archives host, and a result with no
    XBRL listed as `.../xbrl/-`."""
    listed = [
        {
            "xbrl": link.replace(".nseindia.com", ".nseindia.in"),
            "toDate": to,
            "filingDate": filed,
            "consolidated": "Consolidated",
        }
        for link, to, filed in rows
    ]
    listed.append({"xbrl": ARCHIVES.replace(".com", ".in") + "-", "toDate": "31-Mar-2005"})
    return json.dumps(listed).encode()


LISTS: dict[str, bytes] = {
    "RELIANCE": results(
        (REL_STAND, "31-Mar-2024", "22-Apr-2024 19:40"),
        (REL_CONS, "31-Mar-2024", "22-Apr-2024 19:47"),
    ),
    "AXISBANK": results((AXIS_CONS, "31-Mar-2024", "25-Apr-2024 14:36")),
}


class Nse:
    """Answers by request. A request with no answer is a test asking for something it did not
    expect."""

    def __init__(self) -> None:
        self.lists: dict[str, tuple[int, bytes] | Exception] = {
            k: (200, v) for k, v in LISTS.items()
        }
        self.instances: dict[str, tuple[int, bytes] | Exception] = {
            k: (200, v) for k, v in BODIES.items()
        }
        self.calls: list[str] = []

    def filings(self, symbol: str, timeout_s: float = 30.0) -> nse.Answer:
        self.calls.append(f"list {symbol}")
        answer = self.lists.get(symbol)
        if answer is None:
            raise AssertionError(f"unexpected list: {symbol}")
        if isinstance(answer, Exception):
            raise answer
        return nse.Answer(_document(f"list:{symbol}", answer[1]), answer[0])

    def instance(self, link: str, timeout_s: float = 60.0) -> nse.Answer:
        assert link == nse.canonical(link), f"asked on the wrong host: {link}"
        self.calls.append(link.rsplit("/", 1)[1])
        answer = self.instances.get(link)
        if answer is None:
            raise AssertionError(f"unexpected instance: {link}")
        if isinstance(answer, Exception):
            raise answer
        return nse.Answer(_document(link, answer[1]), answer[0])


#: When a request's answer was fetched, by the fake's URL, where a test needs it older than now.
FETCHED_AT: dict[str, datetime] = {}


def _document(url: str, body: bytes) -> Document:
    return Document(
        url=url,
        body=body,
        content_type="text/xml",
        fetched_at=FETCHED_AT.get(url, datetime.now(UTC)),
    )


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch) -> Iterator[Nse]:
    fake = Nse()
    monkeypatch.setattr(nse, "filings", fake.filings)
    monkeypatch.setattr(nse, "instance", fake.instance)
    yield fake


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
                if (
                    r["security_id"] in sids
                    and r["report_type"] == "Annual"
                    and r["accession_number"].endswith(".xml")
                ):
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
            self.rows = [("USD",), ("INR",)]
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
    FILERS.update({"AXISBANK": ["s-in1"], "RELIANCE": ["s-in2"]})
    FILING_ROWS.clear()
    PARSER["version"] = 20
    CALLS.clear()
    FETCHED_AT.clear()
    yield


def instance_with(*keys: str) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(NSE_FILER_PARTITION, list(keys))
    return instance


def run(
    tmp_path: Path, instance: dg.DagsterInstance, keys: Sequence[str]
) -> dg.ExecuteInProcessResult:
    single = len(keys) == 1
    return dg.materialize(
        [raw.raw_nse_filings, core.nse_segment],
        instance=instance,
        partition_key=keys[0] if single else None,
        tags=None
        if single
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
        run_config={"ops": {"raw_nse_filings": {"config": {"pace_seconds": 0.0}}}},
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


def segment_rows() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sql, params in CALLS:
        if sql.startswith("insert into market.security_segment"):
            columns = sql[sql.index("(") + 1 : sql.index(")")].split(", ")
            out += [
                dict(zip(columns, params[n : n + len(columns)], strict=True))
                for n in range(0, len(params), len(columns))
            ]
    return out


def marked() -> set[str]:
    return {acc for (_, acc), r in FILING_ROWS.items() if not _unread(r)}


def test_a_first_visit_files_each_xml_result_and_reads_the_consolidated_one(
    tmp_path: Path, provider: Nse
) -> None:
    """Each filer's list, then its results newest first in rounds across filers, asked on the `.com`
    archives host; the placeholder `.../xbrl/-` is never a filing. The consolidated instances are
    normalised and parsed; the standalone one is read, wanted nothing, and is marked."""
    result = run(tmp_path, instance_with("AXISBANK", "RELIANCE"), ["AXISBANK", "RELIANCE"])
    assert result.success
    assert provider.calls[:2] == ["list AXISBANK", "list RELIANCE"]
    assert sorted(provider.calls[2:]) == sorted(link.rsplit("/", 1)[1] for link in BODIES)
    fetched = meta(result, "raw_nse_filings")
    assert (fetched["requested"], fetched["list_outcomes"], fetched["list_answered"]) == (2, 2, 2)
    assert (fetched["instances_named"], fetched["instance_outcomes"]) == (3, 3)
    wrote = meta(result, "nse_segment")
    assert (wrote["filings"], wrote["outcomes"], wrote["read"], wrote["standalone"]) == (3, 3, 2, 1)
    assert (wrote["members_named"], wrote["contexts_annualised"]) == (60, 36)
    assert {acc for _, acc in FILING_ROWS} == set(BODIES)
    assert marked() == set(BODIES)
    rows = segment_rows()
    assert {r["accession_number"] for r in rows} == {REL_CONS, AXIS_CONS}
    year = {
        r["member_code"]: r["value"]
        for r in rows
        if r["accession_number"] == REL_CONS
        and r["metric_code"] == "revenue"
        and r["period_type"] == "annual"
    }
    assert int(year["nse:RetailMember"]) == 3_068_480_000_000
    assert FILING_ROWS[("s-in2", REL_CONS)]["filing_date"] == "2024-04-22"


def test_a_second_visit_inside_the_floor_asks_nothing_and_fetches_nothing_read(
    tmp_path: Path, provider: Nse
) -> None:
    instance = instance_with("RELIANCE")
    assert run(tmp_path, instance, ["RELIANCE"]).success
    provider.calls.clear()
    result = run(tmp_path, instance, ["RELIANCE"])
    assert result.success
    assert provider.calls == [], "the list is fresh, and nothing is due"
    fetched = meta(result, "raw_nse_filings")
    assert (fetched["list_fresh"], fetched["list_outcomes"], fetched["instances_named"]) == (
        1,
        1,
        0,
    )
    assert meta(result, "nse_segment")["filings"] == 0


@pytest.mark.parametrize(
    ("age_days", "listed_again"),
    # A list fetched on a filer's floor day is due on its next one, whatever the hour of the
    # tick: an hour short of a floor period is old enough.
    [(raw.LIST_MAX_AGE_DAYS - 1, False), (FLOOR_DAYS - 1 / 24, True)],
)
def test_a_list_younger_than_the_floor_is_not_asked_and_one_as_old_is(
    tmp_path: Path, provider: Nse, age_days: float, listed_again: bool
) -> None:
    instance = instance_with("RELIANCE")
    FETCHED_AT["list:RELIANCE"] = datetime.now(UTC) - timedelta(days=age_days)
    assert run(tmp_path, instance, ["RELIANCE"]).success
    FETCHED_AT.clear()
    provider.calls.clear()

    assert run(tmp_path, instance, ["RELIANCE"]).success
    assert provider.calls == (["list RELIANCE"] if listed_again else [])


def test_a_fresh_list_still_names_its_instances_when_stage_2_never_filed_them(
    tmp_path: Path, provider: Nse
) -> None:
    """Stage 1 alone, every instance failing in transit: `security_filing` holds nothing for the
    filer, so the next run can only know its results from the stored list."""
    for link in BODIES:
        provider.instances[link] = nse.NseTransport("reset")
    instance = instance_with("RELIANCE")
    first = dg.materialize(
        [raw.raw_nse_filings],
        instance=instance,
        partition_key="RELIANCE",
        run_config={"ops": {"raw_nse_filings": {"config": {"pace_seconds": 0.0}}}},
        resources={
            "postgres": FakePostgres(),
            "parquet_io": ParquetIOManager(str(tmp_path)),
            "raw_store": RawStore(base_path=str(tmp_path)),
        },
    )
    assert first.success and meta(first, "raw_nse_filings")["instance_transport"] == 2
    assert FILING_ROWS == {}
    provider.instances.update({k: (200, v) for k, v in BODIES.items()})
    provider.calls.clear()

    again = run(tmp_path, instance, ["RELIANCE"])
    assert again.success
    assert sorted(provider.calls) == sorted(
        link.rsplit("/", 1)[1] for link in (REL_CONS, REL_STAND)
    ), "planned from the stored list"
    assert meta(again, "raw_nse_filings")["list_fresh"] == 1


def test_a_list_that_was_not_an_answer_is_asked_again_however_young(
    tmp_path: Path, provider: Nse
) -> None:
    """A gateway page in place of the results is no list to plan from."""
    provider.lists["RELIANCE"] = (200, b"<html>Access Denied</html>")
    instance = instance_with("RELIANCE")
    first = run(tmp_path, instance, ["RELIANCE"])
    assert first.success and meta(first, "raw_nse_filings")["list_not_answer"] == 1
    provider.calls.clear()

    assert run(tmp_path, instance, ["RELIANCE"]).success
    assert provider.calls == ["list RELIANCE"]


def test_an_instance_nse_does_not_have_is_marked_and_writes_nothing(
    tmp_path: Path, provider: Nse
) -> None:
    provider.instances[REL_CONS] = (404, b"")
    result = run(tmp_path, instance_with("RELIANCE"), ["RELIANCE"])
    assert result.success
    wrote = meta(result, "nse_segment")
    assert (wrote["no_instance"], wrote["standalone"], wrote["outcomes"]) == (1, 1, 2)
    assert marked() == {REL_CONS, REL_STAND}
    assert segment_rows() == []


def test_a_refusal_stops_the_run_and_marks_nothing_unasked(tmp_path: Path, provider: Nse) -> None:
    """A 403, a 429 or a redirect (which would leave the cache) is never an absence."""
    provider.lists["AXISBANK"] = nse.NseRefused(301, "https://www.nseindia.com/api/x")
    result = run(tmp_path, instance_with("AXISBANK", "RELIANCE"), ["AXISBANK", "RELIANCE"])
    assert result.success
    fetched = meta(result, "raw_nse_filings")
    assert (fetched["list_refused"], fetched["list_unasked"], fetched["list_outcomes"]) == (1, 1, 2)
    assert provider.calls == ["list AXISBANK"]
    assert marked() == set()


def test_a_body_that_is_not_an_instance_is_asked_again(tmp_path: Path, provider: Nse) -> None:
    provider.instances[REL_CONS] = (200, b"<html>Access Denied</html>")
    instance = instance_with("RELIANCE")
    result = run(tmp_path, instance, ["RELIANCE"])
    assert meta(result, "nse_segment")["not_answer"] == 1
    assert marked() == {REL_STAND}
    provider.calls.clear()
    provider.instances[REL_CONS] = (200, BODIES[REL_CONS])
    assert run(tmp_path, instance, ["RELIANCE"]).success
    assert provider.calls == [REL_CONS.rsplit("/", 1)[1]], "the list is fresh; the instance is due"
    assert marked() == {REL_CONS, REL_STAND}


def test_a_filer_the_registry_does_not_hold_is_not_asked(tmp_path: Path, provider: Nse) -> None:
    result = run(tmp_path, instance_with("NOTHELD"), ["NOTHELD"])
    assert result.success
    assert provider.calls == []
    assert meta(result, "raw_nse_filings")["list_not_askable"] == 1
