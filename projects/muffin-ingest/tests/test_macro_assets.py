"""The macro lane's assets, schedule, sensor and check, over answers captured on 2026-11-21."""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import macro
from muffin_ingest.providers import openbb

from muffin_ingest_dagster.defs.macro import automation as macro_automation
from muffin_ingest_dagster.defs.macro import checks as macro_checks
from muffin_ingest_dagster.defs.macro import core as macro_core
from muffin_ingest_dagster.defs.macro import partitions as macro_partitions
from muffin_ingest_dagster.defs.macro import raw as macro_raw
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES, loaded_defs

FRED = "/api/v1/economy/fred_series"
CPI = "/api/v1/economy/cpi"
CURVE = "/api/v1/fixedincome/government/yield_curve"
UNEMPLOYMENT = "/api/v1/economy/unemployment"

#: (code, route, provider, params, provider_country, enabled), the catalogue's shape.
CATALOGUE: list[tuple[Any, ...]] = [
    ("us-cpi", CPI, "oecd", {}, "united_states", True),
    ("jp-cpi", CPI, "oecd", {}, "japan", True),
    ("de-10y-yield-fred", FRED, "fred", {"symbol": "IRLTLT01DEM156N"}, None, True),
    ("us-yield-curve", CURVE, "federal_reserve", {}, None, True),
    ("us-unemployment", UNEMPLOYMENT, "oecd", {}, "united_states", True),
    ("us-cpi-fred", FRED, "fred", {"symbol": "CPALTT01USM659N"}, None, False),
]


def captured_rows(code: str) -> list[dict[str, Any]]:
    data: dict[str, Any] = json.loads((FIXTURES / "macro" / f"{code}.json").read_text())
    return list(data["rows"])


class Hub:
    """`openbb.macro_series`, answering from captures, refusing, or raising, recording each ask."""

    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def macro_series(self, route: str, **kwargs: Any) -> openbb.Answer:
        code = next(
            c
            for c, r, _, p, country, _ in CATALOGUE
            if r == route
            and (p == (kwargs.get("params") or {}) and country == kwargs.get("country"))
        )
        self.calls.append((code, kwargs))
        answer = self.answers[code]
        if isinstance(answer, Exception):
            raise answer
        return openbb.Answer(rows=[dict(r) for r in answer])


@contextmanager
def hub(fake: Hub) -> Iterator[Hub]:
    saved = openbb.macro_series
    openbb.macro_series = fake.macro_series
    try:
        yield fake
    finally:
        openbb.macro_series = saved


# --- the database --------------------------------------------------------------------------------

OFFERED: list[dict[str, Any]] = []
DELETES: list[tuple[str, list[Any]]] = []
AGES: list[tuple[Any, ...]] = []


def _columns(text: str) -> list[str]:
    return [c.strip() for c in text.split("(", 1)[1].split(")", 1)[0].split(",")]


class Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        self.rows, self.rowcount = [], 0
        if text.startswith("insert into"):
            columns = _columns(text)
            values = list(params)
            rows = [
                dict(zip(columns, values[i : i + len(columns)], strict=True))
                for i in range(0, len(values), len(columns))
            ]
            OFFERED.extend(rows)
            self.rowcount = len(rows)
        elif text.startswith("delete from"):
            DELETES.append((text, list(params)))
            self.rowcount = 1
        elif text.startswith("select i.code, i.frequency"):
            self.rows = list(AGES)
        elif text.startswith("select code, route, provider"):
            self.rows = list(CATALOGUE)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        return None

    def __enter__(self) -> Conn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    OFFERED.clear()
    DELETES.clear()
    AGES.clear()
    yield


def run(
    tmp_path: Path, keys: list[str], *, stage_two: bool = True, raise_on_error: bool = True
) -> dg.ExecuteInProcessResult:
    chosen: list[Any] = [macro_raw.raw_macro_series]
    if stage_two:
        chosen.append(macro_core.macro_observation)
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(macro_partitions.MACRO_PARTITIONS, keys)
        return dg.materialize(
            chosen,
            instance=instance,
            tags={
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
            },
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
            },
            raise_on_error=raise_on_error,
        )


def meta(result: dg.ExecuteInProcessResult, key: str) -> dict[str, Any]:
    for event in result.get_asset_materialization_events():
        materialization = event.step_materialization_data.materialization
        if materialization.asset_key == dg.AssetKey(key):
            return {k: v.value for k, v in materialization.metadata.items()}
    raise AssertionError(f"{key} did not materialize")


def stored(tmp_path: Path, code: str) -> list[tuple[Any, ...]]:
    path = tmp_path / "raw_macro_series" / f"{code}.parquet"
    if not path.exists():
        return []
    described = duckdb.sql(f"describe select * from read_parquet('{path}')").fetchall()
    columns = {row[0] for row in described}
    if "fetched_at" not in columns:  # the manager's "collected nothing" marker
        return []
    return duckdb.sql(
        f"select indicator_code, fetched_at, count(*) from read_parquet('{path}', "
        f"union_by_name=true) where fetched_at is not null group by 1, 2 order by 2"
    ).fetchall()


ALL = [c for c, *_ in CATALOGUE]


# --- stage 1 -------------------------------------------------------------------------------------


def test_each_enabled_series_is_asked_once_and_every_key_is_counted(tmp_path: Path) -> None:
    fake = Hub(
        {
            "us-cpi": captured_rows("us-cpi"),
            "jp-cpi": captured_rows("jp-cpi"),
            "de-10y-yield-fred": captured_rows("de-10y-yield-fred"),
            "us-yield-curve": captured_rows("us-yield-curve"),
            "us-unemployment": RuntimeError("Connection reset by peer"),
        }
    )
    with hub(fake):
        result = run(tmp_path, [*ALL, "gone"])
    m = meta(result, "raw_macro_series")
    # The disabled row and the key the catalogue no longer holds are never asked.
    assert [code for code, _ in fake.calls] == ALL[:5]
    assert (m["answered"], m["empty"], m["failed"]) == (3, 1, 1)
    assert (m["disabled"], m["uncatalogued"]) == (1, 1)
    assert sum(int(m[o]) for o in macro_raw.OUTCOMES) == m["requested"] == 7
    assert m["errors"] == "us-unemployment"
    # What was asked: the catalogue's country and params, from 400 days back.
    asks = dict(fake.calls)
    assert asks["us-cpi"]["country"] == "united_states"
    assert asks["de-10y-yield-fred"]["params"] == {"symbol": "IRLTLT01DEM156N"}
    assert asks["us-cpi"]["start"] == datetime.now(UTC).date() - macro.WINDOW
    # A file for each answered series only; the empty and the failed one left nothing.
    assert [r[2] for r in stored(tmp_path, "us-cpi")] == [11]
    assert stored(tmp_path, "jp-cpi") == stored(tmp_path, "us-unemployment") == []


def test_an_empty_or_failed_answer_replaces_nothing_and_an_answer_replaces_all(
    tmp_path: Path,
) -> None:
    keys = ["us-cpi"]
    with hub(Hub({"us-cpi": captured_rows("us-cpi")})):
        run(tmp_path, keys, stage_two=False)
    first = stored(tmp_path, "us-cpi")
    with hub(Hub({"us-cpi": []})):
        run(tmp_path, keys, stage_two=False, raise_on_error=False)
    with hub(Hub({"us-cpi": RuntimeError("boom")})):
        run(tmp_path, keys, stage_two=False, raise_on_error=False)
    assert stored(tmp_path, "us-cpi") == first
    with hub(Hub({"us-cpi": captured_rows("us-cpi")[:4]})):
        run(tmp_path, keys, stage_two=False)
    [(code, fetched_at, count)] = stored(tmp_path, "us-cpi")
    assert (code, count) == ("us-cpi", 4) and fetched_at != first[0][1]


def test_a_run_in_which_nothing_answered_fails(tmp_path: Path) -> None:
    """A provider or network event, not five dead series: it must not read as a quiet day."""
    fake = Hub({code: RuntimeError("Connection refused") for code in ALL})
    with hub(fake):
        result = run(tmp_path, ALL, raise_on_error=False)
    assert not result.success
    assert stored(tmp_path, "us-cpi") == []


def test_a_credential_in_a_provider_error_never_reaches_the_logs_or_metadata(
    tmp_path: Path,
) -> None:
    key = "s3cr3tfredkey"
    fake = Hub(
        {
            "us-cpi": captured_rows("us-cpi"),
            "jp-cpi": [],
            "de-10y-yield-fred": RuntimeError(
                f"400 for https://api.stlouisfed.org/fred/series/observations?api_key={key}"
            ),
            "us-yield-curve": [],
            "us-unemployment": [],
        }
    )
    # THE WHOLE EVENT LOG, log lines included: a run result's events hold no log message, and the
    # warning naming each failed series is exactly where an unredacted URL would land.
    with hub(fake), dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(macro_partitions.MACRO_PARTITIONS, ALL)
        result = dg.materialize(
            [macro_raw.raw_macro_series],
            instance=instance,
            tags={
                ASSET_PARTITION_RANGE_START_TAG: ALL[0],
                ASSET_PARTITION_RANGE_END_TAG: ALL[-1],
            },
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
            },
        )
        logged = [entry.user_message for entry in instance.all_logs(result.run_id)]
    assert any("de-10y-yield-fred" in line and "REDACTED" in line for line in logged)
    assert not any(key in line for line in logged)
    assert key not in json.dumps({k: str(v) for k, v in meta(result, "raw_macro_series").items()})
    assert meta(result, "raw_macro_series")["failed"] == 1


# --- stage 2 -------------------------------------------------------------------------------------


def test_points_are_written_and_retracted_within_each_answer_s_window(tmp_path: Path) -> None:
    fake = Hub(
        {
            "us-cpi": captured_rows("us-cpi"),
            "jp-cpi": [],
            "de-10y-yield-fred": captured_rows("de-10y-yield-fred"),
            "us-yield-curve": captured_rows("us-yield-curve"),
            "us-unemployment": captured_rows("us-unemployment"),
        }
    )
    with hub(fake):
        result = run(tmp_path, ALL)
    m = meta(result, "macro_observation")
    assert (m["answered"], m["no_rows"], m["points"]) == (4, 2, 11 + 13 + 11 + 11)
    assert "jp-cpi" in m["series_without_rows"]
    by_code = {row["indicator_code"] for row in OFFERED}
    assert by_code == {"us-cpi", "de-10y-yield-fred", "us-yield-curve", "us-unemployment"}

    windows = {params[0]: (params[1], params[2]) for _, params in DELETES}
    today = datetime.now(UTC).date()
    start = (today - macro.WINDOW).isoformat()
    # A series retracts from the start it asked to the day it was asked; the snapshot only its date.
    assert windows["us-unemployment"] == (start, today.isoformat())
    assert windows["us-yield-curve"] == ("2026-11-19", "2026-11-19")
    # Nothing is retracted for a series with no answer.
    assert "jp-cpi" not in windows
    assert all("between %s and %s" in sql for sql, _ in DELETES)


def test_a_re_parse_of_a_stored_answer_asks_no_provider(tmp_path: Path) -> None:
    with hub(Hub({"us-cpi": captured_rows("us-cpi")})):
        run(tmp_path, ["us-cpi"], stage_two=False)
    OFFERED.clear()
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(macro_partitions.MACRO_PARTITIONS, ["us-cpi"])
        with hub(Hub({})) as fake:
            dg.materialize(
                [macro_raw.raw_macro_series.to_source_asset(), macro_core.macro_observation],
                instance=instance,
                partition_key="us-cpi",
                resources={
                    "postgres": FakePostgres(),
                    "parquet_io": ParquetIOManager(str(tmp_path)),
                    "raw_store": RawStore(base_path=str(tmp_path)),
                },
            )
    assert fake.calls == []
    assert len(OFFERED) == 11


# --- the schedule, the sensor, the check ---------------------------------------------------------


def test_the_schedule_asks_the_whole_grid_in_one_run_keyed_by_its_day() -> None:
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(macro_partitions.MACRO_PARTITIONS, ALL)
        # A CONTEXT MANAGER, or the context outlives the instance and breaks a later test file.
        with dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime(2026, 11, 22, 7, 40, tzinfo=UTC)
        ) as context:
            requests = macro_automation.macro_daily(context)
    assert isinstance(requests, list)
    [request] = requests
    assert request.run_key == "macro_daily:2026-11-22"
    assert request.tags[ASSET_PARTITION_RANGE_START_TAG] == ALL[0]
    assert request.tags[ASSET_PARTITION_RANGE_END_TAG] == ALL[-1]


def test_the_sensor_adds_enabled_series_and_never_a_disabled_one() -> None:
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(macro_partitions.MACRO_PARTITIONS, ["us-cpi"])
        with dg.build_sensor_context(
            instance=instance, resources={"postgres": FakePostgres()}
        ) as context:
            result = macro_automation.new_macro_series(context)
    assert isinstance(result, dg.SensorResult)
    [request] = result.dynamic_partitions_requests or []
    assert isinstance(request, dg.AddDynamicPartitionsRequest)
    assert list(request.partition_keys) == [
        "jp-cpi",
        "de-10y-yield-fred",
        "us-yield-curve",
        "us-unemployment",
    ]
    assert result.run_requests == []


def test_the_freshness_check_names_a_stale_series_and_one_with_no_point() -> None:
    AGES[:] = [
        ("btc", "daily", 1),
        ("gb-unemployment-fred", "monthly", 143),
        ("jp-cpi", "monthly", None),
        ("us-unemployment-fred", "monthly", 446),
        ("us-sofr", "daily", 9),
    ]
    result = macro_checks.macro_series_fresh(FakePostgres())
    assert isinstance(result, dg.AssetCheckResult)
    assert not result.passed
    assert result.severity == dg.AssetCheckSeverity.WARN
    which = str(result.metadata["which"].value)
    assert "jp-cpi (no point)" in which and "us-unemployment-fred (446 days)" in which
    assert "us-sofr (9 days)" in which
    assert "btc" not in which and "gb-unemployment-fred" not in which


def test_the_lane_ships_with_its_schedule_stopped_and_its_sensor_running() -> None:
    defs = loaded_defs()
    assert defs.get_schedule_def("macro_daily").default_status == dg.DefaultScheduleStatus.STOPPED
    assert defs.get_sensor_def("new_macro_series").default_status == dg.DefaultSensorStatus.RUNNING
    job = defs.resolve_job_def("macro_daily")
    keys = {k.to_user_string() for k in job.asset_layer.selected_asset_keys}
    assert keys == {"raw_macro_series", "macro_observation"}
