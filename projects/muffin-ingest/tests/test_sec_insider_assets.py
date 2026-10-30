"""The SEC family's insider lanes, end to end through Dagster, over bodies captured from SEC.

SEC is faked at `sec_edgar.fetch`, the one function every request goes through, so each test can
count the requests a run made. The database is a recording fake.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator, Sequence
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
from muffin_ingest.providers import sec_edgar
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.sec import automation as sec_automation
from muffin_ingest_dagster.defs.sec import core as sec_core
from muffin_ingest_dagster.defs.sec import raw as sec_raw
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres

FIXTURES = Path(__file__).parents[3] / "libs" / "muffin-ingest-lib" / "tests" / "fixtures" / "sec"
LISTING = (FIXTURES / "daily_index_2026_QTR4_listing.json").read_bytes()
INDEX_1023 = (FIXTURES / "form.20261023.trimmed.idx").read_bytes()
DATASET = (FIXTURES / "insider_2026q1.trimmed.zip").read_bytes()
AGREE, AGREE_OWNER = 917251, 1275238
#: Agree Realty's two Form 4s that day; the first is listed under the issuer AND its owner.
BOTH_LISTED, ISSUER_ONLY = "0001275238-26-000009", "0001348490-26-000008"


# --- SEC -----------------------------------------------------------------------------------------


class Sec:
    """Answers by URL. A URL with no answer is a test asking for something it did not expect."""

    def __init__(self) -> None:
        self.answers: dict[str, tuple[int, bytes] | Exception] = {}
        self.calls: list[str] = []

    def fetch(self, url: str, *, provider: str, timeout_s: float = 60.0) -> sec_edgar.SecAnswer:
        self.calls.append(url)
        answer = self.answers.get(url)
        if answer is None:
            raise AssertionError(f"unexpected request: {url}")
        if isinstance(answer, Exception):
            raise answer
        status, body = answer
        return sec_edgar.SecAnswer(
            document=Document(
                url=url, body=body, content_type="text/plain", fetched_at=datetime.now(UTC)
            ),
            status=status,
        )


@pytest.fixture
def sec(monkeypatch: pytest.MonkeyPatch) -> Iterator[Sec]:
    fake = Sec()
    monkeypatch.setattr(sec_edgar, "fetch", fake.fetch)
    yield fake


def listing_url() -> str:
    return sec_edgar.quarter_listing_url(date(2026, 10, 23))


def form4_url(cik: int, accession: str) -> str:
    return sec_edgar.filing_text_url(cik, accession)


# --- the database --------------------------------------------------------------------------------

#: Module level, as in the other asset tests: a resource's class attribute becomes a config field.
ISSUERS: list[tuple[int, list[str]]] = []
CALLS: list[tuple[str, list[Any]]] = []


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
        CALLS.append((text, list(params)))
        self.rows, self.rowcount = [], 0
        if "from market.security" in text and "group by cik" in text:
            self.rows = list(ISSUERS)
        elif text.startswith("select code, name from market.insider_transaction_code"):
            self.rows = [(c, f"code {c}") for c in "PSVADFIMCEHOXGLWZJKU"]
        elif text.startswith("insert into"):
            self.rowcount = text.split(" values ", 1)[1].split(" on conflict", 1)[0].count("(")
        elif text.startswith("delete from"):
            self.rowcount = 7

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        return None


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    ISSUERS[:] = [(AGREE, ["s-agree"]), (AGREE_OWNER, ["s-owner"])]
    CALLS.clear()
    yield


def run(
    tmp_path: Path,
    assets: Sequence[Any],
    keys: Sequence[str],
    *,
    others: Sequence[Any] = (),
) -> dg.ExecuteInProcessResult:
    single = len(keys) == 1
    return dg.materialize(
        [*assets, *others],
        selection=assets,
        partition_key=keys[0] if single else None,
        tags=None
        if single
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
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


def stored(tmp_path: Path, asset: str, key: str) -> list[dict[str, Any]]:
    return [
        dict(r) for r in RawStore(base_path=str(tmp_path)).stored_rows_for(dg.AssetKey(asset), key)
    ]


# --- the daily index -----------------------------------------------------------------------------


def test_a_range_asks_the_listing_once_and_each_published_day_once(
    tmp_path: Path, sec: Sec
) -> None:
    """10-10 to 10-13: a weekend costs nothing, Columbus Day keeps the listing that proves it a
    holiday, and 10-13 is the only day asked for. A day with no index answers 403, which reads as
    SEC refusing us, so no day the listing does not name is ever requested."""
    sec.answers[listing_url()] = (200, LISTING)
    sec.answers[sec_edgar.daily_index_url(date(2026, 10, 13))] = (200, INDEX_1023)
    result = run(tmp_path, [sec_raw.raw_sec_daily_index], ["2026-10-10", "2026-10-13"])
    assert result.success
    assert sec.calls == [listing_url(), sec_edgar.daily_index_url(date(2026, 10, 13))]
    m = meta(result, "raw_sec_daily_index")
    assert (m["index_weekend"], m["index_holiday"], m["index_published"]) == (2, 1, 1)

    assert stored(tmp_path, "raw_sec_daily_index", "2026-10-10") == []
    assert stored(tmp_path, "raw_sec_daily_index", "2026-10-11") == []
    [holiday] = stored(tmp_path, "raw_sec_daily_index", "2026-10-12")
    assert holiday["document"] == "quarter_listing" and bytes(holiday["body"]) == LISTING
    [published] = stored(tmp_path, "raw_sec_daily_index", "2026-10-13")
    assert published["document"] == "form_index" and bytes(published["body"]) == INDEX_1023


def test_a_weekend_run_asks_nothing(tmp_path: Path, sec: Sec) -> None:
    """The schedule runs Saturday on Sunday and Sunday on Monday. `classify_day` names a weekend
    too, but only after the quarter's listing is asked for, so the asset decides it first."""
    result = run(tmp_path, [sec_raw.raw_sec_daily_index], ["2026-10-11"])
    assert result.success
    assert sec.calls == []
    assert meta(result, "raw_sec_daily_index")["index_weekend"] == 1


def test_a_day_is_placed_by_its_partition_and_never_stored_with_it(
    tmp_path: Path, sec: Sec
) -> None:
    """Placement is not storage: the raw row carries the document and SEC's status, nothing else."""
    sec.answers[listing_url()] = (200, LISTING)
    sec.answers[sec_edgar.daily_index_url(date(2026, 10, 13))] = (200, INDEX_1023)
    assert run(tmp_path, [sec_raw.raw_sec_daily_index], ["2026-10-12", "2026-10-13"]).success
    for key in ("2026-10-12", "2026-10-13"):
        [row] = stored(tmp_path, "raw_sec_daily_index", key)
        assert set(row) == {
            "url", "body", "content_type", "sha256", "fetched_at", "run_id", "document",
            "http_status",
        }  # fmt: skip


def test_a_day_not_yet_published_fails_the_run_and_claims_nothing(tmp_path: Path, sec: Sec) -> None:
    """The listing names nothing after 10-23, so 10-26 is not published, not a holiday."""
    sec.answers[listing_url()] = (200, LISTING)
    sec.answers[sec_edgar.daily_index_url(date(2026, 10, 23))] = (200, INDEX_1023)
    result = run(tmp_path, [sec_raw.raw_sec_daily_index], ["2026-10-23", "2026-10-26"])
    assert not result.success
    assert not list(tmp_path.rglob("*.parquet"))
    assert sec_edgar.daily_index_url(date(2026, 10, 26)) not in sec.calls


# --- the day's Form 4s ---------------------------------------------------------------------------


def collect_day(tmp_path: Path, sec: Sec) -> None:
    sec.answers[listing_url()] = (200, LISTING)
    sec.answers[sec_edgar.daily_index_url(date(2026, 10, 23))] = (200, INDEX_1023)
    assert run(tmp_path, [sec_raw.raw_sec_daily_index], ["2026-10-23"]).success
    sec.calls.clear()


def test_a_tracked_issuer_s_form_4s_are_asked_once_each(tmp_path: Path, sec: Sec) -> None:
    collect_day(tmp_path, sec)
    sec.answers[form4_url(AGREE_OWNER, BOTH_LISTED)] = (200, b"<SEC-DOCUMENT>one")
    sec.answers[form4_url(AGREE, BOTH_LISTED)] = (200, b"<SEC-DOCUMENT>one")
    sec.answers[form4_url(AGREE, ISSUER_ONLY)] = (200, b"<SEC-DOCUMENT>two")
    result = run(tmp_path, [sec_raw.raw_sec_form4], ["2026-10-23"])
    assert result.success
    # A filing listed under the issuer and its owner is one filing.
    assert len(sec.calls) == 2
    m = meta(result, "raw_sec_form4")
    assert m["named"] == 2 and m["form4_answered"] == 2 and m["outcomes"] == 2
    held = {r["accession"] for r in stored(tmp_path, "raw_sec_form4", "2026-10-23")}
    assert held == {BOTH_LISTED, ISSUER_ONLY}


def test_a_second_run_asks_nothing_and_leaves_the_file_alone(tmp_path: Path, sec: Sec) -> None:
    collect_day(tmp_path, sec)
    for cik, accession in ((AGREE, BOTH_LISTED), (AGREE_OWNER, BOTH_LISTED), (AGREE, ISSUER_ONLY)):
        sec.answers[form4_url(cik, accession)] = (200, b"<SEC-DOCUMENT>x")
    assert run(tmp_path, [sec_raw.raw_sec_form4], ["2026-10-23"]).success
    file = tmp_path / "raw_sec_form4" / "2026-10-23.parquet"
    before = file.read_bytes()
    sec.calls.clear()
    result = run(tmp_path, [sec_raw.raw_sec_form4], ["2026-10-23"])
    assert result.success
    assert sec.calls == []
    m = meta(result, "raw_sec_form4")
    assert m["form4_held_already"] == 2 and m["outcomes"] == 2
    assert m["partitions_unchanged"] == 1
    assert file.read_bytes() == before


def test_a_refusal_stops_the_run_and_the_rest_are_unasked(tmp_path: Path, sec: Sec) -> None:
    collect_day(tmp_path, sec)
    refused = sec_edgar.SecRefused(429, "x")
    for cik, accession in ((AGREE, BOTH_LISTED), (AGREE_OWNER, BOTH_LISTED), (AGREE, ISSUER_ONLY)):
        sec.answers[form4_url(cik, accession)] = refused
    result = run(tmp_path, [sec_raw.raw_sec_form4], ["2026-10-23"])
    assert result.success
    assert len(sec.calls) == 1
    m = meta(result, "raw_sec_form4")
    assert (m["form4_refused"], m["form4_unasked"], m["outcomes"]) == (1, 1, 2)


def test_a_quarter_whose_data_set_is_held_asks_nothing(tmp_path: Path, sec: Sec) -> None:
    collect_day(tmp_path, sec)
    ParquetIOManager(str(tmp_path))  # the manager writes under the asset key's path
    folder = tmp_path / "raw_sec_insider_dataset"
    folder.mkdir()
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.table({"fetched_at": ["2027-01-08T00:00:00+00:00"]}), str(folder / "2026-10.parquet")
    )
    result = run(tmp_path, [sec_raw.raw_sec_form4], ["2026-10-23"])
    assert result.success
    assert sec.calls == []
    assert meta(result, "raw_sec_form4")["covered_by_dataset"] is True


def test_a_day_whose_index_is_not_collected_fails(tmp_path: Path, sec: Sec) -> None:
    result = run(tmp_path, [sec_raw.raw_sec_form4], ["2026-10-23"])
    assert not result.success
    assert sec.calls == []


# --- the trades ----------------------------------------------------------------------------------


def form4_text(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_form_4_trades_are_inserted_and_never_overwrite(tmp_path: Path, sec: Sec) -> None:
    """A data set's row and this lane's row for one line share a trade key: insert-only."""
    ISSUERS[:] = [(1652044, ["goog", "googl"])]
    folder = tmp_path / "raw_sec_form4"
    folder.mkdir()
    import pyarrow as pa
    import pyarrow.parquet as pq

    body = form4_text("form4_0001193125-26-135334.txt")
    pq.write_table(
        pa.Table.from_pylist(
            [{"accession": "0001193125-26-135334", "body": body,
              "fetched_at": "2026-04-01T11:00:00+00:00", "url": "u", "run_id": "r"}]
        ),
        str(folder / "2026-04-02.parquet"),
    )  # fmt: skip
    result = run(
        tmp_path, [sec_core.sec_form4_trade], ["2026-04-02"], others=[sec_raw.raw_sec_form4]
    )
    assert result.success
    [insert] = [sql for sql, _ in CALLS if sql.startswith("insert into market.insider_trade")]
    assert insert.endswith("on conflict (trade_key) do nothing")
    m = meta(result, "sec_form4_trade")
    assert m["rows"] == 14 and m["lines_nd"] == 7


def test_a_quarter_s_data_set_retracts_both_lanes_then_writes(tmp_path: Path, sec: Sec) -> None:
    ISSUERS[:] = [(1652044, ["goog", "googl"]), (1760965, ["s-1760965"])]
    sec.answers[sec_edgar.insider_dataset_url(2026, 1)] = (200, DATASET)
    result = run(
        tmp_path,
        [sec_raw.raw_sec_insider_dataset, sec_core.sec_insider_trade],
        ["2026-01"],
    )
    assert result.success
    statements = [sql for sql, _ in CALLS if sql.startswith(("delete", "insert"))]
    assert statements[0].startswith("delete from market.insider_trade")
    assert "do update set" in statements[1]
    [(_, params)] = [(s, p) for s, p in CALLS if s.startswith("delete")]
    assert params[0] == ["sec-form345", "sec-form4"]
    assert (params[1], params[2]) == (date(2026, 1, 1), date(2026, 3, 31))
    assert "0001193125-26-135334" in params[3]
    m = meta(result, "sec_insider_trade")
    assert m["filings"] == 2 and m["retracted"] == 7


def test_a_quarter_not_published_ends_the_run_and_claims_nothing(tmp_path: Path, sec: Sec) -> None:
    """A 404 is SEC saying "not yet", which is correct behaviour and must not fail the run: a failed
    run fires "A Dagster run failed" for six hours, and SEC was four months late with 2026q2.

    Nothing is materialised, so the quarter still reads as not held; stage 2 is skipped in the same
    run; and an observation records that it was asked.
    """
    sec.answers[sec_edgar.insider_dataset_url(2026, 2)] = (404, b"<Error/>")
    result = run(
        tmp_path, [sec_raw.raw_sec_insider_dataset, sec_core.sec_insider_trade], ["2026-04"]
    )
    assert result.success
    assert not result.get_asset_materialization_events()
    assert not list(tmp_path.rglob("*.parquet"))
    observed = [
        e.asset_observation_data.asset_observation
        for e in result.all_events
        if e.event_type_value == "ASSET_OBSERVATION"
    ]
    assert [(o.partition, o.metadata["published"].value) for o in observed] == [("2026-04", False)]
    skipped = [e.step_key for e in result.all_events if e.event_type_value == "STEP_SKIPPED"]
    assert skipped == ["sec_insider_trade"]
    assert not [sql for sql, _ in CALLS if sql.startswith(("insert", "delete"))]


# --- the data-set schedule -----------------------------------------------------------------------


def _requests(instance: dg.DagsterInstance, when: datetime) -> list[dg.RunRequest]:
    with dg.build_schedule_context(instance=instance, scheduled_execution_time=when) as context:
        requests = sec_automation.sec_insider_datasets(context)
    assert isinstance(requests, list)
    return requests


def test_the_schedule_asks_every_unheld_quarter_from_the_form_4_grid_s_start() -> None:
    """2026q2 and 2026q3 on 2026-11-02, oldest first; never 2026q1 or earlier, which is history."""
    monday = datetime(2026, 11, 2, 11, 45, tzinfo=UTC)
    with dg.instance_for_test() as instance:
        asked = [r.partition_key for r in _requests(instance, monday)]
        assert asked == ["2026-04", "2026-07"], "every quarter, not just the newest"

        instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=sec_raw.DATASET, partition="2026-04")
        )
        assert [r.partition_key for r in _requests(instance, monday)] == ["2026-07"]


def test_an_unpublished_quarter_is_asked_again_the_next_week() -> None:
    """Dagster launches a run key once per schedule, so the key must change with the tick."""
    with dg.instance_for_test() as instance:
        first = _requests(instance, datetime(2026, 11, 2, 11, 45, tzinfo=UTC))
        second = _requests(instance, datetime(2026, 11, 9, 11, 45, tzinfo=UTC))
    assert [r.partition_key for r in first] == [r.partition_key for r in second]
    assert not {r.run_key for r in first} & {r.run_key for r in second}


def test_the_fixture_zip_is_a_data_set() -> None:
    assert "SUBMISSION.tsv" in zipfile.ZipFile(io.BytesIO(DATASET)).namelist()
