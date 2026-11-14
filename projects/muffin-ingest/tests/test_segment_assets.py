"""The segments family's SEC lane, end to end through Dagster, over a real instance.

SEC is faked at `sec_edgar.fetch`, so the lane's own search for the instance runs and each test
counts the requests a run made. The database is a fake that HOLDS the filing table's marks: a
filing a run reads stops being due, and raising the parser version makes it due again, as in
production. The instance is Apple's 10-Q from the library's golden fixtures, whose 46 rows the
edge's own parser produced.
"""

from __future__ import annotations

import gzip
import hashlib
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
    MAX_RUNTIME_SECONDS_TAG,
)
from muffin_ingest.facets import sec_segments, segments
from muffin_ingest.providers import sec_edgar
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.sec.partitions import FILERS_PER_RUN, SEC_FILER_PARTITION, cik_key
from muffin_ingest_dagster.defs.segments import automation, checks, core, raw
from muffin_ingest_dagster.lib import runtime
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

GOLDEN = FIXTURES / "segments"
VOCABULARY = json.loads((GOLDEN / "vocabulary.json").read_bytes())
AAPL_INSTANCE = gzip.decompress((GOLDEN / "sec_aapl_10q_2026q3.xml.gz").read_bytes())
AAPL_ROWS = len(json.loads((GOLDEN / "expected_aapl_10q_2026q3.json").read_bytes())["rows"])

AAPL, TSM = 320193, 1046179
K_AAPL, K_TSM = cik_key(AAPL), cik_key(TSM)
Q3, Q2, TSM_20F = "0000320193-26-000020", "0000320193-26-000011", "0001628280-26-025362"
NAME = "aapl-20260627_htm.xml"


# --- SEC -----------------------------------------------------------------------------------------


class Sec:
    """Answers by URL. A URL with no answer is a test asking for something it did not expect."""

    def __init__(self) -> None:
        self.answers: dict[str, tuple[int, bytes] | Exception] = {}
        self.calls: list[str] = []

    def fetch(
        self, url: str, *, provider: str, timeout_s: float = 60.0, max_bytes: int | None = None
    ) -> sec_edgar.SecAnswer:
        self.calls.append(url)
        answer = self.answers.get(url)
        if answer is None:
            raise AssertionError(f"unexpected request: {url}")
        if isinstance(answer, Exception):
            raise answer
        status, body = answer
        return sec_edgar.SecAnswer(
            document=Document(
                url=url, body=body, content_type="text/xml", fetched_at=datetime.now(UTC)
            ),
            status=status,
        )

    def filing(self, cik: int, accession: str, *, size: int = 962_405) -> None:
        """A filing whose `index.json` names an instance, and the instance."""
        listing = {"directory": {"item": [{"name": NAME, "size": str(size)}]}}
        self.answers[sec_edgar.filing_index_url(cik, accession)] = (
            200,
            json.dumps(listing).encode(),
        )
        self.answers[sec_edgar.filing_document_url(cik, accession, NAME)] = (200, AAPL_INSTANCE)

    def no_instance(self, cik: int, accession: str) -> None:
        self.answers[sec_edgar.filing_index_url(cik, accession)] = (
            200,
            b'{"directory":{"item":[]}}',
        )
        self.answers[sec_edgar.filing_index_html_url(cik, accession)] = (404, b"")


@pytest.fixture
def sec(monkeypatch: pytest.MonkeyPatch) -> Iterator[Sec]:
    fake = Sec()
    monkeypatch.setattr(sec_edgar, "fetch", fake.fetch)
    yield fake


# --- the database --------------------------------------------------------------------------------

#: Module level: a resource's class attribute becomes a config field.
#: Each filing: cik, accession, filed, and per security the parser version it was read at (or None).
FILINGS: list[dict[str, Any]] = []
PARSER = {"version": 20}
CALLS: list[tuple[str, Any]] = []
#: An accession whose rows the database refuses, as a bad date or an unknown metric would be.
POISON: set[str] = set()


def _due(mark: int | None) -> bool:
    return mark is None or mark < PARSER["version"]


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
        named_params = dict(given) if isinstance(given, Mapping) else {}
        params = [] if isinstance(given, Mapping) else list(given)
        CALLS.append((text, named_params or params))
        self.rows, self.rowcount = [], 0
        if "having bool_or" in text:
            ciks, named = set(params[0]), set(params[1])
            for f in FILINGS:
                unread = sorted(s for s, mark in f["marks"].items() if _due(mark))
                if f["cik"] in ciks and (unread or f["accession"] in named):
                    self.rows.append(
                        (f["cik"], f["accession"], f["filed"], unread or None, sorted(f["marks"]))
                    )
        elif "make_interval(days" in text:
            days = named_params["days"]
            never = [
                f
                for f in FILINGS
                if any(m is None for m in f["marks"].values()) and f["held_days"] >= days
            ]
            behind = [
                f for f in FILINGS if any(m is not None and _due(m) for m in f["marks"].values())
            ]
            oldest = min((f["filed"] for f in never), default=None)
            self.rows = [(len(never), len({f["cik"] for f in never}), oldest, len(behind))]
        elif "from market.security_filing f" in text and "group by s.cik" in text:
            counts: dict[int, int] = {}
            for f in FILINGS:
                if any(_due(m) for m in f["marks"].values()):
                    counts[f["cik"]] = counts.get(f["cik"], 0) + 1
            self.rows = sorted(counts.items())
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
            self.rows = [("USD",)]
        elif text.startswith("insert into market.security_segment"):
            if POISON & {p for p in params if isinstance(p, str)}:
                raise RuntimeError("date/time field value out of range")
        elif text.startswith("update market.security_filing"):
            _, version, accession, security_ids = params
            for f in FILINGS:
                if f["accession"] == accession:
                    for security_id in security_ids:
                        f["marks"][security_id] = version

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


def filing(
    cik: int, accession: str, filed: str, *security_ids: str, held_days: int = 0
) -> dict[str, Any]:
    return {
        "cik": cik,
        "accession": accession,
        "filed": date.fromisoformat(filed),
        "marks": dict.fromkeys(security_ids),
        # How long ago the filer lane wrote the row (`security_filing.as_of`).
        "held_days": held_days,
    }


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    FILINGS[:] = [filing(AAPL, Q3, "2026-07-31", "s-a1", "s-a2")]
    PARSER["version"] = 20
    CALLS.clear()
    POISON.clear()
    yield


def instance_with(*keys: str) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(SEC_FILER_PARTITION, list(keys))
    return instance


def run(
    tmp_path: Path,
    instance: dg.DagsterInstance,
    keys: Sequence[str],
    *,
    config: dict[str, Any] | None = None,
) -> dg.ExecuteInProcessResult:
    single = len(keys) == 1
    return dg.materialize(
        [raw.raw_sec_instance, core.sec_segment],
        instance=instance,
        partition_key=keys[0] if single else None,
        tags=None
        if single
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
        run_config={"ops": {"raw_sec_instance": {"config": config or {}}}},
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


def stored(tmp_path: Path, key: str) -> list[dict[str, Any]]:
    held = RawStore(base_path=str(tmp_path)).stored_rows_for(raw.SEC_INSTANCE, key)
    return [dict(r) for r in held]


def writes(prefix: str = "") -> list[tuple[str, list[Any]]]:
    return [
        (sql, params)
        for sql, params in CALLS
        if sql.startswith(("insert", "update", "delete")) and prefix in sql
    ]


def outcomes(m: dict[str, Any]) -> int:
    return sum(m[f"instance_{o}"] for o in raw.INSTANCE_OUTCOMES)


def marks(accession: str) -> dict[str, int | None]:
    return next(dict(f["marks"]) for f in FILINGS if f["accession"] == accession)


# --- a filing read end to end --------------------------------------------------------------------


def test_a_due_filing_is_fetched_once_written_for_each_share_class_and_marked(
    tmp_path: Path, sec: Sec
) -> None:
    sec.filing(AAPL, Q3)
    result = run(tmp_path, instance_with(K_AAPL, K_TSM), [K_AAPL, K_TSM])
    assert result.success

    # ONE FETCH FOR THE FILER, however many securities hold its CIK.
    assert sec.calls == [
        sec_edgar.filing_index_url(AAPL, Q3),
        sec_edgar.filing_document_url(AAPL, Q3, NAME),
    ]
    m = meta(result, "raw_sec_instance")
    assert (m["requested"], m["named"], m["instance_answered"], m["documents"]) == (2, 1, 1, 2)
    assert outcomes(m) == m["outcomes"] == m["named"]

    # RAW IS WHAT SEC SERVED, byte for byte, plus only the declared columns.
    index, instance = stored(tmp_path, K_AAPL)
    assert (index["document"], instance["document"]) == ("index_json", "instance")
    assert bytes(instance["body"]) == AAPL_INSTANCE
    assert instance["sha256"] == hashlib.sha256(AAPL_INSTANCE).hexdigest()
    assert {r["outcome"] for r in (index, instance)} == {"answered"}
    assert {r["accession"] for r in (index, instance)} == {Q3}
    assert stored(tmp_path, K_TSM) == [], "a filer with nothing due holds the empty marker"

    s = meta(result, "sec_segment")
    assert (s["filings"], s["read"], s["no_segments"], s["stamped"]) == (1, 1, 0, 2)
    assert s["rows"] == 2 * AAPL_ROWS and s["outcomes"] == s["filings"]
    assert s["placed"] + s["unplaced"] == 2 * AAPL_ROWS and s["currency_unknown"] == 0
    assert s["parser_version"] == 20

    inserts = writes("insert into market.security_segment")
    assert len(inserts) == 2, "one statement per security"
    sql, params = inserts[0]
    assert (
        "on conflict (security_id, axis, member_code, parent_key, metric_code, period_type, "
        "period_ending) do update" in sql
    )
    columns = sql[sql.index("(") + 1 : sql.index(")")].split(", ")
    assert "parent_key" not in columns, "a generated column is never inserted"
    assert "sec-segments" in params and Q3 in params
    # WHAT THE FILING NO LONGER STATES IS RETRACTED, within the filing and the security.
    deletes = writes("delete from market.security_segment")
    assert len(deletes) == 2 and all(
        params[:2] in (["s-a1", Q3], ["s-a2", Q3]) for _, params in deletes
    )
    assert marks(Q3) == {"s-a1": 20, "s-a2": 20}
    # Each filing lands in one transaction: rows, retraction and mark, then the commit.
    assert [sql.split(" ")[0] for sql, _ in CALLS if sql.split(" ")[0] != "select"] == [
        "insert", "delete", "insert", "delete", "update", "commit",
    ]  # fmt: skip


def test_a_filing_that_was_read_is_never_asked_for_again(tmp_path: Path, sec: Sec) -> None:
    sec.filing(AAPL, Q3)
    instance = instance_with(K_AAPL, K_TSM)
    assert run(tmp_path, instance, [K_AAPL, K_TSM]).success
    asked, before = len(sec.calls), stored(tmp_path, K_AAPL)
    CALLS.clear()

    second = run(tmp_path, instance, [K_AAPL, K_TSM])
    assert second.success
    assert len(sec.calls) == asked
    m = meta(second, "raw_sec_instance")
    assert (m["named"], m["outcomes"], m["partitions_unchanged"]) == (0, 0, 2)
    assert stored(tmp_path, K_AAPL) == before
    assert writes() == [] and meta(second, "sec_segment")["filings"] == 0


def test_a_raised_parser_version_rereads_from_raw_without_asking_sec(
    tmp_path: Path, sec: Sec
) -> None:
    """The point of keeping the instance: a parser change costs a re-read of files on disk."""
    sec.filing(AAPL, Q3)
    instance = instance_with(K_AAPL)
    assert run(tmp_path, instance, [K_AAPL]).success
    asked = len(sec.calls)
    PARSER["version"] = 21

    again = run(tmp_path, instance, [K_AAPL])
    assert again.success and len(sec.calls) == asked
    m = meta(again, "raw_sec_instance")
    assert (m["named"], m["instance_held_already"], outcomes(m)) == (1, 1, 1)
    s = meta(again, "sec_segment")
    assert (s["read"], s["rows"], s["parser_version"]) == (1, 2 * AAPL_ROWS, 21)
    assert marks(Q3) == {"s-a1": 21, "s-a2": 21}


def test_a_filing_named_by_hand_is_fetched_and_read_again_for_every_security(
    tmp_path: Path, sec: Sec
) -> None:
    sec.filing(AAPL, Q3)
    instance = instance_with(K_AAPL)
    assert run(tmp_path, instance, [K_AAPL]).success
    asked = len(sec.calls)
    CALLS.clear()

    again = run(tmp_path, instance, [K_AAPL], config={"reread": {K_AAPL: [Q3]}})
    assert again.success and len(sec.calls) == asked + 2
    assert meta(again, "raw_sec_instance")["instance_answered"] == 1
    s = meta(again, "sec_segment")
    assert (s["read"], s["stamped"]) == (1, 2), "not due, and still read: this run fetched it"
    assert len(stored(tmp_path, K_AAPL)) == 2, "the re-fetch replaced the documents, not added"


def test_only_the_share_class_still_unread_is_written(tmp_path: Path, sec: Sec) -> None:
    """A share class newly mapped to the CIK: the other one's rows are not rewritten."""
    FILINGS[0]["marks"]["s-a1"] = 20
    sec.filing(AAPL, Q3)
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL])
    s = meta(result, "sec_segment")
    assert (s["rows"], s["stamped"]) == (AAPL_ROWS, 1)
    [(_, params)] = writes("update market.security_filing")
    assert params[2:] == [Q3, ["s-a2"]]


def test_two_filings_of_one_filer_are_written_oldest_first(tmp_path: Path, sec: Sec) -> None:
    """The row's key leaves the accession out, so the last filing written owns a period both
    state. That must be the newer one."""
    FILINGS.append(filing(AAPL, Q2, "2026-05-01", "s-a1", "s-a2"))
    sec.filing(AAPL, Q3)
    sec.filing(AAPL, Q2)
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL])
    assert result.success and meta(result, "sec_segment")["read"] == 2
    stamped = [params[2] for _, params in writes("update market.security_filing")]
    assert stamped == [Q2, Q3]
    assert len(stored(tmp_path, K_AAPL)) == 4


# --- what is not an absence ----------------------------------------------------------------------


def test_a_refusal_stops_the_run_files_nothing_and_leaves_every_filing_due(
    tmp_path: Path, sec: Sec
) -> None:
    FILINGS.append(filing(TSM, TSM_20F, "2026-04-16", "s-tsm"))
    sec.answers[sec_edgar.filing_index_url(AAPL, Q3)] = sec_edgar.SecRefused(503, "index.json")
    result = run(tmp_path, instance_with(K_AAPL, K_TSM), [K_AAPL, K_TSM])
    assert result.success

    m = meta(result, "raw_sec_instance")
    assert (m["named"], m["instance_refused"], m["instance_unasked"]) == (2, 1, 1)
    assert outcomes(m) == m["named"] and len(sec.calls) == 1
    assert stored(tmp_path, K_AAPL) == [] and stored(tmp_path, K_TSM) == []
    s = meta(result, "sec_segment")
    assert (s["filings"], s["not_held"], s["stamped"]) == (2, 2, 0)
    assert writes() == []
    assert marks(Q3) == {"s-a1": None, "s-a2": None}, "a refusal marks nothing as read"


def test_three_transport_failures_before_any_answer_stop_the_run(tmp_path: Path, sec: Sec) -> None:
    accessions = [f"0000320193-26-00003{n}" for n in range(4)]
    FILINGS[:] = [filing(AAPL, a, f"2026-0{n + 1}-15", "s-a1") for n, a in enumerate(accessions)]
    for accession in accessions:
        sec.answers[sec_edgar.filing_index_url(AAPL, accession)] = sec_edgar.SecTransport("reset")
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL])
    m = meta(result, "raw_sec_instance")
    assert (m["instance_transport"], m["instance_unasked"]) == (3, 1)
    assert outcomes(m) == m["named"] == 4 and len(sec.calls) == 3


def test_no_instance_and_too_large_are_marked_and_retract_nothing(tmp_path: Path, sec: Sec) -> None:
    """Neither says anything about the filing's segments, so its rows stand; both are permanent
    facts about an immutable document, so the filing is marked."""
    FILINGS.append(filing(TSM, TSM_20F, "2026-04-16", "s-tsm"))
    sec.no_instance(AAPL, Q3)
    sec.filing(TSM, TSM_20F, size=133_922_045)
    result = run(tmp_path, instance_with(K_AAPL, K_TSM), [K_AAPL, K_TSM])
    assert result.success

    m = meta(result, "raw_sec_instance")
    assert (m["instance_absent"], m["instance_too_large"], outcomes(m)) == (1, 1, 2)
    assert sec_edgar.filing_document_url(TSM, TSM_20F, NAME) not in sec.calls
    assert [r["outcome"] for r in stored(tmp_path, K_AAPL)] == ["absent", "absent"]
    assert [r["outcome"] for r in stored(tmp_path, K_TSM)] == ["too_large"]
    s = meta(result, "sec_segment")
    assert (s["no_instance"], s["too_large"], s["read"], s["stamped"]) == (1, 1, 0, 3)
    assert s["outcomes"] == s["filings"] == 2
    assert writes("market.security_segment") == [], "nothing written, nothing retracted"
    assert marks(Q3) == {"s-a1": 20, "s-a2": 20} and marks(TSM_20F) == {"s-tsm": 20}


def test_an_instance_with_no_segments_retracts_what_the_filing_owned(
    tmp_path: Path, sec: Sec
) -> None:
    """A document that was read and discloses nothing IS the filing speaking."""
    sec.filing(AAPL, Q3)
    sec.answers[sec_edgar.filing_document_url(AAPL, Q3, NAME)] = (200, b"<xbrl></xbrl>")
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL])
    s = meta(result, "sec_segment")
    assert (s["read"], s["no_segments"], s["rows"], s["stamped"]) == (1, 1, 0, 2)
    assert writes("insert into market.security_segment") == []
    assert len(writes("delete from market.security_segment")) == 2


def test_a_spent_byte_budget_leaves_the_rest_unasked_and_due(tmp_path: Path, sec: Sec) -> None:
    FILINGS.append(filing(AAPL, Q2, "2026-05-01", "s-a1", "s-a2"))
    sec.filing(AAPL, Q3)
    sec.filing(AAPL, Q2)
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL], config={"budget_bytes": 1})
    m = meta(result, "raw_sec_instance")
    assert (m["instance_answered"], m["instance_unasked"], outcomes(m)) == (1, 1, 2)
    s = meta(result, "sec_segment")
    assert (s["read"], s["not_held"]) == (1, 1)
    assert marks(Q2) == {"s-a1": 20, "s-a2": 20}, "the older filing is asked first"
    assert marks(Q3) == {"s-a1": None, "s-a2": None}


def test_a_filing_that_cannot_be_written_is_rolled_back_and_the_rest_still_land(
    tmp_path: Path, sec: Sec
) -> None:
    FILINGS.append(filing(AAPL, Q2, "2026-05-01", "s-a1", "s-a2"))
    sec.filing(AAPL, Q3)
    sec.filing(AAPL, Q2)
    POISON.add(Q2)
    result = run(tmp_path, instance_with(K_AAPL), [K_AAPL])

    assert not result.success, "a filing that fails every read must not be silent"
    assert marks(Q2) == {"s-a1": None, "s-a2": None}, "the failed filing stays due"
    assert marks(Q3) == {"s-a1": 20, "s-a2": 20}, "the filing after it was still written"
    assert [sql for sql, _ in CALLS if sql in ("commit", "rollback")] == ["rollback", "commit"]
    failure = result.failure_data_for_node("sec_segment")
    assert failure is not None and failure.user_failure_data is not None
    assert Q2 in str(failure.user_failure_data.description), "the failure names the filing"


# --- the sensor ----------------------------------------------------------------------------------


def _grid(n: int) -> list[str]:
    return [cik_key(1000 + i) for i in range(n)]


def _sensor(
    instance: dg.DagsterInstance, cursor: str | None = None
) -> dg.SensorResult | dg.SkipReason:
    with dg.build_sensor_context(
        instance=instance, cursor=cursor, resources={"postgres": FakePostgres()}
    ) as context:
        result = automation.sec_instances_due(context)
    assert isinstance(result, dg.SensorResult | dg.SkipReason)
    return result


def test_the_sensor_requests_each_chunk_holding_a_due_filer_once() -> None:
    grid = _grid(2 * FILERS_PER_RUN + 20)
    instance = instance_with(*grid)
    first, also_first, third = 1003, 1040, 1000 + 2 * FILERS_PER_RUN + 5
    FILINGS[:] = [
        filing(first, "a-1", "2026-11-05", "s1"),
        filing(first, "a-2", "2026-11-06", "s1"),
        filing(also_first, "a-3", "2026-11-06", "s2"),
        filing(third, "a-4", "2026-11-06", "s3"),
        filing(999_999, "a-5", "2026-11-06", "s4"),  # a filer the grid does not hold yet
    ]
    result = _sensor(instance)
    assert isinstance(result, dg.SensorResult) and result.cursor == "1"
    requests = result.run_requests or []
    assert [r.run_key for r in requests] == [f"1:{grid[0]}", f"1:{grid[2 * FILERS_PER_RUN]}"]
    assert requests[0].tags[ASSET_PARTITION_RANGE_START_TAG] == grid[0]
    assert requests[0].tags[ASSET_PARTITION_RANGE_END_TAG] == grid[FILERS_PER_RUN - 1]
    assert requests[1].tags[ASSET_PARTITION_RANGE_END_TAG] == grid[-1], "the last chunk is short"
    assert all(
        r.tags[MAX_RUNTIME_SECONDS_TAG] == runtime.SEC_RUN[MAX_RUNTIME_SECONDS_TAG]
        for r in requests
    )
    # The next wave may ask for the same chunk again: its key carries the wave.
    again = _sensor(instance, cursor="1")
    assert isinstance(again, dg.SensorResult)
    assert next(r.run_key for r in again.run_requests or []) == f"2:{grid[0]}"


def test_the_sensor_waits_for_the_last_wave_to_finish() -> None:
    instance = instance_with(*_grid(3))
    FILINGS[:] = [filing(1001, "a-1", "2026-11-06", "s1")]
    run = dg.DagsterRun(job_name=automation.SEC_SEGMENTS_JOB, status=dg.DagsterRunStatus.STARTED)
    instance.add_run(run)
    result = _sensor(instance)
    assert isinstance(result, dg.SkipReason) and "has not finished" in str(result.skip_message)

    # A run that ended, however it ended, no longer holds the next wave back: a failed run's
    # filings are still due, and the next wave is what asks for them again.
    instance.report_run_failed(run)
    assert isinstance(_sensor(instance), dg.SensorResult)


def test_the_sensor_asks_nothing_when_nothing_is_due_or_the_queue_is_a_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = instance_with(*_grid(3))
    FILINGS[:] = []
    nothing = _sensor(instance)
    assert isinstance(nothing, dg.SkipReason) and "no filing is due" in str(nothing.skip_message)

    monkeypatch.setattr(automation, "MAX_DUE_PER_TICK", 2)
    FILINGS[:] = [filing(1001, f"a-{n}", "2026-11-06", "s1") for n in range(3)]
    history = _sensor(instance)
    assert isinstance(history, dg.SkipReason)
    assert "3 filings of 1 filers" in str(history.skip_message)
    assert "backfill" in str(history.skip_message)

    FILINGS.pop()
    assert isinstance(_sensor(instance), dg.SensorResult), "at the bound it is still a day"


def test_the_sensor_runs_and_the_bound_is_above_the_busiest_day() -> None:
    """Declared RUNNING so a reset of Dagster's state cannot stop the lane. 506 accounts filings
    arrived on 2026-11-05, the busiest day measured."""
    assert automation.sec_instances_due.default_status is dg.DefaultSensorStatus.RUNNING
    assert automation.MAX_DUE_PER_TICK >= 3 * 506


# --- the check -----------------------------------------------------------------------------------


def _check() -> tuple[dg.AssetCheckResult, dict[str, Any]]:
    result = checks.every_sec_filing_was_read(postgres=FakePostgres())
    assert isinstance(result, dg.AssetCheckResult)
    return result, {k: v.value for k, v in (result.metadata or {}).items()}


def test_a_filing_held_a_day_and_never_read_is_an_error() -> None:
    """A transport failure on every wave leaves no failed run to see: the run succeeds with one
    `instance_transport`, every half hour, for ever. This is what makes it visible."""
    FILINGS[:] = [
        filing(AAPL, Q3, "2026-07-31", "s-a1", held_days=2),
        filing(TSM, TSM_20F, "2026-04-16", "s-t", held_days=5),
    ]
    behind, found = _check()
    assert not behind.passed and behind.severity is dg.AssetCheckSeverity.ERROR
    assert found == {
        "never_read": 2,
        "filers": 2,
        "oldest_filed": "2026-04-16",
        "behind_the_parser": 0,
    }
    assert CALLS[-1][1] == {"days": checks.UNREAD_AFTER_DAYS}


def test_a_filing_written_today_is_not_late_whatever_its_filing_date() -> None:
    """Friday's filing reaches the table on Monday. Its own date is three days old; we have held
    it for an hour."""
    FILINGS[:] = [filing(AAPL, Q3, "2026-11-06", "s-a1", held_days=0)]
    result, found = _check()
    assert result.passed and found["never_read"] == 0


def test_a_re_read_that_has_not_drained_is_a_warning_with_its_count() -> None:
    FILINGS[:] = [
        filing(AAPL, Q3, "2026-07-31", "s-a1", held_days=90),
        filing(TSM, TSM_20F, "2026-04-16", "s-t", held_days=200),
    ]
    for f in FILINGS:
        f["marks"] = dict.fromkeys(f["marks"], 20)
    assert _check()[0].passed

    PARSER["version"] = 21
    result, found = _check()
    assert not result.passed and result.severity is dg.AssetCheckSeverity.WARN
    assert (found["never_read"], found["behind_the_parser"]) == (0, 2)

    # One never read beside them is still the fault that decides the severity.
    FILINGS.append(filing(AAPL, Q2, "2026-05-01", "s-a1", held_days=3))
    result, found = _check()
    assert result.severity is dg.AssetCheckSeverity.ERROR
    assert (found["never_read"], found["behind_the_parser"]) == (1, 2)


def test_the_lane_s_names_and_pools() -> None:
    assert raw.raw_sec_instance.key == raw.SEC_INSTANCE
    assert core.sec_segment.key == checks.SEC_SEGMENT
    [stage1] = raw.raw_sec_instance.op.pools
    [stage2] = core.sec_segment.op.pools
    assert (stage1, stage2) == ("sec", "sql")
    assert raw.raw_sec_instance.metadata_by_key[raw.SEC_INSTANCE]["merge_on"] == list(
        sec_segments.MERGE_ON
    )
    assert set(segments.SEGMENT_KEY) - {"parent_key"} <= {*segments.SEGMENT_KEY}
