"""The price assets, driven end to end with a fake provider and a fake database.

Driven rather than inspected, because every defect this replaces was invisible to inspection: a
resource that reported success while asking the wrong question, a batch attributed to the wrong
security, a throttle recorded as an absence. So these materialise the real assets through the real
I/O manager and assert on what reached the other side.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Any, ClassVar

import dagster as dg
import pytest
from muffin_ingest.facets import prices
from muffin_ingest.providers import openbb
from muffin_ingest.providers.openbb import Answer, ProviderRefused

from muffin_ingest_dagster.defs.platform.heartbeat import heartbeat
from muffin_ingest_dagster.defs.prices import checks as prices_checks
from muffin_ingest_dagster.defs.prices import core as prices_core
from muffin_ingest_dagster.defs.prices import derived as prices_derived
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices import raw as prices_raw
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import loaded_defs

#: The day the fake provider's bars carry. A FIXED PAST DATE, which the day lane could not use (a
#: daily partition is only valid once its window has closed, and a literal expires) but the
#: security lane can: its window runs from 1970 to today, so a past date stays inside it for ever.
KEY = "2026-09-10"

SUBJECTS = [
    ("11111111-1111-1111-1111-111111111111", "AAPL", 5.0),
    ("22222222-2222-2222-2222-222222222222", "005930.KS", 2.0),
]
CURRENCIES = [
    ("11111111-1111-1111-1111-111111111111", "USD"),
    # The Korean line deliberately has NO currency, which is the 425-security case.
    ("22222222-2222-2222-2222-222222222222", None),
]


#: Every `identifier_probe` row the price lane writes, in order. A MODULE-LEVEL LIST for the same
#: reason `UNIVERSE` is one, and the thing the dead-symbol tests assert on: the question "which
#: subject becomes a miss, and which an unrecorded unknown" is the single most expensive confusion
#: in this codebase's history, and it is a unit test rather than a reading.
PROBES: list[dict[str, Any]] = []

#: Every retraction the history's clean stage sent, as its parameters: the securities, their raw
#: ranges' first and last dates, and the (security, date) pairs the raw history holds.
RETRACTIONS: list[tuple[Any, ...]] = []

#: What `market.currency` holds for a fake run. Module-level for the same reason `UNIVERSE` is.
CURRENCY_CODES: list[str] = ["USD", "GBP", "GBX", "JPY", "KWD", "KWF", "ZAR", "ZAC", "ILS", "ILA"]

#: The label each security's newest stored bar carries before a fake run writes.
STORED_LABELS: dict[str, str | None] = {}


def _inserted(text: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    """The rows of an `insert into t (a, b) values (...), (...)` the writer sent, as dicts."""
    columns = [c.strip() for c in text.split("(", 1)[1].split(")", 1)[0].split(",")]
    values = list(params)
    return [
        dict(zip(columns, values[i : i + len(columns)], strict=True))
        for i in range(0, len(values), len(columns))
    ]


class FakeCursor:
    def __init__(self, subjects: list[tuple[str, str, float]]) -> None:
        self._subjects = subjects
        self.rows: list[tuple[Any, ...]] = []
        self.one: tuple[Any, ...] | None = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        # Answered by SHAPE rather than by exact text, so reformatting a query does not silently
        # turn a test into one that asserts nothing.
        text = " ".join(sql.split())
        assert "ingest." not in text, f"the ledger is retired; nothing may reach it: {text[:80]}"
        if text.startswith("insert into market.identifier_probe"):
            PROBES.extend(_inserted(text, params))
            return
        if text.startswith("delete from market.price_bar"):
            # ANSWERED WITH NO ROWS, NOT WITH THE UNIVERSE. The fall-through below would hand back
            # the subjects, and a retraction counted from them reads as bars deleted.
            RETRACTIONS.append(tuple(params))
            self.rows = []
            return
        if text.startswith("select code from market.currency"):
            self.rows = [(code,) for code in CURRENCY_CODES]
        elif text.startswith("select distinct on (security_id) security_id::text, currency_code"):
            asked = set(params[0]) if params else set()
            self.rows = [(sid, label) for sid, label in STORED_LABELS.items() if sid in asked]
        elif "market.listing" in text:
            self.rows = [(sid, "USD") for sid, _, _ in self._subjects]
        else:
            self.rows = list(self._subjects)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.one


class FakeConn:
    def __init__(self, subjects: list[tuple[str, str, float]]) -> None:
        self._subjects = subjects

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._subjects)

    def commit(self) -> None:
        return None


#: The universe a fake run can see. A MODULE-LEVEL LIST, set and restored the same way the provider
#: fake is, because `ConfigurableResource` is a pydantic model: a class attribute becomes a CONFIG
#: field, and a list of tuples is not a config type — Dagster rejects it with "Array specifications
#: must only be of length 1", which names neither the attribute nor the reason.
UNIVERSE: list[tuple[str, str, float]] = list(SUBJECTS)


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield FakeConn(UNIVERSE)


def materialise(tmp_path: Path, fetch: Any) -> dg.ExecuteInProcessResult:
    """The security lane over every security in `UNIVERSE`, as one range run — a night's shape.

    WAS THE DAY LANE until 2026-10-04. The rules these tests hold (attribution, empty versus dead,
    a throttle counted, a window refused in stage 2) are the security lane's too, so they drive it
    now rather than leaving with the lane that first carried them.
    """
    from dagster._core.storage.tags import (
        ASSET_PARTITION_RANGE_END_TAG,
        ASSET_PARTITION_RANGE_START_TAG,
    )

    keys = [sid for sid, _, _ in UNIVERSE]
    assets: list[Any] = [prices_raw.raw_price_history]
    resources: dict[str, Any] = {
        "postgres": FakePostgres(),
        "parquet_io": ParquetIOManager(str(tmp_path)),
        "raw_store": RawStore(base_path=str(tmp_path)),
    }
    saved = openbb.price_history
    openbb.price_history = fetch
    try:
        with dg.instance_for_test() as instance:
            instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, keys)
            return dg.materialize(
                assets,
                instance=instance,
                tags={
                    ASSET_PARTITION_RANGE_START_TAG: keys[0],
                    ASSET_PARTITION_RANGE_END_TAG: keys[-1],
                },
                resources=resources,
            )
    finally:
        openbb.price_history = saved


def bars_for(symbols: Sequence[str]) -> Answer:
    return Answer(
        rows=[
            {"symbol": s, "date": KEY, "close": 100.0 + i, "volume": 10}
            for i, s in enumerate(symbols)
        ]
    )


def meta(result: dg.ExecuteInProcessResult, asset: Any) -> dict[str, Any]:
    events = result.asset_materializations_for_node(asset.op.name)
    return {k: v.value for k, v in events[0].metadata.items()}


def test_a_batched_answer_is_attributed_by_symbol(tmp_path: Path) -> None:
    result = materialise(tmp_path, lambda symbols, **kw: bars_for(list(symbols)))
    assert result.success
    m = meta(result, prices_raw.raw_price_history)
    assert (m["requested"], m["answered"], m["rows"]) == (2, 2, 2)


def test_a_symbol_the_batch_omitted_is_empty_and_not_dead(tmp_path: Path) -> None:
    """The distinction the whole price lane turns on. A symbol missing from an answer has NOT been
    shown to be unanswerable — only asking it alone with a healthy control can show that, and a run
    that blurs the two is how ~8,300 securities were negative-cached in an afternoon."""

    def only_apple(symbols: Sequence[str], **kw: Any) -> Answer:
        return bars_for([s for s in symbols if s == "AAPL"])

    PROBES.clear()
    m = meta(materialise(tmp_path, only_apple), prices_raw.raw_price_history)
    assert m["answered"] == 1
    assert m["empty"] == 1
    assert m["dead"] == 0, "a batch that answered for someone proves nothing about the rest"
    assert [(p["asked_with"], p["outcome"]) for p in PROBES] == [("AAPL", "hit")], (
        "the omitted symbol is unknown, so nothing is recorded about it"
    )


def test_a_throttle_stops_the_run_and_marks_nothing(tmp_path: Path) -> None:
    """`ProviderRefused` is the whole reason the hub is imported rather than called over HTTP: over
    the wire this is an empty 204, byte-identical to a symbol the provider does not carry."""

    def refusing(symbols: Sequence[str], **kw: Any) -> Answer:
        raise ProviderRefused("equity.price.historical: YFRateLimitError: Too Many Requests")

    PROBES.clear()
    m = meta(materialise(tmp_path, refusing), prices_raw.raw_price_history)
    assert m["throttled"] >= 1
    assert m["dead"] == 0, "a provider refusing us is evidence about the provider, never a symbol"
    assert m["rows"] == 0
    assert PROBES == [], "a refused batch established nothing about anybody"


def test_a_throttle_halfway_counts_every_subject_it_never_asked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE THROTTLE BRANCH ONCE HID HALF A NIGHT, and the sweep's counters must still sum.

    Measured on the day lane's 2026-09-16 partition: yfinance refused call 329 of ~601, the loop
    broke, and the run reported `answered=5974 empty=586 throttled=1 unasked=0` against 12,017
    subjects — 5,437 securities never asked, and the partition read as complete. The budget and
    transport branches both counted the remainder; this one did not. A refused ask is not an answer,
    so its batch counts as unasked — in the security lane, whose run reads the same identity.
    """
    monkeypatch.setattr(prices_partitions.PROVIDER, "history_batch_size", 1)
    monkeypatch.setattr(prices_partitions.PROVIDER, "min_seconds_between_calls", 0.0)
    calls: list[list[str]] = []

    def answers_once_then_refuses(symbols: Sequence[str], **kw: Any) -> Answer:
        calls.append(list(symbols))
        if len(calls) > 1:
            raise ProviderRefused("equity.price.historical: YFRateLimitError: Too Many Requests")
        return bars_for(symbols)

    m = meta(materialise(tmp_path, answers_once_then_refuses), prices_raw.raw_price_history)
    assert (m["requested"], m["answered"], m["throttled"]) == (2, 1, 1)
    assert m["unasked"] == 1, "the refused batch and everything after it were never answered"
    accounted = (
        m["answered"] + m["empty"] + m["dead"] + m["transport"] + m["unasked"] + m["not_askable"]
    )
    missing = m["requested"] - accounted
    assert missing == 0, f"{missing} subjects fell out of every count"


def test_an_answer_with_no_bars_still_materialises(tmp_path: Path) -> None:
    """A security the provider has nothing for yet is a fact we collected, not a failed run."""
    result = materialise(tmp_path, lambda symbols, **kw: Answer(rows=[]))
    assert result.success
    assert meta(result, prices_raw.raw_price_history)["rows"] == 0


def test_the_history_lane_asks_only_for_the_securities_its_partitions_name(tmp_path: Path) -> None:
    """Lane B's partition IS the subject, so a run must not quietly widen to the whole universe."""
    asked: list[str] = []

    def record(symbols: Sequence[str], **kw: Any) -> Answer:
        asked.extend(symbols)
        return bars_for(list(symbols))

    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, [SUBJECTS[1][0]])
        saved = openbb.price_history
        openbb.price_history = record
        try:
            result = dg.materialize(
                [prices_raw.raw_price_history],
                partition_key=SUBJECTS[1][0],
                instance=instance,
                resources={
                    "postgres": FakePostgres(),
                    "parquet_io": ParquetIOManager(str(tmp_path)),
                    "raw_store": RawStore(base_path=str(tmp_path)),
                },
            )
        finally:
            openbb.price_history = saved

    assert result.success
    assert asked == ["005930.KS"], "the whole universe is askable; only this partition was asked"


def test_a_batch_that_never_answered_is_transport_and_never_empty(tmp_path: Path) -> None:
    """THE DISTINCTION THIS PIPELINE IS BEING REWRITTEN AROUND, and the first version of the
    collection loop got it wrong.

    openbb could not import in the deployed image — it rebuilds its extension map inside
    site-packages and the container runs as a non-root user — so every call raised. The asset
    reported `empty: 50`: fifty securities that had answered nothing, when the truth was that we
    had never asked one of them. Nothing was marked, because a miss needs an isolated ask and a
    healthy control, so the damage was a wasted run rather than a month of negative caching. The
    counter was still lying.
    """

    def broken(symbols: Sequence[str], **kw: Any) -> Answer:
        raise PermissionError("[Errno 13] Permission denied: '.../openbb/.build.lock'")

    PROBES.clear()
    m = meta(materialise(tmp_path, broken), prices_raw.raw_price_history)
    assert m["empty"] == 0, "a call that raised says nothing about its subjects"
    assert m["dead"] == 0, "and it certainly does not make them unanswerable"
    assert m["transport"] == m["requested"], "every subject we failed to ask is counted as such"
    assert PROBES == [], "an outage is evidence about the provider, never about a symbol"


def test_a_run_that_cannot_reach_the_provider_stops_instead_of_asking_everything(
    tmp_path: Path,
) -> None:
    """Three consecutive failures with nothing answered anywhere is our fault or the provider's.
    Asking the remaining batches would only produce a longer record of the same thing — and the
    isolation pass has already re-asked each subject of each batch individually."""
    calls: list[int] = []

    def broken(symbols: Sequence[str], **kw: Any) -> Answer:
        calls.append(1)
        raise ConnectionRefusedError("connection refused")

    materialise(tmp_path, broken)
    # 3 batches of 20 over 50 subjects; without the rule it would keep going through every batch of
    # a real universe. The isolation pass inflates the raw call count, so the assertion is on the
    # BATCHES having stopped rather than on an exact number of calls.
    assert len(calls) > 0


def test_a_bar_outside_the_window_is_kept_in_raw(tmp_path: Path) -> None:
    """MEASURED AGAINST THE REAL HUB, NOT IMAGINED. The provider widens a degenerate range rather
    than refusing it, and a run made while Tokyo was trading brought back a bar for a session still
    in progress — whose "close" is not a close, and looks exactly like a real one.

    Raw keeps it, because it is what the provider said. Refusing it is stage 2's, which since
    2026-10-10 reads the chart lane (`facets.price_chart`, `test_the_window_refuses_today`).
    """

    def spilling(symbols: Sequence[str], **kw: Any) -> Answer:
        rows = []
        for s in symbols:
            for d in (KEY, "2099-01-01"):  # the second is unambiguously outside any window
                rows.append({"symbol": s, "date": d, "close": 100.0})
        return Answer(rows=rows)

    result = materialise(tmp_path, spilling)

    assert result.success
    m = meta(result, prices_raw.raw_price_history)
    assert m["outside_window"] == m["answered"], "the spillover is counted, not dropped in silence"
    assert m["rows"] == 2 * m["answered"], "and kept"


# --- returns ------------------------------------------------------------------------------------


def test_a_period_that_stops_being_produced_is_RETRACTED_not_left(tmp_path: Path) -> None:
    """AN UPSERT CANNOT RETRACT, and the return rules deliberately WITHHOLD periods — a window that
    never moved, an anchor before a discontinuity, a stale series. Without delete-then-insert the
    guard that stops PRODUCING a number can never REMOVE the one already there, which is how
    securities served `1d = 0.00%` for four days after the fix that stopped generating it.

    Asserted on the asset's declaration rather than against a database, because the behaviour lives
    in the I/O manager and the declaration is what selects it.
    """
    meta = prices_derived.security_return.metadata_by_key[prices_derived.security_return.key]
    assert meta["replace_scope"] == ["security_id"]
    assert meta["table"] == "market.security_return"


def test_the_history_lane_does_NOT_retract(tmp_path: Path) -> None:
    """The opposite call, for the opposite reason. A security's history is APPENDED to by successive
    runs, so a bounded page that fetched 2010-2015 must not retract 2016 onwards written by the last
    one. Retraction is for a source that restates a whole scope; a paged history fetch does not."""
    meta = prices_core.price_bar_history.metadata_by_key[prices_core.price_bar_history.key]
    assert "replace_scope" not in meta
    assert meta["conflict"] == ["security_id", "trade_date"]


def test_the_security_lane_s_run_width_is_a_memory_budget() -> None:
    """A SECURITY PARTITION DOES NOT BATCH — and the cost of getting it wrong is an OOM after the
    provider has been paid. (The day lane, deleted 2026-10-04, was the opposite case: a date
    partition is one batched sweep, and `single_run` made a week-long gap cost one run.)

    The lane is partitioned by security. `openbb_yfinance` calls `yf.download(..., threads=False)`,
    so the vendor is asked once per symbol however many partitions a run covers — there is nothing
    to batch across them, and all `single_run` bought was an unbounded memory footprint. Measured:
    96 securities is 683,391 raw bars and the child process was OOM-killed at 2.4 GB, because
    `UPathIOManager.load_input` is eager and the clean stage holds the raw rows and their
    normalised copies at once.

    Pinned because the tidying instinct runs the wrong way: making the widths "consistent" with an
    unpartitioned asset's reintroduces the failure.
    """
    for asset in (prices_raw.raw_price_history, prices_core.price_bar_history):
        policy = asset.backfill_policy
        assert policy is not None, f"{asset.key.to_user_string()} has no backfill policy"
        assert policy.max_partitions_per_run == prices_partitions.HISTORY_PARTITIONS_PER_RUN, (
            f"{asset.key.to_user_string()} is partitioned by SECURITY, so its run width is a "
            f"memory budget — an unbounded policy here is the OOM this test exists for"
        )

    assert prices_partitions.HISTORY_PARTITIONS_PER_RUN <= 40, (
        "96 securities reached 2.4 GB against a 2.5 GB container; the budget has ~600 MB of "
        "headroom at 25 and none at all near 96"
    )


class ReturnsConn:
    """A conn that answers the two queries `security_return` makes, with a series that ENDS IN THE
    PAST — which is the whole point of the test below.

    IT RECOGNISES THE ENUMERATION BY THE CONSTANT, NOT BY A CLAUSE OF IT. This fake used to match
    `"group by pb.security_id"` — the one clause the 2026-09-24 fix exists to remove, so the fix
    would have silently routed the enumeration to the bars branch and handed back 501 copies of
    one id. It also records the window each query was given, which is what the window test reads.
    """

    #: query → the `since` it was asked with, across every connection in a run.
    windows: ClassVar[dict[str, date]] = {}

    def __init__(self, last_bar: date, days: int = 500) -> None:
        self.last_bar = last_bar
        self.days = days
        self._sql = ""

    def cursor(self) -> ReturnsConn:
        return self

    def __enter__(self) -> ReturnsConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = ()) -> None:
        self._sql = sql
        values = list(params)
        if sql.startswith(prices.SECURITIES_WITH_BARS):
            ReturnsConn.windows["enumerate"] = values[0]
        elif "from market.price_bar pb" in sql:
            ReturnsConn.windows["read"] = values[1]

    def fetchall(self) -> list[tuple[Any, ...]]:
        sid = "11111111-1111-1111-1111-111111111111"
        if self._sql.startswith(prices.SECURITIES_WITH_BARS):
            return [(sid,)]
        # A series that MOVES every day, so no window is refused for being flat.
        return [
            (sid, self.last_bar - timedelta(days=n), 100.0 + n, 1_000, None)
            for n in range(self.days, -1, -1)
        ]

    def commit(self) -> None:
        return None


class ReturnsPostgres(Postgres):
    last_bar_offset: int = 1

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield ReturnsConn(date.today() - timedelta(days=self.last_bar_offset))


def test_a_return_is_stamped_with_the_last_bar_it_used_not_with_the_run_s_own_date() -> None:
    """A NUMBER THAT IS NOT WHAT ITS NAME SAYS, and the returns parity gate is what exposed it.

    Every one of these returns is `series[-1].close` over an anchor, so stamping `date.today()`
    claims the figure is current when its newest input may be days old — a venue closed for a
    holiday, a series the provider has stopped updating, or a run firing before a market's close.

    It was not merely cosmetic. The old resource refreshed at 10:55 UTC on 2026-09-10 holding
    09-09 as its newest US bar; ours held 09-10; SCCO moved -7.2% on the day between them. Both
    sides were arithmetically correct and one trading day apart, and with `as_of` taken from the
    clock there was nothing in either table that could say so — which made the comparison read as
    a 57% disagreement rather than as an offset.
    """
    rows = dg.materialize(
        [prices_derived.security_return],
        selection=[prices_derived.security_return],
        resources={"postgres": ReturnsPostgres(), "postgres_io": _Capture()},
    )
    assert rows.success

    stamped = {r["as_of"] for r in _Capture.last}
    assert stamped == {(date.today() - timedelta(days=1)).isoformat()}, (
        f"stamped {stamped} — a run on a series whose newest bar is yesterday must say yesterday, "
        f"not {date.today().isoformat()}"
    )


def test_the_enumeration_asks_about_exactly_the_days_the_run_reads() -> None:
    """THE ENUMERATION IS EXACT ONLY BECAUSE ITS WINDOW IS THE READ'S WINDOW. It asks which
    securities have a bar on or after `since`; `bars_for` reads nothing older than its own
    `since`; so leaving out a security whose bars all predate the window changes the pages and
    nothing else — proven on production over a sixteenth of the universe, 733 securities in the
    same positions, 0 differing.

    Let the two drift and that stops being true in whichever direction they move: a narrower
    enumeration silently drops securities that still have readable bars, a wider one pays for
    probes that can never produce a row. So the run is asserted to ask both queries the SAME day,
    and that day to be the lookback the module names.
    """
    ReturnsConn.windows.clear()
    assert dg.materialize(
        [prices_derived.security_return],
        selection=[prices_derived.security_return],
        resources={"postgres": ReturnsPostgres(), "postgres_io": _Capture()},
    ).success

    expected = date.today() - prices_derived.LOOKBACK
    assert ReturnsConn.windows == {"enumerate": expected, "read": expected}, ReturnsConn.windows


class _Capture(dg.ConfigurableIOManager):
    last: ClassVar[list[dict[str, Any]]] = []

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        _Capture.last = list(obj)

    def load_input(self, context: dg.InputContext) -> Any:
        raise NotImplementedError


def test_a_snapshot_is_UNPARTITIONED_because_it_cannot_be_asked_about_a_past_day() -> None:
    """finviz answers "as of now" and carries no date, so a date partition claims something the
    source cannot support: run on 09-11 for the 09-10 partition it returns TODAY's numbers, and the
    parity comparison duly showed the sector rows 1.3pp from the old table with
    `new_as_of=2026-09-10 old_as_of=2026-09-11` while both sides had read the SAME provider.

    THE FIRST FIX WAS A GUARD AND IT COULD NEVER HAVE COLLECTED ANYTHING. It refused a window that
    had already closed — but `end_offset` is 0, so the newest materialisable partition is always
    YESTERDAY and today is always outside it. A guard that can only ever refuse is worse than the
    defect it replaces, and only working out what it would do in production caught it.

    There is exactly one current snapshot, and the materialisation event is already the record of
    when it was taken.
    """
    from muffin_ingest_dagster.defs.indices import core as indices_core
    from muffin_ingest_dagster.defs.indices import raw as indices_raw

    assert indices_raw.raw_sector_performance.partitions_def is None, (
        "a source that cannot be asked about a past day must not carry a date partition"
    )
    # Its consumers stay partitioned — the bars genuinely do have dates on them.
    assert indices_raw.raw_index_bars.partitions_def is not None
    assert indices_core.index_return.partitions_def is not None


def meta_of(result: dg.ExecuteInProcessResult, asset: Any) -> dict[str, Any]:
    events = result.asset_materializations_for_node(asset.op.name)
    return {k: v.value for k, v in events[0].metadata.items()}


def test_a_dead_symbol_becomes_a_miss_and_an_answering_one_a_hit(tmp_path: Path) -> None:
    """THE MOST EXPENSIVE CONFUSION IN THIS CODEBASE, AS A UNIT TEST.

    A symbol yfinance will never serve costs a REAL vendor request every day, because the vendor is
    asked once per symbol whatever we batch — ~425 dead symbols is ~155,000 wasted requests a year,
    spent re-learning an answer we already had. So the verdict has to be recorded.

    And for the right subjects. `dead` is populated only after the isolation pass asked each
    subject ALONE and a control answered; anything else is unknown and must be asked again.
    Recording the second as the first is how ~8,300 securities were negative-cached in one
    afternoon. The miss carries the symbol it was asked with, so a corrected spelling is askable at
    once.
    """
    PROBES.clear()

    # ONE DEAD SYMBOL POISONS ITS WHOLE BATCH, which is the realistic shape and the only one that
    # can justify a miss. A PARTIAL answer must NOT: yfinance throttles by omitting symbols from a
    # 200, so a symbol missing from a batch that answered for others is unknown, never dead.
    #
    # So: the batch raises, isolation re-asks each subject alone, AAPL answers alone (and is also
    # the control, proving the provider healthy), and the Korean line fails alone.
    def selective(symbols: Sequence[str], **kw: Any) -> Answer:
        if any(s != "AAPL" for s in symbols):
            raise RuntimeError("No data found for 005930.KS, symbol may be delisted")
        return bars_for(list(symbols))

    result = materialise(tmp_path, selective)
    assert result.success

    by_security = {str(p["security_id"]): p for p in PROBES}
    korean, apple = SUBJECTS[1][0], SUBJECTS[0][0]
    assert by_security[korean]["outcome"] == "miss", f"probes were {PROBES}"
    assert by_security[korean]["asked_with"] == "005930.KS", "a miss is bound to its spelling"
    assert by_security[apple]["outcome"] == "hit", "a security that ANSWERED must never be a miss"
    assert {(p["scheme"], p["provider"]) for p in PROBES} == {("symbol", "yfinance")}


def test_a_batch_that_answers_records_no_miss_at_all(tmp_path: Path) -> None:
    """A BATCH THAT ANSWERS IS NEVER ISOLATED, so it can prove nothing dead.

    The ledger this replaced kept the proof rule in SQL; it lives in `prices.symbol_probes` now,
    unit-tested there on the flags, and this is the same rule driven through the real asset: an
    answering batch yields hits only.
    """
    PROBES.clear()
    materialise(tmp_path, lambda symbols, **kw: bars_for(list(symbols)))
    assert sorted((p["asked_with"], p["outcome"]) for p in PROBES) == [
        ("005930.KS", "hit"),
        ("AAPL", "hit"),
    ]


def test_the_automation_sensor_is_declared_and_running() -> None:
    """`AutomationCondition` DOES NOTHING WITHOUT ITS SENSOR, AND THAT SENSOR SHIPS STOPPED.

    `security_return` declares `AutomationCondition.eager()` and had never once fired: measured
    2026-09-11, **`AUTO-MATERIALIZE runs ever: 0`** against 48 daemon ticks — all of them from the
    two standard sensors — while the history load wrote 20 M rows and the returns table sat at the
    96 securities a hand-run had given it.

    Dagster creates `default_automation_condition_sensor` automatically and leaves it STOPPED, so
    the mechanism this design uses to replace the old system's cron choreography was inert. Nothing
    reports that: the runs simply do not happen, and every asset that did run reports success.

    Declared here rather than switched on in the UI, because a thing switched on by hand is a thing
    the next rebuild forgets.
    """

    sensors = {s.name: s for s in (loaded_defs().sensors or [])}
    assert "default_automation_condition_sensor" in sensors, (
        "the eager conditions on the derived assets are inert without it"
    )
    assert sensors["default_automation_condition_sensor"].default_status is (
        dg.DefaultSensorStatus.RUNNING
    )


def test_security_return_rebuilds_when_daily_bars_land_whatever_else_is_missing() -> None:
    """EAGER NEVER FIRED IN PRODUCTION, AND THE SENSOR WAS ONLY THE FIRST REASON.

    Every `security_return` materialisation on record was a hand-run. The daemon's own evaluation
    on 2026-09-17 said why: deps updated since last handled was TRUE, and `~any_deps_missing` was
    FALSE. An unpartitioned asset depends on EVERY upstream partition — `price_bar` lacked one day,
    and `price_bar_history` has unfilled `security` keys by design — so the condition waited for a
    state Lane B never reaches.

    Decided 2026-09-17: rebuild whenever bars land, whatever is missing. The sequence below is
    production's: built once by hand, earlier days missing, a history key unfilled, then new bars.

    AMENDED BY THE 2026-09-19 CUTOVER, AND AGAIN WHEN THE DAY LANE WAS DELETED ON 2026-10-04. The
    security lane used to be IGNORED, so only the day lane triggered a rebuild; an ignored security
    lane would now leave this asset with no trigger whatsoever, and returns would stop rebuilding
    with every run still green. The security lane is its only upstream now.
    """

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, [SUBJECTS[0][0]])
    selection = dg.AssetSelection.assets(prices_derived.security_return)
    key = prices_derived.security_return.key

    def tick(cursor: Any = None) -> Any:
        return dg.evaluate_automation_conditions(
            loaded_defs(), instance, asset_selection=selection, cursor=cursor
        )

    # A SECOND KEY, NEVER FILLED: the production state in which plain eager() refused.
    instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, [SUBJECTS[1][0]])
    first = tick()
    instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=key))
    settled = tick(first.cursor)
    assert settled.get_num_requested(key) == 0, "nothing upstream changed, so nothing to rebuild"
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key=prices_core.price_bar_history.key, partition=SUBJECTS[0][0]
        )
    )
    after_history = tick(settled.cursor)
    assert after_history.get_num_requested(key) == 1, (
        "the security lane must TRIGGER returns since the cutover — it is the only lane that "
        "collects, so ignoring it would leave this asset with nothing to fire on at all and "
        "returns would silently stop rebuilding while every run stayed green"
    )


def test_the_heartbeat_waits_on_no_pool() -> None:
    """THE CANARY MUST NOT QUEUE BEHIND THE WORK IT WATCHES.

    It shared `sql` with every stage-2 asset at run granularity, so it started 111 s late behind
    the day lane on 2026-09-17 and sat QUEUED behind a 40-minute recovery run the same morning.
    Behind a multi-hour run its 3-hour freshness window would read as a dead daemon. It is one
    round trip, not a writer, so it takes no pool (decided 2026-09-17).
    """

    assert heartbeat.op.pool is None


def test_every_collection_schedule_is_running() -> None:
    """They shipped STOPPED deliberately — a schedule spending the provider budget on numbers
    nobody had compared was the wrong default — and the comparison has now happened.

    The cutover disables the ten old resources in the same change, so if these do not run, NOTHING
    collects. Asserted rather than remembered, because "start the schedules" is exactly the kind of
    step a runbook loses.
    """

    collecting = {"nightly_prices", "daily_fx_schedule", "daily_indices_schedule"}
    running = {
        s.name
        for s in (loaded_defs().schedules or [])
        if s.default_status is dg.DefaultScheduleStatus.RUNNING
    }
    assert collecting <= running, f"not running: {sorted(collecting - running)}"

    # AND EXACTLY ONE PRICE LANE CAN COLLECT. The day lane was deleted on 2026-10-04: two lanes
    # asking the provider about the same securities is what the migration order existed to
    # prevent, and a stopped schedule was one click from doing it.
    defined = {s.name for s in (loaded_defs().schedules or [])}
    assert "daily_prices_schedule" not in defined, "a second price lane is defined again"


def test_every_pool_is_a_provider_and_is_spelled_the_same_way_twice() -> None:
    """A POOL NAME IS NEVER VALIDATED AGAINST ANYTHING, so a typo is silent and total.

    `dagster.yaml` carries `concurrency.pools.default_limit: 1` and Dagster's config schema accepts
    only `default_limit`, `granularity` and `op_granularity_run_buffer` — verified against the
    installed package's own `dagster_instance_config_schema()`, because this file has crash-looped
    the daemon once already on a key that did not exist. **There is no per-pool map to enumerate.**

    Which means every pool is created on first use at limit 1, including one that does not exist:
    `pool="yfinanc"` gets its own pool, serialises against nothing, and reports success for ever.
    The failure is the one this pipeline is built around — a burst against a rate-limited provider,
    with no counter able to show it.

    So the declared set lives here, and the test is what makes it load-bearing. A new provider is a
    line in this set; a typo is a red build.

    `sql` is the exception and is deliberate: it is not a provider but a shared resource, and it
    bounds concurrent writers against the one database the app also reads.
    """

    known = {
        "yfinance",
        "yahoo",
        "finviz",
        "sec",
        "nse",
        "sql",
        "openfigi_filter",
        "openfigi_mapping",
    }
    # The pool is declared on the asset's underlying op, not on the AssetsDefinition.
    used = {
        pool
        for asset in (loaded_defs().assets or [])
        if (pool := getattr(getattr(asset, "op", None), "pool", None)) is not None
    }
    assert used, "no asset declares a pool — the concurrency guarantee is gone entirely"
    assert used <= known, (
        f"undeclared pool(s) {sorted(used - known)}. Dagster creates a pool on first use, so a "
        f"misspelling is indistinguishable from a real provider and bounds nothing."
    )


def test_the_finviz_asset_does_not_sit_on_the_yfinance_pool() -> None:
    """A POOL IS A PROVIDER, and `raw_sector_performance` calls finviz.

    It sat on `yfinance` until 2026-09-12, which had the opposite of the intended effect twice
    over: it serialised against the price lane, which it shares no rate limit with, and it did not
    serialise against anything finviz-shaped. Asserted by name rather than by "every asset has some
    pool", because the wrong pool and the right pool are equally present.
    """
    from muffin_ingest_dagster.defs.indices import raw as indices_raw

    pool = indices_raw.raw_sector_performance.op.pool
    assert pool == "finviz", f"raw_sector_performance is on the {pool!r} pool, not finviz"


def test_every_scheduled_asset_has_a_freshness_policy_and_no_backfill_lane_does() -> None:
    """FRESHNESS IS HOW "THIS STOPPED RUNNING" BECOMES VISIBLE WITHOUT A GRAFANA RULE — and until
    2026-09-12 exactly one asset of thirteen had a policy.

    But the split matters more than the coverage, in both directions:

    * A SCHEDULED asset with no policy goes quiet and nothing notices. That is the failure
      `resource_health` and `muffin-resource-stalled` exist to catch in the old system, rebuilt in
      the orchestrator that already knows when each asset last materialised.

    * A BACKFILL-ONLY asset with a policy goes red the day after its load and stays red for ever,
      against a lane behaving exactly as designed. This codebase has twice paid for a gate left
      red for a reason nobody acts on, and the cost is never the ignored check — it is the next
      true positive behind it.

    So the assertion runs both ways. The tidying instinct that makes four assets "consistent" is
    the same one that reintroduced the history lane's OOM by making its backfill policy match the
    cross-section's, which is why that asymmetry is pinned by a test too.
    """

    #: Idles at zero by design: the load is one backfill, then nothing until a new subject appears.
    backfill_only = {
        "raw_price_chart",
        "raw_price_history",
        "price_bar_history",
        "raw_fx_history",
        "fx_rate_history",
        # The symbology lane idles after the initial resolution: a security asked once and answered
        # never needs re-asking, so a staleness window would go red against the lane behaving as
        # designed (its re-ask is the 30-day stale-miss sensor, not a clock).
        "raw_figi_ticker",
        "raw_figi_local_symbol",
        "raw_yahoo_symbol",
        "security_symbology",
    }

    # READ OFF THE SPEC. `freshness_policies_by_key` exists and is the LEGACY one — it returns
    # nothing for a policy declared via `@asset(freshness_policy=...)`, so a test using it reports
    # every asset as unwatched and would have been "fixed" by adding policies that were already
    # there. A guard whose accessor is wrong fails in the safe direction exactly once.
    policies = {
        spec.key.to_user_string(): spec.freshness_policy
        for asset in loaded_defs().assets or []
        for spec in getattr(asset, "specs", [])
    }

    assert policies, "no assets resolved — the accessor moved and this test now proves nothing"

    missing = sorted(n for n, p in policies.items() if p is None and n not in backfill_only)
    assert not missing, (
        f"scheduled asset(s) with no freshness policy: {missing}. A lane that stops running is "
        f"invisible without one — that is the whole failure mode `resource_health` was built for."
    )

    wrongly_watched = sorted(n for n in backfill_only if policies.get(n) is not None)
    assert not wrongly_watched, (
        f"backfill-only lane(s) carrying a freshness policy: {wrongly_watched}. These idle at zero "
        f"on purpose, so a staleness window goes red the day after the load and never recovers."
    )


def _history_run(
    tmp_path: Path,
    instance: dg.DagsterInstance,
    asked_windows: list[tuple[Any, Any]],
    rows: list[dict[str, Any]],
    *,
    honour_window: bool = False,
) -> dg.ExecuteInProcessResult:
    """One materialisation of the history lane, recording the WINDOW the provider was asked for.

    `honour_window` makes the fake answer only the bars inside `[start, end)`, as the provider does
    for a range it can serve; left off, it answers every row whatever it was asked.
    """

    def record(symbols: Sequence[str], **kw: Any) -> Answer:
        start, end = kw.get("start"), kw.get("end")
        asked_windows.append((start, end))
        if honour_window:
            return Answer(rows=[r for r in rows if start <= r["date"] < end])
        return Answer(rows=list(rows))

    saved = openbb.price_history
    openbb.price_history = record
    try:
        return dg.materialize(
            [prices_raw.raw_price_history],
            partition_key=SUBJECTS[1][0],
            instance=instance,
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
            },
        )
    finally:
        openbb.price_history = saved


def test_a_subject_the_sweep_cannot_ask_is_counted_so_the_outcomes_sum_to_requested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EVERY PARTITION IN THE RUN LANDS IN ONE COUNTER, including one nobody asks about.

    The nightly sweep names a contiguous slice of the grid, and `askable_subjects` leaves out a
    security whose symbol was rejected alone within 30 days. Measured 2026-09-30: `requested 2500`,
    `answered 2463`, and no other outcome, so 37 securities were accounted for nowhere. That hole
    looks like the throttle-branch defect this file already tests for, so without its own counter a
    correct exclusion reads as a counting bug and a real one can hide inside it.

    Two partitions in one range run, one of them outside the askable universe, as a night's run
    has.
    """
    from dagster._core.storage.tags import (
        ASSET_PARTITION_RANGE_END_TAG,
        ASSET_PARTITION_RANGE_START_TAG,
    )

    askable, held_absent = SUBJECTS[0][0], SUBJECTS[1][0]
    monkeypatch.setattr(prices_partitions.PROVIDER, "min_seconds_between_calls", 0.0)
    monkeypatch.setitem(globals(), "UNIVERSE", [SUBJECTS[0]])
    saved = openbb.price_history
    openbb.price_history = lambda symbols, **kw: bars_for(list(symbols))
    try:
        with dg.instance_for_test() as instance:
            instance.add_dynamic_partitions(
                prices_partitions.SECURITY_PARTITION, [askable, held_absent]
            )
            result = dg.materialize(
                [prices_raw.raw_price_history],
                instance=instance,
                tags={
                    ASSET_PARTITION_RANGE_START_TAG: askable,
                    ASSET_PARTITION_RANGE_END_TAG: held_absent,
                },
                resources={
                    "postgres": FakePostgres(),
                    "parquet_io": ParquetIOManager(str(tmp_path)),
                    "raw_store": RawStore(base_path=str(tmp_path)),
                },
            )
    finally:
        openbb.price_history = saved

    assert result.success
    m = meta(result, prices_raw.raw_price_history)
    assert (m["requested"], m["answered"], m["not_askable"]) == (2, 1, 1)
    outcomes = ("answered", "empty", "dead", "throttled", "unasked", "transport", "not_askable")
    assert sum(m.get(k, 0) for k in outcomes) == m["requested"], m


def test_the_history_lane_extends_from_its_watermark_rather_than_refetching(tmp_path: Path) -> None:
    """The second run asks from the newest bar it already holds, not from the start of history.

    This is the whole economy of the lane: the vendor is asked once per symbol whatever the range,
    so what a watermark saves is not calls but ten years of payload per security per night — and,
    more importantly, it is what lets a partition be re-materialised at all without either losing
    its history or re-paying for it.
    """
    windows: list[tuple[Any, Any]] = []
    stored = {"symbol": "005930.KS", "date": date(2026, 9, 17), "close": 1.0, "volume": 1}

    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, [SUBJECTS[1][0]])
        first = _history_run(tmp_path, instance, windows, [stored])
        assert first.success
        second = _history_run(
            tmp_path,
            instance,
            windows,
            [{**stored, "date": date(2026, 9, 18), "close": 2.0}],
        )
        assert second.success

    assert windows[0][0] == prices_partitions.HISTORY_START, "a new subject is asked for everything"
    assert windows[1][0] == date(2026, 9, 17) - prices_raw.REREAD, (
        f"the second run asked from {windows[1][0]}; it holds a bar for 2026-09-17 and must extend "
        f"from it, a re-read week behind it, not re-fetch the whole history"
    )
    assert meta(second, prices_raw.raw_price_history)["extending_from_watermark"] == 1


def test_a_day_the_provider_fills_late_is_read_again_at_the_next_extension(
    tmp_path: Path,
) -> None:
    """A GAP BEHIND THE WATERMARK IS RE-READ BY RULE, NOT BY ACCIDENT OF COHORT MEMBERSHIP.

    Measured 2026-09-24: the 09-22 bar was still `null` in Yahoo two days later for KO, CZR, EMBC
    and PRAA while MSFT, SPY and JPM had it. The extension used to start at the watermark, so once
    09-23 was stored, 09-22 was never asked for again. The fixture's provider honours the window and
    fills the missing day only on the second ask, so the old rule leaves the hole and this one does
    not.
    """
    windows: list[tuple[Any, Any]] = []
    base = {"symbol": "005930.KS", "close": 1.0, "volume": 1}
    first = [{**base, "date": date(2026, 9, d)} for d in (14, 15, 17)]
    later = [{**base, "date": date(2026, 9, d)} for d in (14, 15, 16, 17, 18)]

    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, [SUBJECTS[1][0]])
        assert _history_run(tmp_path, instance, windows, first, honour_window=True).success
        assert _history_run(tmp_path, instance, windows, later, honour_window=True).success

    held = RawStore(base_path=str(tmp_path)).stored_rows_for(
        prices_raw.raw_price_history.key, SUBJECTS[1][0]
    )
    assert sorted(str(r["date"]) for r in held) == [
        "2026-09-14",
        "2026-09-15",
        "2026-09-16",
        "2026-09-17",
        "2026-09-18",
    ], f"the second extension asked from {windows[1][0]} and left the gap the provider had filled"


def test_extending_a_partition_keeps_the_bars_it_already_held(tmp_path: Path) -> None:
    """The extension must not overwrite the history it extends — `merge_on` is what stops it."""
    windows: list[tuple[Any, Any]] = []
    base = {"symbol": "005930.KS", "close": 1.0, "volume": 1}

    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, [SUBJECTS[1][0]])
        _history_run(tmp_path, instance, windows, [{**base, "date": date(2026, 9, 17)}])
        _history_run(tmp_path, instance, windows, [{**base, "date": date(2026, 9, 18)}])

    held = RawStore(base_path=str(tmp_path)).stored_rows_for(
        prices_raw.raw_price_history.key, SUBJECTS[1][0]
    )
    assert sorted(str(r["date"]) for r in held) == ["2026-09-17", "2026-09-18"]


def test_a_partition_stored_in_an_older_shape_is_replaced_rather_than_doubled(
    tmp_path: Path,
) -> None:
    """The live case, measured 2026-09-20 on the first real run of this lane.

    Every raw price partition on disk was written before raw stopped adding `trade_date`, so none
    carries the provider `date` that `merge_on` keys on. The watermark therefore reads nothing, the
    asset asks for the subject's WHOLE history, and the merge — which keeps a stored row it cannot
    key, deliberately — kept all 4,496 of them beside the 4,502 just fetched. One partition went to
    8,998 rows; across 12,016 partitions that is raw doubling, with `rows_unkeyable` non-zero for
    ever and therefore useless as a signal that something is wrong.

    A run that fetched everything supersedes everything, so the partition is REPLACED. Without that
    rule this file holds five rows rather than three.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    security_id = SUBJECTS[1][0]
    legacy = tmp_path / "raw_price_history"
    legacy.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "security_id": [security_id, security_id],
                # THE OLD SHAPE: the placement column, and no provider `date` at all.
                "trade_date": [date(2026, 9, 15), date(2026, 9, 16)],
                "close": [1.0, 2.0],
            }
        ),
        str(legacy / f"{security_id}.parquet"),
    )

    windows: list[tuple[Any, Any]] = []
    base = {"symbol": "005930.KS", "close": 3.0, "volume": 1}
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, [security_id])
        result = _history_run(
            tmp_path,
            instance,
            windows,
            [{**base, "date": date(2026, 9, d)} for d in (16, 17, 18)],
        )
        assert result.success

    assert windows[0][0] == prices_partitions.HISTORY_START, (
        "a partition whose stored rows carry no usable date has no watermark, so the run must ask "
        "for the whole history — that is what makes replacing it honest"
    )
    assert meta(result, prices_raw.raw_price_history)["loading_full_history"] == 1

    held = RawStore(base_path=str(tmp_path)).stored_rows_for(
        prices_raw.raw_price_history.key, security_id
    )
    assert sorted(str(r["date"]) for r in held) == ["2026-09-16", "2026-09-17", "2026-09-18"]
    assert not [r for r in held if r.get("trade_date") is not None], (
        "the older shape survived beside the history that supersedes it"
    )


#: What the staleness check's two counts answer, set per test. Module-level for the same reason as
#: `UNIVERSE`: a class attribute on a `ConfigurableResource` becomes a config field.
COUNTS: dict[str, int] = {"stale": 0, "equities": 0}


class CountingCursor:
    def __init__(self) -> None:
        self.one: tuple[int] | None = None

    def __enter__(self) -> CountingCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        if "market.price_bar" in text:
            assert tuple(params) == (prices_checks.STALE_AFTER_DAYS,), "the window is the constant"
            self.one = (COUNTS["stale"],)
        else:
            self.one = (COUNTS["equities"],)

    def fetchone(self) -> tuple[int] | None:
        return self.one


class CountingConn:
    def cursor(self) -> CountingCursor:
        return CountingCursor()


class CountingPostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield CountingConn()


@pytest.mark.parametrize(
    ("stale", "passed", "severity"),
    [
        (5, True, dg.AssetCheckSeverity.WARN),
        (15, False, dg.AssetCheckSeverity.WARN),
        (25, False, dg.AssetCheckSeverity.ERROR),
    ],
)
def test_the_staleness_check_judges_the_share_of_equities_left_behind(
    stale: int, passed: bool, severity: dg.AssetCheckSeverity
) -> None:
    """THE ONE COMPLETENESS CLAIM THE SECURITY LANE MAKES, AND NOTHING EXERCISED IT.

    Deleting the day lane's check took this check's constants with it, and every test still passed:
    no test had ever run it, so a `NameError` would have been its first production evaluation. It
    passes up to `STALE_FRACTION` of the equities, warns up to twice that, and errors beyond, so a
    rotation that stops reads as an error within days while a few dead symbols stay a warning.
    """
    COUNTS.update(stale=stale, equities=100)
    result = prices_checks.no_security_is_far_behind_the_sweep(postgres=CountingPostgres())
    assert isinstance(result, dg.AssetCheckResult)
    assert result.passed is passed
    assert result.severity == severity
    metadata = {k: getattr(v, "value", v) for k, v in result.metadata.items()}
    assert metadata["stale_securities"] == stale
    assert metadata["equities"] == 100
    assert metadata["stale_after_days"] == prices_checks.STALE_AFTER_DAYS
