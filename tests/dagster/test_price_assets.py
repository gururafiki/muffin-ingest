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

from muffin_ingest.providers import openbb
from muffin_ingest.providers.openbb import Answer, ProviderRefused
from muffin_ingest_dagster.assets import prices as asset_prices
from muffin_ingest_dagster.io_managers import ParquetIOManager
from muffin_ingest_dagster.resources import Postgres

#: DERIVED FROM THE DEFINITION, NEVER HARDCODED. The first version pinned a date that had not
#: happened yet, so every asset test failed with "could not find a partition" — a daily partition is
#: only valid once its window has closed, and a literal date silently expires in both directions.
KEY = asset_prices.trading_day.get_last_partition_key() or "2026-09-10"

SUBJECTS = [
    ("11111111-1111-1111-1111-111111111111", "AAPL", 5.0),
    ("22222222-2222-2222-2222-222222222222", "005930.KS", 2.0),
]
CURRENCIES = [
    ("11111111-1111-1111-1111-111111111111", "USD"),
    # The Korean line deliberately has NO currency, which is the 425-security case.
    ("22222222-2222-2222-2222-222222222222", None),
]


#: Every ledger call the price lane makes, in order — `(function, params)`. A MODULE-LEVEL LIST for
#: the same reason `UNIVERSE` is one, and the thing the absent-marking tests assert on: the question
#: "which function does a dead subject reach, and which does an empty one reach" is the single most
#: expensive confusion in this codebase's history, and it is now a unit test rather than a reading.
LEDGER_CALLS: list[tuple[str, Any]] = []


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
        for fn in ("ingest.mark_absent", "ingest.complete", "ingest.sync_population"):
            if fn in text:
                LEDGER_CALLS.append((fn.split(".")[1], tuple(params)))
                self.one = (0,)
                return
        if "insert into ingest.attempt" in text:
            LEDGER_CALLS.append(("attempt", tuple(params)))
            self.one = (1,)
            return
        if "update ingest.attempt" in text:
            LEDGER_CALLS.append(("close", tuple(params)))
            self.one = None
            return
        if "market.listing" in text:
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


def materialise(
    tmp_path: Path, fetch: Any, assets: list[Any] | None = None, key: str = KEY
) -> dg.ExecuteInProcessResult:
    saved = openbb.price_history
    openbb.price_history = fetch
    try:
        return dg.materialize(
            assets or [asset_prices.raw_price_bars],
            partition_key=key,
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
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
    m = meta(result, asset_prices.raw_price_bars)
    assert (m["subjects"], m["answered"], m["rows"]) == (2, 2, 2)


def test_a_symbol_the_batch_omitted_is_empty_and_not_dead(tmp_path: Path) -> None:
    """The distinction the whole ledger turns on. A symbol missing from an answer has NOT been
    shown to be unanswerable — only asking it alone with a healthy control can show that, and a run
    that blurs the two is how ~8,300 securities were negative-cached in an afternoon."""

    def only_apple(symbols: Sequence[str], **kw: Any) -> Answer:
        return bars_for([s for s in symbols if s == "AAPL"])

    m = meta(materialise(tmp_path, only_apple), asset_prices.raw_price_bars)
    assert m["answered"] == 1
    assert m["empty"] == 1
    assert m["dead"] == 0, "a batch that answered for someone proves nothing about the rest"


def test_a_throttle_stops_the_run_and_marks_nothing(tmp_path: Path) -> None:
    """`ProviderRefused` is the whole reason the hub is imported rather than called over HTTP: over
    the wire this is an empty 204, byte-identical to a symbol the provider does not carry."""

    def refusing(symbols: Sequence[str], **kw: Any) -> Answer:
        raise ProviderRefused("equity.price.historical: YFRateLimitError: Too Many Requests")

    m = meta(materialise(tmp_path, refusing), asset_prices.raw_price_bars)
    assert m["throttled"] >= 1
    assert m["dead"] == 0, "a provider refusing us is evidence about the provider, never a symbol"
    assert m["rows"] == 0


def test_an_empty_day_still_materialises(tmp_path: Path) -> None:
    """A market holiday produces no bars, and the partition is still a fact we collected."""
    result = materialise(tmp_path, lambda symbols, **kw: Answer(rows=[]))
    assert result.success
    assert meta(result, asset_prices.raw_price_bars)["rows"] == 0


def test_normalisation_carries_the_currency_it_has_and_withholds_the_one_it_does_not(
    tmp_path: Path,
) -> None:
    """Nullable by measurement: 425 of 10,894 askable equities have no currency from either source,
    and refusing them a bar would be worse than the unlabelled number the app renders correctly."""
    from muffin_ingest.facets import prices

    raw = [
        {"security_id": SUBJECTS[0][0], "trade_date": KEY, "close": 100.0, "volume": 1},
        {"security_id": SUBJECTS[1][0], "trade_date": KEY, "close": 200.0, "volume": 2},
    ]
    rows = prices.normalise(raw, {SUBJECTS[0][0]: "USD"}, source_code="yfinance")
    assert [r["currency_code"] for r in rows] == ["USD", None]
    assert all(r["source_code"] == "yfinance" for r in rows)


@pytest.mark.parametrize("close", [0, -1, None, "100.0", True])
def test_normalisation_refuses_a_close_that_is_not_a_positive_number(close: object) -> None:
    from muffin_ingest.facets import prices

    raw = [{"security_id": SUBJECTS[0][0], "trade_date": KEY, "close": close}]
    assert prices.normalise(raw, {}, source_code="yfinance") == []


def test_the_history_lane_asks_only_for_the_securities_its_partitions_name(tmp_path: Path) -> None:
    """Lane B's partition IS the subject, so a run must not quietly widen to the whole universe."""
    asked: list[str] = []

    def record(symbols: Sequence[str], **kw: Any) -> Answer:
        asked.extend(symbols)
        return bars_for(list(symbols))

    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(asset_prices.SECURITY_PARTITION, [SUBJECTS[1][0]])
        saved = openbb.price_history
        openbb.price_history = record
        try:
            result = dg.materialize(
                [asset_prices.raw_price_history],
                partition_key=SUBJECTS[1][0],
                instance=instance,
                resources={
                    "postgres": FakePostgres(),
                    "parquet_io": ParquetIOManager(str(tmp_path)),
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
    had never asked one of them. Nothing was marked, because `mark_absent` refuses without an
    isolated attempt and a healthy control, so the damage was a wasted run rather than a month of
    negative caching. The counter was still lying.
    """

    def broken(symbols: Sequence[str], **kw: Any) -> Answer:
        raise PermissionError("[Errno 13] Permission denied: '.../openbb/.build.lock'")

    m = meta(materialise(tmp_path, broken), asset_prices.raw_price_bars)
    assert m["empty"] == 0, "a call that raised says nothing about its subjects"
    assert m["dead"] == 0, "and it certainly does not make them unanswerable"
    assert m["transport"] == m["subjects"], "every subject we failed to ask is counted as such"


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


def test_a_partition_contains_only_its_own_window(tmp_path: Path) -> None:
    """MEASURED AGAINST THE REAL HUB, NOT IMAGINED. `start_date=2026-09-09&end_date=2026-09-09`
    returns bars for BOTH 09-09 and 09-10 — a degenerate range is widened rather than refused — and
    a run made while Tokyo was trading brought back a bar dated 09-11 as well.

    The second is the dangerous half: that is a session in progress, so its "close" is not a close,
    and it looks exactly like a real one. The first production run wrote all three dates into a
    partition that had asked for one day.
    """

    def spilling(symbols: Sequence[str], **kw: Any) -> Answer:
        rows = []
        for s in symbols:
            for d in (KEY, "2099-01-01"):  # the second is unambiguously outside any window
                rows.append({"symbol": s, "date": d, "close": 100.0})
        return Answer(rows=rows)

    m = meta(materialise(tmp_path, spilling), asset_prices.raw_price_bars)
    assert m["outside_window"] > 0, "the spillover is counted, not dropped in silence"
    assert m["rows"] == m["answered"], "exactly one bar per answering security — its own day"


# --- returns ------------------------------------------------------------------------------------


def test_a_period_that_stops_being_produced_is_RETRACTED_not_left(tmp_path: Path) -> None:
    """AN UPSERT CANNOT RETRACT, and the return rules deliberately WITHHOLD periods — a window that
    never moved, an anchor before a discontinuity, a stale series. Without delete-then-insert the
    guard that stops PRODUCING a number can never REMOVE the one already there, which is how
    securities served `1d = 0.00%` for four days after the fix that stopped generating it.

    Asserted on the asset's declaration rather than against a database, because the behaviour lives
    in the I/O manager and the declaration is what selects it.
    """
    meta = asset_prices.security_return.metadata_by_key[asset_prices.security_return.key]
    assert meta["replace_scope"] == ["security_id"]
    assert meta["table"] == "market.security_return"


def test_the_history_lane_does_NOT_retract(tmp_path: Path) -> None:
    """The opposite call, for the opposite reason. A security's history is APPENDED to by successive
    runs, so a bounded page that fetched 2010-2015 must not retract 2016 onwards written by the last
    one. Retraction is for a source that restates a whole scope; a paged history fetch does not."""
    meta = asset_prices.price_bar_history.metadata_by_key[asset_prices.price_bar_history.key]
    assert "replace_scope" not in meta
    assert meta["conflict"] == ["security_id", "trade_date"]


def test_both_lanes_write_the_same_table_on_the_same_key(tmp_path: Path) -> None:
    """Two assets over one table is the cost of two partition schemes, and the shared key is what
    makes the overlap harmless: whichever lane last collected a day writes the same value for it."""
    a = asset_prices.price_bar.metadata_by_key[asset_prices.price_bar.key]
    b = asset_prices.price_bar_history.metadata_by_key[asset_prices.price_bar_history.key]
    assert a["table"] == b["table"] == "market.price_bar"
    assert a["conflict"] == b["conflict"] == ["security_id", "trade_date"]


def test_the_two_lanes_have_opposite_backfill_policies_and_that_is_deliberate() -> None:
    """A DATE partition batches; A SECURITY PARTITION DOES NOT — and the cost of getting it wrong
    is an OOM after the provider has been paid.

    Lane A is partitioned by trading day, so one run is one batched sweep over the whole universe
    and `single_run` is what makes a week-long gap cost one run instead of seven.

    Lane B is partitioned by security. `openbb_yfinance` calls `yf.download(..., threads=False)`,
    so the vendor is asked once per symbol however many partitions a run covers — there is nothing
    to batch across them, and all `single_run` bought was an unbounded memory footprint. Measured:
    96 securities is 683,391 raw bars and the child process was OOM-killed at 2.4 GB, because
    `UPathIOManager.load_input` is eager and the clean stage holds the raw rows and their
    normalised copies at once.

    Pinned because the tidying instinct runs the wrong way: four assets in one file, three words
    different, and making them "consistent" reintroduces the failure.
    """
    for asset in (asset_prices.raw_price_bars, asset_prices.price_bar):
        policy = asset.backfill_policy
        assert policy is not None and policy.max_partitions_per_run is None, (
            f"{asset.key.to_user_string()} is partitioned by DATE — one run per range"
        )

    for asset in (asset_prices.raw_price_history, asset_prices.price_bar_history):
        policy = asset.backfill_policy
        assert policy is not None, f"{asset.key.to_user_string()} has no backfill policy"
        assert policy.max_partitions_per_run == asset_prices.HISTORY_PARTITIONS_PER_RUN, (
            f"{asset.key.to_user_string()} is partitioned by SECURITY, so its run width is a "
            f"memory budget — an unbounded policy here is the OOM this test exists for"
        )

    assert asset_prices.HISTORY_PARTITIONS_PER_RUN <= 40, (
        "96 securities reached 2.4 GB against a 2.5 GB container; the budget has ~600 MB of "
        "headroom at 25 and none at all near 96"
    )


class ReturnsConn:
    """A conn that answers the two queries `security_return` makes, with a series that ENDS IN THE
    PAST — which is the whole point of the test below."""

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

    def fetchall(self) -> list[tuple[Any, ...]]:
        sid = "11111111-1111-1111-1111-111111111111"
        if "group by pb.security_id" in self._sql:
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
        [asset_prices.security_return],
        selection=[asset_prices.security_return],
        resources={"postgres": ReturnsPostgres(), "postgres_io": _Capture()},
    )
    assert rows.success

    stamped = {r["as_of"] for r in _Capture.last}
    assert stamped == {(date.today() - timedelta(days=1)).isoformat()}, (
        f"stamped {stamped} — a run on a series whose newest bar is yesterday must say yesterday, "
        f"not {date.today().isoformat()}"
    )


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
    from muffin_ingest_dagster.assets import indices as asset_indices

    assert asset_indices.raw_sector_performance.partitions_def is None, (
        "a source that cannot be asked about a past day must not carry a date partition"
    )
    # Its consumers stay partitioned — the bars genuinely do have dates on them.
    assert asset_indices.raw_index_bars.partitions_def is not None
    assert asset_indices.index_return.partitions_def is not None


def meta_of(result: dg.ExecuteInProcessResult, asset: Any) -> dict[str, Any]:
    events = result.asset_materializations_for_node(asset.op.name)
    return {k: v.value for k, v in events[0].metadata.items()}


def test_a_dead_symbol_is_OFFERED_to_mark_absent_and_an_empty_one_is_not(tmp_path: Path) -> None:
    """THE MOST EXPENSIVE CONFUSION IN THIS CODEBASE, AS A UNIT TEST.

    A symbol yfinance will never serve costs a REAL vendor request every day, because the vendor is
    asked once per symbol whatever we batch — ~425 dead symbols is ~155,000 wasted requests a year,
    spent re-learning an answer we already had. So the marking has to happen.

    And it has to happen for the right subjects. `dead` is populated only after the isolation pass
    asked each subject ALONE and a control answered; `empty` means asked, answered nothing, and NOT
    justified — that one must back off and be asked again. Recording the second as the first is how
    ~8,300 securities were negative-cached in one afternoon.

    The assertion is on which ledger FUNCTION each subject reaches, because `ingest.mark_absent`
    refuses what `ingest.complete` accepts.
    """
    LEDGER_CALLS.clear()

    # ONE DEAD SYMBOL POISONS ITS WHOLE BATCH, which is the realistic shape and the only one that
    # can justify a mark. A PARTIAL answer must NOT: yfinance throttles by omitting symbols from a
    # 200, so a symbol missing from a batch that answered for others is `empty`, never `dead` —
    # deliberately, and the reason the first version of this fixture proved nothing.
    #
    # So: the batch raises, isolation re-asks each subject alone, AAPL answers alone (and is also
    # the control, proving the provider healthy), and the Korean line fails alone.
    def selective(symbols: Sequence[str], **kw: Any) -> Answer:
        if any(s != "AAPL" for s in symbols):
            raise RuntimeError("No data found for 005930.KS, symbol may be delisted")
        return bars_for(list(symbols))

    result = materialise(tmp_path, selective)
    assert result.success

    marked = [params for fn, params in LEDGER_CALLS if fn == "mark_absent"]
    completed = [params for fn, params in LEDGER_CALLS if fn == "complete"]

    korean = SUBJECTS[1][0]
    apple = SUBJECTS[0][0]

    assert any(korean in p for p in marked), (
        f"the dead subject never reached mark_absent — ledger calls were "
        f"{[fn for fn, _ in LEDGER_CALLS]}"
    )
    assert not any(apple in p for p in marked), "a security that ANSWERED must never be marked"
    assert any(apple in p for p in completed), "an answering security is completed, not marked"


def test_the_population_is_enqueued_before_anything_is_asked(tmp_path: Path) -> None:
    """`mark_absent` UPDATES `ingest.task`; with no row there is nothing to update and the mark is
    silently lost. Every count would still look right, because the marks would be written and never
    read — which is this file's most repeated shape.

    Ordering matters as much as presence: enqueue, then ask.
    """
    LEDGER_CALLS.clear()
    result = materialise(tmp_path, lambda symbols, **kw: bars_for(list(symbols)))
    assert result.success

    order = [fn for fn, _ in LEDGER_CALLS]
    assert "sync_population" in order, "a security promoted since the last run needs a ledger row"
    assert order.index("sync_population") < order.index("attempt"), (
        f"the population must be enqueued before the provider is called, got {order[:4]}"
    )


def test_an_attempt_is_opened_BEFORE_the_call_and_closed_after(tmp_path: Path) -> None:
    """A worker killed mid-call writes nothing at all — it goes silent rather than red — so the row
    that says "something started here" has to exist before the thing that might kill us.

    `ingest.reap()` is what closes whatever is left open; it can only find rows that exist.
    """
    LEDGER_CALLS.clear()
    materialise(tmp_path, lambda symbols, **kw: bars_for(list(symbols)))

    order = [fn for fn, _ in LEDGER_CALLS]
    assert order.count("attempt") == order.count("close") == 1
    assert order.index("attempt") < order.index("close")


def test_the_attempt_records_what_the_isolation_pass_ACTUALLY_established(tmp_path: Path) -> None:
    """THE ONE LIE THAT WOULD LET AN OUTAGE NEGATIVE-CACHE THE UNIVERSE.

    `ingest.mark_absent` reads exactly two fields off the attempt — `isolated` and
    `control_answered` — and refuses without both. That refusal is the whole guard, and it is only
    as good as what the caller writes there: passing `isolated=True` on a batch that was never
    isolated walks straight around it, and the database has no way to know.

    Mutation-proven, because hardcoding both to True passed every other test in this file.
    """
    LEDGER_CALLS.clear()

    # A batch that ANSWERS never triggers the isolation pass, so the attempt must say so.
    materialise(tmp_path, lambda symbols, **kw: bars_for(list(symbols)))
    closed = [params for fn, params in LEDGER_CALLS if fn == "close"]
    assert closed, "the attempt must be closed"
    # `Attempt.close` binds (outcome, rows_written, error, duration_ms, isolated, control_answered)
    isolated, control = closed[0][4], closed[0][5]
    assert isolated is False, (
        "a batch that answered was never isolated, and claiming otherwise is what lets a run-wide "
        "tally masquerade as evidence about one subject"
    )
    assert control is not True, "no control was probed, so `control_answered` cannot be true"

    # And a batch that FAILED and was isolated must say THAT — or the guard refuses a real mark and
    # the negative cache can never fill, which is the opposite failure and equally silent.
    LEDGER_CALLS.clear()

    def poisoned(symbols: Sequence[str], **kw: Any) -> Answer:
        if any(s != "AAPL" for s in symbols):
            raise RuntimeError("No data found, symbol may be delisted")
        return bars_for(list(symbols))

    materialise(tmp_path, poisoned)
    closed = [params for fn, params in LEDGER_CALLS if fn == "close"]
    assert closed[0][4] is True, "the isolation pass ran and the attempt must record it"
    assert closed[0][5] is True, "the control answered and the attempt must record it"


def test_the_cross_section_budget_can_actually_cover_the_universe() -> None:
    """A DAY-PARTITIONED ASSET CLAIMS ITS CROSS-SECTION; THE BUDGET DECIDES WHETHER IT CAN.

    Measured on the node: 200 securities in 244 seconds, so ~1.22 s each, and the 11,446 askable
    equities are **3.9 hours**. At the previous default of one hour a nightly run would have covered
    a quarter of them and stopped — materialising a partition that still claims the full
    cross-section, which is the false claim the whole partitioning argument exists to prevent.

    Pinned as arithmetic rather than as a number, so that if the universe grows or the provider
    slows, this fails instead of the claim quietly becoming untrue.
    """
    seconds_per_security = 244 / 200
    askable = 11_446
    needed = askable * seconds_per_security

    assert asset_prices.PriceRun().budget_seconds >= needed, (
        f"the universe needs {needed / 3600:.1f}h and the budget is "
        f"{asset_prices.PriceRun().budget_seconds / 3600:.1f}h — a run that stops early still "
        f"materialises its partition, and the partition is the claim"
    )
    # And not absurdly generous either: a budget far past what the work takes stops being a bound.
    assert asset_prices.PriceRun().budget_seconds <= needed * 2


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
    from muffin_ingest_dagster import definitions as d

    sensors = {s.name: s for s in (d.defs.sensors or [])}
    assert "default_automation_condition_sensor" in sensors, (
        "the eager conditions on the derived assets are inert without it"
    )
    assert sensors["default_automation_condition_sensor"].default_status is (
        dg.DefaultSensorStatus.RUNNING
    )


def test_every_collection_schedule_is_running() -> None:
    """They shipped STOPPED deliberately — a schedule spending the provider budget on numbers
    nobody had compared was the wrong default — and the comparison has now happened.

    The cutover disables the ten old resources in the same change, so if these do not run, NOTHING
    collects. Asserted rather than remembered, because "start the schedules" is exactly the kind of
    step a runbook loses.
    """
    from muffin_ingest_dagster import definitions as d

    collecting = {"daily_prices_schedule", "daily_fx_schedule", "daily_indices_schedule"}
    running = {
        s.name
        for s in (d.defs.schedules or [])
        if s.default_status is dg.DefaultScheduleStatus.RUNNING
    }
    assert collecting <= running, f"not running: {sorted(collecting - running)}"


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
    from muffin_ingest_dagster import definitions as d

    known = {"yfinance", "yahoo", "finviz", "sec", "nse", "sql"}
    # The pool is declared on the asset's underlying op, not on the AssetsDefinition.
    used = {
        pool
        for asset in (d.defs.assets or [])
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
    from muffin_ingest_dagster.assets import indices as asset_indices

    pool = asset_indices.raw_sector_performance.op.pool
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
    from muffin_ingest_dagster import definitions as d

    #: Idles at zero by design: the load is one backfill, then nothing until a new subject appears.
    backfill_only = {
        "raw_price_history",
        "price_bar_history",
        "raw_fx_history",
        "fx_rate_history",
    }

    # READ OFF THE SPEC. `freshness_policies_by_key` exists and is the LEGACY one — it returns
    # nothing for a policy declared via `@asset(freshness_policy=...)`, so a test using it reports
    # every asset as unwatched and would have been "fixed" by adding policies that were already
    # there. A guard whose accessor is wrong fails in the safe direction exactly once.
    policies = {
        spec.key.to_user_string(): spec.freshness_policy
        for asset in d.defs.assets or []
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
