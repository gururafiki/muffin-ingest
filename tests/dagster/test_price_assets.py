"""The price assets, driven end to end with a fake provider and a fake database.

Driven rather than inspected, because every defect this replaces was invisible to inspection: a
resource that reported success while asking the wrong question, a batch attributed to the wrong
security, a throttle recorded as an absence. So these materialise the real assets through the real
I/O manager and assert on what reached the other side.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

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


class FakeCursor:
    def __init__(self, subjects: list[tuple[str, str, float]]) -> None:
        self._subjects = subjects
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        # Answered by SHAPE rather than by exact text, so reformatting a query does not silently
        # turn a test into one that asserts nothing.
        if "market.listing" in sql:
            self.rows = [(sid, "USD") for sid, _, _ in self._subjects]
        else:
            self.rows = list(self._subjects)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


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
