"""The promotion lane, driven over Yahoo's own bytes and a fake database.

Which share classes are candidates, the wave's order, size and pause are SQL, tested against real
Postgres in muffin-deployment (`tests/a-wave-promotes-each-share-class-once-from-its-home.sql`).
What this side owns, and what is driven here:
  - the capacity, because the price sweep's grid is a Dagster partition set;
  - what stage 1 asks, and that it asks the control first and stops on a throttle;
  - that stage 2 hands over only the lines Yahoo priced, in the order asked, and records an
    absence only when the control priced in the same run;
  - the weekly slot.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, ClassVar

import dagster as dg
import pytest
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

WEEK = "2026-10-26"

#: Candidate lines, in the wave's order, with the captured body Yahoo answers each with.
LINES = [
    ("BBG-PRICED-1", "AAPL"),
    ("BBG-ABSENT", "BDMS-F.BK"),
    ("BBG-QUOTE-ONLY", "PCTN.L"),
    ("BBG-PRICED-2", "VOD.L"),
    ("BBG-GATEWAY", "GATE.L"),
]
BODIES = {
    "AAPL": "aapl_ext",
    "VOD.L": "vod_l_ext",
    "BDMS-F.BK": "bdms_f_bk_404",
    "PCTN.L": "pctn_l_quote_only",
}


def _body(symbol: str) -> bytes:
    if symbol == "GATE.L":
        return b"<html>bad gateway</html>"
    return (FIXTURES / f"yahoo_chart_{BODIES[symbol]}.body").read_bytes()


#: When the fake Yahoo says it served a body: the day after AAPL's newest bar, so the captured
#: bodies read as recent. Also VOD.L's: both were captured the same evening.
SERVED = datetime.combine(
    max(
        p.as_of
        for p in yahoo_chart.parse(_body("AAPL")).points
        if not p.is_live and p.close is not None
    ),
    time(22),
    tzinfo=UTC,
) + timedelta(days=1)

Answer = bytes | Exception


@contextmanager
def yahoo(override: dict[str, Answer] | None = None) -> Iterator[list[str]]:
    """Yahoo answering each symbol from the captures, recording every symbol it was asked."""
    asked: list[str] = []

    def fetch(symbol: str, **kwargs: Any) -> Document:
        asked.append(symbol)
        answer = (override or {}).get(symbol, None)
        if isinstance(answer, Exception):
            raise answer
        return Document(
            url=f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
            body=answer if isinstance(answer, bytes) else _body(symbol),
            content_type="application/json",
            fetched_at=SERVED,
        )

    saved = yahoo_chart.fetch
    yahoo_chart.fetch = fetch
    try:
        yield asked
    finally:
        yahoo_chart.fetch = saved


class _Cursor:
    def __init__(self, db: type[FakePostgres]) -> None:
        self._db = db
        self._rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        if text.startswith(
            "select paused, wave_size, max_pass_nights from market.promotion_policy"
        ):
            self._rows = [(self._db.paused, self._db.wave_size, self._db.nights)]
        elif text.startswith("select figi, provider_symbol from market.promotion_candidate"):
            self._db.candidate_limits.append(int(params[0]))
            self._rows = list(self._db.lines[: int(params[0])])
        elif text.startswith("insert into market.promotion_probe"):
            found = re.search(r"\(([^)]*)\) values", text)
            assert found is not None
            columns = [c.strip() for c in found.group(1).split(",")]
            values = list(params)
            for i in range(0, len(values), len(columns)):
                self._db.probes.append(
                    dict(zip(columns, values[i : i + len(columns)], strict=True))
                )
            self.rowcount = len(values) // len(columns)
        elif text == "select market.promote_share_classes(%s::text[], %s)":
            handed, limit = list(params[0]), int(params[1])
            self._db.handed.append((handed, limit))
            self._rows = [
                ({"promoted": min(len(handed), limit), "by_venue": {}, "candidates_left": 7},)
            ]
        else:
            raise AssertionError(f"unexpected query: {text[:140]}")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _Conn:
    def __init__(self, db: type[FakePostgres]) -> None:
        self._db = db

    def cursor(self) -> _Cursor:
        return _Cursor(self._db)


class FakePostgres(Postgres):
    paused: ClassVar[bool] = False
    wave_size: ClassVar[int] = 500
    nights: ClassVar[int] = 7
    lines: ClassVar[list[tuple[str, str]]] = []
    candidate_limits: ClassVar[list[int]] = []
    probes: ClassVar[list[dict[str, Any]]] = []
    handed: ClassVar[list[tuple[list[str], int]]] = []

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield _Conn(type(self))


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> None:
    from muffin_ingest_dagster.defs.promotion import checks, raw

    monkeypatch.setattr(prices_partitions, "CHART_SECONDS_BETWEEN_REQUESTS", 0.0)
    # A night of 10 securities, so a grid the size of a pass is a handful of partition keys.
    monkeypatch.setattr(raw, "SWEEP_SLICE", 10)
    monkeypatch.setattr(checks, "SWEEP_SLICE", 10)
    FakePostgres.paused = False
    FakePostgres.wave_size = 500
    FakePostgres.nights = 7
    FakePostgres.lines = list(LINES)
    FakePostgres.candidate_limits = []
    FakePostgres.probes = []
    FakePostgres.handed = []


def _wave(
    tmp_path: Path,
    *,
    securities: int = 60,
    stage_2: bool = True,
    config: dict[str, Any] | None = None,
) -> dg.ExecuteInProcessResult:
    from muffin_ingest_dagster.defs.promotion.checks import promotion_stays_within_the_sweep
    from muffin_ingest_dagster.defs.promotion.core import promotion_wave
    from muffin_ingest_dagster.defs.promotion.raw import raw_promotion_probe

    assets: list[Any] = [raw_promotion_probe]
    if stage_2:
        assets += [promotion_wave, promotion_stays_within_the_sweep]
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(
            SECURITY_PARTITION, [f"s{i:04d}" for i in range(securities)]
        )
        return dg.materialize(
            assets,
            instance=instance,
            partition_key=WEEK,
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(base_path=str(tmp_path)),
            },
            run_config={"ops": {"raw_promotion_probe": {"config": config or {}}}},
        )


def _meta(result: dg.ExecuteInProcessResult, node: str) -> dict[str, Any]:
    return {k: v.value for k, v in result.asset_materializations_for_node(node)[0].metadata.items()}


# ── capacity ───────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("securities", "paused", "wave_size", "want"),
    [
        (60, False, 500, 10),
        (60, False, 3, 3),
        (80, False, 500, 0),
        (95, False, 500, 0),
        (60, True, 500, 0),
    ],
    ids=["room-for-10", "capped-by-the-wave", "full", "over-full", "paused"],
)
def test_a_wave_has_as_many_places_as_the_sweep_can_take(
    securities: int, paused: bool, wave_size: int, want: int
) -> None:
    """7 nights of 10 is 70 a pass; a negative capacity is zero, never "no limit"."""
    from muffin_ingest_dagster.defs.promotion import raw

    policy = raw.Policy(paused=paused, wave_size=wave_size, max_pass_nights=7)
    assert raw.places(policy, securities) == want


# ── stage 1 ───────────────────────────────────────────────────────────────────────────────────


def test_the_probe_asks_the_control_first_then_twice_the_places(tmp_path: Path) -> None:
    """60 in the grid leaves 10 places, so up to 20 candidates; there are five."""
    with yahoo() as asked:
        result = _wave(tmp_path, stage_2=False)
    assert result.success
    assert asked == ["AAPL"] + [symbol for _, symbol in LINES]
    assert FakePostgres.candidate_limits == [20]
    meta = _meta(result, "raw_promotion_probe")
    assert meta["answered"] == 6 and meta["places"] == 10 and meta["reason"] == "none"


def test_a_paused_policy_asks_yahoo_nothing(tmp_path: Path) -> None:
    FakePostgres.paused = True
    with yahoo() as asked:
        result = _wave(tmp_path)
    assert result.success
    assert asked == []
    assert _meta(result, "raw_promotion_probe")["reason"] == "paused"
    assert FakePostgres.handed == [([], 10)]


def test_a_live_subset_asks_about_fewer(tmp_path: Path) -> None:
    with yahoo() as asked:
        _wave(tmp_path, stage_2=False, config={"limit": 2})
    assert asked == ["AAPL", "AAPL", "BDMS-F.BK"]


def test_a_throttle_stops_the_probe_and_counts_the_rest_unasked(tmp_path: Path) -> None:
    refused = yahoo_chart.YahooRefused("HTTP 429 Too Many Requests for chart")
    with yahoo({"PCTN.L": refused}) as asked:
        result = _wave(tmp_path, stage_2=False)
    assert asked == ["AAPL", "AAPL", "BDMS-F.BK", "PCTN.L"]
    meta = _meta(result, "raw_promotion_probe")
    assert (meta["answered"], meta["throttled"], meta["unasked"], meta["transport"]) == (3, 1, 2, 0)
    assert meta["answered"] + meta["throttled"] + meta["unasked"] + meta["transport"] == 6


# ── stage 2 ───────────────────────────────────────────────────────────────────────────────────


def test_the_wave_hands_over_the_priced_lines_in_the_order_asked(tmp_path: Path) -> None:
    with yahoo():
        result = _wave(tmp_path)
    assert result.success
    assert FakePostgres.handed == [(["BBG-PRICED-1", "BBG-PRICED-2"], 10)]
    meta = _meta(result, "promotion_wave")
    assert meta["outcomes"] == {"absent": 1, "priced": 2, "quote_only": 1, "unreadable": 1}
    assert meta["control_priced"] is True


def test_an_absence_is_recorded_when_the_control_priced(tmp_path: Path) -> None:
    """A gateway page is not a statement about the line, so it is never recorded."""
    with yahoo():
        _wave(tmp_path)
    recorded = {p["figi"]: p["outcome"] for p in FakePostgres.probes}
    assert recorded == {
        "BBG-PRICED-1": "priced",
        "BBG-ABSENT": "absent",
        "BBG-QUOTE-ONLY": "quote_only",
        "BBG-PRICED-2": "priced",
    }
    assert {p["source_code"] for p in FakePostgres.probes} == {"yfinance"}


def test_no_absence_is_recorded_when_the_control_did_not_price(tmp_path: Path) -> None:
    """The control answered with an error page: whatever the candidates got says nothing about
    them, and recording it would keep them out of waves for 90 days."""
    with yahoo():
        # The control is asked first, so the first AAPL request is the control's.
        calls: dict[str, int] = {"AAPL": 0}
        saved = yahoo_chart.fetch

        def fetch(symbol: str, **kwargs: Any) -> Document:
            if symbol == "AAPL":
                calls["AAPL"] += 1
                if calls["AAPL"] == 1:
                    return Document(
                        url="u",
                        body=b"<html>busy</html>",
                        content_type="text/html",
                        fetched_at=SERVED,
                    )
            return saved(symbol, **kwargs)

        yahoo_chart.fetch = fetch
        try:
            _wave(tmp_path)
        finally:
            yahoo_chart.fetch = saved
    assert {p["outcome"] for p in FakePostgres.probes} == {"priced"}
    assert FakePostgres.handed == [(["BBG-PRICED-1", "BBG-PRICED-2"], 10)]


# ── the check ─────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("securities", "passed"), [(70, True), (71, False)])
def test_the_check_fails_when_a_pass_needs_more_nights(
    tmp_path: Path, securities: int, passed: bool
) -> None:
    """71 securities at 10 a night is an eighth night against a policy of seven."""
    with yahoo():
        result = _wave(tmp_path, securities=securities)
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 1
    assert evaluations[0].passed is passed
    assert evaluations[0].severity == dg.AssetCheckSeverity.WARN
    meta = {k: v.value for k, v in evaluations[0].metadata.items()}
    assert meta["price_pass_nights"] == (7 if passed else 8)


# ── when it runs ──────────────────────────────────────────────────────────────────────────────


def test_the_schedule_asks_for_the_week_that_starts_at_its_tick() -> None:
    from muffin_ingest_dagster.defs.promotion.automation import weekly_promotion_wave

    tick = datetime(2026, 11, 2, 6, 17, tzinfo=UTC)
    with dg.build_schedule_context(scheduled_execution_time=tick) as context:
        request = weekly_promotion_wave(context)
    assert isinstance(request, dg.RunRequest)
    assert (request.partition_key, request.run_key) == ("2026-11-02", "2026-11-02")


def test_the_wave_follows_its_probe_and_the_listings_follow_the_wave() -> None:
    """Stage 2 is `eager()` on its own partition, in a run of its own; `security_listing` lists the
    wave as an upstream, so a new class's listings are derived the same morning."""
    from muffin_ingest_dagster.defs.discovery.derived import security_listing
    from muffin_ingest_dagster.defs.promotion.core import promotion_wave

    condition = next(iter(promotion_wave.automation_conditions_by_key.values()), None)
    assert condition is not None and condition.is_serializable
    assert dg.AssetKey("promotion_wave") in security_listing.dependency_keys
