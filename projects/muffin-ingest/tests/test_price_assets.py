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

from muffin_ingest_dagster.defs.platform.heartbeat import heartbeat
from muffin_ingest_dagster.defs.prices import checks as prices_checks
from muffin_ingest_dagster.defs.prices import core as prices_core
from muffin_ingest_dagster.defs.prices import derived as prices_derived
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices import raw as prices_raw
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

#: Every corporate-action retraction sent, as its parameters.
ACTION_RETRACTIONS: list[tuple[Any, ...]] = []

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
        if text.startswith("delete from market.security_corporate_action"):
            ACTION_RETRACTIONS.append(tuple(params))
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

    The lane is partitioned by security, and Yahoo's chart is asked once per symbol however many
    partitions a run covers, so there is nothing to batch across them and all `single_run` bought
    was an unbounded memory footprint. Measured on the openbb lane this replaced (whose adapter
    asked the same endpoint once per ticker): 96 securities is 683,391 raw bars and the child
    process was OOM-killed at 2.4 GB, because
    `UPathIOManager.load_input` is eager and the clean stage holds the raw rows and their
    normalised copies at once.

    Pinned because the tidying instinct runs the wrong way: making the widths "consistent" with an
    unpartitioned asset's reintroduces the failure.
    """
    for asset in (prices_raw.raw_price_chart, prices_core.price_bar_history):
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
