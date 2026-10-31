"""The daily index schedule asks again for any recent day not wholly collected.

The instance is a real ephemeral one: the schedule reads partition status and each day's newest
Form 4 counters from Dagster's own event log, so a fake would test nothing.
"""

from datetime import UTC, datetime

import dagster as dg

from muffin_ingest_dagster.defs.sec import automation as sec_automation
from muffin_ingest_dagster.defs.sec import core as sec_core
from muffin_ingest_dagster.defs.sec import raw as sec_raw

#: The 11:00 UTC tick of 2026-10-31. Its lookback is 10-21..10-30, and yesterday is 10-30.
TICK = datetime(2026, 10, 31, 11, 0, tzinfo=UTC)
LOOKBACK = [f"2026-10-{d}" for d in range(21, 31)]


def _collected(
    instance: dg.DagsterInstance, day: str, *, trades: bool = True, **form4: int
) -> None:
    """A day whose assets materialised, the Form 4 run reporting `form4` as its counters."""
    for key in (sec_raw.INDEX, *([sec_core.sec_form4_trade.key] if trades else [])):
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=key, partition=day))
    instance.report_runless_asset_event(
        dg.AssetMaterialization(asset_key=sec_raw.FORM4, partition=day, metadata=dict(form4))
    )


def _requested(instance: dg.DagsterInstance, tick: datetime = TICK) -> dict[str, str]:
    with dg.build_schedule_context(instance=instance, scheduled_execution_time=tick) as context:
        requests = sec_automation.sec_daily_index(context)
    assert isinstance(requests, list)
    return {str(r.partition_key): str(r.run_key) for r in requests}


def test_a_day_a_refusal_left_short_is_asked_again_and_a_whole_one_is_not() -> None:
    """Five ways a day can stand, of which only the whole ones are left alone.

    A day the index lane collected but whose Form 4 run stopped is what the 2026-10-30 backfill left
    behind seven times; before this, the schedule asked only for a missing index.
    """
    with dg.instance_for_test() as instance:
        for day in LOOKBACK:
            _collected(instance, day, named=10, outcomes=10, form4_answered=10)
        # Stopped by a 503 after three filings: the run materialised, the day is short.
        _collected(instance, "2026-10-22", named=10, outcomes=10, form4_answered=3,
                   form4_refused=1, form4_unasked=6)  # fmt: skip
        # A weekend, and a quarter the data set covers: nothing named, nothing short.
        _collected(instance, "2026-10-24", named=0, outcomes=0)
        # Re-run: the three held, the rest answered. Whole again, and only the newest run counts.
        _collected(instance, "2026-10-25", named=10, outcomes=10, form4_answered=3,
                   form4_unasked=7)  # fmt: skip
        _collected(instance, "2026-10-25", named=10, outcomes=10, form4_held_already=3,
                   form4_answered=7)  # fmt: skip
        # A 404 the index named: not held, so asked again while the day is in the lookback.
        _collected(instance, "2026-10-26", named=10, outcomes=10, form4_answered=9,
                   form4_absent=1)  # fmt: skip

        requested = _requested(instance)

    assert set(requested) == {"2026-10-22", "2026-10-26", "2026-10-30"}


def test_a_day_whose_trades_never_landed_is_asked_again() -> None:
    """Stage 2 failing after stage 1 leaves the Form 4s held and no trades: the day is not done."""
    with dg.instance_for_test() as instance:
        for day in LOOKBACK:
            _collected(instance, day, trades=day != "2026-10-23", named=0, outcomes=0)

        requested = _requested(instance)

    assert set(requested) == {"2026-10-23", "2026-10-30"}


def test_the_run_key_carries_the_tick_s_day_so_a_short_day_is_asked_on_each() -> None:
    """Dagster launches a run key once per schedule: keyed on the partition alone, a day left short
    by the tick that first asked it would never be asked again."""
    with dg.instance_for_test() as instance:
        for day in LOOKBACK:
            _collected(instance, day, named=0, outcomes=0)
        _collected(instance, "2026-10-28", named=5, outcomes=5, form4_refused=1, form4_unasked=4)

        first = _requested(instance)
        second = _requested(instance, datetime(2026, 11, 1, 11, 0, tzinfo=UTC))

    assert first["2026-10-28"] == "sec_daily_index:2026-10-31:2026-10-28"
    assert second["2026-10-28"] == "sec_daily_index:2026-11-01:2026-10-28"
