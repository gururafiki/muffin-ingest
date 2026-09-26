"""Every automation condition in the location must be evaluable where it is evaluated.

DAGSTER EVALUATES A CONDITION IN ONE OF TWO PLACES, AND ONLY ONE OF THEM CAN SEE PYTHON. The
default sensor runs in the AssetDaemon, which receives each asset's condition SERIALISED from the
code location. A condition containing any node that is not whitelisted for serialisation — any
`AutomationCondition` subclass of ours — is shipped as a display snapshot with `automation_condition
= None` instead (`dagster._core.remote_representation.external_data.
resolve_automation_condition_args`), and the daemon's default sensor covers only assets whose
condition is not None. So the asset is silently left out: the WHOLE condition, built-in branches
and all, never fires.

Measured 2026-09-24 on the symbology rungs: 6,984 partitions seeded, fifteen hours, 1,807 default
sensor ticks, nothing requested — and every test green, because `evaluate_automation_conditions`
evaluates in-process where the Python object exists. That is why this guard is structural: no
in-process evaluation can reproduce the daemon's view.

The remedy is an `AutomationConditionSensorDefinition` with `use_user_code_server=True`, which is
evaluated in the code location. Its public tell is `sensor_type == SensorType.AUTOMATION`.
"""

from __future__ import annotations

import dagster as dg
from dagster._core.definitions.sensor_definition import SensorType

from muffin_ingest_dagster.defs.symbology import raw as symbology_raw
from tests import loaded_defs


def _unreadable_by_the_daemon(graph: dg.AssetGraph) -> set[dg.AssetKey]:
    """Assets whose condition the AssetDaemon would receive as `None`."""
    out: set[dg.AssetKey] = set()
    for key in graph.materializable_asset_keys:
        condition = graph.get(key).automation_condition
        if condition is not None and not condition.is_serializable:
            out.add(key)
    return out


def _evaluated_in_the_code_location(defs: dg.Definitions, graph: dg.AssetGraph) -> set[dg.AssetKey]:
    """Assets targeted by an automation sensor that runs in the code location AND ships running."""
    out: set[dg.AssetKey] = set()
    for sensor in defs.get_repository_def().sensor_defs:
        if not isinstance(sensor, dg.AutomationConditionSensorDefinition):
            continue
        # A sensor that ships STOPPED covers nothing until someone clicks it on in the UI, which
        # is the state this family sat in for a week — so it does not count as coverage here.
        if sensor.sensor_type != SensorType.AUTOMATION:
            continue
        if sensor.default_status != dg.DefaultSensorStatus.RUNNING:
            continue
        out |= sensor.asset_selection.resolve(graph)
    return out


def test_every_condition_the_daemon_cannot_read_is_evaluated_in_the_code_location() -> None:
    defs = loaded_defs()
    graph = defs.resolve_asset_graph()

    unreadable = _unreadable_by_the_daemon(graph)
    covered = _evaluated_in_the_code_location(defs, graph)

    # THE GUARD MUST REACH ITS OWN BRANCH. If `ReAskAfter` is ever made serialisable this fails,
    # and that is information rather than noise: the code-location sensor can then be retired.
    rungs = {symbology_raw.raw_figi_ticker.key, symbology_raw.raw_figi_local_symbol.key}
    assert rungs <= unreadable, (
        "the rungs' condition is now serialisable "
        f"({sorted(k.to_user_string() for k in rungs - unreadable)}); "
        "`symbology_rungs` may no longer be needed"
    )

    silent = unreadable - covered
    assert not silent, (
        "these assets carry a condition the AssetDaemon cannot deserialise and no running "
        "code-location automation sensor targets them, so they will never be requested: "
        f"{sorted(k.to_user_string() for k in silent)}"
    )


def test_the_adopting_step_is_evaluated_by_the_sensor_that_requests_its_rungs() -> None:
    """ONE SENSOR FOR THE WHOLE LADDER, and the reason is `will_be_requested()`.

    `eager()` schedules a child in the SAME tick as a parent being requested — its trigger includes
    `will_be_requested()` — but that sees only what the evaluating sensor is requesting. Left on the
    default sensor, `security_symbology` could not see the rungs being requested; it saw their
    partitions finish one materialisation at a time (`in_progress()` covers a partition only until
    the run has executed it), and each 30-second tick requested whatever scattered subset had
    landed: 145 runs for 6,981 subjects on 2026-09-24, ~420 queued for 5,512 on 2026-09-26.

    Its condition is all built-ins, so it could be read by either sensor — which is exactly why
    nothing else would catch it being moved back.
    """
    defs = loaded_defs()
    graph = defs.resolve_asset_graph()
    adopting = dg.AssetKey("security_symbology")
    condition = graph.get(adopting).automation_condition
    assert condition is not None and condition.is_serializable

    rungs = {symbology_raw.raw_figi_ticker.key, symbology_raw.raw_figi_local_symbol.key}
    owners = [
        sensor.name
        for sensor in defs.get_repository_def().sensor_defs
        if isinstance(sensor, dg.AutomationConditionSensorDefinition)
        and {adopting, *rungs} <= sensor.asset_selection.resolve(graph)
    ]
    assert owners == ["symbology_rungs"], (
        "the adopting step and its rungs must share one automation sensor, or it is requested in "
        f"fragments as their partitions land; sensors covering all three: {owners}"
    )


def test_the_code_location_sensor_requests_the_seeded_subjects() -> None:
    """THE GUARD ABOVE PROVES COVERAGE; THIS PROVES THE COVERAGE DOES SOMETHING. It drives
    `evaluate_tick` on the real sensor — the call the code server makes for an `AUTOMATION` sensor
    (`dagster/_daemon/sensor.py`, `get_sensor_execution_data`) — against a grid holding two
    subjects nobody has asked about, and requires both rungs to be requested for both.

    Two subjects, not one: a backfill is emitted only when more than one partition of an asset is
    requested, so one subject would exercise a different branch from the one production takes."""
    from muffin_ingest_dagster.defs.platform.automation import code_location_automation
    from muffin_ingest_dagster.defs.symbology.partitions import SYMBOLOGY_PARTITIONS

    subjects = ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]
    defs = loaded_defs()
    with dg.instance_for_test() as instance:
        # A FIRST TICK ON AN EMPTY GRID, which is where production's sensor has long been: the
        # first evaluation of an `eager()` asset counts as handled, so a single tick on a fresh
        # cursor would assert on a state production never sees.
        first = code_location_automation.evaluate_tick(
            dg.build_sensor_context(instance=instance, repository_def=defs.get_repository_def())
        )
        instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, subjects)
        result = code_location_automation.evaluate_tick(
            dg.build_sensor_context(
                instance=instance, repository_def=defs.get_repository_def(), cursor=first.cursor
            )
        )

    requests = [_requested_partitions(r) for r in result.run_requests or []]
    requested: dict[str, set[str]] = {}
    for request in requests:
        for key, subset in request.items():
            requested.setdefault(key, set()).update(subset)
    for rung in ("raw_figi_ticker", "raw_figi_local_symbol"):
        assert requested.get(rung) == set(subjects), (rung, requested)
    # AND THE ADOPTING STEP IN THE SAME REQUEST: one backfill carrying the ladder, which runs a rung
    # and the step that adopts its answers for the same subjects in one run, instead of the step
    # trailing its parents' materialisations in fragments.
    ladder = {"raw_figi_ticker", "raw_figi_local_symbol", "security_symbology"}
    assert any(set(r) >= ladder and all(r[k] == set(subjects) for k in ladder) for r in requests), (
        f"the ladder was not requested as one unit: {requests}"
    )


def test_a_new_filing_is_fetched_and_ingested_in_one_tick() -> None:
    """THE FILING LANE RUNS ITSELF, AND ONLY FOR WHAT IS NEW.

    It shipped with no condition, and measured 2026-09-26 it had never run: the sensor added 78
    filings as partitions, nothing fetched them, and the edge's `fund-holdings` kept writing every
    table the lane owns. Retiring that resource needs a new filing to be fetched, resolved and
    ingested with nobody pressing anything — all three requested in ONE tick, which is what makes
    the sensor emit them as one backfill and run them together.

    AND A KEY ALREADY IN THE GRID IS NOT REQUESTED. `on_missing()` fires when a partition BECOMES
    missing; the filings in the grid when this shipped were backfilled by hand, and a rule that
    fetched every stale key on its first tick would be a burst against SEC nobody chose.

    Evaluated in-process because the default sensor refuses `evaluate_tick` outside the daemon;
    these conditions are all built-ins, which is what makes the in-process result the daemon's.
    """
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw
    from muffin_ingest_dagster.defs.discovery.partitions import NPORT_PARTITIONS

    lane = [
        discovery_raw.raw_nport_filing,
        discovery_core.discovered_security,
        discovery_core.fund_holding,
    ]
    for asset in lane:
        condition = next(iter(asset.automation_conditions_by_key.values()), None)
        assert condition is not None and condition.is_serializable, asset.key

    already = "0001100663:000000000000000009"
    new = ["0001100663:000000000000000001", "0001100663:000000000000000002"]
    # Resources only so the definitions validate; nothing is executed. `postgres_io` is stood in
    # by the Parquet manager for the same reason — the evaluation never writes.
    from muffin_ingest_dagster.lib.io_managers import ParquetIOManager
    from muffin_ingest_dagster.lib.resources import Postgres

    defs = dg.Definitions(
        assets=lane,
        resources={
            "postgres": Postgres(),
            "parquet_io": ParquetIOManager("/nonexistent"),
            "postgres_io": ParquetIOManager("/nonexistent"),
        },
    )
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(NPORT_PARTITIONS, [already])
        first = dg.evaluate_automation_conditions(defs=defs, instance=instance)
        instance.add_dynamic_partitions(NPORT_PARTITIONS, new)
        second = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, cursor=first.cursor
        )

    requested = {
        r.key.to_user_string(): set(r.true_subset.expensively_compute_partition_keys())
        for r in second.results
    }
    for name in ("raw_nport_filing", "discovered_security", "fund_holding"):
        assert requested.get(name) == set(new), (name, requested)


def _requested_partitions(request: dg.RunRequest) -> dict[str, set[str]]:
    """asset → partitions, whether the tick emitted a run or a backfill."""
    out: dict[str, set[str]] = {}
    if request.asset_graph_subset is not None:
        for key in request.asset_graph_subset.asset_keys:
            subset = request.asset_graph_subset.get_asset_subset(key)
            assert subset is not None, key
            out[key.to_user_string()] = set(subset.subset_value.get_partition_keys())
    elif request.partition_key is not None:
        for key in request.asset_selection or []:
            out.setdefault(key.to_user_string(), set()).add(request.partition_key)
    return out
