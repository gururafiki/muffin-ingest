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


def test_the_adopting_step_stays_on_the_daemon_because_its_condition_is_all_built_ins() -> None:
    """`security_symbology` is NOT in the code-location sensor, deliberately — it is eager with a
    `replace(...ignore(...))`, every node of which is Dagster's own. If that ever stops being true
    the guard above catches it; this says why the asset is absent from `symbology_rungs`."""
    graph = loaded_defs().resolve_asset_graph()
    condition = graph.get(dg.AssetKey("security_symbology")).automation_condition
    assert condition is not None and condition.is_serializable


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
        instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, subjects)
        result = code_location_automation.evaluate_tick(
            dg.build_sensor_context(instance=instance, repository_def=defs.get_repository_def())
        )

    requested: dict[str, set[str]] = {}
    for request in result.run_requests or []:
        for key, subset in _requested_partitions(request).items():
            requested.setdefault(key, set()).update(subset)
    for rung in ("raw_figi_ticker", "raw_figi_local_symbol"):
        assert requested.get(rung) == set(subjects), (rung, requested)


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
