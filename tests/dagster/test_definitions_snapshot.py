"""The code location's identities, pinned.

DAGSTER KEYS ITS STATE ON NAMES. Materialization history and partition status hang off the asset
key, schedule and sensor state (running or stopped, cursors) off the instigator name, and a job's
run history off the job name. Renaming any of them does not fail anything: the code location loads,
every test passes, and the UI shows a new object with no history beside an old one that never runs
again — a partition set that took four hours of provider budget to materialize reads as never done.

So this snapshots what must survive a refactor, and nothing that may change freely (descriptions,
code layout, metadata). It is written against APIs that exist unchanged in Dagster 1.12 and 1.13,
and the fingerprint was measured identical on both for the same code, so the one golden file holds
across the workspace move AND the version bump.

To accept a deliberate change, regenerate and read the diff before committing it:

    MUFFIN_UPDATE_SNAPSHOT=1 pytest tests/dagster/test_definitions_snapshot.py

An ADDED name is ordinary. A REMOVED or CHANGED one is a rename or a behaviour change — decide what
happens to its history first.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import dagster as dg

from muffin_ingest_dagster.definitions import defs

SNAPSHOT = Path(__file__).with_name("definitions.snapshot.json")


def _seconds(window: Any) -> float | None:
    return None if window is None else float(window.to_timedelta().total_seconds())


def _partitions(pd: dg.PartitionsDefinition[Any] | None) -> dict[str, Any] | None:
    if pd is None:
        return None
    if isinstance(pd, dg.TimeWindowPartitionsDefinition):
        return {
            "kind": "time_window",
            "cron_schedule": pd.cron_schedule,
            "start": pd.start.isoformat(),
            "end": pd.end.isoformat() if pd.end else None,
            "timezone": pd.timezone,
            "fmt": pd.fmt,
            "end_offset": pd.end_offset,
        }
    if isinstance(pd, dg.StaticPartitionsDefinition):
        return {"kind": "static", "keys": list(pd.get_partition_keys())}
    if isinstance(pd, dg.DynamicPartitionsDefinition):
        return {"kind": "dynamic", "name": pd.name}
    if isinstance(pd, dg.MultiPartitionsDefinition):
        return {
            "kind": "multi",
            "dimensions": {d.name: _partitions(d.partitions_def) for d in pd.partitions_defs},
        }
    return {"kind": type(pd).__name__}


def _freshness(policy: Any) -> dict[str, Any] | None:
    if policy is None:
        return None
    if type(policy).__name__ == "TimeWindowFreshnessPolicy":
        return {
            "kind": "time_window",
            "fail_window_seconds": _seconds(policy.fail_window),
            "warn_window_seconds": _seconds(policy.warn_window),
        }
    # A policy kind this file has not met yet: fail loudly into the diff rather than guess.
    return {"kind": type(policy).__name__, "repr": repr(policy)}


def fingerprint(definitions: dg.Definitions) -> dict[str, Any]:
    graph = definitions.resolve_asset_graph()

    assets: dict[str, Any] = {}
    for key in sorted(graph.get_all_asset_keys(), key=lambda k: k.to_user_string()):
        node = graph.get(key)
        condition = node.automation_condition
        assets[key.to_user_string()] = {
            "group_name": node.group_name,
            "kinds": sorted(node.kinds),
            "execution_type": node.execution_type.value,
            "partitions": _partitions(node.partitions_def),
            "backfill_policy": (
                None
                if node.backfill_policy is None
                else {
                    "type": node.backfill_policy.policy_type.value,
                    "max_partitions_per_run": node.backfill_policy.max_partitions_per_run,
                }
            ),
            "pools": sorted(node.pools or ()),
            "automation_condition": None if condition is None else condition.get_label(),
            "freshness_policy": _freshness(node.freshness_policy),
            "io_manager_key": node.io_manager_key,
            "code_version": node.code_version,
            "owners": sorted(node.owners),
            "tags": {k: v for k, v in sorted(node.tags.items()) if not k.startswith("dagster/")},
            # The mapping class only where both ends are partitioned; elsewhere there is none.
            "deps": {
                parent.to_user_string(): (
                    type(graph.get_partition_mapping(key, parent)).__name__
                    if node.partitions_def is not None
                    and graph.get(parent).partitions_def is not None
                    else None
                )
                for parent in sorted(node.parent_keys, key=lambda k: k.to_user_string())
            },
        }

    checks: dict[str, Any] = {}
    for check_key in sorted(
        graph.asset_check_keys, key=lambda c: (c.asset_key.to_user_string(), c.name)
    ):
        spec = graph.get_check_spec(check_key)
        checks[f"{check_key.asset_key.to_user_string()}:{check_key.name}"] = {
            "blocking": spec.blocking,
            "additional_deps": sorted(d.asset_key.to_user_string() for d in spec.additional_deps),
        }

    jobs: dict[str, Any] = {}
    for job in sorted(definitions.resolve_all_job_defs(), key=lambda j: j.name):
        # `__ASSET_JOB` and its siblings are Dagster's own, named by version, not by us.
        if job.name.startswith("__ASSET_JOB"):
            continue
        asset_keys = sorted(k.to_user_string() for k in job.asset_layer.executable_asset_keys)
        jobs[job.name] = {
            "assets": asset_keys,
            "ops": [] if asset_keys else sorted(n.name for n in job.graph.node_defs),
            "partitions": _partitions(job.partitions_def),
        }

    repository = definitions.get_repository_def()
    schedules = {
        s.name: {
            "cron_schedule": s.cron_schedule,
            "execution_timezone": s.execution_timezone,
            "default_status": s.default_status.value,
            "job": s.job_name,
        }
        for s in sorted(repository.schedule_defs, key=lambda s: s.name)
    }
    sensors = {
        s.name: {
            "type": s.sensor_type.value,
            "default_status": s.default_status.value,
            "minimum_interval_seconds": s.minimum_interval_seconds,
            "jobs": sorted(t.job_name for t in s.targets),
        }
        for s in sorted(repository.sensor_defs, key=lambda s: s.name)
    }
    resources = {k: type(v).__name__ for k, v in sorted((definitions.resources or {}).items())}

    return {
        "assets": assets,
        "asset_checks": checks,
        "jobs": jobs,
        "schedules": schedules,
        "sensors": sensors,
        "resources": resources,
    }


def _describe(expected: dict[str, Any], actual: dict[str, Any]) -> str:
    lines = []
    for section in sorted(set(expected) | set(actual)):
        before, after = expected.get(section, {}), actual.get(section, {})
        for name in sorted(set(before) - set(after)):
            lines.append(f"REMOVED {section}/{name} — a rename loses its history and state")
        for name in sorted(set(after) - set(before)):
            lines.append(f"added   {section}/{name}")
        for name in sorted(set(before) & set(after)):
            if before[name] != after[name]:
                lines.append(f"CHANGED {section}/{name}: {before[name]} -> {after[name]}")
    return "\n".join(lines)


def test_the_code_location_keeps_every_name_and_setting_state_is_keyed_on() -> None:
    actual = fingerprint(defs)
    if os.environ.get("MUFFIN_UPDATE_SNAPSHOT") == "1":
        SNAPSHOT.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
    expected = json.loads(SNAPSHOT.read_text())
    assert actual == expected, (
        "the definitions differ from definitions.snapshot.json:\n"
        + _describe(expected, actual)
        + "\n\nIf deliberate: MUFFIN_UPDATE_SNAPSHOT=1 pytest "
        "tests/dagster/test_definitions_snapshot.py, then read the diff."
    )
