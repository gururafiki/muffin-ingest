"""The Dagster code location.

Everything it serves is autoloaded from `defs/`, one package per family plus `platform/`. This
module exists to start the exporter and to name that folder, so the container's start command
(`dagster api grpc -m muffin_ingest_dagster.definitions`) never changes as families are added.

TWO CONVENTIONS THAT ARE LOAD-BEARING AND EASY TO LOSE:

* AN ASSET IS A TABLE, never a security. Dagster's per-item primitive is partitions, bounded around
  25,000 per asset and meant for time windows, against ~12,350 securities times ~40 facets here —
  so the per-item grain lives in `ingest.task` and an asset materialises "as much of a backlog as
  fits in this run".
* A POOL IS A PROVIDER. `concurrency.pools` in `dagster.yaml` gives each provider a limit of one,
  which is what replaced the five-minute rotation as the guarantee that nothing bursts. Requests
  per second and per day are a separate concern and belong to the limiter, because a pool cannot
  express a rate.
"""

from pathlib import Path

import dagster as dg
from muffin_ingest import metrics

# THE EXPORTER STARTS IN THE CODE-LOCATION PARENT, ONCE, AT IMPORT.
#
# Each Dagster run is a SUBPROCESS, so a counter incremented during a run lives in a child that
# exits moments later — a registry in the parent would report zero for ever while the work
# happened. `prometheus_client`'s multiprocess mode has every process write its own file under
# `PROMETHEUS_MULTIPROC_DIR` and the exporter aggregate them at scrape time; it is the only shape
# that survives this process model.
#
# AT IMPORT RATHER THAN IN A RESOURCE, because the gRPC server never "runs" an asset — it serves
# definitions — and a resource is only constructed inside a run, i.e. inside the child that has
# no port. `enabled()` is false wherever `PROMETHEUS_MULTIPROC_DIR` is unset, so the unit tests,
# `dagster definitions validate` and a local checkout all import this without binding a socket.
#
# BUT EVERY RUN IMPORTS THIS MODULE TOO, with the variable inherited — so this line executes once
# per run as well as once per code-location start. Until `start_exporter` learned to take an
# already-served port as "I am a run" (see its docstring), every run died here with
# `OSError: [Errno 98] Address already in use`, from 2026-09-13 to 2026-09-16.
#
# `prometheus.yml` has carried the matching scrape job COMMENTED OUT since the service was
# created, because it pointed at a port nothing listened on: a permanently-red target is the same
# failure as a permanently-red gate, and the cost is the next real one behind it.
metrics.start_exporter()


@dg.definitions
def defs() -> dg.Definitions:
    """Every module under `defs/` — assets, checks, jobs, schedules, sensors and resources.

    `load_from_defs_folder` finds the project by walking up to this project's `pyproject.toml`
    (`[tool.dg.project] root_module`), so the image must install the project rather than copy the
    package in beside its dependencies.
    """
    return dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
