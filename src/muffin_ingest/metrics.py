"""Provider counters, in the only place they can be counted.

WHY THIS EXISTS AT ALL. `http-cache` sits in FRONT of openbb-api and counts what passes through
it — but openbb's own egress does not: yfinance fetches through `curl_cffi` and ignores our base
URLs entirely. That blind spot was already recorded for the `openbb-api` service, and importing
the hub in-process moved it inside our own worker rather than closing it. **In-process is the only
place those requests can be counted**, which is why this ships with the first facet rather than
later.

MULTIPROCESS MODE IS NOT OPTIONAL AND IS THE WHOLE DIFFICULTY. Each Dagster run is a SUBPROCESS of
the code location, so a counter incremented during a run lives in a child that exits moments
later. A plain in-memory registry in the parent would report zero for ever while the work
happened. `prometheus_client`'s multiprocess mode has each process write its own file under
`PROMETHEUS_MULTIPROC_DIR` and the exporter aggregate them at scrape time — the only shape that
survives the process model.

AND `mark_process_dead` DOES NOT STOP IT GROWING — the plan for this exporter said it would, and
that was wrong. Read from the installed package rather than assumed:

    def mark_process_dead(pid, path=None):
        for mode in _LIVE_GAUGE_MULTIPROCESS_MODES:
            for f in glob.glob(os.path.join(path, f'gauge_{mode}_{pid}.db')):
                os.remove(f)

It removes GAUGE files only. Counter and histogram files are deliberately left behind, because
their values must outlive the process that produced them or every total would drop when a run
ended. Measured: three child processes, each calling it on exit, left **6 files**.

So the directory is cleared when the EXPORTER STARTS, which `prometheus_client`'s own multiprocess
documentation requires anyway and for a stronger reason than tidiness — files surviving a restart
are reported as current, so a code location that came back after a crash would serve the previous
incarnation's counters as if they were live. Clearing bounds the directory to one service uptime
and makes the counters honestly per-uptime, which is what `rate()` wants regardless.
"""

from __future__ import annotations

import atexit
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

#: Set on the code-location service. Its ABSENCE is meaningful: the unit tests and `dagster
#: definitions validate` run without it, and must not need a metrics directory to import a facet.
MULTIPROC_ENV = "PROMETHEUS_MULTIPROC_DIR"


def enabled() -> bool:
    return bool(os.environ.get(MULTIPROC_ENV))


def _counters() -> Any:
    """Built lazily and cached on the module.

    `prometheus_client` reads `PROMETHEUS_MULTIPROC_DIR` when a metric is CREATED, so creating one
    at import time in a process that has not set it yet binds the wrong mode permanently.
    """
    global _CACHE
    if _CACHE is None:
        from prometheus_client import Counter, Histogram

        # REGISTERED HERE, NOT AT IMPORT, so only a process that actually wrote metric files
        # tries to remove them. Every Dagster run is a subprocess; `atexit` in the child is the
        # hook that fires when that run ends, and without it the directory grows one file set
        # per run for ever. The counters stay CORRECT while it leaks — `MultiProcessCollector`
        # sums whatever is present — so nothing reports it until the disk does.
        atexit.register(mark_dead)

        _CACHE = {
            # LABELLED BY OUTCOME, NOT JUST COUNTED. "A request that failed and a request that
            # answered nothing are different facts" is this codebase's most repeated rule, and a
            # bare request counter cannot express it — which is how ~8,300 securities were once
            # negative-cached in an afternoon with every count looking healthy.
            "requests": Counter(
                "muffin_ingest_provider_requests_total",
                "Provider requests issued by the ingest worker, by outcome.",
                ["provider", "outcome"],
            ),
            "duration": Histogram(
                "muffin_ingest_provider_request_duration_seconds",
                "Wall time of a provider request, as the worker sees it.",
                ["provider"],
                buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
            ),
        }
    return _CACHE


_CACHE: dict[str, Any] | None = None


@contextmanager
def request(provider: str) -> Iterator[dict[str, str]]:
    """Time one provider request and record how it ended.

    The caller sets `outcome` in the yielded dict; an exception escaping the block records
    `transport` and re-raises, because a call that never answered must never be recorded as one
    that answered nothing.
    """
    if not enabled():
        yield {"outcome": "answered"}
        return

    counters = _counters()
    state = {"outcome": "answered"}
    started = time.monotonic()
    try:
        yield state
    except BaseException:
        state["outcome"] = "transport"
        raise
    finally:
        counters["duration"].labels(provider=provider).observe(time.monotonic() - started)
        counters["requests"].labels(provider=provider, outcome=state["outcome"]).inc()


def start_exporter(port: int = 9102) -> None:
    """Serve the aggregated multiprocess registry. Called once, in the code-location parent.

    IT CLEARS THE DIRECTORY FIRST, and that is a correctness requirement rather than housekeeping:
    `MultiProcessCollector` sums every file it finds, so a restart that inherits the previous
    incarnation's files serves those counts as current. It is also the only thing that bounds the
    directory at all — see the module docstring on why `mark_process_dead` does not.
    """
    if not enabled():
        return
    from prometheus_client import CollectorRegistry, multiprocess, start_http_server

    directory = os.environ[MULTIPROC_ENV]
    os.makedirs(directory, exist_ok=True)
    for stale in os.listdir(directory):
        if stale.endswith(".db"):
            os.unlink(os.path.join(directory, stale))

    registry: CollectorRegistry = CollectorRegistry()
    # `prometheus_client` ships no stubs for the multiprocess helpers. Ignored by CODE
    # rather than blanket-ignored, so a DIFFERENT error here still fails the build.
    multiprocess.MultiProcessCollector(registry)  # type: ignore[no-untyped-call]
    start_http_server(port, registry=registry)


def mark_dead(pid: int | None = None) -> None:
    """Drop a finished process's LIVE GAUGE files. It does nothing for counters — see the module
    docstring; it is kept because it is correct and free, and the day this module grows a gauge
    it is already in the right place."""
    if not enabled():
        return
    from prometheus_client import multiprocess

    multiprocess.mark_process_dead(  # type: ignore[no-untyped-call]
        pid if pid is not None else os.getpid()
    )
