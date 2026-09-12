"""The worker's provider counters, and the three things about multiprocess mode that bite.

Driven across REAL child processes rather than by calling the counter twice in one interpreter,
because the process model is the entire difficulty: a Dagster run is a subprocess, so a counter
incremented during a run lives in a child that exits moments later. A single-process test would
pass against an in-memory registry that reports zero for ever in production.
"""

from __future__ import annotations

import multiprocessing
import os
import urllib.request
from pathlib import Path

import pytest

from muffin_ingest import metrics


def _child(ok: bool) -> None:
    try:
        with metrics.request("yfinance") as outcome:
            if not ok:
                raise RuntimeError("transport")
            outcome["outcome"] = "answered"
    except RuntimeError:
        pass


def test_the_counters_survive_the_process_that_incremented_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A RUN IS A SUBPROCESS, which is why multiprocess mode is not optional.

    And the labels keep the outcomes apart: "a request that failed and a request that answered
    nothing are different facts" is the most repeated rule in this codebase, and a bare request
    counter cannot express it — conflating them is how ~8,300 securities were negative-cached in
    one afternoon with every count looking healthy.
    """
    monkeypatch.setenv(metrics.MULTIPROC_ENV, str(tmp_path))
    metrics.start_exporter(19201)

    for ok in (True, True, False):
        proc = multiprocessing.Process(target=_child, args=(ok,))
        proc.start()
        proc.join()

    body = urllib.request.urlopen("http://127.0.0.1:19201/metrics").read().decode()
    lines = {
        line.split(" ")[0]: float(line.split(" ")[1])
        for line in body.splitlines()
        if line.startswith("muffin_ingest_provider_requests_total{")
    }
    stem = "muffin_ingest_provider_requests_total"
    assert lines.get(f'{stem}{{outcome="answered",provider="yfinance"}}') == 2.0
    assert lines.get(f'{stem}{{outcome="transport",provider="yfinance"}}') == 1.0, (
        "an exception escaping the block must record `transport` — a call that never answered is "
        "not a call that answered nothing"
    )


def test_the_exporter_clears_the_directory_because_stale_files_are_reported_as_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """NOT HOUSEKEEPING — A CORRECTNESS REQUIREMENT.

    `MultiProcessCollector` sums every file it finds, so a code location that came back after a
    crash would serve the previous incarnation's counters as if they were live. Clearing at
    startup is also the only thing that bounds the directory at all, since `mark_process_dead`
    does not (see below).
    """
    monkeypatch.setenv(metrics.MULTIPROC_ENV, str(tmp_path))
    stale = tmp_path / "counter_999999.db"
    stale.write_bytes(b"not really a metric file")

    metrics.start_exporter(19202)

    assert not stale.exists(), (
        "a file from a previous incarnation survived startup; its counts would be summed into "
        "this one's and served as current"
    )


def test_mark_process_dead_does_not_remove_counter_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PINNED BECAUSE THE PLAN FOR THIS EXPORTER SAID THE OPPOSITE.

    `prometheus.yml`'s parked scrape job records that the exporter "needs `mark_process_dead` on
    run exit or the per-PID files grow without bound". It does not: read from the installed
    package, it globs `gauge_{mode}_{pid}.db` and nothing else, because a counter's value must
    outlive the process that produced it or every total would drop when a run ended.

    So the files DO accumulate one pair per run within an uptime, and clearing at startup is what
    bounds them. Asserting the real behaviour here means the docstring above cannot quietly drift
    back to the comfortable version.
    """
    monkeypatch.setenv(metrics.MULTIPROC_ENV, str(tmp_path))
    pid = 424242
    counter = tmp_path / f"counter_{pid}.db"
    counter.write_bytes(b"")
    gauge = tmp_path / f"gauge_livesum_{pid}.db"
    gauge.write_bytes(b"")

    metrics.mark_dead(pid)

    assert counter.exists(), (
        "if this ever starts passing, prometheus_client changed and the module docstring — and "
        "the reason the directory is cleared at startup — need re-deriving"
    )
    assert not gauge.exists(), "it does remove live gauge files, which is what it is for"


def test_without_the_env_var_it_is_inert(monkeypatch: pytest.MonkeyPatch) -> None:
    """The unit suite and `dagster definitions validate` run with no metrics directory, and must
    not need one to import a facet or call a provider."""
    monkeypatch.delenv(metrics.MULTIPROC_ENV, raising=False)
    assert metrics.enabled() is False
    with metrics.request("yfinance") as outcome:
        outcome["outcome"] = "answered"
    metrics.mark_dead()
    metrics.start_exporter(19203)  # binds nothing


def test_the_directory_the_stack_sets_is_the_one_the_exporter_reads() -> None:
    """`PROMETHEUS_MULTIPROC_DIR` has been set on the code-location service since it was created
    and read by nothing — the same shape as `HTTP_CACHE_URL` before the FX lane needed it."""
    assert metrics.MULTIPROC_ENV == "PROMETHEUS_MULTIPROC_DIR"
    assert os.environ.get(metrics.MULTIPROC_ENV) is None or metrics.enabled()
