"""Each CNINFO report is read in a child process of its own, under an address-space limit (Phase 6
decision 7): a runaway PDF kills only its child, is counted `unreadable`, and the run goes on.

WHY A CHILD. The edge's worker died on reports it could not hold (a 9.15 MB report took 129 MB of
RSS in a 256 MB isolate; a killed worker stamps nothing, so the same report returned at the head of
the queue for days). Here a run reads tens of reports in the code location's 2.5 GB, beside other
runs, and pdfium is C: a pathological document can allocate past anything Python would notice.

WHY A FRESH INTERPRETER, NOT `multiprocessing`. Every `multiprocessing` start method that does not
fork the run's own process (`spawn`, `forkserver`) starts its children by re-running the PARENT's
`__main__` from its path (`multiprocessing.spawn.prepare`). Under Dagster that is the console script
that launched the run; in a probe it is the probe, which re-entered itself without a main guard.
Forking the run itself is worse: it holds threads and database connections. A plain
`python -m muffin_ingest.facets.cn_child` imports this module and nothing else, measured at 0.21 s
and 24 MB per start in the production image (2026-11-15), against a median parse of ~0.6 s.

THE PROTOCOL. The PDF goes in on stdin; the child sets its own limit first, then answers with one
pickled `("ok", CnParseResult)` or `("error", reason)` on stdout. Anything pdfium or a library
writes to file descriptor 1 is moved to stderr before parsing, so it cannot corrupt the answer. A
child the kernel or its limit kills answers nothing and exits non-zero: `Unreadable`, with the last
line of its stderr.
"""

from __future__ import annotations

import os
import pickle
import resource
import subprocess
import sys
from dataclasses import dataclass

from muffin_ingest.facets.cn_pdf import CnParseResult

#: The child's address-space limit (decision 7).
LIMIT_MB = 768
#: A report taking longer than this is a pathological one: the child is killed.
TIMEOUT_S = 120.0
#: How the child is started. `-P` keeps the working directory off `sys.path`.
_COMMAND = (sys.executable, "-P", "-m", "muffin_ingest.facets.cn_child")


@dataclass(frozen=True)
class Unreadable:
    """The report could not be read: the child raised, died, ran out of memory or of time."""

    reason: str


def read(
    body: bytes,
    *,
    limit_mb: int = LIMIT_MB,
    timeout_s: float = TIMEOUT_S,
    scan_pages: int | None = None,
) -> CnParseResult | Unreadable:
    """One report, read in its own child. Never raises for the report's sake."""
    from muffin_ingest.facets import cn_pdf

    pages = cn_pdf.SCAN_PAGES if scan_pages is None else scan_pages
    try:
        done = subprocess.run(
            [*_COMMAND, str(limit_mb), str(pages)],
            input=body,
            capture_output=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:  # `run` has already killed the child
        return Unreadable(f"took longer than {timeout_s:.0f} s")
    if done.returncode != 0:
        lines = done.stderr.decode("utf-8", "replace").strip().splitlines()
        tail = lines[-1][:200] if lines else "no output"
        return Unreadable(f"the child exited {done.returncode}: {tail}")
    try:
        kind, payload = pickle.loads(done.stdout)
    except Exception:  # an answer cut short: the child died after starting to write
        return Unreadable(f"the child's answer was unreadable ({len(done.stdout)} bytes)")
    if kind == "ok" and isinstance(payload, CnParseResult):
        return payload
    return Unreadable(str(payload))


def _main(argv: list[str]) -> int:
    """The child: limit itself, read the PDF from stdin, answer on stdout."""
    limit_mb, scan_pages = int(argv[1]), int(argv[2])
    limit = limit_mb * 1024 * 1024
    try:
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    except (ValueError, OSError) as exc:
        # macOS refuses an address-space limit below what a process already maps, so a laptop
        # reads without one. Linux, where every run happens, sets it; failing there is refused.
        if sys.platform != "darwin":
            sys.stderr.write(f"cannot limit the child's memory: {exc}\n")
            return 3
    answer = os.fdopen(os.dup(1), "wb")
    os.dup2(2, 1)  # whatever else writes to stdout lands in stderr
    body = sys.stdin.buffer.read()
    payload: tuple[str, object]
    try:
        from muffin_ingest.facets import cn_pdf

        payload = ("ok", cn_pdf.segment_facts_from_pdf(body, scan_pages))
    except MemoryError:
        payload = ("error", f"memory limit of {limit_mb} MB")
    except Exception as exc:  # the report, not the run: reported as unreadable
        payload = ("error", f"{type(exc).__name__}: {exc}"[:300])
    pickle.dump(payload, answer)
    answer.close()
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
