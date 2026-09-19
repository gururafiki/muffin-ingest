"""Helpers shared by the code location's tests."""

from functools import cache
from pathlib import Path

import dagster as dg

#: The one copy of the captured provider payloads, which belong to the library that parses them.
FIXTURES = Path(__file__).resolve().parents[3] / "libs" / "muffin-ingest-lib" / "tests" / "fixtures"


@cache
def loaded_defs() -> dg.Definitions:
    """Everything `defs/` autoloads, built once per process.

    `@dg.definitions` makes the module attribute a callable rather than a `Definitions`, and each
    call re-walks the folder — so tests that ask several questions of the location share one load.
    """
    from muffin_ingest_dagster.definitions import defs

    return defs()
