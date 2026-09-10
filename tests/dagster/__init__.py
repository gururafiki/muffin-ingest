"""Tests that need an orchestrator installed.

SEPARATE FROM `tests/unit`, AND THE SEPARATION IS THE POINT. `muffin_ingest` deliberately imports no
Dagster, and the `checks` job proves that by type-checking and running the library WITHOUT one
installed. The moment a test that imports dagster lands in `tests/unit`, that job starts failing for
a reason unrelated to what it asserts — and the tempting fix is to install dagster there, which
would quietly delete the guarantee.

So these live on the other side of the line, beside the code they exercise.
"""
