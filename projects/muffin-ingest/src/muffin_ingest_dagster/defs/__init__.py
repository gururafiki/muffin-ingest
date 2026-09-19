"""Everything Dagster serves, autoloaded: `platform/` plus one package per family.

A family package is ordered by stage and the imports only ever point down:
`partitions.py` (partitions definitions and shared constants) <- `raw.py` (stage 1, pool = the
provider) <- `core.py` (stage 2, pool = sql) <- `derived.py` (stage 3) <- `checks.py` and
`automation.py` (jobs, schedules, sensors). Importing an asset into a later module is safe: the
loader de-duplicates the same object seen through several modules.

NO `from __future__ import annotations` ANYWHERE UNDER THIS PACKAGE. It stringifies every
annotation, and Dagster resolves the `context` parameter by comparing the actual CLASS — so
validation fails with "Cannot annotate `context` parameter with type AssetExecutionContext" while
the annotation plainly IS `AssetExecutionContext`. The message names the parameter, never the
cause, and qualifying or unqualifying the name changes nothing because both are strings by then.

Do not name a module here `definitions.py` or `component.py`: those are keyword files that
terminate autoloading for the folder they sit in.
"""
