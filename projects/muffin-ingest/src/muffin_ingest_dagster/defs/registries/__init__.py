"""The two whole-file registries: SEC's ticker->CIK map and NSE's equity list.

THE SIMPLEST SHAPE IN THE STANDARD, AND THE ONE THAT DELETES THE MOST. Both are backlog-driven
resources in the edge function purely because of the 90-second worker — `sec-cik-map` reached
6,645 of ~27,000 rows and *restarted from zero every run*, which is why it was rewritten as a
whole-file apply there and why `in-symbols` was built the same way beside it. Neither is
incremental by nature: the file IS the answer, there is nothing to page and nothing to resume.

Under the partition decision table that is the "one whole file" row: **no partition at all**, a
schedule, and a freshness policy carrying everything the backlog used to. No `pending_*` view, no
cursor, no negative cache, no `remaining` to report — because none of those questions exists.

WHY THESE WRITE THROUGH AN RPC RATHER THAN `postgres_io`, which is a deliberate exception to
"assets return records, they do not open connections". The Postgres I/O manager exists to make
`dedupe_by`, chunking and `require_currency` unavoidable for ROW writes. Neither of these is a row
write: each hands a whole map to a function that resolves identity across two sources, applies a
PRECEDENCE LADDER, and REFUSES AN AMBIGUOUS MATCH rather than breaking the tie. That logic is
exactly what rule 8 says must live in SQL — a wrong CIK is far worse than no CIK, because it makes
every downstream number look populated and fiction — so the asset calls it and reports what it
returned.
"""
