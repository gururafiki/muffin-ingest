---
name: dagster-ingestion-pipeline
description:
  How to build a data-ingestion family on Dagster in muffin-ingest — the three stages, what is a
  partition and what is data, which layer owns which rule, and the mistakes that have already been
  made so they are not made again. Use when migrating a family from the market-refresh edge function
  (universe, symbols, fundamentals, SEC, regulators, macro, derived) or when adding any new provider.
license: AGPL-3.0
metadata:
  author: muffin
  version: '1.0.0'
---

# Building an ingestion family on Dagster

The price family (Phase 2) is the worked example. It cost **eleven defects**, and not one was found
by review, a type checker or CI — every single one was found by driving the thing and reading a
number. This skill exists so the next family costs fewer.

**The one rule behind all of it:** *a gauge that cannot fail certifies everything.* Most of what
follows is a specific instance of that.

---

## The three stages

```
  1 ACQUIRE                  2 NORMALISE                3 DERIVE / SERVE
  talks to the provider      never talks to a provider  never reads raw
  writes the provider's  ──► reads raw, writes      ──► pure computation over core
  words, unchanged           typed core rows            + the serving views
  pool = the provider        pool = sql                 eager on its upstream
```

**Stage 1 is the only stage allowed a network call.** Re-running stage 2 or 3 after a logic fix must
cost no provider request. That is what makes the transformation rules testable against frozen bytes,
and it is the property the whole design is arranged around.

## What is a partition, and what is data

> **Partition the question the data cannot answer about itself.**

* *"Did the collection run on Tuesday?"* — the table **cannot** answer this. A security with no bar
  looks identical whether its market was shut, its symbol is dead, or nothing ran. **Partition by
  date.**
* *"Does this security have its history?"* — the table **can** answer this (`min(trade_date)`). A
  partition grid for it is a second copy of a fact already stated. **Do not partition by subject
  for that.**

**Materialising a partition is a claim of completeness for that slice.** If a run cannot make that
claim for the whole slice, it must not write the partition — it belongs in a different lane.

### Two lanes, and why

| Lane | Partition | Claim | Idles at |
|---|---|---|---|
| **A — cross-section** | daily, `single_run` backfill | "the whole universe for this window was collected" | never |
| **B — history & repair** | one per subject, dynamic, `single_run` | "this subject is loaded" | **zero** |

They write the **same table on the same key**, which is what makes the overlap harmless. Lane B
cannot be date-partitioned (a new subject would cost a re-fetch of the universe); Lane A cannot be
subject-partitioned (the only question the data cannot answer is lost).

### Partition-key sizing, measured

| | |
|---|---|
| Dagster's documented ceiling | **≤100,000 partitions per asset** |
| date × subject | 10,894 × 7,300 = **80 M** — impossible |
| year × subject | 29 × 10,894 = **316 k** — also over |
| subject only (dynamic) | 10,894 — fits, ~10,894 event-log rows **per pass** |

`BackfillPolicy.single_run()` works for **static and dynamic** partitions, not just time windows —
`get_partition_keys_in_range` is implemented on the base `PartitionsDefinition` by index over the
ordered key list. So batching survives subject partitioning. **A scattered selection fragments**
into one run per contiguous key block, which is fine for a lane that idles at zero and wrong for a
daily one.

---

## Layers and dependencies

```
muffin_ingest/              THE LIBRARY. Imports NO Dagster. CI proves this by type-checking and
                            running it with no dagster installed — that absence IS the guarantee.
  providers/<name>.py       one module per provider: spell(), classify(), batch size, control subject
  providers/openbb.py       the hub, imported in-process; one named typed function per route
  facets/<family>.py        pure parsing + the subject query + normalise(). No network, no Dagster.
  derive/<thing>.py         arithmetic over what we already hold
  writers.py                upsert / replace_scope / dedupe_by
  ledger.py                 claim / record / mark_absent — thin; the rules live in SQL

muffin_ingest_dagster/      THE ORCHESTRATION. Imports the library, never the reverse.
  assets/<family>.py        assets, sensors, schedules
  io_managers.py            ONE per storage class, never one per asset
  resources.py              the database connection

tests/unit/                 the library, NO dagster installed
tests/dagster/              the assets, dagster installed
tests/fixtures/             CAPTURED provider payloads + a re-capture recipe
```

**Put a Dagster-importing test in `tests/unit` and the `checks` job fails for a reason unrelated to
what it asserts** — and the tempting fix (install dagster there) silently deletes the guarantee.

### Dependencies

* **One environment now; the split is one packaging change away.** Because stage 1 hands off through
  Parquet and stage 2 through Postgres, **no asset passes a Python object to another asset across a
  stage boundary** — so any asset can move to a second code location, or behind `dagster-pipes`,
  without changing a dependency edge.
* **Do not add a dependency before a caller exists.** `pyrate-limiter` was declared and imported
  nowhere; `http/client.py` was 136 lines with zero callers; both were deleted unused.
* **Two kinds of openbb extension.** A **provider** supplies data (`openbb-yfinance`); a **router**
  supplies the namespace (`openbb-equity`). With providers alone the hub imports perfectly and every
  call dies on `'App' object has no attribute 'equity'`.

---

## Rate, concurrency and quota

| Bound | Mechanism |
|---|---|
| Concurrency | a Dagster **pool per provider**, `limit: 1` |
| Requests/sec | **in-run pacing** — a minimum interval between calls |
| Requests/day | **schedule × page size** — a 25/day provider is one run a day with a page of 25 |
| "it told us no" | `provider_budget.cooldown_until` |

**Do not build a distributed rate limiter.** A pool at limit 1 means exactly one process is calling
the provider, so a Postgres token bucket arbitrates between runs that cannot overlap. *If a pool is
ever widened past 1, in-run pacing silently becomes N× looser* — note it at the pool, not only at the
code.

**Denominate the budget in the unit the vendor sees.** `openbb_yfinance` calls
`yf.download(tickers="A,B,C", threads=False)` — **one request per symbol, serially**. Batching
collapses *our* call count, not the vendor's. "Batching saves the provider budget" is false.

---

## WHAT NOT TO DO

Each of these happened, in this order, in one phase.

### Do not trust a counter you have not tried to make lie

A run reported `SUCCESS`, wrote a 176-byte file and zero rows, with `empty: 50`. Fifty securities
recorded as *having answered nothing* when we had never asked one of them — openbb could not import
in the image. **`transport` and `empty` are different outcomes.** Never fold "we never got an
answer" into "the provider answered and had nothing"; that conflation is the most expensive one this
codebase has.

### Do not put ingestion state on a fact table

21 `%_missing_at` columns and 8 cursors on `market.security` is what the ledger replaces. A new
facet is a **row**, not a column.

### Do not let a guard check the wrong gate

`every-table-is-reachable` asked `has_table_privilege` — a question about **grants** — and passed
while RLS blocked every write. 83 tables, none with an INSERT policy, and the writer it replaced
held `BYPASSRLS`. **Grants and RLS are two independent gates.**

### Do not write a control table that encodes nothing

`ROUTES` had 26 entries and **all 26 mapped a string to itself**, justified by a REST irregularity
that stopped applying when the hub moved in-process. Before adding a lookup table, check that it
maps something to something else.

### Do not claim a rule is structural when it is a helper

The writers module's docstring said its rules apply "without any caller remembering" — while
`require_currency` was never called by `upsert`. And **wiring it in would have been worse**: it would
have refused a bar for the 425 securities with no known currency. *The rule was real; the enforcement
point was wrong.* `not null` on the column is what cannot be forgotten.

### Do not assume a provider honours the range you asked for

```
start=2026-09-01 end=2026-09-01  ->  7 rows, 2026-09-01..2026-09-10   (degenerate range IGNORED)
start=2026-09-01 end=2026-09-02  ->  2 rows, exactly those two
```

Filter the **answer** to the partition's window, and **count what you dropped**. `outside_window`
non-zero is a statement about the provider. It is also how the degenerate-range bug was found.

### Do not store a bar from a session that is still open

A "close" that is not a close looks exactly like one. The old pipeline's disagreements were
overwhelmingly this: the stored value sits **inside** that session's own high/low. History must end
*before* today for the same reason.

### Do not probe with the display symbol

`ALMARAI.SR` is what the app shows; yfinance wants `2280.SR`. A baseline sample asked with the
display symbol reported **48% "unanswered"** — a fact about the probe, not the data. **A wrong name
is not a missing security**, and a tool that asks with the wrong name manufactures the absence it
went looking for.

### Do not infer a pattern from one instance

One security held the next day's close, and a "systematic date shift" was inferred. Across ~180
sampled pairs there were **zero** shifts. Sample before generalising, and sample the population
where the defect appeared — a weight-ordered sample is mostly large US and European names and is
structurally blind to a defect in Chile or Qatar.

### Do not write an artifact only your own loader can read

An empty partition written as `pa.table({})` is a Parquet file with **zero columns**. DuckDB:
`Need at least one non-root column in the file`. Raw that only its own loader can open is not
inspectable raw.

### Do not believe a fixture that cannot fail

Three tests passed for the wrong reason: two were too short to reach the window they asserted; one
built a series with no zero close while claiming to test the zero-close branch; one could not tell
"the previous bar" from "a one-day lookback" because the fixture made them **agree**. **When a guard
distinguishes between candidate rules, the fixture must make those rules disagree** — and a mutation
harness must report a no-op as loudly as a miss.

### Do not assume static typing will save you

openbb ships `py.typed`, so typed direct calls *look* checkable. The signature is
`(symbol, start_date, end_date, provider, **kwargs)` — **`**kwargs` swallows anything**, so a
misspelling binds happily and silently uses the default. mypy cannot see it; nor can
`signature().bind()`; and mypy 2.3.1 crashes on openbb's generated package anyway. What catches it
is **behavioural**: the window filter's count moves.

---

## The checklist for a new family

1. **Design doc first** — the model has to be right the first time, because it lands in one
   migration. A correction mid-build-out costs a deploy.
2. **Capture real provider payloads** before writing a parser. Include one subject the provider has
   *nothing* for, and one whose response has a different shape (a single-item request often does).
3. **Library first, assets second.** Parsing and rules in `muffin_ingest`, tested with no Dagster.
4. **Ship the schema additively**, beside what it replaces. Nothing reads it yet.
5. **Drive one bounded run against production** and read every counter. This is where the defects
   are.
6. **Parity with a tolerance, not equality** — float32 makes `1010.26000976562` and
   `1010.260009765625` the same number. The gate is *every disagreement explained*, not "identical",
   because the old pipeline may be the wrong one.
7. **Adjudicate disagreements against the provider.** It is the only thing that settles one.
8. **Cut over behind a compatibility view**, so the UI moves on its own PR.

## Shipping

**Code ships by image roll, config and schema ship by deploy.** `maintenance.yml roll-ingest` takes
~1 minute against a ~10-minute deploy. It is safe only because `image_ingest` is a moving tag
everywhere it is set — pin it to a sha and the roll becomes something a deploy undoes.

**A roll kills in-flight runs** (a run is a subprocess of the code location), which is why it is
deliberate rather than automatic.
