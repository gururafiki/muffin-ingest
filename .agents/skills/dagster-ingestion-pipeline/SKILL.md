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

### Do not assume `single_run` works — the WRITE is where it fails

`BackfillPolicy.single_run()` hands the asset every partition at once, and **`UPathIOManager`
refuses a multi-partition output outright**. It had never worked on either lane, and nothing before
the write could see it: the asset ran, fetched 96 securities' full history, **paid the provider for
all of it**, and died afterwards.

**Override `handle_output`** so a run covering several partitions returns a mapping of partition key
to rows, one file each.

*(An earlier version of this entry went on to reject a multi-run policy because it "turns ten calls
into ninety-six". That is false for this provider and the correction is the next rule.)*

**AND THE FIX FOR THE WRITE IS HALF THE FIX.** `UPathIOManager.load_input` hands a DOWNSTREAM
step covering several partitions a `{partition_key: obj}` mapping too — so the next backfill got one
stage further and died on the input type check:

    Type check failed for step input "raw_price_history" - expected type "[Dict[String,Any]]"

with the provider paid a second time and 96 raw files already on disk. **A backfill policy that
hands an asset every partition at once changes the shape at EVERY seam in the lane**, not only at
the one that failed first. Fix both ends together: a `_by_partition` on the way out and a
`_loaded_rows` on the way in, with the input annotated `Any` so Dagster's check does not reject the
mapping. The reason this survived the first fix is worth naming — the write had a test and the read
did not, because **no test had ever materialised stage 2 at all**. A test that drives only the
stage that broke cannot see the stage after it.

Three adjacent traps, each naming neither cause nor fix:

* `has_asset_partitions` is an **OutputContext** attribute. An asset context has `has_partition_key`
  and `has_partition_key_range`; reaching for the wrong one is a bare `AttributeError` inside the op.
* Dagster derives a DagsterType from the return annotation and **refuses a union** — an asset whose
  shape depends on the run returns `Any`.
* `dg.materialize` has no `asset_selection`; a partition range goes through the
  `dagster/asset_partition_range_{start,end}` tags.

### `single_run` is right for a DATE partition and wrong for a SUBJECT partition

The deciding question is **whether there is anything to batch ACROSS the partitions in a run**.

* A **date** partition holds many subjects. One run is one batched sweep, and `single_run` is what
  makes a week-long gap cost one run instead of seven. Keep it.
* A **subject** partition holds one subject. `openbb_yfinance` calls `yf.download(...,
  threads=False)`, so the vendor is asked **once per symbol** however many partitions the run
  covers — joining symbols collapses OUR call count, never theirs. So `single_run` buys no provider
  saving at all here, and costs an unbounded memory footprint.

That footprint is not a tuning problem: **`UPathIOManager.load_input` is EAGER**, so a clean stage
covering N partitions holds every one of their raw rows AND the normalised copies at the same time.
No arrangement of that step makes the peak independent of the run's width. **The width IS the
memory budget** — `BackfillPolicy.multi_run(N)`, with N chosen by measurement.

Measured here, and the numbers are worth carrying: 96 securities is **683,391 raw bars** (~7,119
each) and the child process was **OOM-killed at 2.4 GB** against a 2.5 GB container. 25 is ~178k
rows and ~600 MB. So the full 10,894-security load is **~436 runs, not one** — and the extra run
overhead buys the only property that lets it complete at all.

Pin the asymmetry in a test. Four assets sit in one file with three words different between them,
and the tidying instinct runs toward making them consistent — which reintroduces the OOM.

### Do not build one statement per write — 65,535 bind parameters is a PROTOCOL ceiling

Postgres sends the parameter count as an int16, so a single statement carries at most 65,535 bind
parameters. A multi-VALUES insert spends one per column per row, so the ceiling is a ROW count that
moves with how wide the table is — which is why a daily lane never meets it and a history backfill
does immediately. psycopg refuses the whole statement with

    number of parameters must be between 0 and 65535

naming neither the table nor the row count, after everything upstream has already been paid for.

**Chunk inside the shared writer, never at a call site** — every facet writes through it, and a rule
written at one call site is not a rule. **Dedupe the whole set FIRST, then chunk**: split first and
one conflict key survives in two chunks, the second statement silently overwrites the first via
`do update`, and the final stored value is the SAME — so a test asserting on the value certifies
both rules. The difference is in how many rows were SENT and in the collapse count going to zero,
so assert `rows sent == rows written`. And the fixture has to put the repeat past a chunk boundary,
or every candidate rule agrees and the mutation passes clean.

### Do not let a derived asset read the wall clock for anything but staleness

A derived asset re-run over unchanged inputs must produce an unchanged number. Two clock reads break
that, and both were live in the price family until a parity gate found them:

* **The window anchor.** A return takes its VALUE from the last bar and used to take its WINDOW from
  `date.today()` — so a 3-month return over a series ending Friday started three months before
  Friday on Friday and three months before Wednesday on Wednesday, silently dropping or including a
  bar at the far end. Anchor the window on the last bar.
* **The `as_of` label.** Stamping the run's date claims a figure is current when its newest input
  may be days old. Stamp the date of the last input actually used.

Wall-clock has exactly one legitimate job here: asking whether the series is still being updated.
Nothing else can answer that — a series judged only against its own last bar calls a dead listing
current for ever. Keep that one use and pass it separately.

**Both were invisible to every existing test**, because a fixture whose series ends today cannot
tell "anchored on the clock" from "anchored on the last bar". Make the series end well before `now`
and the rules disagree.

### Do not read a provider's timestamp in UTC, and do not assume the last point is a bar

Two facts that live in a chart response's `meta` block and nowhere else, both of which cost a
production run:

* **A bar is dated in its EXCHANGE's timezone.** An FX daily bar is stamped at the session's *open*
  in `exchangeTimezoneName` — `Europe/London` — so the 2026-09-10 session arrives as
  `2026-09-09T23:00Z`. A UTC `.date()` dates every bar a day early, and a partition filtering to
  its own window then discards the lot: `outside_window=190`, 38 currencies × 5 points, **writing
  nothing while reporting success**. Read `gmtoffset` from the response; never assume a venue.
* **The last point is often a LIVE QUOTE, not a completed bar**, and it is exactly identifiable:
  its timestamp *equals* `regularMarketTime`. Storing it publishes a mid-session price wearing a
  close's clothes — the defect that makes the resource being replaced disagree with this one.
  `GELUSD=X` is the extreme: its only point is the live quote, so the provider has no completed bar
  for the lari at all, which is what the negative cache needs to hear.

Count both drops. `live_dropped` non-zero is normal during a session and zero after it closes;
`nulls_dropped` says the provider is padding. **Capture the `meta` with the fixture** — the first
capture here omitted it, and that omission is what let both defects ship.

### A DATE MUST COME FROM THE DATA — four times in one family, in four different disguises

Every one of these produced a plausible number with a wrong date, and none was visible to a test:

1. **The clock.** `as_of = date.today()` on a derived asset claims a figure is current when its
   newest input may be days old.
2. **The window's anchor.** Measuring the window from `now` while measuring the value from the last
   bar makes the same bars give different numbers on different days.
3. **A partition key, for a source that has no dates at all.** A snapshot provider answers "as of
   now"; stamping its answer with the partition being materialised misdates it by however long ago
   that partition was.
4. **The TOP of a lookback series.** A lane asking for 1,900 days *up to the partition's end*
   brings back today's in-progress bar, and the newest close is then a mid-session price. The first
   three were all at the BOTTOM of a window, which is exactly why this one was not looked for.

The rule that covers all four: **the date travels with the data.** Stamp from the last input
actually used; anchor windows on the data; cut a series at BOTH ends of the partition's window; and
where a source genuinely has no date, record when it was READ, in the raw artifact, so nothing
downstream has to invent one.

### A source that cannot be asked about a past day must not be date-partitioned

finviz answers "as of now" and carries no date — it cannot be backfilled. A daily partition there
claims something the source cannot support.

**And the obvious guard is worse than the defect.** Refusing a partition whose window has closed
looks right and can never collect anything: with `end_offset` at 0 the newest *materialisable*
partition is always yesterday, so today is always outside it. It would have run for ever, collecting
nothing, reporting success. Only working out what it would do in production caught it — the test
used a deliberately-closed window and passed for the same reason the real thing would have failed.

There is exactly one current snapshot, and the materialisation event is already the record of when
it was taken. Leave it unpartitioned; let its consumers stay partitioned, because their inputs
genuinely do have dates.

### "Is this done?" is a question for Dagster, not for the data it produced

A resumable loader has to ask what is still outstanding. The tempting predicate is a property of the
OUTPUT — "this security's history reaches back before go-live" — and it has a head-of-line stall
built in: **a subject the provider has nothing for never acquires that property**, so its window
stays outstanding for ever and every round re-fetches everything beside it.

Measured, in the driver written to avoid exactly this: two consecutive rounds wrote 139,732 and
139,708 rows — the same securities twice — while the progress counter sat at 120 and
`outstanding_windows` never moved. Six occurrences of this shape are already recorded in CLAUDE.md
and it still got written.

`instance.get_materialized_partitions(asset_key)` is the honest question. **Materialised means "we
asked and stored whatever came back, including nothing"** — which is a fact about our work, where
depth is a fact about the provider's coverage. A subject with no history leaves a materialised,
empty partition, and the window moves on.

The same distinction decides the progress counter a stall detector reads: count partitions
materialised, not rows that look right, or a healthy round over a thin part of the universe reads as
a stall.

### Do not map a many-to-one relation with a dict comprehension

`{symbol: code for code, symbol in scopes}` keeps the LAST code per symbol. Measured: 62 index
scopes over **53 distinct symbols** — `EEM` backs three of them, `IVV` backs three including
`country:US` — so nine scopes silently got no data. The run said `answered=52` beside `empty=0`,
two numbers that cannot both be right, and that was the only trace.

It is usually not a modelling error. Those scopes genuinely *are* the same index; the relation is
many-to-one and has to be stored as one. **Make the counters count the thing you care about** — here
`answered` counting scopes rather than symbols is what stops the two figures from disagreeing.

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
