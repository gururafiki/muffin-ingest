# Pitfalls this pipeline already paid for

The price family (Phase 2) cost **eleven defects**, and not one was found by review, a type checker
or CI — each was found by driving the thing and reading a number. The rule behind all of them: *a
gauge that cannot fail certifies everything.*

## Lanes and partition sizing

| Lane | Partition | Claim | Idles at |
|---|---|---|---|
| **A — cross-section** | daily, `single_run` backfill | "the whole universe for this window was collected" | never |
| **B — history and repair** | one per subject, dynamic | "this subject is loaded" | **zero** |

Both write the same table on the same key, which is what makes the overlap harmless. Lane B cannot be
date-partitioned (a new subject would cost a re-fetch of the universe); Lane A cannot be
subject-partitioned (the one question the data cannot answer would be lost).

| | |
|---|---|
| Dagster's documented ceiling | ≤ 100,000 partitions per asset |
| date × subject | 10,894 × 7,300 = **80 M** — impossible |
| year × subject | 29 × 10,894 = **316 k** — also over |
| subject only (dynamic) | 10,894 — fits; ~10,894 event-log rows per pass |

`get_partition_keys_in_range` works by index for static and dynamic partitions too, so batching
survives subject partitioning. A scattered selection fragments into one run per contiguous key block
— fine for a lane that idles at zero, wrong for a daily one.

## Do not trust a counter you have not tried to make lie

A run reported `SUCCESS`, wrote a 176-byte file and zero rows, with `empty: 50` — fifty securities
recorded as having answered nothing when not one had been asked, because openbb could not import in
the image. **`transport` and `empty` are different outcomes.** Folding "no answer" into "answered
with nothing" is the most expensive confusion this codebase has.

**Sum the outcome counters against `subjects`; a gap is a branch that forgot to count.** The 09-16
price partition reported `answered=5974 empty=586 throttled=1 unasked=0` of 12,017. The throttle
branch broke out of the loop without counting the rest, so 5,437 securities were never asked and the
"every subject was asked" check passed (fixed in #40). Assert the sum in a test that stops mid-run.

## Do not let a guard check the wrong gate

`every-table-is-reachable` asked `has_table_privilege` — a question about grants — and passed while
RLS blocked every write: 83 tables, none with an INSERT policy, because the writer being replaced held
`BYPASSRLS`. **Grants and RLS are independent gates.**

## Do not write a control table that encodes nothing

`ROUTES` had 26 entries and all 26 mapped a string to itself. Before adding a lookup, check that it
maps something to something else.

## Do not claim a rule is structural when it is a helper

The writers' docstring said its rules applied "without any caller remembering" while
`require_currency` was never called by `upsert` — and wiring it in would have refused bars for 425
securities with no known currency. The rule was real; the enforcement point was wrong. A `not null`
on the column is what cannot be forgotten.

## Do not assume a provider honours the range you asked for

```
start=2026-09-01 end=2026-09-01  ->  7 rows, 2026-09-01..2026-09-10   (degenerate range IGNORED)
start=2026-09-01 end=2026-09-02  ->  2 rows, exactly those two
```

Window the answer in stage 2 and **count what you dropped**: a non-zero `outside_window` is a
statement about the provider, and is how the degenerate range was found.

## Do not store a bar from a session that is still open

A "close" that is not a close looks exactly like one; the old pipeline's disagreements were
overwhelmingly this (the stored value sits *inside* that session's own high and low). History must end
before today for the same reason.

**A session that has ended can still have no close.** At 00:00:34 UTC, four hours after the US close,
yfinance returned 09-16 for 60 of 61 proxies with open/high/low/volume and **`close: NaN`**. NaN is a
`float`, so a type check admits it. Postgres cannot backstop it: `numeric` stores `'NaN'` and sorts
it above every number, so `CHECK (close > 0)` passes. Read every close through the one
finite-and-positive rule, `prices.close_of`, and count the refusals (`null_closes`).

## Do not window the flattened run — publish each partition from its own file

Keeping the provider's whole answer means one partition's file can hold a row belonging to another.
Windowing the run's flattened input is wrong three ways:

- **prices:** the stray bar and the real one sit in different files and the writer dedupes last-wins,
  so file order picks the winner — right only by accident of sorting;
- **fx:** one chart body covers a range and is filed under every day it covers, so a flattened window
  publishes each rate once per copy;
- **indices:** every daily run stores its 1,900-day lookback, so a range re-parse fed each bar in once
  per file — a day beside its own duplicate, in a series read positionally.

Keep input per partition (`partitioned.rows_per_partition`), window each file to its own day, and
where files legitimately overlap keep the newest file's row.

## Do not probe with the display symbol

`ALMARAI.SR` is what the app shows; yfinance wants `2280.SR`. A sample asked with display symbols read
**48% "unanswered"** — a fact about the probe. A wrong name is not a missing security.

## Do not infer a pattern from one instance

One security held the next day's close and a "systematic date shift" was inferred; across ~180
sampled pairs there were **zero** shifts. Sample the population where the defect appeared — a
weight-ordered sample is mostly large US and European names and cannot see Chile or Qatar.

## Do not assume `single_run` works — the write fails, then the load does

`BackfillPolicy.single_run()` hands the asset every partition at once and **`UPathIOManager` refuses a
multi-partition output**. The asset ran, paid the provider for 96 securities' history, and died at the
write. The fix for the write is half the fix: `load_input` hands a downstream step covering several
partitions a `{partition_key: obj}` mapping, and the next backfill died at the input type check —
with the provider paid a second time. Fix **both seams** together (`by_partition` out,
`loaded_rows` / `rows_per_partition` in, the input annotated `Any`). It survived the first fix because
no test had ever materialized stage 2.

Three adjacent traps, each naming neither cause nor fix:

- `has_asset_partitions` is an **OutputContext** attribute; an asset context has `has_partition_key`
  and `has_partition_key_range`.
- Dagster derives a type from the return annotation and refuses a union — a shape that depends on the
  run is annotated `Any`.
- `dg.materialize` has no `asset_selection`; a partition range goes through the
  `dagster/asset_partition_range_{start,end}` tags.

## `single_run` is right for a date partition and wrong for a subject partition

The question is whether anything batches *across* the partitions in a run. A date partition holds many
subjects — one run is one batched sweep. A subject partition holds one, and openbb's yfinance adapter
asks the vendor once per symbol regardless, so `single_run` saves nothing and costs unbounded memory:
`load_input` is **eager**, holding every partition's raw rows and their normalised copies at once.
Measured: 96 securities = **683,391 bars**, OOM-killed at **2.4 GB** in a 2.5 GB container; 25 ≈ 178k
rows ≈ 600 MB. Run width is the memory budget — `multi_run(N)`, N by measurement. Pin the asymmetry in
a test, because tidying the four near-identical assets reintroduces the OOM.

## Do not build one statement per write — 65,535 bind parameters is a protocol ceiling

A multi-VALUES insert spends one parameter per column per row, so the ceiling is a row count that
moves with table width; psycopg refuses with `number of parameters must be between 0 and 65535`, after
everything upstream was paid for. Chunk inside the shared writer, never at a call site. **Dedupe the
whole set first, then chunk** — the other order stores the same final value, so assert
`rows sent == rows written`, with a fixture that puts the repeat past a chunk boundary.

## Do not let a derived asset read the wall clock except for staleness

- **Window anchor:** a return windowed from `date.today()` but valued at the last bar gives different
  numbers on different days. Anchor on the last bar.
- **`as_of`:** stamping the run's date claims currency the inputs may not have. Stamp the last input
  used.

The clock has one job — asking whether a series is still updating. Both defects were invisible to
fixtures whose series ended today; make the series end well before `now`.

## Do not read a provider's timestamp in UTC, or assume the last point is a bar

- **A bar is dated in its exchange's timezone.** FX daily bars are stamped at the session *open* in
  `Europe/London`, so 2026-09-10 arrives as `2026-09-09T23:00Z`; a UTC `.date()` dated every bar a day
  early and the window discarded all of them (`outside_window=190`) while reporting success. Read
  `gmtoffset` from the response.
- **The last point is often a live quote**, identifiable exactly: its timestamp equals
  `regularMarketTime`. Key the rule on the stamp, never the date — a captured EURUSD body holds
  Friday's last tick (1.16009) beside that day's completed bar (1.16099), both dated 2026-09-11.

Observe both at parse, refuse in stage 2, count them (`live_points`, `null_closes`), and capture the
whole body — including `meta` — as the fixture.

## A date must come from the data — four disguises

1. The clock (`as_of = date.today()`).
2. The window's anchor (window from `now`, value from the last bar).
3. A partition key for a source with no dates (a snapshot stamped with the partition being run).
4. The **top** of a lookback series (asking up to the partition's end returns today's in-progress bar).

Stamp from the last input used, anchor windows on the data, cut a series at both ends of the window,
and where a source has no date, record when it was **read** in the raw artifact.

Disguise 1 is still live: the finviz sector snapshot stores `taken = date.today()` as `as_of`, so the
00:00 UTC run files the US session under the next day (open:
`docs/deferred/2026-09-17-sector-snapshot-dated-by-the-clock.md`).

## A source that cannot be asked about a past day must not be date-partitioned

finviz answers "as of now" with no date. The obvious guard — refuse a partition whose window has
closed — can never collect anything: with `end_offset` 0 the newest materializable partition is always
yesterday. Leave the snapshot unpartitioned; the materialization event records when it was taken.

## "Is this done?" is a question for Dagster, not for the data it produced

A resumable loader asking "does the output reach back far enough" stalls on every subject the provider
has nothing for: two rounds rewrote the same 139,7xx rows while progress sat still. Materialized means
"we asked and stored whatever came back, including nothing" —
`instance.get_materialized_partitions(asset_key)`. Count partitions materialized, not rows that look
right. **Caveat:** runs older than 90 days are pruned with their events — see
[dagster-native.md](dagster-native.md#dagster-state-has-a-retention-period).

**Partition metadata from a `single_run` range is the run's, not the partition's.** Every partition of
a four-day FX retry read `rows=82`, including a Sunday with no rates. Find empty days by counting the
table per date.

## Do not map a many-to-one relation with a dict comprehension

`{symbol: code for code, symbol in scopes}` keeps the last code per symbol: 62 scopes over 53 symbols
(`EEM` and `IVV` back three each), nine scopes silently got nothing, and the only trace was
`answered=52` beside `empty=0`. Make counters count the thing you care about (scopes, not symbols).

## Do not write an artifact only your own loader can read

An empty partition written as `pa.table({})` has zero columns; DuckDB refuses it
(`Need at least one non-root column in the file`).

## Do not believe a fixture that cannot fail

Tests passed for the wrong reason: too short to reach the window they asserted, no zero close while
testing the zero-close branch, "the previous bar" indistinguishable from "a one-day lookback". When a
guard distinguishes candidate rules, the fixture must make them disagree — and a mutation harness must
report a no-op as loudly as a miss.

## Do not assume static typing will save you

openbb routes are `(symbol, start_date, end_date, provider, **kwargs)`: a misspelled keyword binds and
silently uses the default. mypy cannot see it (and crashes on openbb's generated package); what catches
it is behavioural — the window filter's count moves.

## Do not enumerate a growing table with an aggregate

`security_return` listed "securities with bars" as a `group by` over `market.price_bar` — O(rows), and
the per-security history lane adds rows every night. It answered inside `ingest_rw`'s 120 s
`statement_timeout` through 2026-09-20 and was cancelled at 126 s two nights running before a single
return was computed. **The semi-join is not the fix**: `where exists (...)` is flattened by the
planner into a Parallel Hash Semi Join over every partition, measured at a 110 s bound. A
`LATERAL (... LIMIT 1)` cannot be flattened, so it stays one index probe per entity — and bounding it
by the window the caller READS prunes the partitions it never reads: 11,760 securities in 26 s, flat
in history depth. The window must be the read's own value, passed once to both queries, because then
leaving out an entity with no bar in it changes the pages and nothing else — proven on production
over a sixteenth of the universe, 733 securities in the same positions, 0 differing.

## Do not call `add_output_metadata` from code that runs once per partition

It may be called ONCE per output. An I/O manager's per-partition writer that calls it works for a
single partition and raises `DagsterInvalidMetadata: Tried to add metadata for key(s) that already
have metadata` on the SECOND partition of a range — which is how all 66 bounded runs of
`nightly_prices` failed from 2026-09-19 to 09-22 while every single-partition test passed. Tally
per partition and emit once at the end; Dagster replicates the one dict onto every partition's
materialization, so per-partition metadata is not expressible this way at all. Verified live: 67 of
67 runs failed on 09-22, 100 of 100 succeeded on 09-23.

## Do not let two code paths write rows for one key

`dedupe_by` keeps the LAST row per conflict key, so when two writers emit a row for the same key,
the ORDER of the appends decides which one lands — and nothing reports that a decision was made.
`security_symbology` appended the local rung's `symbol/hit` and then `plan_symbols`' own
`symbol/miss` for the same `(security_id, scheme, provider)`; `plan_symbols` could not see the local
rung, so the miss came last and won. After the first full drain: 0 symbol hits, 759 misses on
securities that had just been given a symbol. Put the decision in ONE function that sees every
source, and test it with a fixture where the sources DISAGREE — while they agree, both orders pass.

## Do not derive a rotation's position from the size of what it rotates over

`start = (day * SWEEP_SLICE) % len(keys)` is stateless and advances one slice a night — while the
grid never changes size. Adding ONE key re-maps every future slice, because `day * 2500 mod N` and
`day * 2500 mod (N+1)` are unrelated. Measured 2026-09-25: the grid went from 12,267 to 12,268 keys
and that night swept [2300, 4800), all of it swept on the two nights before, while 6,056 securities
sat 8-14 days stale. Anchor on a KEY instead: dynamic partitions are listed in insertion order, so
growth appends and never moves one. `nightly_prices` stamps the night's last key on every run it
launches and the next tick resumes after it, excluding its own night so a retried tick is
idempotent. **Its tests needed a growing grid to mean anything**: over a constant grid the date rule
lands on exactly the same keys, and two guards passed with their rule deleted until the fixtures
grew between nights.

## Do not leave queue order to whichever schedule ticked first

At a shared tick every queued run waits in creation order, so a 20-second lane can sit behind a
hundred runs of a long one that happened to be created a moment earlier: `daily_indices` waited
6,185 s on 2026-09-23 and `daily_fx` 6,069 s on 09-24, and on 09-25 neither waited. Give short lanes
`dagster/priority` through the job's `run_tags`; `QueuedRunCoordinator` sorts by it before checking
pools, so the short lane is first in line when the pool frees and waits at most one long run.

## Dependencies

- **Two kinds of openbb extension:** a *provider* supplies data (`openbb-yfinance`), a *router* supplies
  the namespace (`openbb-equity`). With providers alone the hub imports and every call dies on
  `'App' object has no attribute 'equity'`.
- **No dependency before a caller:** `pyrate-limiter` was declared and imported nowhere; a 136-line HTTP
  client had zero callers. Both were deleted.

## Shipping

- A roll restarts the code location and **kills in-flight runs**: `DefaultRunLauncher` starts each
  run as a `multiprocessing` child of the code server (`dagster/_grpc/server.py`, `StartRun`), and
  `run_monitoring` is off. Wait for long runs, and look for runs left `STARTED` afterwards.
- **Verify the first scheduled night, not only the backfill.** The 09-16 recovery backfills ran at
  noon over settled sessions at ~14 calls/min, and all passed. The first night at 00:00 UTC succeeded
  on every run and published half a price day (throttled at ~42 calls/min), 1 of 61 index scopes
  (NaN closes) and no FX (a stale cache body). Read the counters of the first scheduled run.
- **A backfill range must cover every missing partition.** One day left outside it (09-11) kept
  `security_return`'s `eager()` blocked; list missing partitions from Dagster before choosing the
  range.
- The roll is safe because `image_ingest` is the moving `:latest` tag everywhere it is set; pin it to a
  sha and the next deploy silently undoes a roll.
