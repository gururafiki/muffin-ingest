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

## Dependencies

- **Two kinds of openbb extension:** a *provider* supplies data (`openbb-yfinance`), a *router* supplies
  the namespace (`openbb-equity`). With providers alone the hub imports and every call dies on
  `'App' object has no attribute 'equity'`.
- **No dependency before a caller:** `pyrate-limiter` was declared and imported nowhere; a 136-line HTTP
  client had zero callers. Both were deleted.

## Shipping

- A roll restarts the code location and **kills in-flight runs** (runs are its subprocesses) — roll
  deliberately.
- The roll is safe because `image_ingest` is the moving `:latest` tag everywhere it is set; pin it to a
  sha and the next deploy silently undoes a roll.
