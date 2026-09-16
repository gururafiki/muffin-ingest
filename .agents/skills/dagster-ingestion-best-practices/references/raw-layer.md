# Stage 1 — raw

**Raw is what the provider sent, kept whole.** A field nobody reads today must still be on disk
tomorrow, or adopting it costs a re-fetch of everything held — the one cost the stage split exists
to remove. Every narrowing belongs to stage 2.

## Allowed and forbidden

| Allowed | Forbidden |
|---|---|
| The response body byte for byte (`bytes` → Parquet `binary`) | Dropping rows or fields, "bad" ones included |
| A library's parsed rows stored whole (`model_dump()`), union of keys across rows | Renaming, casting, parsing dates or numbers |
| Conversions Parquet itself requires | Windowing, filtering, deduplicating, pivoting or reshaping |
| Columns **added** under the test below | Writing a derived value into a row, even "just the key" |
| Redacting a credential in stored request context | Persisting a secret |

## The test for an added column

Add a column only if it is one of these, and name the rule in the spec:

1. **A mapping key** our model needs and cannot derive from the answer — `security_id` (otherwise
   stage 2 re-resolves the symbol with *today's* mapping and silently re-attributes a series), or
   `asked_symbol` beside the provider's own `observed_symbol`.
2. **A partition key** that cannot be derived from the data or the request — `fetched_at` for a
   snapshot with no date of its own. A key that can be derived only *places* the file and is not
   written into rows.
3. **Metadata Dagster cannot hold**: it varies per row, must outlive event-log retention (runs older
   than 90 days are pruned), or stage 2 reads it — `run_id`, `fetched_at`, `url` (redacted),
   `sha256`, `content_type`, `provider`, the provider's warnings.

Keep each lane's set in one constant (`prices.CONTEXT_COLUMNS`) and test raw against it.

## Two shapes

- **Document** — a JSON, XML, CSV or PDF body. One row: `body`, `url`, `sha256`, `content_type`,
  `fetched_at`, `run_id`, plus the subject. Prefer it whenever the provider returns a document. Split
  the adapter into `fetch` (the network call, returning bytes) and `parse` (pure, run in stage 2): a
  pivot is already an interpretation, however faithful.
- **Rows from a library that has already parsed** — openbb returns typed `Data`. Store
  `model_dump()` whole. Its `extra="allow"` keeps undeclared vendor fields, but dates and floats are
  already coerced: record that the layer beneath is not raw. Owning it means calling the vendor
  directly.

## Files and partitions

- One file per partition, replaced on re-materialization.
- A partition that collected nothing still gets a zero-row file **with a column**, so DuckDB can open
  it (`pa.table({})` has no columns and is unreadable outside our loader).
- A file can hold rows that belong to another partition (a lookback, a widened range, a session still
  trading). Stage 2 reads each partition's own file and windows it to its own key
  (`partitioned.rows_per_partition`); where files legitimately overlap, keep the newest file's row.
- Schemas may differ across partitions; read a set with `union_by_name=true`.
- Raw in an older format the current parser cannot read is counted (`legacy_rows`), never skipped
  silently — a skipped file reads exactly like a day the provider had nothing for.

## Tests

- **Documents:** stored bytes equal served bytes by **sha256**, never by a JSON round trip, which
  passes a stage 1 that re-serialised.
- **Rows:** stored columns are a **superset** of the provider's, never an equality — a vendor adding a
  field must not turn CI red.
- **Added columns:** exactly the declared constant.
- **Moved rules:** when a rule moves from stage 1 to stage 2, assert it on both sides, with a control
  row that must survive, or "moved" and "vanished" look the same.

## Known gaps

- `Document.as_row` stores `url` verbatim, so a provider with a key in its query string (Alpha Vantage,
  Tiingo, FRED, DART) would write the secret to Parquet. Redaction must land before the first keyed
  provider — `docs/deferred/2026-09-16-raw-request-credentials.md` in the umbrella.
- `ParquetIOManager` writes in place rather than temp-and-rename; a killed write leaves a truncated
  file that fails loudly on load. Re-materialize the partition.
