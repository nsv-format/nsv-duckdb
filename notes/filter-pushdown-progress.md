# Filter Pushdown Implementation — Complete

## Summary
Added `filter_pushdown = true` to the DuckDB `read_nsv` table function.
DuckDB pushes WHERE-clause predicates into the scan function; the scan
evaluates them per-row and skips non-matching rows before materializing
them into the output DataChunk.

All filter logic is in C++ (nsv_extension.cpp). No changes to the Rust
core lib or the FFI layer. See `assessments/duckdb-extension-and-rust-lib.md`
for the cost-breakdown justification.

## DuckDB Filter API (key details for future reference)

**Headers**: `duckdb/planner/table_filter.hpp` + `duckdb/planner/filter/*.hpp`

**CRITICAL: filter key semantics.** `TableFilterSet::filters` is a
`map<idx_t, unique_ptr<TableFilter>>`. The keys are **indices into
`column_ids`**, NOT source column indices. DuckDB remaps them in
`CreateTableFilterSet` (see `duckdb/src/execution/physical_plan/plan_get.cpp`).
This cost a debugging round — the initial implementation treated them as
source column indices, which caused filters to evaluate against wrong columns.

**Filter types handled**:
- `CONSTANT_COMPARISON` — use `ConstantFilter::Compare(val)` directly
- `IS_NULL` / `IS_NOT_NULL` — `val.IsNull()`
- `IN_FILTER` — `Value::NotDistinctFrom(val, candidate)` (static method, 2 args)
- `CONJUNCTION_AND` / `CONJUNCTION_OR` — recursive on `.child_filters`
- Other types (OPTIONAL, DYNAMIC, EXPRESSION, STRUCT_EXTRACT) — pass through

**Interaction with projection pushdown**:
- `column_ids` = union of SELECTed + WHERE'd columns (DuckDB adds filter-only
  columns automatically)
- `projection_ids` maps output DataChunk positions → indices into `column_ids`
- `projection_ids` is empty when all scanned columns appear in output
- Both feed into `nsv_decode_projected` seamlessly — the projected handle
  decodes exactly the columns in `column_ids`

## Performance (100K rows x 5 cols)

| Query | Time (ms) | vs. baseline |
|-------|-----------|--------------|
| Full scan, no filter | 73 | 1.00x |
| 25% selectivity (city = 'NYC') | 62 | 0.85x |
| ~1.7% selectivity (age = 42) | 75 | ~1.0x |
| Filter + projection (1 col, filter on other) | 59 | 0.81x |
| Projection only (1 col, no filter) | 74 | 1.01x |
| IN filter, 50% selectivity | 55 | 0.75x |

The gains are modest at 100K rows because the Rust decode (which dominates
cost) still processes all rows. The savings come from:
- Not calling `SetValue` for rejected rows (skipped `Value` → `Vector` copy)
- Not casting non-filter columns for rejected rows (via early `break`)
- Reduced output cardinality → less downstream work

The no-filter path has zero overhead (fast-path check at top of loop).

## Files Changed

- `nsv-duckdb/src/nsv_extension.cpp` — filter evaluation logic, scan restructure
- `nsv-duckdb/test/sql/nsv.test` — 33 new filter pushdown test assertions

## Gotchas for Future Work

1. `Value::NotDistinctFrom` is a **static** method: `Value::NotDistinctFrom(a, b)`,
   not `a.NotDistinctFrom(b)`.

2. The scan loop changed from column-major to row-major. When there are no
   filters, the fast-path still does row-major (which is fine for correctness
   but slightly different from the old column-major loop). No performance
   regression observed.

3. `ReadAndCastCell` is called per-cell, returning a `Value`. This is
   allocation-heavy but matches the existing `SetValue` pattern. A future
   optimization could batch-read into flat arrays, but that would require
   a different approach to filter evaluation.

4. The `OPTIONAL_FILTER` type (used for IN filters) means DuckDB keeps a
   FILTER node above the scan for correctness. The pushdown still helps by
   skipping materialization of rejected rows, but the post-scan filter is
   redundant work. This is standard DuckDB behavior.
