# DuckDB Extension & Rust Library: State Assessment

## Cost Breakdown (measured)

100K rows x 10 columns, simple data (no escapes):

| Step | Time | % of full |
|------|------|-----------|
| scan only (boundary detection, no alloc) | 6.4 ms | 8% |
| scan + unescape col 0 (alloc 1 col) | 11.7 ms | 15% |
| `decode_bytes_projected` 1 of 10 | 7.7 ms | 10% |
| `decode_bytes_projected` 5 of 10 | 18.6 ms | 23% |
| `decode_bytes` (full) | 80 ms | 100% |

Same shape, escape-heavy data (⅓ cells contain `\n` or `\\`):

| Step | Time | % of full |
|------|------|-----------|
| scan only | 11.1 ms | 18% |
| scan + unescape col 0 | 21.6 ms | 35% |
| `decode_bytes_projected` 1 of 10 | 10.5 ms | 17% |
| `decode_bytes` (full) | 62.6 ms | 100% |

### What this tells us

1. **Scan is cheap.** Finding cell boundaries costs ~8-18% of total. The dominant cost is allocation + copy (unescape implies `to_vec()` for every cell).

2. **Projection already captures most of the win.** `projected 1/10` is ~10% of full decode — almost exactly the scan floor. This means projection already eliminates nearly all allocation for skipped columns.

3. **The overhead of projected vs. raw scan is small.** `projected 1/10` (7.7 ms) vs. `scan_only` (6.4 ms) — the structural overhead of building `Vec<Vec<Vec<u8>>>` for one column is only ~1.3 ms on 100K rows.

4. **Unescape cost tracks escape density.** Simple data: unescape adds ~5 ms for one column. Escape-heavy: ~10 ms. The `contains(&b'\\')` fast-path in `unescape_bytes` means most cells are just `to_vec()`.

## Implications for predicate pushdown

You were right to question my claim that "allocation is cheap compared to I/O." It's not — **allocation dominates parse time.** But projection already solves this for *columns*. The question is whether it matters for *rows*.

For row-level filtering in Rust, the savings would be: avoid `unescape_bytes` + `Vec` allocation for cells in rows that fail the predicate. But:

- The filter columns themselves must still be decoded (you need the value to evaluate the predicate)
- Non-filter, non-projected columns are already skipped by projection
- The remaining waste is: decoding projected-but-not-filter columns for rejected rows

In a typical `SELECT a, b WHERE c > 100` on a 10-column table:
- Projection gives you columns a, b, c only (3 of 10) — 70% cell alloc eliminated
- Filter pushdown in Rust would additionally skip a, b for rejected rows
- If 90% of rows are filtered out, that's 90% of a+b alloc saved
- a+b is 2/10 of columns → 90% × 20% = 18% additional savings on total decode

That's measurable but not huge. And it requires either:
- Type-aware filter evaluation in Rust (wrong abstraction boundary)
- An opaque callback across FFI (complexity for marginal gain)
- A "row-iterator" API where C++ pulls one row at a time and decides whether to keep it (interface proliferation, exactly what you're worried about)

**Verdict: predicate pushdown belongs in C++ only.** The cost is real but the architectural tax of pushing it into the Rust lib isn't justified. The C++ side already has the cast values in hand — just check the filter before `vec.SetValue()` and skip the row. The waste is allocating cells that get immediately discarded, but that's bounded by the projected column set (usually small) and mitigated by the allocator reusing memory across rows.

The one scenario where Rust-side filtering would win big: narrow projection (1-2 columns), high selectivity (99% filtered), huge file. At that point you're paying scan cost + 1% of alloc, and the scan cost dominates anyway.
