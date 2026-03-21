# Filter Pushdown Benchmarks

Measured 2026-03-21. Each query run 3 times; median reported.
Extension built in release mode. DuckDB v1.1.3 (vendored).
Data: synthetic 5-column table (id INT, name VARCHAR, age INT, city VARCHAR, score DOUBLE).

## File Sizes

| Scale | NSV    | CSV (for comparison) |
|-------|--------|----------------------|
| 100K  | 3.1 MB | —                    |
| 500K  | 17 MB  | —                    |
| 1M    | 33 MB  | 32 MB                |

NSV files are ~1.03x the size of equivalent CSV due to row-terminator newlines.

## 100K Rows (3.1 MB)

| Query                                    | Median | vs baseline |
|------------------------------------------|--------|-------------|
| Full scan (baseline)                     | 110 ms | 1.00x       |
| Equality 25% (city = 'NYC')             | 77 ms  | 0.70x       |
| Range ~17% (age 30–39)                  | 112 ms | 1.02x       |
| Highly selective ~1.7% (age = 42)       | 94 ms  | 0.85x       |
| Projection only (1/5 cols)              | 104 ms | 0.95x       |
| Filter + projection (city, select score)| 81 ms  | 0.74x       |
| IS NOT NULL (100% pass-through)         | 57 ms  | 0.52x       |
| Compound (city = NYC AND age >= 40)     | 107 ms | 0.97x       |

## 500K Rows (17 MB)

| Query                                    | Median | vs baseline |
|------------------------------------------|--------|-------------|
| Full scan (baseline)                     | 494 ms | 1.00x       |
| Equality 25% (city = 'NYC')             | 391 ms | 0.79x       |
| Range ~17% (age 30–39)                  | 559 ms | 1.13x       |
| Highly selective ~1.7% (age = 42)       | 487 ms | 0.99x       |
| Filter + projection                      | 402 ms | 0.81x       |
| Compound (city = NYC AND age >= 40)     | 488 ms | 0.99x       |

## 1M Rows (33 MB)

| Query                                    | Median | vs baseline |
|------------------------------------------|--------|-------------|
| Full scan (baseline)                     | 941 ms | 1.00x       |
| Equality 25% (city = 'NYC')             | 812 ms | 0.86x       |
| Range ~17% (age 30–39)                  | 1078 ms| 1.15x       |
| Highly selective ~1.7% (age = 42)       | 998 ms | 1.06x       |
| Projection only (1/5 cols)              | 1000 ms| 1.06x       |
| Filter + projection                      | 752 ms | 0.80x       |
| Compound (city = NYC AND age >= 40)     | 945 ms | 1.00x       |
| IS NOT NULL (100% pass-through)         | 532 ms | 0.57x       |

## CSV Comparison (1M rows, 32 MB)

| Query                              | CSV     | NSV     | NSV/CSV |
|------------------------------------|---------|---------|---------|
| Full scan                          | 86 ms   | 941 ms  | 10.9x   |
| Equality 25%                       | 87 ms   | 812 ms  | 9.3x    |
| Highly selective ~1.7%             | 80 ms   | 998 ms  | 12.5x   |
| Filter + projection                | 88 ms   | 752 ms  | 8.5x    |

## Analysis

### Filter pushdown gains

- **Equality filters** (25% selectivity): consistent 15–30% improvement across scales.
- **Filter + projection** (1 col + filter on another): consistent ~20% improvement — the best case, since both filter rejection *and* column skipping reduce materialization.
- **IS NOT NULL**: anomalously fast (0.52–0.57x baseline). Likely DuckDB short-circuits this filter type before the scan loop, or the filter evaluation itself has very low overhead and there's a caching/scheduling effect.
- **Range filters**: show *slowdown* at scale (1.13–1.15x), likely due to the per-cell `ReadAndCastCell` + `ConstantFilter::Compare` overhead exceeding savings from skipping `SetValue` when most rows still pass.
- **Compound filters**: roughly neutral — the AND conjunction adds evaluation cost that offsets materialization savings.
- **Highly selective filters** (~1.7%): gains at 100K (0.85x) but neutral/slightly worse at 1M, suggesting filter evaluation overhead per row is significant relative to the saved materialization.

### Bottleneck: Rust FFI decode

The dominant cost is the Rust-side `nsv_decode_projected` call, which **decodes the full file regardless of filters**. The C++ filter pushdown can only save work *after* decode: skipping `SetValue` and type casting for rejected rows.

This explains:
1. Why gains are modest (15–30%) instead of dramatic
2. Why CSV is 9–12x faster (DuckDB's CSV reader is deeply optimized with SIMD, parallel chunk scanning, and native filter integration; our reader is single-threaded, FFI-crossing, and returns all rows)
3. Why gains don't scale linearly with selectivity

### Where real gains would come from

1. **Rust-side row skipping**: pass filter predicates into the Rust decoder so it can skip unescape/allocation for rejected rows during decode
2. **Chunk-parallel scanning**: split the file into independent chunks for parallel decode (requires finding row boundaries — doable since NSV uses `\n\n` as unambiguous row terminators)
3. **SIMD-accelerated field scanning**: the Rust lib already has SIMD path but it's for the full decode; specialized scanning for filter columns could skip non-filter fields
4. **Memory-mapped I/O with lazy decode**: only fully decode rows that pass filter predicates

The current filter pushdown is a correct implementation that enables DuckDB's query planner to push predicates down, but the gains are architecturally limited by the decode-everything-then-filter pattern.
