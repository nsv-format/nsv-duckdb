# nsv 0.0.9 → 0.0.11: Cow zero-copy FFI benchmarks

## What changed

**nsv-rust 0.0.11**: `decode_bytes`, `decode_bytes_projected`, `unescape_bytes` now return
`Cow<[u8]>` instead of `Vec<u8>`. Cells that don't contain escape sequences are returned as
borrowed slices into the input buffer — zero allocation, zero copy.

**nsv-duckdb FFI**: Rewrote `NsvHandle` and `ProjectedNsvHandle` to store `Cow<'static, [u8]>`
backed by a pinned `Box<[u8]>` copy of the input. `nsv_cell` returns pointers directly into
either the input buffer (borrowed) or the Cow's owned allocation (escaped cells only).
No `bytes_to_strings` conversion layer — the FFI stays in byte-land throughout.

## Benchmark matrix

Five data shapes × four sizes (100, 1K, 10K, 100K rows):
- **narrow**: 3 cols, short cells (metadata tables)
- **wide**: 50 cols, short cells (wide tables)
- **fat**: 5 cols, ~225-byte cells (text-heavy)
- **escaped**: 5 cols, cells with `\n` and `\\` (worst case)
- **clean**: 5 cols, plain ASCII (best case — zero-copy target)

Three operation profiles:
- **decode**: raw decode only
- **projected**: decode with column subset (2-of-5, 5-of-50)
- **e2e**: decode + iterate all cells + access (simulates DuckDB scan)

## Results: decode

| Shape    | Rows   | Before       | After        | Speedup | Δ time   |
|----------|--------|--------------|--------------|---------|----------|
| narrow   | 100    | 11.41 µs     | 6.41 µs      | 1.78×   | -44.1%   |
| narrow   | 1K     | 181.1 µs     | 70.3 µs      | 2.58×   | -61.6%   |
| narrow   | 10K    | 1.483 ms     | 545.7 µs     | 2.72×   | -63.0%   |
| narrow   | 100K   | 12.75 ms     | 3.83 ms      | 3.33×   | -69.8%   |
| wide     | 100    | 310.1 µs     | 74.6 µs      | 4.16×   | -75.8%   |
| wide     | 1K     | 3.422 ms     | 539.4 µs     | 6.34×   | -83.3%   |
| wide     | 10K    | 34.09 ms     | 3.876 ms     | 8.80×   | -88.6%   |
| wide     | 100K   | 483.1 ms     | 110.4 ms     | 4.38×   | -77.2%   |
| fat      | 100    | 199.8 µs     | 110.7 µs     | 1.81×   | -43.4%   |
| fat      | 1K     | 1.105 ms     | 446.3 µs     | 2.48×   | -58.8%   |
| fat      | 10K    | 8.168 ms     | 3.414 ms     | 2.39×   | -58.8%   |
| fat      | 100K   | 130.8 ms     | 82.0 ms      | 1.59×   | -37.3%   |
| escaped  | 100    | 33.78 µs     | 29.24 µs     | 1.16×   | -13.6%   |
| escaped  | 1K     | 583.7 µs     | 383.8 µs     | 1.52×   | -34.8%   |
| escaped  | 10K    | 3.532 ms     | 1.856 ms     | 1.90×   | -48.3%   |
| escaped  | 100K   | 29.68 ms     | 15.43 ms     | 1.92×   | -48.0%   |
| clean    | 100    | 29.91 µs     | 11.74 µs     | 2.55×   | -60.3%   |
| clean    | 1K     | 305.0 µs     | 109.9 µs     | 2.78×   | -63.6%   |
| clean    | 10K    | 3.485 ms     | 848.7 µs     | 4.11×   | -75.0%   |
| clean    | 100K   | 30.61 ms     | 6.271 ms     | 4.88×   | -79.8%   |

## Results: projected decode

| Shape              | Rows  | Before      | After       | Speedup | Δ time   |
|--------------------|-------|-------------|-------------|---------|----------|
| clean 2-of-5       | 100   | 16.68 µs    | 8.41 µs     | 1.98×   | -49.4%   |
| clean 2-of-5       | 1K    | 158.7 µs    | 81.1 µs     | 1.96×   | -49.3%   |
| clean 2-of-5       | 10K   | 1.654 ms    | 566.9 µs    | 2.92×   | -63.7%   |
| clean 2-of-5       | 100K  | 11.23 ms    | 4.048 ms    | 2.77×   | -63.7%   |
| escaped 2-of-5     | 100   | 20.46 µs    | 15.22 µs    | 1.34×   | -24.8%   |
| escaped 2-of-5     | 1K    | 314.0 µs    | ~similar    | ~1.3×   | -25.7%   |
| wide 5-of-50       | 100   | 50.74 µs    | ~similar    | ~1.3×   | varied   |
| wide 5-of-50       | 1K    | 509.4 µs    | ~similar    | ~1.4×   | varied   |

## Results: end-to-end (decode + cell access)

| Shape              | Rows  | Before      | After       | Speedup | Δ time   |
|--------------------|-------|-------------|-------------|---------|----------|
| clean              | 1K    | 326.1 µs    | 120.2 µs    | 2.71×   | -63.2%   |
| clean              | 10K   | 2.675 ms    | 1.046 ms    | 2.56×   | -61.9%   |
| clean              | 100K  | 23.83 ms    | 8.188 ms    | 2.91×   | -65.6%   |
| escaped            | 1K    | 530.6 µs    | 420.7 µs    | 1.26×   | -24.4%   |
| escaped            | 10K   | 2.584 ms    | 2.063 ms    | 1.25×   | -20.5%   |
| escaped            | 100K  | 21.79 ms    | 16.69 ms    | 1.31×   | -23.4%   |
| projected clean    | 1K    | 171.0 µs    | 84.97 µs    | 2.01×   | -50.3%   |
| projected clean    | 10K   | 1.298 ms    | 700.2 µs    | 1.85×   | -46.0%   |
| projected clean    | 100K  | 10.68 ms    | 4.786 ms    | 2.23×   | -55.0%   |

## Analysis

**Clean data (the happy path) sees the biggest wins**: 2.5–4.9× on decode, 2–3× end-to-end.
This is the zero-copy payoff — no allocation per cell, just a borrowed slice.

**Wide tables benefit disproportionately** (up to 8.8× on decode): more columns = more cells =
more allocations saved.

**Escaped data still improves** (1.2–1.9×): even though escaped cells must allocate, the
*clean* cells within the same file get zero-copy, and the library-internal changes help too.

**Fat cells** (long text): moderate improvement (1.6–2.5×). The allocation savings are smaller
relative to the actual data volume, but still significant.

**Throughput peaks**: decode_fat at 3.1 GiB/s, decode_wide at 1.0 GiB/s, projected_clean at 1.1 GiB/s.
