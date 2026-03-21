//! Comprehensive FFI-layer benchmarks for nsv-duckdb.
//!
//! Measures decode, projected decode, cell access, and encode across
//! different data shapes and sizes. Each benchmark group covers:
//! - Overall wall-clock time
//! - Per-row and per-cell throughput
//!
//! Data shapes:
//! - "narrow"  : 3 columns, short cells (typical metadata)
//! - "wide"    : 50 columns, short cells (wide tables)
//! - "fat"     : 5 columns, long cells (text-heavy data)
//! - "escaped" : 5 columns, cells with backslashes and newlines (worst case for unescape)
//! - "clean"   : 5 columns, plain ASCII, no escaping needed (best case — zero-copy target)

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use std::time::Duration;

// Re-use the FFI functions from the crate.
use nsv_ffi::*;

// ── Data generators ─────────────────────────────────────────────────

fn gen_narrow(nrows: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(nrows * 30);
    // header
    buf.extend_from_slice(b"id\nname\nvalue\n\n");
    for i in 0..nrows {
        let row = format!("{}\nrow_{}\n{}.{}\n\n", i, i, i * 7, i % 100);
        buf.extend_from_slice(row.as_bytes());
    }
    buf
}

fn gen_wide(nrows: usize, ncols: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(nrows * ncols * 10);
    // header
    for c in 0..ncols {
        buf.extend_from_slice(format!("col{}\n", c).as_bytes());
    }
    buf.push(b'\n');
    for i in 0..nrows {
        for c in 0..ncols {
            buf.extend_from_slice(format!("r{}c{}\n", i, c).as_bytes());
        }
        buf.push(b'\n');
    }
    buf
}

fn gen_fat(nrows: usize) -> Vec<u8> {
    let cell = "The quick brown fox jumps over the lazy dog. ".repeat(5); // ~225 bytes
    let mut buf = Vec::with_capacity(nrows * 5 * 250);
    buf.extend_from_slice(b"a\nb\nc\nd\ne\n\n");
    for _ in 0..nrows {
        for _ in 0..5 {
            buf.extend_from_slice(cell.as_bytes());
            buf.push(b'\n');
        }
        buf.push(b'\n');
    }
    buf
}

fn gen_escaped(nrows: usize) -> Vec<u8> {
    // Cells that require unescaping: embedded newlines and backslashes
    let mut buf = Vec::with_capacity(nrows * 5 * 40);
    buf.extend_from_slice(b"a\nb\nc\nd\ne\n\n");
    for i in 0..nrows {
        // Mix of escaped and clean cells
        buf.extend_from_slice(format!("line1\\nline2\\nline3\n").as_bytes());
        buf.extend_from_slice(format!("path\\\\to\\\\file{}\n", i).as_bytes());
        buf.extend_from_slice(format!("clean_cell_{}\n", i).as_bytes());
        buf.extend_from_slice(b"back\\\\slash\n");
        buf.extend_from_slice(b"\\\n"); // empty cell token
        buf.push(b'\n');
    }
    buf
}

fn gen_clean(nrows: usize) -> Vec<u8> {
    // Pure ASCII, no escaping needed — ideal zero-copy scenario
    let mut buf = Vec::with_capacity(nrows * 5 * 20);
    buf.extend_from_slice(b"alpha\nbeta\ngamma\ndelta\nepsilon\n\n");
    for i in 0..nrows {
        buf.extend_from_slice(format!("hello{}\nworld{}\nfoo{}\nbar{}\nbaz{}\n\n", i, i, i, i, i).as_bytes());
    }
    buf
}

// ── Benchmark groups ────────────────────────────────────────────────

fn bench_decode(c: &mut Criterion) {
    let sizes: &[usize] = &[100, 1_000, 10_000, 100_000];

    let mut group = c.benchmark_group("decode_narrow");
    for &n in sizes {
        let data = gen_narrow(n);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode(data.as_ptr(), data.len());
                let nrows = nsv_row_count(h);
                black_box(nrows);
                nsv_free(h);
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("decode_wide");
    for &n in sizes {
        let data = gen_wide(n, 50);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode(data.as_ptr(), data.len());
                black_box(nsv_row_count(h));
                nsv_free(h);
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("decode_fat");
    for &n in sizes {
        let data = gen_fat(n);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode(data.as_ptr(), data.len());
                black_box(nsv_row_count(h));
                nsv_free(h);
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("decode_escaped");
    for &n in sizes {
        let data = gen_escaped(n);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode(data.as_ptr(), data.len());
                black_box(nsv_row_count(h));
                nsv_free(h);
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("decode_clean");
    for &n in sizes {
        let data = gen_clean(n);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode(data.as_ptr(), data.len());
                black_box(nsv_row_count(h));
                nsv_free(h);
            });
        });
    }
    group.finish();
}

fn bench_decode_projected(c: &mut Criterion) {
    let sizes: &[usize] = &[100, 1_000, 10_000, 100_000];

    // Project 2 of 5 columns from clean data
    let mut group = c.benchmark_group("projected_clean_2of5");
    for &n in sizes {
        let data = gen_clean(n);
        let cols: [usize; 2] = [0, 3];
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode_projected(data.as_ptr(), data.len(), cols.as_ptr(), cols.len());
                black_box(nsv_projected_row_count(h));
                nsv_projected_free(h);
            });
        });
    }
    group.finish();

    // Project 2 of 5 columns from escaped data
    let mut group = c.benchmark_group("projected_escaped_2of5");
    for &n in sizes {
        let data = gen_escaped(n);
        let cols: [usize; 2] = [0, 2];
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode_projected(data.as_ptr(), data.len(), cols.as_ptr(), cols.len());
                black_box(nsv_projected_row_count(h));
                nsv_projected_free(h);
            });
        });
    }
    group.finish();

    // Project 5 of 50 from wide
    let mut group = c.benchmark_group("projected_wide_5of50");
    for &n in sizes {
        let data = gen_wide(n, 50);
        let cols: [usize; 5] = [0, 10, 20, 30, 40];
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode_projected(data.as_ptr(), data.len(), cols.as_ptr(), cols.len());
                black_box(nsv_projected_row_count(h));
                nsv_projected_free(h);
            });
        });
    }
    group.finish();
}

fn bench_cell_access(c: &mut Criterion) {
    // Measure the cost of nsv_cell access after decode (should be ~free)
    let data = gen_clean(10_000);
    let handle = nsv_decode(data.as_ptr(), data.len());
    let nrows = nsv_row_count(handle);

    let mut group = c.benchmark_group("cell_access");
    group.throughput(Throughput::Elements(nrows as u64 * 5));
    group.bench_function("sequential_all_cells", |b| {
        b.iter(|| {
            let mut total_len = 0usize;
            for row in 0..nrows {
                let ncols = nsv_col_count(handle, row);
                for col in 0..ncols {
                    let mut len = 0usize;
                    let cell = nsv_cell(handle, row, col, &mut len);
                    total_len += len;
                    black_box(cell);
                }
            }
            black_box(total_len);
        });
    });
    group.finish();

    nsv_free(handle);
}

fn bench_encode(c: &mut Criterion) {
    let sizes: &[usize] = &[100, 1_000, 10_000];

    let mut group = c.benchmark_group("encode_clean");
    for &n in sizes {
        let data = gen_clean(n);
        // Pre-decode so we can re-encode
        let handle = nsv_decode(data.as_ptr(), data.len());
        let nrows = nsv_row_count(handle);

        // Collect cells for encoding
        let mut cells: Vec<Vec<(Vec<u8>, usize)>> = Vec::new();
        for row in 0..nrows {
            let ncols = nsv_col_count(handle, row);
            let mut row_cells = Vec::new();
            for col in 0..ncols {
                let mut len = 0usize;
                let ptr = nsv_cell(handle, row, col, &mut len);
                let bytes = if !ptr.is_null() && len > 0 {
                    unsafe { std::slice::from_raw_parts(ptr as *const u8, len).to_vec() }
                } else {
                    Vec::new()
                };
                row_cells.push((bytes, len));
            }
            cells.push(row_cells);
        }

        group.throughput(Throughput::Elements(nrows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &cells, |b, cells| {
            b.iter(|| {
                let enc = nsv_encoder_new();
                for row in cells {
                    for (cell_bytes, len) in row {
                        nsv_encoder_push_cell(enc, cell_bytes.as_ptr(), *len);
                    }
                    nsv_encoder_end_row(enc);
                }
                let mut out_ptr: *mut u8 = std::ptr::null_mut();
                let mut out_len: usize = 0;
                nsv_encoder_finish(enc, &mut out_ptr, &mut out_len);
                black_box(out_len);
                nsv_free_buf(out_ptr, out_len);
            });
        });

        nsv_free(handle);
    }
    group.finish();
}

fn bench_end_to_end(c: &mut Criterion) {
    // Simulate full DuckDB scan: decode → iterate all cells → access each cell value
    let sizes: &[usize] = &[1_000, 10_000, 100_000];

    let mut group = c.benchmark_group("e2e_clean");
    for &n in sizes {
        let data = gen_clean(n);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode(data.as_ptr(), data.len());
                let nrows = nsv_row_count(h);
                let mut total_bytes = 0usize;
                for row in 0..nrows {
                    let ncols = nsv_col_count(h, row);
                    for col in 0..ncols {
                        let mut len = 0usize;
                        let cell = nsv_cell(h, row, col, &mut len);
                        total_bytes += len;
                        black_box(cell);
                    }
                }
                black_box(total_bytes);
                nsv_free(h);
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("e2e_escaped");
    for &n in sizes {
        let data = gen_escaped(n);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode(data.as_ptr(), data.len());
                let nrows = nsv_row_count(h);
                let mut total_bytes = 0usize;
                for row in 0..nrows {
                    let ncols = nsv_col_count(h, row);
                    for col in 0..ncols {
                        let mut len = 0usize;
                        let cell = nsv_cell(h, row, col, &mut len);
                        total_bytes += len;
                        black_box(cell);
                    }
                }
                black_box(total_bytes);
                nsv_free(h);
            });
        });
    }
    group.finish();

    // Projected e2e
    let mut group = c.benchmark_group("e2e_projected_clean");
    for &n in sizes {
        let data = gen_clean(n);
        let cols: [usize; 2] = [1, 3];
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &data, |b, data| {
            b.iter(|| {
                let h = nsv_decode_projected(data.as_ptr(), data.len(), cols.as_ptr(), cols.len());
                let nrows = nsv_projected_row_count(h);
                let mut total_bytes = 0usize;
                for row in 0..nrows {
                    for proj_col in 0..cols.len() {
                        let mut len = 0usize;
                        let cell = nsv_projected_cell(h, row, proj_col, &mut len);
                        total_bytes += len;
                        black_box(cell);
                    }
                }
                black_box(total_bytes);
                nsv_projected_free(h);
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5))
        .sample_size(50);
    targets = bench_decode, bench_decode_projected, bench_cell_access, bench_encode, bench_end_to_end
}
criterion_main!(benches);
