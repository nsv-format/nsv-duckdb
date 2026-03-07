# NSV DuckDB Extension

A loadable [DuckDB](https://duckdb.org/) extension for reading and writing [NSV (Newline-Separated Values)](https://github.com/nsv-format/nsv) files. Uses Rust FFI for core parsing (from the [`nsv`](https://crates.io/crates/nsv) crate v0.0.9). Built against DuckDB v1.4.2.

## Quick Start

```sql
LOAD './build/release/extension/nsv/nsv.duckdb_extension';

-- Read with auto-detected types
SELECT * FROM read_nsv('examples/users.nsv');

-- Aggregations work directly (no CAST needed)
SELECT city, AVG(age) FROM read_nsv('examples/users.nsv') GROUP BY city;

-- Force all columns to VARCHAR
SELECT * FROM read_nsv('examples/users.nsv', all_varchar=true);

-- Write NSV
COPY my_table TO 'output.nsv' (FORMAT nsv);
```

## Type Detection

Columns are auto-narrowed by sampling up to 1000 data rows. Candidate types are tried in order:

| Priority | Type | Examples |
|----------|------|----------|
| 1 | `BOOLEAN` | `true`, `false`, `TRUE`, `FALSE` (not `1`/`0`) |
| 2 | `BIGINT` | `42`, `-100`, `3000000000` |
| 3 | `DOUBLE` | `3.14`, `1.5e10`, `-0.5` |
| 4 | `DATE` | `2026-01-15` (ISO 8601 only) |
| 5 | `TIMESTAMP` | `2026-01-15 10:30:00`, `2026-01-15T10:30:00` |
| 6 | `VARCHAR` | Everything else (fallback) |

**Known behaviors:**
- `1`/`0` columns narrow to BIGINT (strict cast rejects them as BOOLEAN)
- Leading zeros (`007`) narrow to BIGINT — the zeros are lost. Use `all_varchar=true` to preserve
- Empty cells are treated as NULL and don't influence type detection
- NULL and empty strings are indistinguishable after roundtrip (both become empty cells in NSV)
- Values beyond the 1000-row sample that fail to cast silently become NULL

## Building

**Prerequisites:** CMake 3.5+, C++ compiler, [Rust toolchain](https://rustup.rs/)

```bash
git clone --recursive https://github.com/nsv-format/nsv-duckdb.git
cd nsv-duckdb
make
# Output: build/release/extension/nsv/nsv.duckdb_extension
```

First build compiles DuckDB from source (~20-30 min). On musl/Alpine, add `rustup target add x86_64-unknown-linux-musl` first. macOS requires absolute paths when loading (`duckdb -unsigned`).

## Running Tests

```bash
make test
```

Tests are in `test/sql/nsv.test` and `test/sql/nsv_type_narrowing.test`.

## Architecture

```
nsv-duckdb/
├── src/nsv_extension.cpp    # DuckDB table function + COPY TO
├── rust-ffi/src/lib.rs      # Rust FFI bridge (NsvHandle, NsvEncoder)
├── test/sql/                # DuckDB SQL tests
├── duckdb/                  # DuckDB submodule
└── extension-ci-tools/      # DuckDB CI tools submodule
```

Three layers: **Rust FFI** (eager decode via `nsv::decode_bytes()`) → **C++ extension** (`read_nsv()` table function with projection pushdown, `COPY TO` writer) → **DuckDB** (loaded as extension).

## CI/CD

Uses DuckDB's [extension-ci-tools](https://github.com/duckdb/extension-ci-tools) for multi-platform builds. See `.github/workflows/MainDistributionPipeline.yml`.

**Build platforms:** linux_amd64, linux_arm64, linux_amd64_musl, osx_amd64, osx_arm64, windows_amd64, windows_arm64, windows_amd64_mingw, wasm_mvp, wasm_eh, wasm_threads

**Release:** Triggers on `v*` tags. Downloads build artifacts, renames to `nsv-<platform>.<ext>`, and creates a GitHub release. Pre-release detected from `-rc`/`-beta`/`-alpha` in tag.

## Status

- [x] Core NSV parsing via Rust FFI
- [x] `read_nsv()` table function
- [x] Column projection pushdown
- [x] CSV-style type narrowing
- [x] `COPY TO` (write) support
- [x] Build CI (multi-platform)
- [x] `all_varchar` parameter
- [x] Type narrowing edge cases tested
- [ ] Release CI verified (no tags pushed yet)
- [ ] NULL vs empty string disambiguation
- [ ] Community extension submission
- [ ] ENSV schema support (pending spec)

## License

MIT — see component licenses in `rust-ffi/` (nsv crate) and `duckdb/`.

## Links

- [NSV Format Specification](https://github.com/nsv-format/nsv)
- [NSV Rust Parser](https://github.com/nsv-format/nsv-rust)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview.html)
