# NSV DuckDB Extension

A [DuckDB](https://duckdb.org/) extension for reading and writing [NSV (Newline-Separated Values)](https://github.com/nsv-format/nsv) files.

## Quick Start

```sql
LOAD './build/release/extension/nsv/nsv.duckdb_extension';

-- Read with auto-detected types
SELECT * FROM read_nsv('examples/users.nsv');

-- Aggregations work directly — no CAST needed
SELECT city, AVG(age) as avg_age
FROM read_nsv('examples/users.nsv')
GROUP BY city;

-- Force all columns to VARCHAR
SELECT * FROM read_nsv('examples/users.nsv', all_varchar=true);

-- Write NSV
COPY my_table TO 'output.nsv' (FORMAT nsv);
```

## What is NSV?

NSV is a tabular data format where each field is on its own line and records are separated by blank lines. The first record defines column names.

```
name
age
city

Alice
30
NYC

Bob
25
SF
```

**Escaping:** `\` at end of line = empty string, `\n` = literal newline, `\\` = backslash.

See the [format specification](https://github.com/nsv-format/nsv) for details.

## Features

### `read_nsv(filename, [all_varchar])`

Reads an NSV file as a DuckDB table.

- **Automatic type detection** — samples up to 1000 rows to infer column types:

  | Type | Examples |
  |------|----------|
  | `BOOLEAN` | `true`, `false` |
  | `BIGINT` | `42`, `-100` |
  | `DOUBLE` | `3.14`, `1.0e10` |
  | `DATE` | `2024-01-15` |
  | `TIMESTAMP` | `2024-01-15 10:30:00` |
  | `VARCHAR` | Fallback for everything else |

- **Projection pushdown** — when a query selects a subset of columns, only those columns are decoded in a single pass, skipping work for unused columns.

- **`all_varchar=true`** — disables type detection; all columns returned as `VARCHAR`.

- Reads via DuckDB's filesystem layer (local paths, HTTP, S3).

### `COPY TO ... (FORMAT nsv)`

Writes a DuckDB table to NSV format.

```sql
COPY my_table TO 'output.nsv' (FORMAT nsv);
COPY my_table TO 'output.nsv' (FORMAT nsv, header false);
```

- Optional `header` parameter (default: `true`).
- NULL values are written as empty cells.
- Roundtrip-safe: types are preserved through write → read cycles.

## Architecture

The extension bridges DuckDB (C++) with the Rust [nsv](https://crates.io/crates/nsv) parser via FFI:

```
DuckDB Extension (C++)         ← table function, type detection, projection pushdown
        │ FFI
Rust FFI Layer (rust-ffi/)     ← decode/encode, memory management, UTF-8 handling
        │
nsv crate (crates.io v0.0.9)  ← core parser
```

**Scan pipeline:**
1. **Bind** — read file, eager-decode all rows, sample columns to detect types, extract header names.
2. **Init** — if the query projects a strict column subset, re-decode with only those columns (`nsv_decode_projected`). Full `SELECT *` reuses the eager handle.
3. **Scan** — stream rows in `STANDARD_VECTOR_SIZE` chunks, casting cell strings to detected types.

## Building from Source

### Prerequisites

- CMake 3.5+
- C++ compiler (GCC, Clang, or MSVC)
- Rust toolchain ([rustup.rs](https://rustup.rs/))

### Build

```bash
git clone --recursive https://github.com/nsv-format/nsv-duckdb.git
cd nsv-duckdb
make
# → build/release/extension/nsv/nsv.duckdb_extension
```

First build compiles DuckDB from source (~20–30 min).

### Platform Notes

**Linux (musl/Alpine):** add `rustup target add x86_64-unknown-linux-musl` before building.

**macOS:** if pre-built Linux `.a` files exist, remove them first:
```bash
rm -f rust-ffi/target/release/libnsv_ffi.a
rm -f rust-ffi/target/x86_64-unknown-linux-musl/release/libnsv_ffi.a
make
```
macOS requires absolute paths when loading extensions (hardened runtime).

**Windows:**
```bash
cmake -DCMAKE_BUILD_TYPE=Release -S . -B build
cmake --build build --config Release
```

## Loading the Extension

```bash
duckdb -unsigned
```
```sql
-- Linux (relative path OK)
LOAD './build/release/extension/nsv/nsv.duckdb_extension';

-- macOS (absolute path required)
LOAD '/absolute/path/to/build/release/extension/nsv/nsv.duckdb_extension';
```

**Tip:** use `$(pwd)` in shell scripts:
```bash
duckdb -unsigned -c "LOAD '$(pwd)/build/release/extension/nsv/nsv.duckdb_extension'; SELECT * FROM read_nsv('examples/users.nsv');"
```

## Development

### Tests

```bash
make test
```

Tests are in `test/sql/nsv.test` (DuckDB's SQL test format). Coverage includes type narrowing, `all_varchar`, roundtrip via `COPY TO`, projection pushdown (single column, non-adjacent columns, reversed order, full permutation), aggregations, and boolean filtering.

### Project Structure

```
nsv-duckdb/
├── src/
│   ├── nsv_extension.cpp         # DuckDB table + copy functions
│   └── include/
│       ├── nsv_extension.hpp     # Extension header
│       └── nsv_ffi.h             # C FFI declarations
├── rust-ffi/
│   ├── src/lib.rs                # Rust FFI: decode, encode, projected decode
│   └── Cargo.toml                # Depends on nsv 0.0.9 (crates.io)
├── test/sql/nsv.test             # SQL test suite
├── examples/users.nsv            # Sample data
├── duckdb/                       # DuckDB source (git submodule)
├── extension-ci-tools/           # DuckDB CI tools (git submodule)
└── CMakeLists.txt                # Build: Rust static lib → C++ extension
```

### How the Build Works

1. CMake invokes `cargo build --release` in `rust-ffi/`, producing `libnsv_ffi.a`.
2. The C++ extension links against this static library and implements DuckDB's table function / copy function APIs.
3. DuckDB's extension system packages everything into a single `.duckdb_extension` file.

## CI/CD

Uses DuckDB's [extension-ci-tools](https://github.com/duckdb/extension-ci-tools) (v1.4.2) for cross-platform builds. See `.github/workflows/MainDistributionPipeline.yml`.

## Troubleshooting

**macOS: "relative path not allowed in hardened program"** — use an absolute path to the `.duckdb_extension` file.

**"The file was built for DuckDB version X"** — rebuild against the matching DuckDB version:
```bash
git submodule update --init --recursive
make clean && make
```

**macOS/ARM64: "archive member '/' not a mach-o file"** — Linux `.a` files are present. Remove them:
```bash
rm -f rust-ffi/target/release/libnsv_ffi.a
rm -f rust-ffi/target/x86_64-unknown-linux-musl/release/libnsv_ffi.a
make clean && make
```

## Project Status

**Working:**
- Core read/write (thoroughly tested)
- Type detection: BOOLEAN, BIGINT, DOUBLE, DATE, TIMESTAMP, VARCHAR
- Projection pushdown optimization
- `COPY TO` with roundtrip preservation
- Cross-platform CI (Linux glibc/musl, macOS x86/ARM, Windows)
- Installable from local build

**Not yet done:**
- Performance benchmarking (no measurements exist yet — claims would be premature)
- Feature parity audit against DuckDB's CSV extension
- ENSV metadata integration (typed columns, null handling)
- Release CI (build CI passes; tag-triggered release not yet verified)

## License

MIT — see [LICENSE](LICENSE).

## Links

- [NSV Format Specification](https://github.com/nsv-format/nsv)
- [nsv crate](https://crates.io/crates/nsv) (Rust parser)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview.html)
