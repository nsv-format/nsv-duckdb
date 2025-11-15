# NSV DuckDB Extension

A loadable [DuckDB](https://duckdb.org/) extension for reading and writing [NSV (Newline-Separated Values)](https://github.com/nsv-format/nsv) files.

## Quick Start

```sql
-- Load the extension
LOAD './build/release/extension/nsv/nsv.duckdb_extension';

-- Read an NSV file
SELECT * FROM read_nsv('examples/users.nsv');

-- Query with filters, aggregations, etc.
SELECT city, COUNT(*) as count
FROM read_nsv('examples/users.nsv')
GROUP BY city;
```

## What is NSV?

NSV (Newline-Separated Values) is a simple tabular data format where:
- Each field value is on its own line
- Records are separated by blank lines
- The first record defines column names

Example (`examples/users.nsv`):
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

**Escaping:** `\` = empty string, `\n` = newline, `\\` = backslash

## Architecture

This extension integrates the Rust [nsv](https://crates.io/crates/nsv) parser with DuckDB via FFI:

- **Rust layer** (`rust-glue/`) - FFI wrapper around the vendored NSV parser
- **C++ layer** (`src/`) - DuckDB table function that calls the Rust FFI
- **Vendored parser** (`vendor/nsv-rust/`) - The upstream NSV Rust library

## Building from Source

### Prerequisites

- **CMake** 3.5 or later
- **C++ compiler** (GCC, Clang, or MSVC)
- **Rust toolchain** - Install from [rustup.rs](https://rustup.rs/)
  - On Alpine/musl systems: `rustup target add x86_64-unknown-linux-musl`

### Build Steps

```bash
# Clone with submodules (includes DuckDB and CI tools)
git clone --recursive https://github.com/nsv-format/nsv-duckdb.git
cd nsv-duckdb

# Build the extension
make

# The loadable extension will be at:
# build/release/extension/nsv/nsv.duckdb_extension
```

**Note:** The first build compiles DuckDB from source and can take 20-30 minutes.

### Platform-Specific Notes

**Linux (glibc):**
```bash
make
```

**Linux (musl/Alpine):**
```bash
# Install musl target for Rust
rustup target add x86_64-unknown-linux-musl

# Build (CMake auto-detects musl)
make
```

**macOS:**
```bash
make
```

**Windows:**
```bash
# Use Visual Studio Developer Command Prompt
cmake -DCMAKE_BUILD_TYPE=Release -S . -B build
cmake --build build --config Release
```

## Using the Extension

### Load the Extension

```bash
# Start DuckDB with unsigned extension support
duckdb -unsigned

# Load the extension
D LOAD './build/release/extension/nsv/nsv.duckdb_extension';
```

Or in code:
```sql
LOAD '/path/to/nsv.duckdb_extension';
```

### Read NSV Files

```sql
-- Basic read
SELECT * FROM read_nsv('data.nsv');

-- With filters
SELECT * FROM read_nsv('users.nsv') WHERE CAST(age AS INT) > 25;

-- Aggregations
SELECT city, COUNT(*) FROM read_nsv('users.nsv') GROUP BY city;

-- Joins
SELECT u.name, o.order_id
FROM read_nsv('users.nsv') u
JOIN read_nsv('orders.nsv') o ON u.id = o.user_id;
```

**Note:** All columns are read as `VARCHAR`. Use `CAST()` for type conversions.

## Development

### Running Tests

```bash
# Run the test suite
make test
```

Tests are defined in `test/sql/nsv.test` using DuckDB's SQL test format.

### Project Structure

```
nsv-duckdb/
├── src/                          # C++ extension code
│   ├── nsv_extension.cpp        # Main table function
│   └── include/                 # Headers
├── rust-glue/                   # Rust FFI wrapper
│   ├── src/lib.rs              # FFI interface
│   ├── nsv.h                   # C header for FFI
│   └── Cargo.toml              # Links to vendor/nsv-rust
├── vendor/nsv-rust/            # Vendored NSV parser (git submodule)
├── test/sql/                   # DuckDB SQL tests
├── examples/                   # Example NSV files
├── duckdb/                     # DuckDB submodule
├── extension-ci-tools/         # DuckDB CI tools submodule
└── CMakeLists.txt              # Build configuration
```

### How the Build Works

1. **Rust FFI library** is built first:
   - `rust-glue/` compiles to a static library (`libnsv_ffi.a`)
   - Uses the vendored NSV parser from `vendor/nsv-rust/`
   - Exports C-compatible functions for parsing NSV

2. **C++ extension** is built second:
   - Links against the Rust static library
   - Implements DuckDB's table function API
   - Calls Rust parser via FFI for actual NSV parsing

3. **DuckDB extension system** packages everything:
   - Creates a loadable `.duckdb_extension` file
   - Includes all dependencies (Rust library is statically linked)

## CI/CD

The extension uses DuckDB's [extension-ci-tools](https://github.com/duckdb/extension-ci-tools) for automated building and testing across platforms.

See `.github/workflows/MainDistributionPipeline.yml` for the CI configuration.

## License

This extension follows the licensing of its components:
- NSV parser: MIT (see `vendor/nsv-rust/`)
- DuckDB: MIT
- This extension code: MIT

## Links

- [NSV Format Specification](https://github.com/nsv-format/nsv)
- [NSV Rust Parser](https://github.com/nsv-format/nsv-rust)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview.html)
