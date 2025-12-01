# NSV DuckDB Extension

A loadable [DuckDB](https://duckdb.org/) extension for reading and writing [NSV (Newline-Separated Values)](https://github.com/nsv-format/nsv) files.

## Quick Start

```sql
-- Load the extension (use absolute path on macOS)
LOAD './build/release/extension/nsv/nsv.duckdb_extension';

-- Read an NSV file (types are auto-detected)
SELECT * FROM read_nsv('examples/users.nsv');

-- Types are automatically narrowed - no CAST needed for numeric operations
SELECT city, AVG(age) as avg_age
FROM read_nsv('examples/users.nsv')
GROUP BY city;

-- Force all columns to VARCHAR (disable type detection)
SELECT * FROM read_nsv('examples/users.nsv', all_varchar=true);
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
# If pre-built Linux libraries exist, remove them first
rm -f rust-glue/target/release/libnsv_ffi.a
rm -f rust-glue/target/x86_64-unknown-linux-musl/release/libnsv_ffi.a

# Build (will compile Rust for your platform)
make
```

**Note:** macOS requires absolute paths when loading extensions due to hardened runtime. See "Using the Extension" below.

**Windows:**
```bash
# Use Visual Studio Developer Command Prompt
cmake -DCMAKE_BUILD_TYPE=Release -S . -B build
cmake --build build --config Release
```

## Using the Extension

### Load the Extension

**Linux:**
```bash
duckdb -unsigned
```
```sql
D LOAD './build/release/extension/nsv/nsv.duckdb_extension';
```

**macOS:**
```bash
duckdb -unsigned
```
```sql
-- macOS requires absolute paths due to hardened runtime
D LOAD '/absolute/path/to/nsv-duckdb/build/release/extension/nsv/nsv.duckdb_extension';
```

**Tip:** Use `$(pwd)` to get the absolute path:
```bash
duckdb -unsigned << EOF
LOAD '$(pwd)/build/release/extension/nsv/nsv.duckdb_extension';
SELECT * FROM read_nsv('examples/users.nsv');
EOF
```

### Read NSV Files

```sql
-- Basic read (types auto-detected)
SELECT * FROM read_nsv('data.nsv');

-- With filters (no CAST needed - types are auto-detected)
SELECT * FROM read_nsv('users.nsv') WHERE age > 25;

-- Aggregations work directly on numeric columns
SELECT city, COUNT(*), AVG(salary) FROM read_nsv('users.nsv') GROUP BY city;

-- Joins
SELECT u.name, o.order_id
FROM read_nsv('users.nsv') u
JOIN read_nsv('orders.nsv') o ON u.id = o.user_id;

-- Disable type detection (all columns as VARCHAR)
SELECT * FROM read_nsv('data.nsv', all_varchar=true);
```

### Type Detection

The extension automatically detects and narrows column types by sampling data:

| Detected Type | Example Values |
|---------------|----------------|
| `BOOLEAN` | `true`, `false`, `TRUE`, `FALSE` |
| `BIGINT` | `42`, `-100`, `9999999` |
| `DOUBLE` | `3.14`, `-0.5`, `1.0e10` |
| `DATE` | `2024-01-15` |
| `TIMESTAMP` | `2024-01-15 10:30:00` |
| `VARCHAR` | Everything else (fallback) |

Use `all_varchar=true` to disable type detection and keep all columns as strings.

## Troubleshooting

### macOS: "relative path not allowed in hardened program"

**Error:**
```
IO Error: Extension "./build/..." could not be loaded:
dlopen(...) (relative path not allowed in hardened program)
```

**Solution:** Use an absolute path instead of a relative path:
```sql
-- Replace this with your actual path
LOAD '/Users/yourname/nsv-duckdb/build/release/extension/nsv/nsv.duckdb_extension';
```

### "The file was built for DuckDB version X and can only be loaded with that version"

**Error:**
```
Invalid Input Error: Failed to load '...', The file was built specifically
for DuckDB version '...' (this version of DuckDB is 'v1.4.1')
```

**Solution:** The DuckDB submodule may be out of sync. Update it to v1.4.1:
```bash
git submodule update --init --recursive
make clean
make
```

### macOS/ARM64: "ld: archive member '/' not a mach-o file"

**Error:**
```
ld: archive member '/' not a mach-o file in '.../libnsv_ffi.a'
```

**Solution:** Pre-built Linux libraries are incompatible with macOS. Remove them and rebuild:
```bash
rm -f rust-glue/target/release/libnsv_ffi.a
rm -f rust-glue/target/x86_64-unknown-linux-musl/release/libnsv_ffi.a
make clean
make
```

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
