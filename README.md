# NSV-DuckDB

Loadable DuckDB extension for [NSV](https://github.com/nsv-format/nsv) (Newline-Separated Values) format.

## Quick Start

```bash
# Download or build the extension, then load it in DuckDB:
duckdb -unsigned
D LOAD './nsv.duckdb_extension';
D SELECT * FROM read_nsv('examples/users.nsv');
```

## Architecture

- **Rust** - NSV parser from nsv-rust repository with FFI wrapper
- **C++** - DuckDB loadable extension using table function API

The extension uses the Rust NSV parser via FFI (no code duplication).

## NSV Format

```nsv
name
age

Alice
30

Bob
25
```

**Escaping:** `\` = empty, `\n` = newline, `\\` = backslash

## Building the Extension

**Prerequisites:**
- [Rust toolchain](https://rustup.rs/) - Required for building the NSV parser
- CMake 3.5+
- C++ compiler

**Build:**

```bash
# Clone with submodules
git clone --recursive https://github.com/nsv-format/nsv-duckdb.git
cd nsv-duckdb

# Build (takes ~30 minutes first time)
make

# Output: build/release/extension/nsv/nsv.duckdb_extension
```

### Using the Loadable Extension

```bash
# Load and query NSV files with any DuckDB installation:
duckdb -unsigned
D LOAD './nsv.duckdb_extension';
D SELECT * FROM read_nsv('examples/users.nsv');
D SELECT * FROM read_nsv('data.nsv') WHERE CAST(age AS INT) > 25;
D SELECT city, COUNT(*) FROM read_nsv('users.nsv') GROUP BY city;
```

The `-unsigned` flag allows loading unsigned extensions for development.

## Files

- `src/nsv_extension.cpp` - C++ DuckDB table function
- `src/include/nsv_ffi.h` - C FFI header
- `rust-ffi/` - Rust crate with FFI wrapper (uses nsv from crates.io)
- `nsv_duckdb.py` - Python PoC using Rust FFI
- `demo.py` - Working demonstration
- `test_ffi.c` - C FFI test

## Status

**Loadable extension working!**
- ✅ Python demo working (uses Rust via FFI)
- ✅ C++ loadable extension built and tested
- ✅ Works with any DuckDB installation via `LOAD` command
- ✅ Supports WHERE, GROUP BY, JOIN, and all SQL operations
- ✅ Uses Rust parser via FFI (no reimplementation needed)

Extension size: ~11MB (includes Rust parser)
Tested with DuckDB v1.4.1+

More: https://github.com/nsv-format/nsv
