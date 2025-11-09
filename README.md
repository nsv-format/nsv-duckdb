# NSV-DuckDB

DuckDB extension for [NSV](https://github.com/nsv-format/nsv) (Newline-Separated Values) format.

## Architecture

- **Rust** - NSV parser from crates.io (nsv 0.0.2) with FFI wrapper in `rust-ffi/`
- **C++** - DuckDB table function (in `src/nsv_extension.cpp`)
- **Python** - Working PoC using Rust FFI via ctypes

No pandas dependency. Uses nsv crate from crates.io.

## Quick Start

```bash
# Build Rust library (uses nsv from crates.io)
cd rust-ffi && cargo build --release && cd ..

# Run demo
python demo.py
```

## Usage

```python
import duckdb
from nsv_duckdb import read_nsv, to_nsv

con = duckdb.connect()

# Read NSV
users = read_nsv('users.nsv', con)

# Query with SQL
con.register('users', users)
result = con.query("SELECT * FROM users WHERE age > 25")

# Write NSV
to_nsv(result, 'output.nsv')
```

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

## Building C++ Extension

The full C++ extension is in `src/nsv_extension.cpp`. To build as a loadable DuckDB extension:

```bash
# Requires DuckDB build environment
make release
```

This uses the Rust parser via FFI - no reimplementation needed.

## Files

- `src/nsv_extension.cpp` - C++ DuckDB table function
- `src/include/nsv_ffi.h` - C FFI header
- `rust-ffi/` - Rust crate with FFI wrapper (uses nsv from crates.io)
- `nsv_duckdb.py` - Python PoC using Rust FFI
- `demo.py` - Working demonstration
- `test_ffi.c` - C FFI test

## Status

**PoC complete.**
- ✅ Python demo working (uses Rust via FFI)
- ✅ C++ extension code written
- ⏳ DuckDB submodule cloning (required for C++ build)

More: https://github.com/nsv-format/nsv
