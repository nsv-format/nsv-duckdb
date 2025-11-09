# NSV-DuckDB

DuckDB extension for [NSV](https://github.com/nsv-format/nsv) (Newline-Separated Values) format.

## Architecture

- **Rust** - NSV parser (from nsv-rust) exposed via C FFI
- **C++** - DuckDB table function (in `src/nsv_extension.cpp`)
- **Python** - Working PoC using Rust FFI via ctypes

No pandas dependency. Parser is in Rust, integrated via FFI.

## Quick Start

```bash
# Build Rust library
cd ../nsv-rust && cargo build --release && cd ../nsv-duckdb

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
- `nsv_duckdb.py` - Python PoC using Rust FFI
- `../nsv-rust/src/ffi.rs` - Rust FFI wrapper
- `demo.py` - Working demonstration
- `test_ffi.c` - C FFI test

## Status

**PoC complete.** Python demo works. C++ extension code written, needs DuckDB build system to compile.

More: https://github.com/nsv-format/nsv
