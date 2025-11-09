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

The C++ extension is now **fully functional** and built into DuckDB. To build DuckDB with the NSV extension:

```bash
# Build Rust FFI library
cd rust-ffi && cargo build --release && cd ..

# Build DuckDB with NSV extension (takes ~15 minutes)
mkdir -p build/release && cd build/release
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_EXTENSIONS="nsv" ../../duckdb
make shell -j4
cd ../..
```

The NSV extension is statically linked into DuckDB and available immediately - no `LOAD` command needed.

### Using the Extension

```bash
# Query NSV files directly
./build/release/duckdb

# In DuckDB shell:
D SELECT * FROM read_nsv('examples/users.nsv');
D SELECT * FROM read_nsv('data.nsv') WHERE CAST(age AS INT) > 25;
D SELECT city, COUNT(*) FROM read_nsv('users.nsv') GROUP BY city;
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

**Fully functional!**
- ✅ Python demo working (uses Rust via FFI)
- ✅ C++ extension built and tested
- ✅ DuckDB CLI can query NSV files with `read_nsv()`
- ✅ Supports WHERE, GROUP BY, JOIN, and all SQL operations

Tested with DuckDB v1.5.0-dev2368.

More: https://github.com/nsv-format/nsv
