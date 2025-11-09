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
- **Python** - Working PoC using Rust FFI via ctypes

No pandas dependency. Uses nsv from crates.io.

## Python Demo (Quick Alternative)

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

## Building the Extension

The extension is built using the [DuckDB extension-template](https://github.com/duckdb/extension-template):

```bash
# Clone and setup extension-template
git clone https://github.com/duckdb/extension-template.git
cd extension-template
python3 scripts/bootstrap-template.py nsv

# Add nsv-rust parser
git submodule add https://github.com/nsv-format/nsv-rust.git vendor/nsv-rust
git submodule update --init --recursive

# Copy NSV extension files from this repo:
# - rust-glue/ (Rust FFI wrapper)
# - src/nsv_extension.cpp (C++ extension code)
# - CMakeLists.txt (build configuration)

# Build (takes ~30 minutes)
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
