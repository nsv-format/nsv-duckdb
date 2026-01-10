# NSV DuckDB Extension

A [DuckDB](https://duckdb.org/) extension for reading [NSV (Newline-Separated Values)](https://github.com/nsv-format/nsv) files.

## Quick Start

```sql
LOAD './build/release/extension/nsv/nsv.duckdb_extension';

-- Read NSV file (types auto-detected)
SELECT * FROM read_nsv('data.nsv');

-- Aggregations work directly on detected numeric types
SELECT city, AVG(age) FROM read_nsv('users.nsv') GROUP BY city;

-- Force all columns to VARCHAR
SELECT * FROM read_nsv('data.nsv', all_varchar=true);
```

## What is NSV?

NSV is a tabular format where each field is on its own line, records are separated by blank lines:

```
name
age

Alice
30

Bob
25
```

Escaping: `\` = empty string, `\n` = newline, `\\` = backslash

## Building

**Prerequisites:** CMake 3.5+, C++ compiler, [Rust toolchain](https://rustup.rs/)

```bash
git clone --recursive https://github.com/nsv-format/nsv-duckdb.git
cd nsv-duckdb
make
```

The extension is built to `build/release/extension/nsv/nsv.duckdb_extension`.

First build compiles DuckDB from source (~20-30 min).

**Platform notes:**
- **musl/Alpine:** `rustup target add x86_64-unknown-linux-musl` first
- **macOS:** Use absolute paths when loading; delete any pre-built Linux `.a` files first
- **Windows:** Use Visual Studio Developer Command Prompt with `cmake`

## Usage

```bash
duckdb -unsigned  # Required for unsigned extensions
```

```sql
LOAD '/path/to/nsv.duckdb_extension';  -- macOS needs absolute path

SELECT * FROM read_nsv('data.nsv');
SELECT * FROM read_nsv('data.nsv', all_varchar=true);  -- Disable type detection
```

## Type Detection

Columns are auto-narrowed by sampling up to 1000 rows:

| Type | Examples |
|------|----------|
| BOOLEAN | `true`, `false` |
| BIGINT | `42`, `-100` |
| DOUBLE | `3.14`, `1.0e10` |
| DATE | `2024-01-15` |
| TIMESTAMP | `2024-01-15 10:30:00` |
| VARCHAR | Fallback |

## Architecture

```
src/                  C++ DuckDB table function
rust-glue/            Rust FFI wrapper
vendor/nsv-rust/      Vendored NSV parser (submodule)
vendor/nsv-spec/      NSV specification (submodule)
test/sql/             DuckDB SQL tests
```

The Rust parser is compiled to a static library and linked into the C++ extension via FFI.

## Troubleshooting

**"relative path not allowed in hardened program" (macOS)**
Use absolute path: `LOAD '/full/path/to/nsv.duckdb_extension';`

**"built for DuckDB version X"**
Update submodules and rebuild:
```bash
git submodule update --init --recursive && make clean && make
```

**"archive member '/' not a mach-o file" (macOS)**
Remove Linux binaries:
```bash
rm -f rust-glue/target/release/libnsv_ffi.a
rm -f rust-glue/target/x86_64-unknown-linux-musl/release/libnsv_ffi.a
make clean && make
```

## Running Tests

```bash
make test
```

## Links

- [NSV Specification](https://github.com/nsv-format/nsv)
- [NSV Rust Parser](https://github.com/nsv-format/nsv-rust)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview.html)

## License

MIT (this extension, NSV parser, and DuckDB)

---

## Project Status

### Core Features
- [x] `read_nsv()` table function
- [x] Type narrowing (BOOLEAN, BIGINT, DOUBLE, DATE, TIMESTAMP, VARCHAR)
- [x] `all_varchar` option to disable type detection
- [ ] `write_nsv()` / `COPY TO` support

### Testing
- [x] Basic reading and type detection
- [x] Arithmetic on narrowed types
- [x] Boolean filtering
- [x] `all_varchar` option
- [ ] DATE/TIMESTAMP detection tests
- [ ] Escape sequence tests (`\n`, `\\`, `\`)
- [ ] Ragged row handling tests
- [ ] Error handling tests

### Distribution
- [x] Builds from source (Linux, macOS, Windows)
- [x] CI/CD with DuckDB extension-ci-tools
- [x] GitHub Releases on version tags
- [ ] DuckDB Community Extensions registry

### Documentation
- [x] README with build/usage instructions
- [x] Vendored NSV spec for reference
- [ ] API reference / examples page
