# Building the NSV DuckDB Extension

## Prerequisites

- **CMake** 3.5+
- **C++ compiler** (GCC, Clang, or MSVC)
- **Rust toolchain** — install from [rustup.rs](https://rustup.rs/)

## Build

```bash
git clone --recursive https://github.com/nsv-format/nsv-duckdb.git
cd nsv-duckdb
make
```

The loadable extension will be at `build/release/extension/nsv/nsv.duckdb_extension`.

The first build compiles DuckDB from source and takes 20–30 minutes.

## Platform Notes

**musl/Alpine** — add the musl target before building:
```bash
rustup target add x86_64-unknown-linux-musl
```

**macOS** — requires absolute paths when loading extensions (hardened runtime). If pre-built Linux libraries exist, remove them first:
```bash
rm -f rust-ffi/target/release/libnsv_ffi.a
rm -f rust-ffi/target/x86_64-unknown-linux-musl/release/libnsv_ffi.a
make clean && make
```

**Windows** — use Visual Studio Developer Command Prompt:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -S . -B build
cmake --build build --config Release
```

## Loading

```bash
duckdb -unsigned
```
```sql
LOAD './build/release/extension/nsv/nsv.duckdb_extension';
```

On macOS, use an absolute path:
```sql
LOAD '/absolute/path/to/build/release/extension/nsv/nsv.duckdb_extension';
```

Tip — use `$(pwd)` in a heredoc:
```bash
duckdb -unsigned << EOF
LOAD '$(pwd)/build/release/extension/nsv/nsv.duckdb_extension';
SELECT * FROM read_nsv('data.nsv');
EOF
```

## Running Tests

```bash
make test
```

Tests are in `test/sql/nsv.test` using DuckDB's sqllogictest format.

## Troubleshooting

### "relative path not allowed in hardened program" (macOS)

Use an absolute path — see [Loading](#loading) above.

### "The file was built for DuckDB version X"

The DuckDB submodule is out of sync. Update and rebuild:
```bash
git submodule update --init --recursive
make clean && make
```

### "archive member '/' not a mach-o file" (macOS/ARM64)

Pre-built Linux `.a` files are present. Remove them:
```bash
rm -f rust-ffi/target/release/libnsv_ffi.a
rm -f rust-ffi/target/x86_64-unknown-linux-musl/release/libnsv_ffi.a
make clean && make
```

## Project Structure

```
nsv-duckdb/
├── src/                          # C++ extension code
│   ├── nsv_extension.cpp         # DuckDB table function + COPY handler
│   └── include/
├── rust-ffi/                     # Rust FFI wrapper around the nsv crate
│   ├── src/lib.rs
│   └── Cargo.toml
├── test/sql/                     # SQL tests
├── duckdb/                       # DuckDB submodule (vendored)
├── extension-ci-tools/           # DuckDB CI tools submodule (vendored)
└── CMakeLists.txt
```

The build first compiles `rust-ffi/` into a static library (`libnsv_ffi.a`), then links it into the C++ DuckDB extension. The result is a single `.duckdb_extension` file with everything statically linked.

## CI/CD

Uses DuckDB's [extension-ci-tools](https://github.com/duckdb/extension-ci-tools). See `.github/workflows/MainDistributionPipeline.yml`.
