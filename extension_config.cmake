# This file is included by DuckDB's build system. It specifies which extension to load

# NSV extension requires Rust/Cargo to build
# Extension from this repo
duckdb_extension_load(nsv
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
