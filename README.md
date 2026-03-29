# NSV DuckDB Extension

A [DuckDB](https://duckdb.org/) extension for reading and writing [NSV](https://nsv-format.org) files.

NSV can be used as a simple tabular data format where each cell occupies its own line and rows are separated by blank lines.
This extension follows CSV conventions (headers, type narrowing) to the extent DuckDB's CSV reader does.

## Quick Start

```sql
INSTALL nsv FROM community;
LOAD 'nsv';

-- Read an NSV file
-- By default, types are auto-detected, first row interpreted as column names
SELECT * FROM read_nsv('data.nsv');
-- Don't interpret the first row as column names
SELECT * FROM read_nsv('data.nsv', header=false);
-- Don't infer types
SELECT * FROM read_nsv('data.nsv', all_varchar=true);

-- Write query results as NSV
COPY (SELECT * FROM my_table) TO 'output.nsv' (FORMAT nsv);
```

## Installation

Community extensions are the simplest way to install, but contain at most one version of the extension at any given time.  
If you need an arbitrary release from [releases](https://github.com/nsv-format/nsv-duckdb/releases/), download the artefact matching your architecture.  
The rest is the same process as for locally built artefacts: run `duckdb -unsigned`, then `LOAD '/path/to/nsv.duckdb_extension';` (must be absolute path on macOS).

Locally built artefacts end up in `./build/release/extension/nsv/nsv.duckdb_extension`.  
Extensions only work with the one version of DuckDB they were built for, so if you need a different combination, you may need to [build it yourself](BUILDING.md).

## Type Detection

Column types are automatically narrowed by sampling data:

| Type | Examples |
|------|----------|
| `BOOLEAN` | `true`, `false` |
| `BIGINT` | `42`, `-100` |
| `DOUBLE` | `3.14`, `1.0e10` |
| `DATE` | `2024-01-15` |
| `TIMESTAMP` | `2024-01-15 10:30:00` |
| `VARCHAR` | Everything else (fallback) |

Pass `all_varchar=true` to disable type detection.

## Column Projection

Only the columns you `SELECT` are parsed — unreferenced columns are skipped entirely.
For wide files where you need a few columns, this means less work for the parser and less data materialized in memory.

## Building

See [BUILDING.md](BUILDING.md) for build instructions, platform notes, and troubleshooting.

## Links

- [NSV Format](https://nsv-format.org) — specification
- [nsv](https://crates.io/crates/nsv) — Rust parser powering this extension
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview.html)
