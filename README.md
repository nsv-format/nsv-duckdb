# NSV DuckDB Extension

A [DuckDB](https://duckdb.org/) extension for reading and writing [NSV](https://nsv-format.org) files.

NSV can be used as a simple tabular data format where each cell occupies its own line and rows are separated by blank lines.
This extension treats the first row as column names.

## Quick Start

```sql
LOAD 'nsv';

-- Read an NSV file (types are auto-detected)
SELECT * FROM read_nsv('data.nsv');

-- Write query results as NSV
COPY (SELECT * FROM my_table) TO 'output.nsv' (FORMAT nsv);
```

> **Local build?** Load the extension from its build path:
> ```sql
> LOAD './build/release/extension/nsv/nsv.duckdb_extension';
> ```
> On macOS, use an absolute path — see [Building](BUILDING.md).

## Reading

```sql
-- Types are auto-detected — no CAST needed
SELECT city, AVG(age) FROM read_nsv('users.nsv') GROUP BY city;

-- Joins
SELECT u.name, o.item
FROM read_nsv('users.nsv') u
JOIN read_nsv('orders.nsv') o ON u.id = o.user_id;

-- Disable type detection (all columns as VARCHAR)
SELECT * FROM read_nsv('data.nsv', all_varchar=true);
```

## Writing

```sql
-- From a table
COPY my_table TO 'output.nsv' (FORMAT nsv);

-- From a query
COPY (SELECT name, age FROM read_nsv('users.nsv') WHERE age > 25)
  TO 'filtered.nsv' (FORMAT nsv);
```

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
