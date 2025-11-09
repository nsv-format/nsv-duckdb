# NSV-DuckDB

A DuckDB integration for NSV (Newline-Separated Values) format processing.

## What is NSV?

NSV (Newline-Separated Values) is a text file format for encoding sequences of sequences. It's designed as an alternative to CSV with several advantages:

- **Better Git diffs**: Each cell is on its own line, making changes more granular
- **Simpler implementation**: No quote escaping complexity
- **Better performance**: Simpler parsing rules
- **Vim-friendly**: Easy navigation with standard text editor commands

Learn more at: https://github.com/nsv-format/nsv

## Features

This PoC provides:

- **Read NSV files** into DuckDB tables
- **Query NSV data** using SQL
- **Export DuckDB results** to NSV format
- **Handle complex data** including newlines, backslashes, and empty cells

## Installation

1. Clone this repository:
```bash
git clone https://github.com/nsv-format/nsv-duckdb
cd nsv-duckdb
```

2. Install dependencies:
```bash
pip install -r requirements.txt
pip install pandas  # Required for data conversions
```

3. Clone the NSV Python library (required):
```bash
cd ..
git clone https://github.com/nsv-format/nsv-python.git
cd nsv-duckdb
```

## Quick Start

### Reading an NSV file

```python
from nsv_duckdb import read_nsv

# Read an NSV file into a DuckDB relation
data = read_nsv('examples/simple.nsv')

# Display the data
print(data)

# Convert to pandas DataFrame
df = data.df()
print(df)
```

### Querying NSV data with SQL

```python
from nsv_duckdb import NSVDuckDB

nsv_db = NSVDuckDB()

# Read and register the data
sales = nsv_db.read_nsv('examples/sales.nsv')
nsv_db.con.register('sales', sales)

# Run SQL queries
result = nsv_db.con.query("""
    SELECT product, SUM(CAST(quantity AS INTEGER)) as total_qty
    FROM sales
    GROUP BY product
""")
print(result)
```

### Writing to NSV format

```python
import duckdb
from nsv_duckdb import to_nsv

con = duckdb.connect()

# Query some data
result = con.query("""
    SELECT * FROM (VALUES
        ('Alice', 30, 'Engineer'),
        ('Bob', 25, 'Designer')
    ) AS t(name, age, role)
""")

# Export to NSV
to_nsv(result, 'output.nsv')
```

## Examples

The `examples/` directory contains sample NSV files:

- `simple.nsv` - Basic table with 3 columns
- `complex.nsv` - Demonstrates newlines, escapes, and empty cells
- `sales.nsv` - Sample sales data for SQL queries

## Running the Demo

Interactive demo showcasing all features:

```bash
python demo.py
```

Quick automated tests:

```bash
python test.py
```

## NSV Format Primer

### Basic Structure

Cells are separated by single newlines, rows by double newlines:

```nsv
col1
col2

value1
value2

value3
value4
```

### Escaping Rules

- Empty cells: single backslash `\`
- Literal backslash: `\\`
- Literal newline: `\n`

Example:

```nsv
id
description

1
Simple text

2
Text with\nnewline

3
\

4
Backslash: \\
```

## API Reference

### `NSVDuckDB` Class

Main class for NSV-DuckDB integration.

**Methods:**

- `read_nsv(filepath, column_names=None)` - Read NSV file into DuckDB relation
- `to_nsv(relation, filepath, include_header=True)` - Write relation to NSV file
- `query_nsv(filepath, sql, column_names=None)` - Read and query in one step

### Convenience Functions

- `read_nsv(filepath, column_names=None, con=None)` - Quick read function
- `to_nsv(relation, filepath, include_header=True)` - Quick write function

## Implementation Notes

This is a **Proof of Concept** demonstrating NSV integration with DuckDB using Python.

### Current Approach

- Uses DuckDB's Python API
- Leverages the nsv-python library for parsing
- Converts data through pandas DataFrames

### Future Enhancements

Potential improvements for production use:

- Native C++ DuckDB extension
- Streaming support for large files
- Custom DuckDB table function
- COPY statement integration
- Zero-copy data transfer
- Parallel parsing

## Project Structure

```
nsv-duckdb/
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── nsv_duckdb.py         # Main integration module
├── demo.py               # Interactive demo
├── test.py               # Automated tests
└── examples/             # Sample NSV files
    ├── simple.nsv
    ├── complex.nsv
    └── sales.nsv
```

## Contributing

This is a PoC in the Request for Comments stage. Feedback and contributions are welcome!

- NSV Specification: https://github.com/nsv-format/nsv
- NSV Python Implementation: https://github.com/nsv-format/nsv-python

## License

MIT License (to be confirmed)

## See Also

- [NSV Format Specification](https://github.com/nsv-format/nsv)
- [NSV Python](https://github.com/nsv-format/nsv-python)
- [NSV Rust](https://github.com/nsv-format/nsv-rust)
- [DuckDB](https://duckdb.org/)
