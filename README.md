# NSV-DuckDB

DuckDB extension for [NSV](https://github.com/nsv-format/nsv) (Newline-Separated Values) format.

## Install

```bash
pip install -r requirements.txt
```

## Usage

```python
import duckdb
from nsv_ext import read_nsv, to_nsv

con = duckdb.connect()

# Read
data = read_nsv('file.nsv', con)

# Query
con.register('data', data)
result = con.query("SELECT * FROM data WHERE col > 10")

# Write
to_nsv(result, 'output.nsv')
```

## Demo

```bash
python demo.py
```

## NSV Format

Cells separated by newlines, rows by double newlines:

```nsv
name
age

Alice
30

Bob
25
```

**Escaping:** Empty=`\`, Newline=`\n`, Backslash=`\\`

More: https://github.com/nsv-format/nsv
