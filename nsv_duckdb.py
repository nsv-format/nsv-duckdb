"""
NSV DuckDB Extension PoC

Uses Rust NSV parser via FFI (no pandas).
This demonstrates the architecture for a full C++ extension.

For production: Compile src/nsv_extension.cpp as a DuckDB loadable extension.
"""
import ctypes
import os
import duckdb
from pathlib import Path

# Load Rust library
LIB_PATH = Path(__file__).parent / "rust-ffi/target/release/libnsv_ffi.so"
if not LIB_PATH.exists():
    raise RuntimeError(f"Rust library not found at {LIB_PATH}. Run: cd rust-ffi && cargo build --release")

lib = ctypes.CDLL(str(LIB_PATH))

# Define FFI functions
lib.nsv_parse.argtypes = [ctypes.c_char_p]
lib.nsv_parse.restype = ctypes.c_void_p

lib.nsv_row_count.argtypes = [ctypes.c_void_p]
lib.nsv_row_count.restype = ctypes.c_size_t

lib.nsv_col_count.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
lib.nsv_col_count.restype = ctypes.c_size_t

lib.nsv_get_cell.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t]
lib.nsv_get_cell.restype = ctypes.c_void_p

lib.nsv_free_string.argtypes = [ctypes.c_void_p]
lib.nsv_free_string.restype = None

lib.nsv_free.argtypes = [ctypes.c_void_p]
lib.nsv_free.restype = None


def read_nsv(filename, con=None):
    """
    Read NSV file using Rust parser via FFI.
    No pandas dependency.
    """
    if con is None:
        con = duckdb.connect()

    # Read file
    with open(filename, 'r') as f:
        content = f.read()

    # Parse with Rust
    data_ptr = lib.nsv_parse(content.encode('utf-8'))
    if not data_ptr:
        raise ValueError("Failed to parse NSV")

    try:
        row_count = lib.nsv_row_count(data_ptr)
        if row_count == 0:
            raise ValueError("Empty NSV file")

        # Get header (first row)
        col_count = lib.nsv_col_count(data_ptr, 0)
        columns = []
        for col in range(col_count):
            cell_ptr = lib.nsv_get_cell(data_ptr, 0, col)
            if cell_ptr:
                cell_str = ctypes.string_at(cell_ptr).decode('utf-8')
                columns.append(cell_str)
                lib.nsv_free_string(cell_ptr)
            else:
                columns.append(f'col{col}')

        # Get data rows (skip header)
        rows = []
        for row_idx in range(1, row_count):
            row = []
            this_col_count = lib.nsv_col_count(data_ptr, row_idx)
            for col_idx in range(col_count):
                if col_idx < this_col_count:
                    cell_ptr = lib.nsv_get_cell(data_ptr, row_idx, col_idx)
                    if cell_ptr:
                        cell_str = ctypes.string_at(cell_ptr).decode('utf-8')
                        row.append(cell_str)
                        lib.nsv_free_string(cell_ptr)
                    else:
                        row.append(None)
                else:
                    row.append(None)
            rows.append(tuple(row))

    finally:
        lib.nsv_free(data_ptr)

    # Create DuckDB table using native SQL
    if not rows:
        col_list = ', '.join(f'"{c}"' for c in columns)
        empty_vals = tuple([""] * len(columns))
        return con.query(f'SELECT * FROM (VALUES {empty_vals}) t({col_list}) WHERE FALSE')

    # Use VALUES to insert data
    def escape_sql(s):
        if s is None:
            return 'NULL'
        return "'" + str(s).replace("'", "''") + "'"

    values = ', '.join(
        '(' + ', '.join(escape_sql(cell) for cell in row) + ')'
        for row in rows
    )
    col_list = ', '.join(f'"{c}"' for c in columns)

    return con.query(f'SELECT * FROM (VALUES {values}) AS t({col_list})')


def to_nsv(rel, filename):
    """Write DuckDB relation to NSV file."""
    # Get schema
    cols = [desc[0] for desc in rel.description]

    # Fetch rows
    result = rel.fetchall()

    # Build NSV manually (could use Rust dumps via FFI too)
    lines = []

    # Header
    for col in cols:
        lines.append(escape_nsv(str(col)))
    lines.append('')

    # Data
    for row in result:
        for cell in row:
            lines.append(escape_nsv(str(cell) if cell is not None else ''))
        lines.append('')

    with open(filename, 'w') as f:
        f.write('\n'.join(lines))


def escape_nsv(s):
    """Escape cell for NSV format."""
    if s == '':
        return '\\'
    if '\n' in s or '\\' in s:
        return s.replace('\\', '\\\\').replace('\n', '\\n')
    return s
