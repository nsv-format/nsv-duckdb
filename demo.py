#!/usr/bin/env python3
"""
NSV DuckDB PoC Demo

This script demonstrates the NSV DuckDB integration capabilities.
"""

import sys
import os

# Ensure pandas is available (required for DuckDB relation conversions)
try:
    import pandas as pd
except ImportError:
    print("Installing pandas...")
    os.system(f"{sys.executable} -m pip install pandas -q")
    import pandas as pd

# Ensure duckdb is available
try:
    import duckdb
except ImportError:
    print("Installing duckdb...")
    os.system(f"{sys.executable} -m pip install duckdb -q")
    import duckdb

# Import our NSV DuckDB module
from nsv_duckdb import NSVDuckDB, read_nsv, to_nsv


def demo_basic_read():
    """Demo 1: Basic NSV file reading."""
    print("=" * 60)
    print("DEMO 1: Reading a simple NSV file")
    print("=" * 60)

    nsv_db = NSVDuckDB()

    # Read the simple NSV file
    rel = nsv_db.read_nsv('examples/simple.nsv')

    print("\nData from simple.nsv:")
    print(rel)

    print("\nAs DataFrame:")
    print(rel.df())
    print()


def demo_sql_query():
    """Demo 2: SQL queries on NSV data."""
    print("=" * 60)
    print("DEMO 2: SQL queries on NSV data")
    print("=" * 60)

    nsv_db = NSVDuckDB()

    # Read sales data
    sales = nsv_db.read_nsv('examples/sales.nsv')

    print("\nOriginal sales data:")
    print(sales)

    # Register and query
    nsv_db.con.register('sales', sales)

    print("\nTotal sales by product:")
    result = nsv_db.con.query("""
        SELECT
            product,
            SUM(CAST(quantity AS INTEGER)) as total_quantity,
            ROUND(SUM(CAST(quantity AS INTEGER) * CAST(price AS DECIMAL)), 2) as total_revenue
        FROM sales
        GROUP BY product
        ORDER BY total_revenue DESC
    """)
    print(result)
    print()


def demo_complex_data():
    """Demo 3: Handling complex data with escapes."""
    print("=" * 60)
    print("DEMO 3: Complex data with newlines and escapes")
    print("=" * 60)

    nsv_db = NSVDuckDB()

    # Read complex data
    rel = nsv_db.read_nsv('examples/complex.nsv')

    print("\nData from complex.nsv:")
    df = rel.df()
    print(df)

    print("\n\nDetailed row-by-row view:")
    for idx, row in df.iterrows():
        print(f"\nRow {row['id']}:")
        print(f"  Description: {repr(row['description'])}")
        print(f"  Notes: {repr(row['notes'])}")
    print()


def demo_write_nsv():
    """Demo 4: Writing DuckDB results to NSV."""
    print("=" * 60)
    print("DEMO 4: Writing query results to NSV format")
    print("=" * 60)

    con = duckdb.connect()

    # Create some data
    print("\nCreating sample data...")
    con.execute("""
        CREATE TABLE employees AS
        SELECT * FROM (VALUES
            ('John Doe', 'Engineering', 75000),
            ('Jane Smith', 'Marketing', 65000),
            ('Bob Johnson', 'Engineering', 80000),
            ('Alice Williams', 'Sales', 70000)
        ) AS t(name, department, salary)
    """)

    result = con.query("SELECT * FROM employees")
    print("\nOriginal data:")
    print(result)

    # Write to NSV
    output_file = 'examples/employees_output.nsv'
    print(f"\nWriting to {output_file}...")
    to_nsv(result, output_file)

    # Read it back
    print("\nReading back the NSV file:")
    nsv_db = NSVDuckDB(con)
    rel = nsv_db.read_nsv(output_file)
    print(rel)

    # Show the raw NSV file
    print("\nRaw NSV file content:")
    with open(output_file, 'r') as f:
        content = f.read()
        print(content)
    print()


def demo_chaining():
    """Demo 5: Chaining operations."""
    print("=" * 60)
    print("DEMO 5: Chaining NSV operations")
    print("=" * 60)

    nsv_db = NSVDuckDB()

    # Read, transform, and write
    print("\nReading sales.nsv, filtering for Widget A, and writing to new file...")

    sales = nsv_db.read_nsv('examples/sales.nsv')
    nsv_db.con.register('sales', sales)

    filtered = nsv_db.con.query("""
        SELECT
            date,
            product,
            CAST(quantity AS INTEGER) as quantity,
            CAST(price AS DECIMAL(10,2)) as price,
            CAST(quantity AS INTEGER) * CAST(price AS DECIMAL(10,2)) as total
        FROM sales
        WHERE product = 'Widget A'
    """)

    print("\nFiltered data:")
    print(filtered)

    output_file = 'examples/widget_a_sales.nsv'
    to_nsv(filtered, output_file)

    print(f"\nWritten to {output_file}")
    print("\nRaw NSV content:")
    with open(output_file, 'r') as f:
        print(f.read())
    print()


def main():
    """Run all demos."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 58 + "║")
    print("║" + " " * 10 + "NSV DuckDB Integration PoC Demo" + " " * 17 + "║")
    print("║" + " " * 58 + "║")
    print("╚" + "=" * 58 + "╝")
    print("\n")

    demos = [
        demo_basic_read,
        demo_sql_query,
        demo_complex_data,
        demo_write_nsv,
        demo_chaining,
    ]

    for demo in demos:
        try:
            demo()
        except Exception as e:
            print(f"ERROR in {demo.__name__}: {e}")
            import traceback
            traceback.print_exc()

        input("Press Enter to continue to next demo...")
        print("\n")

    print("=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
