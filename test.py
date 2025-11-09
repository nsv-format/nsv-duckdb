#!/usr/bin/env python3
"""
Quick test script for NSV DuckDB integration.
Runs automated tests without user interaction.
"""

import sys
import os

# Install dependencies if needed
try:
    import pandas as pd
    import duckdb
except ImportError:
    print("Installing dependencies...")
    os.system(f"{sys.executable} -m pip install pandas duckdb -q")
    import pandas as pd
    import duckdb

from nsv_duckdb import NSVDuckDB


def test_basic_operations():
    """Test basic NSV reading and writing."""
    print("Testing basic NSV operations...")

    nsv_db = NSVDuckDB()

    # Test 1: Read simple file
    print("  ✓ Reading simple.nsv...")
    rel = nsv_db.read_nsv('examples/simple.nsv')
    df = rel.df()
    assert len(df) == 3, "Expected 3 rows"
    assert list(df.columns) == ['name', 'age', 'city'], "Column mismatch"
    assert df.iloc[0]['name'] == 'Alice', "Data mismatch"

    # Test 2: Read complex file with escapes
    print("  ✓ Reading complex.nsv with escapes...")
    rel = nsv_db.read_nsv('examples/complex.nsv')
    df = rel.df()
    assert len(df) == 5, "Expected 5 rows"

    # Check newline handling
    row2 = df[df['id'] == '2'].iloc[0]
    assert '\n' in row2['description'], "Newline not preserved"

    # Check empty cell
    row3 = df[df['id'] == '3'].iloc[0]
    assert row3['description'] == '', "Empty cell not handled correctly"

    # Check backslash
    row4 = df[df['id'] == '4'].iloc[0]
    assert '\\' in row4['description'], "Backslash not preserved"

    # Test 3: SQL queries
    print("  ✓ Running SQL queries...")
    sales = nsv_db.read_nsv('examples/sales.nsv')
    nsv_db.con.register('sales', sales)
    result = nsv_db.con.query("SELECT COUNT(*) as cnt FROM sales").fetchone()
    assert result[0] == 4, "Expected 4 sales records"

    # Test 4: Write and read back
    print("  ✓ Writing and reading back NSV...")
    from nsv_duckdb import to_nsv

    con = duckdb.connect()
    test_data = con.query("""
        SELECT * FROM (VALUES
            ('test1', 'value1'),
            ('test2', 'value2')
        ) AS t(col1, col2)
    """)

    output_file = 'examples/test_output.nsv'
    to_nsv(test_data, output_file)

    # Read back
    nsv_db2 = NSVDuckDB(con)
    rel = nsv_db2.read_nsv(output_file)
    df = rel.df()
    assert len(df) == 2, "Expected 2 rows"
    assert df.iloc[0]['col1'] == 'test1', "Data not preserved"

    print("\n✅ All tests passed!")


if __name__ == "__main__":
    try:
        test_basic_operations()
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
