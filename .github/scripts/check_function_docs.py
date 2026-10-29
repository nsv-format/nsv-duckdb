"""Assert every function the nsv extension registers has a description and
real parameter names (not the col0, col1, ... DuckDB falls back to)."""
import re
import sys

import duckdb

ext = sys.argv[1]
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
COLUMNS = "function_name, function_type, parameters, description"
con.execute(f"CREATE TEMP TABLE pre AS SELECT {COLUMNS} FROM duckdb_functions()")
con.execute(f"LOAD '{ext}'")
added = con.execute(
    f"""SELECT {COLUMNS} FROM duckdb_functions()
    EXCEPT SELECT {COLUMNS} FROM pre"""
).fetchall()

if not added:
    sys.exit("no functions registered by the extension")

failed = False
for name, kind, params, description in added:
    if not description:
        print(f"{kind} {name}: no description")
        failed = True
    unnamed = [p for p in params if re.fullmatch(r"col\d+", p)]
    if unnamed:
        print(f"{kind} {name}: unnamed parameters {unnamed}")
        failed = True

if failed:
    sys.exit(1)
print(f"All {len(added)} nsv functions are documented.")
