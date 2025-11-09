#!/usr/bin/env python3
"""Demo of NSV DuckDB extension using Rust FFI"""
import duckdb
from nsv_duckdb import read_nsv, to_nsv

print("NSV DuckDB PoC - Using Rust parser via FFI\n")

con = duckdb.connect()

# Read NSV
print("1. Reading users.nsv...")
users = read_nsv('examples/users.nsv', con)
print(users)
print()

# Query
print("2. SQL Query - users over 25...")
con.register('users', users)
result = con.query("SELECT * FROM users WHERE CAST(age AS INT) > 25 ORDER BY age")
print(result)
print()

# Write
print("3. Writing query result to output.nsv...")
to_nsv(result, 'output.nsv')
print("Done!\n")

# Read back
print("4. Reading output.nsv back...")
output_data = read_nsv('output.nsv', con)
print(output_data)
