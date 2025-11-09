#!/usr/bin/env python3
import duckdb
from nsv_ext import read_nsv, to_nsv

con = duckdb.connect()

# Read
users = read_nsv('examples/users.nsv', con)
print(users)

# Query
con.register('users', users)
result = con.query("SELECT * FROM users WHERE CAST(age AS INT) > 25")
print("\nFiltered:")
print(result)

# Write
to_nsv(result, 'output.nsv')
print("\nWrote output.nsv\n")

# Read back
print(read_nsv('output.nsv', con))
