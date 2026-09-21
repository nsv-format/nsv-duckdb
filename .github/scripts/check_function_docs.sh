#!/bin/sh
# Assert every function the nsv extension registers has a description.
#
# Snapshots duckdb_functions() before and after LOAD, diffs them,
# and fails if any new row has a NULL description.
#
# Usage: check_function_docs.sh <duckdb-binary> <extension-path>

set -e

duckdb="${1:?Usage: check_function_docs.sh <duckdb> <extension>}"
ext="${2:?Usage: check_function_docs.sh <duckdb> <extension>}"

undocumented=$("$duckdb" -unsigned -noheader -csv :memory: <<SQL
CREATE TEMP TABLE pre AS
  SELECT function_name, function_type, parameters
  FROM duckdb_functions();

LOAD '${ext}';

CREATE TEMP TABLE post AS
  SELECT function_name, function_type, parameters, description
  FROM duckdb_functions();

SELECT p.function_name, p.function_type
FROM post p
ANTI JOIN pre b
  ON  b.function_name = p.function_name
  AND b.function_type = p.function_type
  AND b.parameters    = p.parameters
WHERE p.description IS NULL;
SQL
)

if [ -n "$undocumented" ]; then
    echo "Functions registered by nsv extension without a description:"
    echo "$undocumented"
    exit 1
fi

echo "All extension functions have descriptions."
