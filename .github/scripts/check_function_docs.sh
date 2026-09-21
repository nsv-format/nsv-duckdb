#!/bin/sh
# Assert every function the nsv extension registers has a description.
set -e

ext="${1:?path to built nsv.duckdb_extension}"

undocumented=$(duckdb -unsigned -noheader -csv :memory: <<SQL
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
    echo "nsv functions without a description:"
    echo "$undocumented"
    exit 1
fi

echo "All nsv functions have descriptions."
