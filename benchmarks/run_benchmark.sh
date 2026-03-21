#!/usr/bin/env bash
set -euo pipefail

DUCKDB="./build/release/duckdb"
DATA="benchmarks/data/bench_500k.nsv"
CSV_DATA="benchmarks/data/bench_500k.csv"
EXT_PUSHDOWN="benchmarks/builds/pushdown/nsv.duckdb_extension"
EXT_BATCH="benchmarks/builds/batch/nsv.duckdb_extension"
RUNS=5

echo "================================================================"
echo "  NSV DuckDB Extension: Filter Pushdown vs Batch Vectorized"
echo "  Data: 500K rows, 7 columns (~24MB)"
echo "  Runs per query: $RUNS (median reported)"
echo "================================================================"
echo ""

QUERIES=(
  "SELECT COUNT(*) FROM read_nsv('$DATA')"
  "SELECT COUNT(*) FROM read_nsv('$DATA') WHERE city = 'NYC'"
  "SELECT COUNT(*) FROM read_nsv('$DATA') WHERE salary > 200000"
  "SELECT COUNT(*) FROM read_nsv('$DATA') WHERE city = 'NYC' AND age > 50"
  "SELECT COUNT(*) FROM read_nsv('$DATA') WHERE city IN ('NYC', 'SF', 'Seattle')"
  "SELECT * FROM read_nsv('$DATA') WHERE id = 42"
  "SELECT name, salary FROM read_nsv('$DATA') WHERE salary > 200000"
)

QUERY_NAMES=(
  "Q1: Full scan (COUNT *)"
  "Q2: city = 'NYC' (~7%)"
  "Q3: salary > 200000 (~20%)"
  "Q4: city='NYC' AND age>50 (~2%)"
  "Q5: city IN (3 vals) (~21%)"
  "Q6: id = 42 (point lookup)"
  "Q7: projection + filter"
)

CSV_QUERIES=(
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA')"
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA') WHERE city = 'NYC'"
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA') WHERE salary > 200000"
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA') WHERE city = 'NYC' AND age > 50"
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA') WHERE city IN ('NYC', 'SF', 'Seattle')"
  "SELECT * FROM read_csv('$CSV_DATA') WHERE id = 42"
  "SELECT name, salary FROM read_csv('$CSV_DATA') WHERE salary > 200000"
)

time_query() {
  local ext="$1"
  local query="$2"
  local start end elapsed
  start=$(date +%s%N)
  $DUCKDB -unsigned -noheader -csv -c "LOAD '$ext'; $query;" > /dev/null 2>&1
  end=$(date +%s%N)
  elapsed=$(( end - start ))
  # Output in seconds with 3 decimal places
  echo "scale=4; $elapsed / 1000000000" | bc
}

time_csv_query() {
  local query="$1"
  local start end elapsed
  start=$(date +%s%N)
  $DUCKDB -unsigned -noheader -csv -c "$query;" > /dev/null 2>&1
  end=$(date +%s%N)
  elapsed=$(( end - start ))
  echo "scale=4; $elapsed / 1000000000" | bc
}

median() {
  # Read values from args, sort, return median
  local -a vals=("$@")
  local -a sorted
  IFS=$'\n' sorted=($(printf '%s\n' "${vals[@]}" | sort -g)); unset IFS
  local mid=$(( ${#sorted[@]} / 2 ))
  echo "${sorted[$mid]}"
}

echo "Warming up..."
$DUCKDB -unsigned -c "LOAD '$EXT_PUSHDOWN'; SELECT COUNT(*) FROM read_nsv('$DATA');" > /dev/null 2>&1
$DUCKDB -unsigned -c "LOAD '$EXT_BATCH'; SELECT COUNT(*) FROM read_nsv('$DATA');" > /dev/null 2>&1
$DUCKDB -unsigned -c "SELECT COUNT(*) FROM read_csv('$CSV_DATA');" > /dev/null 2>&1

printf "\n%-35s  %10s  %10s  %10s  %10s\n" "Query" "FilterPush" "BatchVec" "CSV" "Push/Batch"
printf "%-35s  %10s  %10s  %10s  %10s\n" \
  "$(printf '%.0s-' {1..35})" "----------" "----------" "----------" "----------"

for i in "${!QUERIES[@]}"; do
  name="${QUERY_NAMES[$i]}"
  q="${QUERIES[$i]}"
  cq="${CSV_QUERIES[$i]}"

  push_times=()
  batch_times=()
  csv_times=()

  for r in $(seq 1 $RUNS); do
    push_times+=($(time_query "$EXT_PUSHDOWN" "$q"))
    batch_times+=($(time_query "$EXT_BATCH" "$q"))
    csv_times+=($(time_csv_query "$cq"))
  done

  t_push=$(median "${push_times[@]}")
  t_batch=$(median "${batch_times[@]}")
  t_csv=$(median "${csv_times[@]}")

  ratio=$(awk "BEGIN { printf \"%.2fx\", $t_push / $t_batch }")

  printf "%-35s  %8ss  %8ss  %8ss  %10s\n" "$name" "$t_push" "$t_batch" "$t_csv" "$ratio"
done

echo ""
echo "Push/Batch > 1.0x means filter pushdown is SLOWER."
echo "Push/Batch < 1.0x means filter pushdown is FASTER."
echo ""
echo "Done."
