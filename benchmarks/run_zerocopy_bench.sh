#!/usr/bin/env bash
set -euo pipefail

DUCKDB="./build/release/duckdb"
DATA="benchmarks/data/bench_500k.nsv"
CSV_DATA="benchmarks/data/bench_500k.csv"
EXT_OLD="benchmarks/builds/pushdown/nsv.duckdb_extension"
EXT_NEW="benchmarks/builds/zerocopy/nsv.duckdb_extension"
RUNS=5

echo "================================================================"
echo "  NSV DuckDB Extension: Old (owned copies) vs Zero-Copy (Cow)"
echo "  Data: 500K rows, 7 columns (~24MB)"
echo "  Runs per query: $RUNS (median reported)"
echo "================================================================"
echo ""

QUERIES=(
  "SELECT COUNT(*) FROM read_nsv('$DATA')"
  "SELECT COUNT(*) FROM read_nsv('$DATA') WHERE city = 'NYC'"
  "SELECT COUNT(*) FROM read_nsv('$DATA') WHERE salary > 200000"
  "SELECT COUNT(*) FROM read_nsv('$DATA') WHERE city = 'NYC' AND age > 50"
  "SELECT name, salary FROM read_nsv('$DATA') WHERE salary > 200000"
  "SELECT * FROM read_nsv('$DATA') WHERE id = 42"
  "SELECT name FROM read_nsv('$DATA')"
)

QUERY_NAMES=(
  "Q1: Full scan (COUNT *)"
  "Q2: city = 'NYC' (~7%)"
  "Q3: salary > 200000 (~20%)"
  "Q4: city='NYC' AND age>50 (~2%)"
  "Q5: proj + filter"
  "Q6: point lookup (id=42)"
  "Q7: single col projection"
)

CSV_QUERIES=(
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA')"
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA') WHERE city = 'NYC'"
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA') WHERE salary > 200000"
  "SELECT COUNT(*) FROM read_csv('$CSV_DATA') WHERE city = 'NYC' AND age > 50"
  "SELECT name, salary FROM read_csv('$CSV_DATA') WHERE salary > 200000"
  "SELECT * FROM read_csv('$CSV_DATA') WHERE id = 42"
  "SELECT name FROM read_csv('$CSV_DATA')"
)

time_query() {
  local ext="$1"
  local query="$2"
  local start end elapsed
  start=$(date +%s%N)
  $DUCKDB -unsigned -noheader -csv -c "LOAD '$ext'; $query;" > /dev/null 2>&1
  end=$(date +%s%N)
  elapsed=$(( end - start ))
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
  local -a vals=("$@")
  local -a sorted
  IFS=$'\n' sorted=($(printf '%s\n' "${vals[@]}" | sort -g)); unset IFS
  local mid=$(( ${#sorted[@]} / 2 ))
  echo "${sorted[$mid]}"
}

echo "Warming up..."
$DUCKDB -unsigned -c "LOAD '$EXT_OLD'; SELECT COUNT(*) FROM read_nsv('$DATA');" > /dev/null 2>&1
$DUCKDB -unsigned -c "LOAD '$EXT_NEW'; SELECT COUNT(*) FROM read_nsv('$DATA');" > /dev/null 2>&1
$DUCKDB -unsigned -c "SELECT COUNT(*) FROM read_csv('$CSV_DATA');" > /dev/null 2>&1

printf "\n%-35s  %10s  %10s  %10s  %10s\n" "Query" "Old(owned)" "ZeroCopy" "CSV" "Old/New"
printf "%-35s  %10s  %10s  %10s  %10s\n" \
  "$(printf '%.0s-' {1..35})" "----------" "----------" "----------" "----------"

for i in "${!QUERIES[@]}"; do
  name="${QUERY_NAMES[$i]}"
  q="${QUERIES[$i]}"
  cq="${CSV_QUERIES[$i]}"

  old_times=()
  new_times=()
  csv_times=()

  for r in $(seq 1 $RUNS); do
    old_times+=($(time_query "$EXT_OLD" "$q"))
    new_times+=($(time_query "$EXT_NEW" "$q"))
    csv_times+=($(time_csv_query "$cq"))
  done

  t_old=$(median "${old_times[@]}")
  t_new=$(median "${new_times[@]}")
  t_csv=$(median "${csv_times[@]}")

  ratio=$(awk "BEGIN { printf \"%.2fx\", $t_old / $t_new }")

  printf "%-35s  %8ss  %8ss  %8ss  %10s\n" "$name" "$t_old" "$t_new" "$t_csv" "$ratio"
done

echo ""
echo "Old/New > 1.0x means zero-copy is FASTER."
echo "Old/New < 1.0x means zero-copy is SLOWER."
echo ""
echo "Done."
