-- Filter prune benchmark: measures query time at different selectivity levels
-- Run with: ./build/release/duckdb/duckdb < bench/filter_prune_bench.sql

-- Generate a 500K row NSV file with columns: id, name, age, salary, department, city
-- age is 1-100 uniformly distributed, so WHERE age > X gives ~(100-X)% selectivity

COPY (
  SELECT * FROM (
    SELECT unnest(['id', 'name', 'age', 'salary', 'department', 'city']) AS col
    UNION ALL
    SELECT unnest(['']) -- record separator
    UNION ALL
    SELECT unnest(list_value(
      i::VARCHAR,
      'person_' || i::VARCHAR,
      ((i * 7 + 13) % 100 + 1)::VARCHAR,
      (30000 + (i * 31 + 17) % 70000)::VARCHAR,
      CASE (i % 5) WHEN 0 THEN 'eng' WHEN 1 THEN 'sales' WHEN 2 THEN 'hr' WHEN 3 THEN 'ops' ELSE 'mkt' END,
      CASE (i % 4) WHEN 0 THEN 'NYC' WHEN 1 THEN 'LON' WHEN 2 THEN 'TKY' ELSE 'SFO' END
    ))
    FROM generate_series(1, 500000) t(i)
  )
) TO '/tmp/bench_large.nsv' (FORMAT CSV, HEADER false, QUOTE '');

.timer on

-- Warm up: read file into OS cache
SELECT COUNT(*) FROM read_nsv('/tmp/bench_large.nsv');

.print '=== BENCHMARK: SELECT name (filter-only: age) ==='
.print '--- Selectivity ~1% pass (age > 99) ---'
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 99;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 99;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 99;

.print '--- Selectivity ~10% pass (age > 90) ---'
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 90;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 90;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 90;

.print '--- Selectivity ~50% pass (age > 50) ---'
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50;

.print '--- Selectivity ~90% pass (age > 10) ---'
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 10;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 10;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 10;

.print '--- Selectivity ~99% pass (age > 1) ---'
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 1;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 1;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 1;

.print '=== BENCHMARK: SELECT name (filter-only: age, salary, department) ==='
.print '--- Multi-filter ~50% pass ---'
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50 AND salary > 50000;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50 AND salary > 50000;
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50 AND salary > 50000;

.print '--- Multi-filter ~25% pass ---'
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50 AND salary > 50000 AND department = 'eng';
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50 AND salary > 50000 AND department = 'eng';
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50 AND salary > 50000 AND department = 'eng';

.print '=== BENCHMARK: No filter (baseline) ==='
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv');
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv');
SELECT COUNT(name) FROM read_nsv('/tmp/bench_large.nsv');

.print '=== BENCHMARK: EXPLAIN to verify filter pruning ==='
EXPLAIN SELECT name FROM read_nsv('/tmp/bench_large.nsv') WHERE age > 50;
