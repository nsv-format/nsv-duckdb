-- In-process benchmark: eliminates startup overhead
-- Run with: duckdb -unsigned < benchmarks/bench_inproc.sql

LOAD 'build/release/extension/nsv/nsv.duckdb_extension';

.timer on

-- Warmup
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv');

-- Q1: Full scan
SELECT '=== Q1: Full scan (COUNT *) ===' as label;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv');
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv');
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv');
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv');

-- Q2: Filter on VARCHAR
SELECT '=== Q2: city = NYC (~7%) ===' as label;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE city = 'NYC';
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE city = 'NYC';

-- Q3: Filter on typed column
SELECT '=== Q3: salary > 200000 (~20%) ===' as label;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE salary > 200000;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE salary > 200000;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE salary > 200000;
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE salary > 200000;
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE salary > 200000;
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE salary > 200000;

-- Q4: Compound filter
SELECT '=== Q4: city=NYC AND age>50 ===' as label;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE city = 'NYC' AND age > 50;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE city = 'NYC' AND age > 50;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/bench_500k.nsv') WHERE city = 'NYC' AND age > 50;
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE city = 'NYC' AND age > 50;
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE city = 'NYC' AND age > 50;
SELECT COUNT(*) FROM read_csv('benchmarks/data/bench_500k.csv') WHERE city = 'NYC' AND age > 50;

-- Q5: SELECT * (all columns)
SELECT '=== Q5: SELECT * ===' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/bench_500k.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/bench_500k.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/bench_500k.csv'));

-- Q6: Single typed column projection
SELECT '=== Q6: SELECT id (typed proj) ===' as label;
SELECT COUNT(*) FROM (SELECT id FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT COUNT(*) FROM (SELECT id FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT COUNT(*) FROM (SELECT id FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT id FROM read_csv('benchmarks/data/bench_500k.csv'));
SELECT COUNT(*) FROM (SELECT id FROM read_csv('benchmarks/data/bench_500k.csv'));
SELECT COUNT(*) FROM (SELECT id FROM read_csv('benchmarks/data/bench_500k.csv'));

-- Q7: Single VARCHAR column
SELECT '=== Q7: SELECT name (varchar proj) ===' as label;
SELECT COUNT(*) FROM (SELECT name FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT COUNT(*) FROM (SELECT name FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT COUNT(*) FROM (SELECT name FROM read_nsv('benchmarks/data/bench_500k.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT name FROM read_csv('benchmarks/data/bench_500k.csv'));
SELECT COUNT(*) FROM (SELECT name FROM read_csv('benchmarks/data/bench_500k.csv'));
SELECT COUNT(*) FROM (SELECT name FROM read_csv('benchmarks/data/bench_500k.csv'));

-- Q8: All-varchar mode
SELECT '=== Q8: all_varchar=true ===' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/bench_500k.nsv', all_varchar=true));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/bench_500k.nsv', all_varchar=true));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/bench_500k.nsv', all_varchar=true));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/bench_500k.csv', all_varchar=true));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/bench_500k.csv', all_varchar=true));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/bench_500k.csv', all_varchar=true));
