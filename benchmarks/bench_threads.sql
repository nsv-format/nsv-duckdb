-- Thread-scaling benchmark on lineitem (6M rows, 16 cols, ~789MB).
-- Run: duckdb < benchmarks/bench_threads.sql

LOAD 'build/release/extension/nsv/nsv.duckdb_extension';

-- Warmup
SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');

.timer on

-- 1 thread
SET threads TO 1;
SELECT '1T NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT '1T CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');
SELECT '1T NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT '1T CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');

-- 2 threads
SET threads TO 2;
SELECT '2T NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT '2T CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');
SELECT '2T NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT '2T CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');

-- 4 threads
SET threads TO 4;
SELECT '4T NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT '4T CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');
SELECT '4T NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT '4T CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');

.timer off
