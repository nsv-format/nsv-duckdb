-- Full-scan benchmark: SELECT * across all dataset shapes.
-- Alternates NSV/CSV to minimize page-cache bias.
-- Run: duckdb < benchmarks/bench_scan.sql

LOAD 'build/release/extension/nsv/nsv.duckdb_extension';

-- Warmup
SELECT COUNT(*) FROM read_nsv('benchmarks/data/narrow1.nsv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/narrow1.csv');
SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');

.timer on

-- narrow1: 1 col BIGINT, 1M rows, ~7.6MB
SELECT 'narrow1 NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/narrow1.nsv'));
SELECT 'narrow1 CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/narrow1.csv'));
SELECT 'narrow1 NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/narrow1.nsv'));
SELECT 'narrow1 CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/narrow1.csv'));

-- narrow2: 2 cols, 1M rows, ~16MB
SELECT 'narrow2 NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/narrow2.nsv'));
SELECT 'narrow2 CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/narrow2.csv'));
SELECT 'narrow2 NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/narrow2.nsv'));
SELECT 'narrow2 CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/narrow2.csv'));

-- allvc: 10 VARCHAR cols, 1M rows, ~75MB
SELECT 'allvc NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/allvc.nsv'));
SELECT 'allvc CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/allvc.csv'));
SELECT 'allvc NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/allvc.nsv'));
SELECT 'allvc CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/allvc.csv'));

-- alldbl: 10 DOUBLE cols, 1M rows, ~175MB
SELECT 'alldbl NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/alldbl.nsv'));
SELECT 'alldbl CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/alldbl.csv'));
SELECT 'alldbl NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/alldbl.nsv'));
SELECT 'alldbl CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/alldbl.csv'));

-- wide50: 50 mixed cols, 500K rows, ~405MB
SELECT 'wide50 NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/wide50.nsv'));
SELECT 'wide50 CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/wide50.csv'));
SELECT 'wide50 NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/wide50.nsv'));
SELECT 'wide50 CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/wide50.csv'));

-- escaped: backslash-heavy VARCHAR, 1M rows, ~80MB
SELECT 'escaped NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/escaped.nsv'));
SELECT 'escaped CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/escaped.csv'));
SELECT 'escaped NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/escaped.nsv'));
SELECT 'escaped CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/escaped.csv'));

-- lineitem: 16 cols, 6M rows, ~789MB
SELECT 'lineitem NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/lineitem.nsv'));
SELECT 'lineitem CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/lineitem.csv'));
SELECT 'lineitem NSV'; SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/lineitem.nsv'));
SELECT 'lineitem CSV'; SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/lineitem.csv'));

.timer off
