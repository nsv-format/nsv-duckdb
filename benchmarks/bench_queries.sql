-- Query-type benchmark: projection, filtering, aggregation.
-- Run: duckdb < benchmarks/bench_queries.sql

LOAD 'build/release/extension/nsv/nsv.duckdb_extension';

-- Warmup
SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv');
SELECT COUNT(*) FROM read_nsv('benchmarks/data/allvc.nsv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/allvc.csv');
SELECT COUNT(*) FROM read_nsv('benchmarks/data/wide50.nsv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/wide50.csv');

.timer on

-- Projection: first column from lineitem (1 of 16)
SELECT 'proj first-col NSV'; SELECT SUM(l_orderkey) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT 'proj first-col CSV'; SELECT SUM(l_orderkey) FROM read_csv('benchmarks/data/lineitem.csv');
SELECT 'proj first-col NSV'; SELECT SUM(l_orderkey) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT 'proj first-col CSV'; SELECT SUM(l_orderkey) FROM read_csv('benchmarks/data/lineitem.csv');

-- Projection: last column from lineitem (16 of 16)
SELECT 'proj last-col NSV'; SELECT COUNT(l_comment) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT 'proj last-col CSV'; SELECT COUNT(l_comment) FROM read_csv('benchmarks/data/lineitem.csv');
SELECT 'proj last-col NSV'; SELECT COUNT(l_comment) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT 'proj last-col CSV'; SELECT COUNT(l_comment) FROM read_csv('benchmarks/data/lineitem.csv');

-- Projection: 1 column from wide50 (1 of 50)
SELECT 'proj wide NSV'; SELECT SUM(v1) FROM read_nsv('benchmarks/data/wide50.nsv');
SELECT 'proj wide CSV'; SELECT SUM(v1) FROM read_csv('benchmarks/data/wide50.csv');
SELECT 'proj wide NSV'; SELECT SUM(v1) FROM read_nsv('benchmarks/data/wide50.nsv');
SELECT 'proj wide CSV'; SELECT SUM(v1) FROM read_csv('benchmarks/data/wide50.csv');

-- Aggregation: SUM on lineitem
SELECT 'sum NSV'; SELECT SUM(l_extendedprice) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT 'sum CSV'; SELECT SUM(l_extendedprice) FROM read_csv('benchmarks/data/lineitem.csv');
SELECT 'sum NSV'; SELECT SUM(l_extendedprice) FROM read_nsv('benchmarks/data/lineitem.nsv');
SELECT 'sum CSV'; SELECT SUM(l_extendedprice) FROM read_csv('benchmarks/data/lineitem.csv');

-- TPC-H Q1 aggregation
SELECT 'q1 NSV';
SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice),
       AVG(l_discount), COUNT(*)
FROM read_nsv('benchmarks/data/lineitem.nsv')
GROUP BY l_returnflag, l_linestatus ORDER BY 1, 2;
SELECT 'q1 CSV';
SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice),
       AVG(l_discount), COUNT(*)
FROM read_csv('benchmarks/data/lineitem.csv')
GROUP BY l_returnflag, l_linestatus ORDER BY 1, 2;
SELECT 'q1 NSV';
SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice),
       AVG(l_discount), COUNT(*)
FROM read_nsv('benchmarks/data/lineitem.nsv')
GROUP BY l_returnflag, l_linestatus ORDER BY 1, 2;
SELECT 'q1 CSV';
SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice),
       AVG(l_discount), COUNT(*)
FROM read_csv('benchmarks/data/lineitem.csv')
GROUP BY l_returnflag, l_linestatus ORDER BY 1, 2;

-- GROUP BY on all-varchar
SELECT 'groupby-vc NSV'; SELECT c1, COUNT(*) FROM read_nsv('benchmarks/data/allvc.nsv') GROUP BY c1;
SELECT 'groupby-vc CSV'; SELECT c1, COUNT(*) FROM read_csv('benchmarks/data/allvc.csv') GROUP BY c1;
SELECT 'groupby-vc NSV'; SELECT c1, COUNT(*) FROM read_nsv('benchmarks/data/allvc.nsv') GROUP BY c1;
SELECT 'groupby-vc CSV'; SELECT c1, COUNT(*) FROM read_csv('benchmarks/data/allvc.csv') GROUP BY c1;

-- Filter: equality on lineitem
SELECT 'filter-eq NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv') WHERE l_returnflag = 'A';
SELECT 'filter-eq CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv') WHERE l_returnflag = 'A';
SELECT 'filter-eq NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/lineitem.nsv') WHERE l_returnflag = 'A';
SELECT 'filter-eq CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/lineitem.csv') WHERE l_returnflag = 'A';

-- Filter: selective on narrow
SELECT 'filter-sel NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/narrow2.nsv') WHERE id < 1000;
SELECT 'filter-sel CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/narrow2.csv') WHERE id < 1000;
SELECT 'filter-sel NSV'; SELECT COUNT(*) FROM read_nsv('benchmarks/data/narrow2.nsv') WHERE id < 1000;
SELECT 'filter-sel CSV'; SELECT COUNT(*) FROM read_csv('benchmarks/data/narrow2.csv') WHERE id < 1000;

.timer off
