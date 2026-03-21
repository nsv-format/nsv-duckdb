LOAD 'build/release/extension/nsv/nsv.duckdb_extension';
.timer on

-- Warmup
SELECT COUNT(*) FROM read_nsv('benchmarks/data/mixed_1m.nsv');
SELECT COUNT(*) FROM read_csv('benchmarks/data/mixed_1m.csv');

-- T1: Wide table (20 cols, 200K rows) — SELECT *
SELECT '=== T1: Wide 20-col SELECT * ===' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/wide_200k.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/wide_200k.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/wide_200k.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/wide_200k.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/wide_200k.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/wide_200k.csv'));

-- T2: Wide table — project 2 of 20
SELECT '=== T2: Wide 20-col project 2 ===' as label;
SELECT COUNT(*) FROM (SELECT col0, col10 FROM read_nsv('benchmarks/data/wide_200k.nsv'));
SELECT COUNT(*) FROM (SELECT col0, col10 FROM read_nsv('benchmarks/data/wide_200k.nsv'));
SELECT COUNT(*) FROM (SELECT col0, col10 FROM read_nsv('benchmarks/data/wide_200k.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT col0, col10 FROM read_csv('benchmarks/data/wide_200k.csv'));
SELECT COUNT(*) FROM (SELECT col0, col10 FROM read_csv('benchmarks/data/wide_200k.csv'));
SELECT COUNT(*) FROM (SELECT col0, col10 FROM read_csv('benchmarks/data/wide_200k.csv'));

-- T3: Narrow typed (2 cols, 1M rows)
SELECT '=== T3: Narrow 2-col 1M rows ===' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/narrow_1m.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/narrow_1m.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/narrow_1m.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/narrow_1m.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/narrow_1m.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/narrow_1m.csv'));

-- T4: Narrow typed — filter
SELECT '=== T4: Narrow 1M filter value>500000 ===' as label;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/narrow_1m.nsv') WHERE value > 500000;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/narrow_1m.nsv') WHERE value > 500000;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/narrow_1m.nsv') WHERE value > 500000;
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM read_csv('benchmarks/data/narrow_1m.csv') WHERE value > 500000;
SELECT COUNT(*) FROM read_csv('benchmarks/data/narrow_1m.csv') WHERE value > 500000;
SELECT COUNT(*) FROM read_csv('benchmarks/data/narrow_1m.csv') WHERE value > 500000;

-- T5: Heavy escape (200K rows)
SELECT '=== T5: Heavy-escape SELECT * ===' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/escaped_200k.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/escaped_200k.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/escaped_200k.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/escaped_200k.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/escaped_200k.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/escaped_200k.csv'));

-- T6: Heavy escape — typed col only
SELECT '=== T6: Heavy-escape typed proj ===' as label;
SELECT COUNT(*) FROM (SELECT num FROM read_nsv('benchmarks/data/escaped_200k.nsv'));
SELECT COUNT(*) FROM (SELECT num FROM read_nsv('benchmarks/data/escaped_200k.nsv'));
SELECT COUNT(*) FROM (SELECT num FROM read_nsv('benchmarks/data/escaped_200k.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT num FROM read_csv('benchmarks/data/escaped_200k.csv'));
SELECT COUNT(*) FROM (SELECT num FROM read_csv('benchmarks/data/escaped_200k.csv'));
SELECT COUNT(*) FROM (SELECT num FROM read_csv('benchmarks/data/escaped_200k.csv'));

-- T7: All-VARCHAR (500K rows)
SELECT '=== T7: All-varchar 500K SELECT * ===' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/varchar_500k.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/varchar_500k.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/varchar_500k.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/varchar_500k.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/varchar_500k.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/varchar_500k.csv'));

-- T8: Large mixed (1M rows, 5 cols)
SELECT '=== T8: Mixed 1M SELECT * ===' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/mixed_1m.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/mixed_1m.nsv'));
SELECT COUNT(*) FROM (SELECT * FROM read_nsv('benchmarks/data/mixed_1m.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/mixed_1m.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/mixed_1m.csv'));
SELECT COUNT(*) FROM (SELECT * FROM read_csv('benchmarks/data/mixed_1m.csv'));

-- T9: Large mixed — filter
SELECT '=== T9: Mixed 1M city=NYC (~20%) ===' as label;
SELECT COUNT(*) FROM read_nsv('benchmarks/data/mixed_1m.nsv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_nsv('benchmarks/data/mixed_1m.nsv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_nsv('benchmarks/data/mixed_1m.nsv') WHERE city = 'NYC';
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM read_csv('benchmarks/data/mixed_1m.csv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_csv('benchmarks/data/mixed_1m.csv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_csv('benchmarks/data/mixed_1m.csv') WHERE city = 'NYC';

-- T10: Large mixed — project 1 typed col
SELECT '=== T10: Mixed 1M project score ===' as label;
SELECT COUNT(*) FROM (SELECT score FROM read_nsv('benchmarks/data/mixed_1m.nsv'));
SELECT COUNT(*) FROM (SELECT score FROM read_nsv('benchmarks/data/mixed_1m.nsv'));
SELECT COUNT(*) FROM (SELECT score FROM read_nsv('benchmarks/data/mixed_1m.nsv'));
SELECT '--- CSV ---' as label;
SELECT COUNT(*) FROM (SELECT score FROM read_csv('benchmarks/data/mixed_1m.csv'));
SELECT COUNT(*) FROM (SELECT score FROM read_csv('benchmarks/data/mixed_1m.csv'));
SELECT COUNT(*) FROM (SELECT score FROM read_csv('benchmarks/data/mixed_1m.csv'));
