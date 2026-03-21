-- Generate test data: 100K rows, 5 columns (id INT, name VARCHAR, age INT, city VARCHAR, score DOUBLE)
CREATE TABLE bench_data AS
SELECT
    i AS id,
    CASE WHEN i % 5 = 0 THEN 'Alice'
         WHEN i % 5 = 1 THEN 'Bob'
         WHEN i % 5 = 2 THEN 'Carol'
         WHEN i % 5 = 3 THEN 'Dave'
         ELSE 'Eve' END AS name,
    20 + (i % 50) AS age,
    CASE WHEN i % 4 = 0 THEN 'NYC'
         WHEN i % 4 = 1 THEN 'London'
         WHEN i % 4 = 2 THEN 'Paris'
         ELSE 'Tokyo' END AS city,
    ROUND(50.0 + (i % 500) / 10.0, 1) AS score
FROM range(1, 100001) t(i);

-- Write as NSV
COPY bench_data TO '/tmp/bench_100k.nsv' (FORMAT nsv);

-- Write as CSV for comparison
COPY bench_data TO '/tmp/bench_100k.csv' (FORMAT CSV, HEADER true);

-- Warmup reads
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv');
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv');

.timer on

-- ── Baseline: full scan ──────────────────────────────
SELECT '--- NSV: Full scan (baseline) ---';
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv');
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv');
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv');

SELECT '--- CSV: Full scan (baseline) ---';
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv');
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv');
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv');

-- ── Equality filter (25% selectivity: city = NYC) ────
SELECT '--- NSV: Equality filter 25% (city=NYC) ---';
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city = 'NYC';

SELECT '--- CSV: Equality filter 25% (city=NYC) ---';
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city = 'NYC';
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city = 'NYC';

-- ── Highly selective filter (~2% selectivity: age = 42) ──
SELECT '--- NSV: Highly selective filter ~2% (age=42) ---';
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE age = 42;
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE age = 42;
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE age = 42;

SELECT '--- CSV: Highly selective filter ~2% (age=42) ---';
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE age = 42;
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE age = 42;
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE age = 42;

-- ── Filter + projection (filter on city, select score only) ──
SELECT '--- NSV: Filter+projection (city=NYC, select score) ---';
SELECT AVG(score) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city = 'NYC';
SELECT AVG(score) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city = 'NYC';
SELECT AVG(score) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city = 'NYC';

SELECT '--- CSV: Filter+projection (city=NYC, select score) ---';
SELECT AVG(score) FROM read_csv('/tmp/bench_100k.csv') WHERE city = 'NYC';
SELECT AVG(score) FROM read_csv('/tmp/bench_100k.csv') WHERE city = 'NYC';
SELECT AVG(score) FROM read_csv('/tmp/bench_100k.csv') WHERE city = 'NYC';

-- ── Projection only (1 of 5 cols, no filter) ──
SELECT '--- NSV: Projection only (1/5 cols) ---';
SELECT SUM(score) FROM read_nsv('/tmp/bench_100k.nsv');
SELECT SUM(score) FROM read_nsv('/tmp/bench_100k.nsv');
SELECT SUM(score) FROM read_nsv('/tmp/bench_100k.nsv');

SELECT '--- CSV: Projection only (1/5 cols) ---';
SELECT SUM(score) FROM read_csv('/tmp/bench_100k.csv');
SELECT SUM(score) FROM read_csv('/tmp/bench_100k.csv');
SELECT SUM(score) FROM read_csv('/tmp/bench_100k.csv');

-- ── IN filter (50% selectivity: city IN NYC, London) ──
SELECT '--- NSV: IN filter 50% ---';
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city IN ('NYC', 'London');
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city IN ('NYC', 'London');
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city IN ('NYC', 'London');

SELECT '--- CSV: IN filter 50% ---';
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city IN ('NYC', 'London');
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city IN ('NYC', 'London');
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city IN ('NYC', 'London');

-- ── Range filter (age 30-39, ~20% selectivity) ──
SELECT '--- NSV: Range filter 20% (age 30-39) ---';
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE age >= 30 AND age <= 39;
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE age >= 30 AND age <= 39;
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE age >= 30 AND age <= 39;

SELECT '--- CSV: Range filter 20% (age 30-39) ---';
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE age >= 30 AND age <= 39;
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE age >= 30 AND age <= 39;
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE age >= 30 AND age <= 39;

-- ── IS NOT NULL filter ──
SELECT '--- NSV: IS NOT NULL filter ---';
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city IS NOT NULL;
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city IS NOT NULL;
SELECT COUNT(*) FROM read_nsv('/tmp/bench_100k.nsv') WHERE city IS NOT NULL;

SELECT '--- CSV: IS NOT NULL filter ---';
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city IS NOT NULL;
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city IS NOT NULL;
SELECT COUNT(*) FROM read_csv('/tmp/bench_100k.csv') WHERE city IS NOT NULL;

-- Cleanup
.timer off
DROP TABLE bench_data;
