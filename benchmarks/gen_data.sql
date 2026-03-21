-- Generate benchmark datasets for NSV vs CSV comparison.
-- Run with: duckdb < benchmarks/gen_data.sql
-- Requires the NSV extension to be loaded for COPY ... FORMAT nsv.

LOAD 'build/release/extension/nsv/nsv.duckdb_extension';

-- narrow1: 1 column (BIGINT), 1M rows
CREATE TABLE narrow1 AS SELECT i AS id FROM generate_series(1, 1000000) t(i);
COPY narrow1 TO 'benchmarks/data/narrow1.csv' (FORMAT CSV, HEADER);
COPY narrow1 TO 'benchmarks/data/narrow1.nsv' (FORMAT nsv);
DROP TABLE narrow1;

-- narrow2: 2 columns (BIGINT + VARCHAR), 1M rows
CREATE TABLE narrow2 AS
SELECT i AS id, 'name_' || (i % 100)::VARCHAR AS name
FROM generate_series(1, 1000000) t(i);
COPY narrow2 TO 'benchmarks/data/narrow2.csv' (FORMAT CSV, HEADER);
COPY narrow2 TO 'benchmarks/data/narrow2.nsv' (FORMAT nsv);
DROP TABLE narrow2;

-- allvc: 10 VARCHAR columns, 1M rows
CREATE TABLE allvc AS SELECT
  'val_' || (i % 1000)::VARCHAR AS c0,
  'city_' || (i % 50)::VARCHAR AS c1,
  'name_' || (i % 200)::VARCHAR AS c2,
  'addr_' || (i % 500)::VARCHAR AS c3,
  'tag_' || (i % 30)::VARCHAR AS c4,
  'cat_' || (i % 20)::VARCHAR AS c5,
  'desc_' || (i % 100)::VARCHAR AS c6,
  'note_' || (i % 80)::VARCHAR AS c7,
  'flag_' || (i % 10)::VARCHAR AS c8,
  'code_' || (i % 60)::VARCHAR AS c9
FROM generate_series(1, 1000000) t(i);
COPY allvc TO 'benchmarks/data/allvc.csv' (FORMAT CSV, HEADER);
COPY allvc TO 'benchmarks/data/allvc.nsv' (FORMAT nsv);
DROP TABLE allvc;

-- alldbl: 10 DOUBLE columns, 1M rows
CREATE TABLE alldbl AS SELECT
  RANDOM() * 1000 AS d0, RANDOM() * 1000 AS d1,
  RANDOM() * 1000 AS d2, RANDOM() * 1000 AS d3,
  RANDOM() * 1000 AS d4, RANDOM() * 1000 AS d5,
  RANDOM() * 1000 AS d6, RANDOM() * 1000 AS d7,
  RANDOM() * 1000 AS d8, RANDOM() * 1000 AS d9
FROM generate_series(1, 1000000) t(i);
COPY alldbl TO 'benchmarks/data/alldbl.csv' (FORMAT CSV, HEADER);
COPY alldbl TO 'benchmarks/data/alldbl.nsv' (FORMAT nsv);
DROP TABLE alldbl;

-- wide50: 50 mixed columns, 500K rows
CREATE TABLE wide50 AS SELECT
  i AS id,
  RANDOM() * 100 AS v1, RANDOM() * 100 AS v2, RANDOM() * 100 AS v3,
  RANDOM() * 100 AS v4, RANDOM() * 100 AS v5, RANDOM() * 100 AS v6,
  RANDOM() * 100 AS v7, RANDOM() * 100 AS v8, RANDOM() * 100 AS v9,
  'str_' || (i % 100)::VARCHAR AS s1, 'str_' || (i % 200)::VARCHAR AS s2,
  'str_' || (i % 300)::VARCHAR AS s3, 'str_' || (i % 400)::VARCHAR AS s4,
  RANDOM() * 100 AS v10, RANDOM() * 100 AS v11, RANDOM() * 100 AS v12,
  RANDOM() * 100 AS v13, RANDOM() * 100 AS v14, RANDOM() * 100 AS v15,
  DATE '2020-01-01' + INTERVAL (i % 1000) DAY AS d1,
  DATE '2020-01-01' + INTERVAL (i % 500) DAY AS d2,
  i % 2 = 0 AS b1, i % 3 = 0 AS b2,
  RANDOM() * 100 AS v16, RANDOM() * 100 AS v17, RANDOM() * 100 AS v18,
  RANDOM() * 100 AS v19, RANDOM() * 100 AS v20, RANDOM() * 100 AS v21,
  'long_string_value_' || (i % 1000)::VARCHAR AS s5,
  'another_value_' || (i % 500)::VARCHAR AS s6,
  RANDOM() * 100 AS v22, RANDOM() * 100 AS v23, RANDOM() * 100 AS v24,
  RANDOM() * 100 AS v25, RANDOM() * 100 AS v26, RANDOM() * 100 AS v27,
  RANDOM() * 100 AS v28, RANDOM() * 100 AS v29, RANDOM() * 100 AS v30,
  RANDOM() * 100 AS v31, RANDOM() * 100 AS v32, RANDOM() * 100 AS v33,
  DATE '2020-01-01' + INTERVAL (i % 2000) DAY AS d3,
  RANDOM() * 100 AS v34, RANDOM() * 100 AS v35, RANDOM() * 100 AS v36,
  'final_' || (i % 100)::VARCHAR AS s7,
  RANDOM() * 100 AS v37, RANDOM() * 100 AS v38
FROM generate_series(1, 500000) t(i);
COPY wide50 TO 'benchmarks/data/wide50.csv' (FORMAT CSV, HEADER);
COPY wide50 TO 'benchmarks/data/wide50.nsv' (FORMAT nsv);
DROP TABLE wide50;

-- escaped: 5 columns with backslash-heavy VARCHAR, 1M rows
CREATE TABLE escaped AS SELECT
  i AS id,
  'line1\nline2\nline3_' || (i % 100)::VARCHAR AS multiline,
  'path\\to\\file_' || (i % 50)::VARCHAR AS winpath,
  'normal_' || (i % 200)::VARCHAR AS clean,
  RANDOM() * 1000 AS val
FROM generate_series(1, 1000000) t(i);
COPY escaped TO 'benchmarks/data/escaped.csv' (FORMAT CSV, HEADER);
COPY escaped TO 'benchmarks/data/escaped.nsv' (FORMAT nsv);
DROP TABLE escaped;

-- lineitem: TPC-H lineitem shape, 6M rows, 16 columns
CREATE TABLE lineitem AS SELECT
  i AS l_orderkey,
  (i % 7 + 1) AS l_partkey,
  (i % 5 + 1) AS l_suppkey,
  (i % 4 + 1) AS l_linenumber,
  ROUND(RANDOM() * 50 + 1, 2)::DOUBLE AS l_quantity,
  ROUND(RANDOM() * 100000, 2)::DOUBLE AS l_extendedprice,
  ROUND(RANDOM() * 0.1, 2)::DOUBLE AS l_discount,
  ROUND(RANDOM() * 0.08, 2)::DOUBLE AS l_tax,
  CASE (i % 3) WHEN 0 THEN 'A' WHEN 1 THEN 'R' ELSE 'N' END AS l_returnflag,
  CASE (i % 2) WHEN 0 THEN 'O' WHEN 1 THEN 'F' END AS l_linestatus,
  DATE '1992-01-01' + INTERVAL (i % 2500) DAY AS l_shipdate,
  DATE '1992-01-01' + INTERVAL (i % 2500 + 7) DAY AS l_commitdate,
  DATE '1992-01-01' + INTERVAL (i % 2500 + 3) DAY AS l_receiptdate,
  CASE (i % 7) WHEN 0 THEN 'DELIVER IN PERSON' WHEN 1 THEN 'COLLECT COD'
    WHEN 2 THEN 'NONE' WHEN 3 THEN 'TAKE BACK RETURN'
    WHEN 4 THEN 'DELIVER IN PERSON' WHEN 5 THEN 'COLLECT COD'
    ELSE 'NONE' END AS l_shipinstruct,
  CASE (i % 7) WHEN 0 THEN 'TRUCK' WHEN 1 THEN 'MAIL' WHEN 2 THEN 'SHIP'
    WHEN 3 THEN 'AIR' WHEN 4 THEN 'RAIL' WHEN 5 THEN 'FOB'
    ELSE 'REG AIR' END AS l_shipmode,
  'Comment line ' || (i % 1000)::VARCHAR AS l_comment
FROM generate_series(1, 6001215) t(i);
COPY lineitem TO 'benchmarks/data/lineitem.csv' (FORMAT CSV, HEADER);
COPY lineitem TO 'benchmarks/data/lineitem.nsv' (FORMAT nsv);
DROP TABLE lineitem;
