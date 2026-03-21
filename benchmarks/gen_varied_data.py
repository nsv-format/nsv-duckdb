#!/usr/bin/env python3
"""Generate varied benchmark data files to find worst cases."""
import random
import os

random.seed(42)
os.makedirs("benchmarks/data", exist_ok=True)

def escape_nsv(s):
    return s.replace("\\", "\\\\").replace("\n", "\\n")

def write_nsv(path, header, rows):
    with open(path, "w") as f:
        for h in header:
            f.write(escape_nsv(h) + "\n")
        f.write("\n")
        for row in rows:
            for cell in row:
                f.write(escape_nsv(str(cell)) + "\n")
            f.write("\n")

def write_csv(path, header, rows):
    with open(path, "w") as f:
        f.write(",".join(header) + "\n")
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")

# 1. Wide table (20 columns, 200K rows)
print("Generating wide table (20 cols, 200K rows)...")
header = [f"col{i}" for i in range(20)]
rows = []
for i in range(200_000):
    row = [str(i)] + [f"val_{i}_{j}" for j in range(1, 20)]
    rows.append(row)
write_nsv("benchmarks/data/wide_200k.nsv", header, rows)
write_csv("benchmarks/data/wide_200k.csv", header, rows)

# 2. Narrow table (2 columns, 1M rows) - typed only
print("Generating narrow typed table (2 cols, 1M rows)...")
header = ["id", "value"]
rows = [[i, random.randint(0, 1000000)] for i in range(1_000_000)]
write_nsv("benchmarks/data/narrow_1m.nsv", header, rows)
write_csv("benchmarks/data/narrow_1m.csv", header, rows)

# 3. Heavy escape table (cells with lots of backslashes and newlines)
print("Generating heavy-escape table (5 cols, 200K rows)...")
header = ["id", "text1", "text2", "num", "text3"]
rows = []
for i in range(200_000):
    text1 = f"line1\\with\\backslashes{i}"
    text2 = f"multi\nline\ntext\n{i}"
    text3 = f"mixed\\slash\nand\nnewline{i}"
    rows.append([i, text1, text2, random.randint(0, 100), text3])
write_nsv("benchmarks/data/escaped_200k.nsv", header, rows)
write_csv("benchmarks/data/escaped_200k.csv", header, rows)

# 4. All-VARCHAR table (5 cols, 500K rows)
print("Generating all-varchar table (5 cols, 500K rows)...")
WORDS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta",
         "theta", "iota", "kappa", "lambda", "mu", "nu", "xi"]
header = ["a", "b", "c", "d", "e"]
rows = []
for i in range(500_000):
    rows.append([random.choice(WORDS) for _ in range(5)])
write_nsv("benchmarks/data/varchar_500k.nsv", header, rows)
write_csv("benchmarks/data/varchar_500k.csv", header, rows)

# 5. Large file (1M rows, 5 cols mixed)
print("Generating large table (5 cols, 1M rows)...")
CITIES = ["NYC", "SF", "Seattle", "Chicago", "Boston"]
header = ["id", "name", "age", "city", "score"]
rows = []
for i in range(1_000_000):
    rows.append([i, f"user_{i:07d}", random.randint(18, 70),
                 random.choice(CITIES), round(random.uniform(0, 100), 2)])
write_nsv("benchmarks/data/mixed_1m.nsv", header, rows)
write_csv("benchmarks/data/mixed_1m.csv", header, rows)

for f in ["wide_200k", "narrow_1m", "escaped_200k", "varchar_500k", "mixed_1m"]:
    nsv_size = os.path.getsize(f"benchmarks/data/{f}.nsv")
    csv_size = os.path.getsize(f"benchmarks/data/{f}.csv")
    print(f"  {f}: NSV={nsv_size/1e6:.1f}MB CSV={csv_size/1e6:.1f}MB")
