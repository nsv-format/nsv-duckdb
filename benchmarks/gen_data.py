#!/usr/bin/env python3
"""Generate benchmark NSV data files."""
import random
import os

random.seed(42)

CITIES = ["NYC", "SF", "Seattle", "Chicago", "Boston", "Austin", "Denver",
          "Portland", "Miami", "Atlanta", "Dallas", "Phoenix", "LA", "DC"]
DEPTS = ["Engineering", "Sales", "Marketing", "Finance", "HR", "Legal",
         "Product", "Design", "Support", "Operations"]

def escape_nsv(s):
    return s.replace("\\", "\\\\").replace("\n", "\\n")

def write_nsv(path, rows):
    with open(path, "w") as f:
        # header
        f.write("id\nname\nage\ncity\ndept\nsalary\nactive\n\n")
        for row in rows:
            for cell in row:
                f.write(escape_nsv(str(cell)) + "\n")
            f.write("\n")

N = 500_000
rows = []
for i in range(N):
    name = f"user_{i:06d}"
    age = random.randint(18, 70)
    city = random.choice(CITIES)
    dept = random.choice(DEPTS)
    salary = random.randint(30000, 250000)
    active = random.choice(["true", "false"])
    rows.append([i, name, age, city, dept, salary, active])

os.makedirs("benchmarks/data", exist_ok=True)
write_nsv("benchmarks/data/bench_500k.nsv", rows)
print(f"Wrote {N} rows to benchmarks/data/bench_500k.nsv")

# Also write CSV for comparison
with open("benchmarks/data/bench_500k.csv", "w") as f:
    f.write("id,name,age,city,dept,salary,active\n")
    for row in rows:
        f.write(",".join(str(c) for c in row) + "\n")
print(f"Wrote {N} rows to benchmarks/data/bench_500k.csv")
