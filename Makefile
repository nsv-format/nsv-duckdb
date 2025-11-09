.PHONY: all clean rust test-python

all: rust

rust:
	cd rust-ffi && cargo build --release

clean:
	rm -rf build duckdb/extension/nsv
	cd rust-ffi && cargo clean

# Test with Python (working now)
test-python:
	python demo.py

# For C++ extension build (requires ~15min to build DuckDB)
# See build instructions in README
