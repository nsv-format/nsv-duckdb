.PHONY: all clean format debug release test

all: release

# Build Rust library first
rust:
	cd ../nsv-rust && cargo build --release

# Clean build
clean:
	rm -rf build
	cd ../nsv-rust && cargo clean

# Build debug
debug: rust
	mkdir -p build/debug && \
	cd build/debug && \
	cmake -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

# Build release
release: rust
	mkdir -p build/release && \
	cd build/release && \
	cmake -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build .

#Test
test: release
	./build/release/test/unittest
