cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME nsv)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# ── Rust FFI ─────────────────────────────────────────────────────────
set(RUST_FFI_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust-ffi)

find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "Cargo not found. Install Rust from https://rustup.rs/")
endif()

# Determine Rust target triple.
# 1) Rust_CARGO_TARGET is set by the DuckDB CI Makefile for macOS/mingw cross-compilation.
# 2) EMSCRIPTEN is set by emcmake cmake for WASM builds.
# 3) Fall back to DUCKDB_EXPLICIT_PLATFORM for musl/windows MSVC.
# 4) If none match, use the host default.
if(DEFINED Rust_CARGO_TARGET AND NOT Rust_CARGO_TARGET STREQUAL "")
    set(RUST_TARGET "${Rust_CARGO_TARGET}")
elseif(EMSCRIPTEN OR DUCKDB_EXPLICIT_PLATFORM MATCHES "^wasm")
    set(RUST_TARGET "wasm32-unknown-emscripten")
elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "linux_amd64_musl")
    set(RUST_TARGET "x86_64-unknown-linux-musl")
elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "windows_amd64")
    set(RUST_TARGET "x86_64-pc-windows-msvc")
elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "windows_arm64")
    set(RUST_TARGET "aarch64-pc-windows-msvc")
endif()

# Determine library output path and filename.
# Rust staticlib produces libfoo.a on Unix and foo.lib on MSVC.
if(DEFINED RUST_TARGET)
    set(RUST_OUTPUT_DIR ${RUST_FFI_DIR}/target/${RUST_TARGET}/release)
else()
    set(RUST_OUTPUT_DIR ${RUST_FFI_DIR}/target/release)
endif()

if(RUST_TARGET MATCHES "windows-msvc")
    set(RUST_FFI_LIB ${RUST_OUTPUT_DIR}/nsv_ffi.lib)
else()
    set(RUST_FFI_LIB ${RUST_OUTPUT_DIR}/libnsv_ffi.a)
endif()

message(STATUS "Rust FFI target: ${RUST_TARGET}")
message(STATUS "Rust FFI lib: ${RUST_FFI_LIB}")

if(DEFINED RUST_TARGET)
    if(EMSCRIPTEN OR DUCKDB_EXPLICIT_PLATFORM MATCHES "^wasm")
        set(CARGO_EXTRA_FLAGS "--no-default-features")
    endif()
    add_custom_command(
        OUTPUT ${RUST_FFI_LIB}
        COMMAND ${CARGO_EXECUTABLE} build --release --target ${RUST_TARGET} ${CARGO_EXTRA_FLAGS}
        WORKING_DIRECTORY ${RUST_FFI_DIR}
        COMMENT "Building Rust FFI (target: ${RUST_TARGET})"
    )
else()
    add_custom_command(
        OUTPUT ${RUST_FFI_LIB}
        COMMAND ${CARGO_EXECUTABLE} build --release
        WORKING_DIRECTORY ${RUST_FFI_DIR}
        COMMENT "Building Rust FFI"
    )
endif()

add_custom_target(rust_ffi_build DEPENDS ${RUST_FFI_LIB})

add_library(rust_ffi STATIC IMPORTED)
set_target_properties(rust_ffi PROPERTIES IMPORTED_LOCATION ${RUST_FFI_LIB})
add_dependencies(rust_ffi rust_ffi_build)

# ── Extension ────────────────────────────────────────────────────────
set(EXTENSION_SOURCES src/nsv_extension.cpp)

# For WASM builds, DuckDB's extension_build_tools.cmake uses
# DUCKDB_EXTENSION_<NAME>_LINKED_LIBS in the emcc post-build link step.
# This must be set BEFORE build_loadable_extension() which reads it.
if(EMSCRIPTEN)
    set(DUCKDB_EXTENSION_NSV_LINKED_LIBS "${RUST_FFI_LIB}" CACHE STRING "" FORCE)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} rust_ffi)
target_link_libraries(${LOADABLE_EXTENSION_NAME} rust_ffi)

# Rust staticlib needs platform-specific system libraries.
# Set as INTERFACE on the imported target so they propagate transitively
# to all targets that link rust_ffi (including DuckDB internals like
# test_sqlite3_api_wrapper, parquet, etc. on the mingw build).
if(WIN32)
    set_target_properties(rust_ffi PROPERTIES
        INTERFACE_LINK_LIBRARIES "ws2_32;bcrypt;userenv;ntdll")
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
