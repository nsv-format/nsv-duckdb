cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME nsv)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# ── Rust FFI ─────────────────────────────────────────────────────────
set(RUST_FFI_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust-ffi)

# Detect musl
if(MUSL_ENABLED)
    set(RUST_TARGET "x86_64-unknown-linux-musl")
    set(RUST_FFI_LIB ${RUST_FFI_DIR}/target/${RUST_TARGET}/release/libnsv_ffi.a)
    message(STATUS "musl detected, Rust target: ${RUST_TARGET}")
else()
    set(RUST_FFI_LIB ${RUST_FFI_DIR}/target/release/libnsv_ffi.a)
endif()

# Always build from source — no pre-built binaries
find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "Cargo not found. Install Rust from https://rustup.rs/")
endif()

if(DEFINED RUST_TARGET)
    add_custom_command(
        OUTPUT ${RUST_FFI_LIB}
        COMMAND ${CARGO_EXECUTABLE} build --release --target ${RUST_TARGET}
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

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} rust_ffi)
target_link_libraries(${LOADABLE_EXTENSION_NAME} rust_ffi)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
