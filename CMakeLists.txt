cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME nsv)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Build Rust FFI
set(RUST_GLUE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust-glue)

# Detect if we're on a musl system and set appropriate library path
if(DEFINED __MUSL_ENABLED__)
    set(RUST_TARGET "x86_64-unknown-linux-musl")
    set(RUST_GLUE_LIB ${RUST_GLUE_DIR}/target/${RUST_TARGET}/release/libnsv_ffi.a)
    message(STATUS "Detected musl system, using Rust target: ${RUST_TARGET}")
else()
    set(RUST_GLUE_LIB ${RUST_GLUE_DIR}/target/release/libnsv_ffi.a)
endif()

# Check if pre-built library exists
if(EXISTS ${RUST_GLUE_LIB})
    message(STATUS "Using pre-built Rust library: ${RUST_GLUE_LIB}")
else()
    # Pre-built library not found, try to build with cargo
    find_program(CARGO_EXECUTABLE cargo)
    if(NOT CARGO_EXECUTABLE)
        message(FATAL_ERROR "Pre-built Rust library not found and cargo not available. Please install Rust from https://rustup.rs/ or use a repository with pre-built libraries.")
    endif()

    message(STATUS "Building Rust FFI with cargo...")
    if(DEFINED RUST_TARGET)
        add_custom_command(
            OUTPUT ${RUST_GLUE_LIB}
            COMMAND ${CARGO_EXECUTABLE} build --release --target ${RUST_TARGET}
            WORKING_DIRECTORY ${RUST_GLUE_DIR}
            COMMENT "Building Rust FFI for ${RUST_TARGET}"
        )
    else()
        add_custom_command(
            OUTPUT ${RUST_GLUE_LIB}
            COMMAND ${CARGO_EXECUTABLE} build --release
            WORKING_DIRECTORY ${RUST_GLUE_DIR}
            COMMENT "Building Rust FFI"
        )
    endif()
    add_custom_target(rust_glue_build DEPENDS ${RUST_GLUE_LIB})
endif()

# Add rust-glue headers to include path
include_directories(${RUST_GLUE_DIR})

# Create imported library target
add_library(rust_glue STATIC IMPORTED)
set_target_properties(rust_glue PROPERTIES IMPORTED_LOCATION ${RUST_GLUE_LIB})

# Only add dependency if we're building (not using pre-built)
if(TARGET rust_glue_build)
    add_dependencies(rust_glue rust_glue_build)
endif()

set(EXTENSION_SOURCES src/nsv_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link Rust FFI library
target_link_libraries(${EXTENSION_NAME} rust_glue)
target_link_libraries(${LOADABLE_EXTENSION_NAME} rust_glue)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
