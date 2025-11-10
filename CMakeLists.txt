cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME nsv)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Build Rust FFI
set(RUST_GLUE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust-glue)
set(RUST_GLUE_LIB ${RUST_GLUE_DIR}/target/release/libnsv_ffi.a)

add_custom_command(
    OUTPUT ${RUST_GLUE_LIB}
    COMMAND cargo build --release
    WORKING_DIRECTORY ${RUST_GLUE_DIR}
    COMMENT "Building Rust FFI"
)

add_custom_target(rust_glue_build DEPENDS ${RUST_GLUE_LIB})

add_library(rust_glue STATIC IMPORTED)
set_target_properties(rust_glue PROPERTIES IMPORTED_LOCATION ${RUST_GLUE_LIB})
add_dependencies(rust_glue rust_glue_build)

# Add rust-glue headers to include path
include_directories(${RUST_GLUE_DIR})

set(EXTENSION_SOURCES src/nsv_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link Rust FFI library
add_dependencies(${EXTENSION_NAME} rust_glue_build)
add_dependencies(${LOADABLE_EXTENSION_NAME} rust_glue_build)
target_link_libraries(${EXTENSION_NAME} rust_glue)
target_link_libraries(${LOADABLE_EXTENSION_NAME} rust_glue)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
