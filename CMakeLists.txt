cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME nsv)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/nsv_extension.cpp)

# Find the Rust NSV library
# Try from extension dir first (when symlinked into DuckDB tree)
find_library(NSV_LIB nsv_ffi
    HINTS
        "${CMAKE_CURRENT_SOURCE_DIR}/../../../rust-ffi/target/release"
        "${CMAKE_CURRENT_SOURCE_DIR}/rust-ffi/target/release"
)
if(NOT NSV_LIB)
    message(FATAL_ERROR "Could not find libnsv_ffi. Build rust-ffi first with 'cd rust-ffi && cargo build --release'")
endif()
message(STATUS "Found NSV library: ${NSV_LIB}")

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link NSV Rust library
target_link_libraries(${EXTENSION_NAME} ${NSV_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${NSV_LIB})

# Add Rust library include path
target_include_directories(${EXTENSION_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/include)
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/include})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
