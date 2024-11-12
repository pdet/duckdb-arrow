cmake_minimum_required(VERSION 3.5)
include(FetchContent)

# Set extension name here
set(TARGET_NAME nanoarrow)

set(DUCKDB_NANOARROW_TARGET_VERSION "0.6.0")
set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DuckDBExt${TARGET_NAME}")
fetchcontent_declare(nanoarrow
                     URL "https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/apache-arrow-nanoarrow-${DUCKDB_NANOARROW_TARGET_VERSION}/apache-arrow-nanoarrow-${DUCKDB_NANOARROW_TARGET_VERSION}.tar.gz"
                         "https://github.com/apache/arrow-nanoarrow/releases/download/apache-arrow-nanoarrow-${DUCKDB_NANOARROW_TARGET_VERSION}/apache-arrow-nanoarrow-${DUCKDB_NANOARROW_TARGET_VERSION}.tar.gz"
                     URL_HASH SHA256=e4a02ac51002ad1875bf09317e70adb959005fad52b240ff59f73b970fa485d1
)
fetchcontent_makeavailable(nanoarrow)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/nanoarrow_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link nanoarrow in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} nanoarrow::nanoarrow nanoarrow::nanoarrow_ipc)
target_link_libraries(${LOADABLE_EXTENSION_NAME} nanoarrow::nanoarrow
                      nanoarrow::nanoarrow_ipc)

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
