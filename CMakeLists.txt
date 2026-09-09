cmake_minimum_required(VERSION 3.5)
include(FetchContent)

# Set extension name here
set(TARGET_NAME nanoarrow)

set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DuckDBExt${TARGET_NAME}")
# nanoarrow 0.9.0 plus the IPC body compression API for the encoder/writer
# (ArrowIpcEncoderSetCompression and friends, pdet/arrow-nanoarrow branch
# ipc-write-compression). Point this back at apache/arrow-nanoarrow once a release
# ships that API.
# TODO: add URL_HASH SHA256=<sha256 of the zip> once commit cf9d060 is pushed to the fork
fetchcontent_declare(nanoarrow
                     URL "https://github.com/pdet/arrow-nanoarrow/archive/cf9d0604b7f15c6f455a712155c50d8b4a129292.zip"
)
# nanoarrow compresses and decompresses IPC bodies itself, using liblz4 and libzstd.
# In CI both come from vcpkg (see vcpkg.json), otherwise system copies are found.
# DuckDB's bundled zstd cannot be used: it is compiled as C++ inside a namespace, so
# nanoarrow's C sources cannot link against it. The extension links both libraries
# transitively through nanoarrow_ipc (needs CMake 3.24 or newer) and never includes
# their headers itself.
set(NANOARROW_IPC_WITH_ZSTD ON)
find_path(ZSTD_INCLUDE_DIR zstd.h REQUIRED)
find_path(LZ4_INCLUDE_DIR lz4frame.h REQUIRED)
find_library(LZ4_LIBRARY NAMES lz4 liblz4 REQUIRED)
get_filename_component(LZ4_LIBRARY_DIR "${LZ4_LIBRARY}" DIRECTORY)
set(NANOARROW_IPC_WITH_LZ4 ON)
set(LZ4_INCLUDE_DIRS "${LZ4_INCLUDE_DIR}")
set(LZ4_LIBRARY_DIRS "${LZ4_LIBRARY_DIR}")
set(LZ4_LIBRARIES "${LZ4_LIBRARY}")
fetchcontent_makeavailable(nanoarrow)
# DuckDB adds its own (namespaced, C++) zstd.h to the include path of every target in
# the tree, nanoarrow's C sources included. Put the real libzstd header ahead of it:
# BEFORE makes it the first include directory, and NO_SYSTEM_FROM_IMPORTED stops CMake
# from turning it into -isystem (which is searched after every -I) because the
# imported zstd target exports the same directory.
foreach(nanoarrow_ipc_target nanoarrow_ipc_static nanoarrow_ipc_shared)
  if(TARGET ${nanoarrow_ipc_target})
    set_target_properties(${nanoarrow_ipc_target} PROPERTIES NO_SYSTEM_FROM_IMPORTED ON)
    target_include_directories(${nanoarrow_ipc_target} BEFORE
                               PRIVATE "${ZSTD_INCLUDE_DIR}")
  endif()
endforeach()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/file_scanner/arrow_file_scan.cpp
    src/file_scanner/arrow_multi_file_info.cpp
    src/ipc/array_stream.cpp
    src/ipc/codecs.cpp
    src/ipc/stream_factory.cpp
    src/ipc/stream_reader/base_stream_reader.cpp
    src/ipc/stream_reader/ipc_file_stream_reader.cpp
    src/ipc/stream_reader/ipc_buffer_stream_reader.cpp
    src/scanner/read_arrow.cpp
    src/scanner/scan_arrow_ipc.cpp
    src/nanoarrow_extension.cpp
    src/writer/arrow_stream_writer.cpp
    src/writer/column_data_collection_serializer.cpp
    src/writer/write_arrow_stream.cpp
    src/writer/to_arrow_ipc.cpp)

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
