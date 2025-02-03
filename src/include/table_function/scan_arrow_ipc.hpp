//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// table_function/scan_arrow_ipc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table/arrow.hpp"
//#include "arrow_stream_buffer.hpp"

#include "duckdb.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! IPC Table scan is identical to ArrowTableFunction arrow scan except instead
//! of CDataInterface header pointers, it takes a bunch of pointers pointing to
//! buffers containing data in Arrow IPC format
// struct ScanArrowIPC {
//   static void RegisterReadArrowStream(DatabaseInstance& db);
// };
}  // namespace ext_nanoarrow
}  // namespace duckdb
