//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// table_function/arrow_ipc_function_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table/arrow.hpp"
#include "ipc/stream_factory.hpp"

namespace duckdb {
namespace ext_nanoarrow {
//! Our FunctionData is the same as the ArrowScanFunctionData except we extend it
//! to keep the ArrowIpcArrowArrayStreamFactory alive.
struct ArrowIPCFunctionData : public ArrowScanFunctionData {
  explicit ArrowIPCFunctionData(std::unique_ptr<ArrowIPCStreamFactory> factory)
      : ArrowScanFunctionData(ArrowIPCStreamFactory::Produce,
                              reinterpret_cast<uintptr_t>(factory.get())),
        factory(std::move(factory)) {}
  std::unique_ptr<ArrowIPCStreamFactory> factory;
};
}  // namespace ext_nanoarrow
}  // namespace duckdb
