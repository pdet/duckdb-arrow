//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// ipc/ipc_stream_factory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/array_stream.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {
namespace ext_nanoarrow {

class ArrowStreamFactory {
  ArrowStreamFactory(){};
};
//! This Factory is a type invented by DuckDB. Notably, the Produce()
//! function pointer is passed to the constructor of the ArrowScanFunctionData
//! constructor (which we wrap).
class ArrowIPCStreamFactory {
public:
  explicit ArrowIPCStreamFactory(ClientContext& context,
                                           std::string  src_string);

  //! Called once when initializing Scan States
  static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr,  ArrowStreamParameters& parameters);

  //! Get the schema of the arrow object
  void GetFileSchema(ArrowSchemaWrapper& schema) const;

  //! Opens the file, wraps it in the ArrowIpcInputStream, and wraps it in
  //! the ArrowArrayStream reader.
  void InitReader();

  FileSystem& fs;
  Allocator& allocator;
  std::string src_string;
  unique_ptr<IPCStreamReader> reader;
  ArrowError error{};
};
} // namespace ext_nanoarrow
} // namespace duckdb
