//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// ipc_stream_factory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/array_stream.hpp"

namespace duckdb {
namespace ext_nanoarrow {


//! This Factory is a type invented by DuckDB. Notably, the Produce()
//! function pointer is passed to the constructor of the ArrowScanFunctionData
//! constructor (which we wrap).
class ArrowIpcArrowArrayStreamFactory {
public:
  explicit ArrowIpcArrowArrayStreamFactory(ClientContext& context,
                                           const std::string& src_string)
      : fs(FileSystem::GetFileSystem(context)),
        allocator(BufferAllocator::Get(context)),
        src_string(src_string) {};

  // Called once when initializing Scan States
  static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr,
                                                     ArrowStreamParameters& parameters) {
    auto factory = static_cast<ArrowIpcArrowArrayStreamFactory*>(
        reinterpret_cast<void*>(factory_ptr));

    if (!factory->reader) {
      throw InternalException("IpcStreamReader was not initialized or was already moved");
    }

    if (!parameters.projected_columns.columns.empty()) {
      factory->reader->SetColumnProjection(parameters.projected_columns.columns);
    }

    auto out = make_uniq<ArrowArrayStreamWrapper>();
    IpcArrayStream(std::move(factory->reader)).ToArrayStream(&out->arrow_array_stream);
    return out;
  }

  // Get the schema of the arrow object
  void GetFileSchema(ArrowSchemaWrapper& schema) {
    if (!reader) {
      throw InternalException("IpcStreamReader is no longer valid");
    }

    NANOARROW_THROW_NOT_OK(
        ArrowSchemaDeepCopy(reader->GetFileSchema(), &schema.arrow_schema));
  }

  // Opens the file, wraps it in the ArrowIpcInputStream, and wraps it in
  // the ArrowArrayStream reader.
  void InitReader() {
    if (reader) {
      throw InternalException("ArrowArrayStream or IpcStreamReader already initialized");
    }

    unique_ptr<FileHandle> handle =
        fs.OpenFile(src_string, FileOpenFlags::FILE_FLAGS_READ);
    reader = make_uniq<IpcStreamReader>(fs, std::move(handle), allocator);
  }

  FileSystem& fs;
  Allocator& allocator;
  std::string src_string;
  unique_ptr<IpcStreamReader> reader;
  ArrowError error{};
};
}
}