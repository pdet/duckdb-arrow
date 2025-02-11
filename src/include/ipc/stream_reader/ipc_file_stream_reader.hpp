//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// ipc/stream_reader/ipc_file_stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader/base_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! IPC File
class IPCFileStreamReader final : public IPCStreamReader {
 public:
  IPCFileStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle,
                      Allocator& allocator);

  ArrowIpcMessageType ReadNextMessage() override;

 private:
  static constexpr uint32_t kContinuationToken = 0xFFFFFFFF;

  BufferedFileReader file_reader;

  void EnsureInputStreamAligned() override;

  void ReadData(data_ptr_t ptr, idx_t size) override;
  static void DecodeArray(nanoarrow::ipc::UniqueDecoder& decoder, ArrowArray* out,
                          ArrowBufferView& body_view, ArrowError* error);

  void PopulateNames(vector<string>& names);
};

}  // namespace ext_nanoarrow
}  // namespace duckdb