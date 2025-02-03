//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// ipc/stream_reader/ipc_file_stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader/stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! IPC File
class IPCFileStreamReader final : public IPCStreamReader {
 public:
  IPCFileStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle, Allocator& allocator);

  ArrowIpcMessageType ReadNextMessage() override;

 private:
  static constexpr uint32_t kContinuationToken = 0xFFFFFFFF;

  BufferedFileReader file_reader;

  ArrowIpcMessagePrefix message_prefix{};
  AllocatedData message_header;
  shared_ptr<AllocatedData> message_body;

  void EnsureInputStreamAligned();

  static void DecodeArray(nanoarrow::ipc::UniqueDecoder &decoder, ArrowArray* out,  ArrowBufferView& body_view, ArrowError *error);

  void PopulateNames(vector<string>& names);
};

} // namespace ext_nanoarrow
} // namespace duckdb