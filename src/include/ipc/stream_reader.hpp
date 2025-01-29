//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// ipc/stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "nanoarrow_errors.hpp"

namespace duckdb {
namespace ext_nanoarrow {

// Missing in nanoarrow_ipc.hpp
struct UniqueSharedBuffer {
  struct ArrowIpcSharedBuffer data{};

  ~UniqueSharedBuffer() {
    if (data.private_src.allocator.free != nullptr) {
      ArrowIpcSharedBufferReset(&data);
    }
  }
};

struct ArrowIpcMessagePrefix {
  uint32_t continuation_token;
  int32_t metadata_size;
};

class IpcStreamReader {
 public:
  IpcStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle, Allocator& allocator);

  const ArrowSchema* GetFileSchema();

  bool HasProjection();

  const ArrowSchema* GetOutputSchema();

  void PopulateNames(vector<string>& names);

  static nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder();

  bool GetNextBatch(ArrowArray* out);

  void SetColumnProjection(vector<string> column_names);

 private:
    static constexpr uint32_t kContinuationToken = 0xFFFFFFFF;
  nanoarrow::ipc::UniqueDecoder decoder{};
  bool finished{false};
  BufferedFileReader file_reader;
  Allocator& allocator;
  ArrowError error{};

  ArrowIpcMessagePrefix message_prefix{};
  AllocatedData message_header;
  shared_ptr<AllocatedData> message_body;

  nanoarrow::UniqueSchema file_schema;
  nanoarrow::UniqueSchema projected_schema;
  vector<int64_t> projected_fields;

  ArrowIpcMessageType ReadNextMessage(vector<ArrowIpcMessageType> expected_types,
                                      bool end_of_stream_ok = true);

  ArrowIpcMessageType ReadNextMessage();

  void EnsureInputStreamAligned();

  static int64_t CountFields(const ArrowSchema* schema);

  static ArrowBufferView AllocatedDataView(const AllocatedData& data);

  static nanoarrow::UniqueBuffer AllocatedDataToOwningBuffer(shared_ptr<AllocatedData> data);

  static const char* MessageTypeString(ArrowIpcMessageType message_type);

};
}
}