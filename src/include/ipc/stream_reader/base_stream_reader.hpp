//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/stream_reader/base_stream_reader.hpp
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

#include "table_function/scan_arrow_ipc.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Missing in nanoarrow_ipc.hpp
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

//! Base IPC Reader
class IPCStreamReader {
 public:
  virtual ~IPCStreamReader() = default;
  explicit IPCStreamReader(Allocator& allocator)
      : decoder(NewDuckDBArrowDecoder()), allocator(allocator) {};
  //! Gets the output schema, which is the file schema with projection pushdown being
  //! considered
  const ArrowSchema* GetOutputSchema();
  //! Gets the next batch
  bool GetNextBatch(ArrowArray* out);

  //! Sets the projection pushdown for this reader
  void SetColumnProjection(const vector<string>& column_names);
  //! Gets the base schema with no projection pushdown
  const ArrowSchema* GetBaseSchema();

  ArrowIpcMessageType ReadNextMessage(vector<ArrowIpcMessageType> expected_types,
                                      bool end_of_stream_ok = true);
  virtual ArrowIpcMessageType ReadNextMessage() {
    throw InternalException("IPCStreamReader::ReadNextMessage not implemented");
  }

 protected:
  virtual void EnsureInputStreamAligned() {
    //! nop by default. This method is called in the DecodeMessage() and necessary only
    //! for the file reader, for the buffer reader it does nothing.
  }
  virtual void ReadData(data_ptr_t ptr, idx_t size) {
    throw InternalException("IPCStreamReader::ReadData not implemented");
  }
  ArrowIpcMessageType DecodeMessage();
  bool HasProjection() const;
  static nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder();

  static ArrowBufferView AllocatedDataView(const_data_ptr_t data, int64_t size);
  static nanoarrow::UniqueBuffer AllocatedDataToOwningBuffer(
      const shared_ptr<AllocatedData>& data);

  static const char* MessageTypeString(ArrowIpcMessageType message_type);

  static int64_t CountFields(const ArrowSchema* schema);

  ArrowError error{};
  nanoarrow::ipc::UniqueDecoder decoder{};
  vector<int64_t> projected_fields;
  nanoarrow::UniqueSchema projected_schema;
  //! Schema without projection applied to it
  nanoarrow::UniqueSchema base_schema;

  //! Information on current buffer
  data_ptr_t cur_ptr{};
  int64_t cur_size{};

  //! Allocator used to allocate buffers with decoded arrow information
  Allocator& allocator;

  bool finished{false};

  ArrowIpcMessagePrefix message_prefix{};
  AllocatedData message_header;
  shared_ptr<AllocatedData> message_body;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
