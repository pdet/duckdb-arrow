//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/stream_reader/ipc_buffer_stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader/base_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Buffer Stream
class IPCBufferStreamReader final : public IPCStreamReader {
 public:
  IPCBufferStreamReader(vector<ArrowIPCBuffer> buffers, Allocator& allocator);

  ArrowIpcMessageType ReadNextMessage() override;

 private:
  data_ptr_t ReadData(data_ptr_t ptr, idx_t size) override;
  vector<ArrowIPCBuffer> buffers;
  idx_t cur_idx = 0;
  idx_t cur_buffer_pos = 0;
  data_ptr_t cur_buffer_ptr = nullptr;
  int64_t cur_buffer_size = 0;
  bool initialized = false;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
