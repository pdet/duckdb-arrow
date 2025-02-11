//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
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
  void ReadData(data_ptr_t ptr, idx_t size) override;
  vector<ArrowIPCBuffer> buffers;
  idx_t cur_idx = 0;
  idx_t cur_buffer_pos = 0;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb