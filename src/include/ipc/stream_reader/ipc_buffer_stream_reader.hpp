//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// ipc/stream_reader/ipc_buffer_stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader/stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Buffer Stream
class IPCBufferStreamReader final : public IPCStreamReader {
public:
  IPCBufferStreamReader(vector<ArrowIPCBuffer> buffers, Allocator& allocator);

  ArrowIpcMessageType ReadNextMessage() override;
private:
  vector<ArrowIPCBuffer> buffers;
  idx_t cur_idx = 0;
};

} // namespace ext_nanoarrow
} // namespace duckdb