#include "ipc/stream_reader/ipc_buffer_stream_reader.hpp"

#include <iostream>

namespace duckdb {
namespace ext_nanoarrow {

IPCBufferStreamReader::IPCBufferStreamReader(vector<ArrowIPCBuffer> buffers,
                                             Allocator& allocator)
    : IPCStreamReader(allocator), buffers(std::move(buffers)) {}

ArrowIpcMessageType IPCBufferStreamReader::ReadNextMessage() {
  if ((!initialized && cur_idx == buffers.size()) || finished) {
    finished = true;
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }

  if (!initialized || cur_buffer_pos >= buffers[cur_idx].size) {
    if (initialized) {
      cur_idx++;
    }
    if (cur_idx >= buffers.size()) {
      finished = true;
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    }
    cur_buffer_ptr = reinterpret_cast<data_ptr_t>(buffers[cur_idx].ptr);
    cur_buffer_size = static_cast<int64_t>(buffers[cur_idx].size);
    cur_buffer_pos = 0;
    initialized = true;
  }
  ReadData(reinterpret_cast<data_ptr_t>(&message_prefix), sizeof(message_prefix));
  return DecodeMessage();
}

void IPCBufferStreamReader::ReadData(data_ptr_t ptr, idx_t size) {
  D_ASSERT(size + cur_buffer_pos < cur_buffer_size);
  memcpy(ptr, cur_buffer_ptr + cur_buffer_pos, size);
  cur_buffer_pos += size;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
