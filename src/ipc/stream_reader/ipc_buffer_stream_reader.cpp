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
  int32_t continuation_token;
  int32_t metadata_size;
  ReadData(reinterpret_cast<data_ptr_t>(&continuation_token), sizeof(continuation_token));
  message_prefix.continuation_token = continuation_token;
  if (continuation_token != kContinuationToken) {
    if (message_prefix.continuation_token < 0) {
      throw IOException(std::string("Expected continuation token (0xFFFFFFFF) but got " +
                                    std::to_string(message_prefix.continuation_token)));
    }
    metadata_size = continuation_token;
    message_prefix.continuation_token = kContinuationToken;
  } else {
    ReadData(reinterpret_cast<data_ptr_t>(&metadata_size), sizeof(metadata_size));
  }
  message_prefix.metadata_size = metadata_size;
  return DecodeMessage();
}

void IPCBufferStreamReader::ReadData(data_ptr_t ptr, idx_t size) {
  D_ASSERT(size + cur_buffer_pos < cur_buffer_size);
  memcpy(ptr, cur_buffer_ptr + cur_buffer_pos, size);
  cur_buffer_pos += size;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
