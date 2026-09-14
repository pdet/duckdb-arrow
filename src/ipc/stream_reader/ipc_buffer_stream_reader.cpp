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
  if (!initialized || cur_buffer.pos >= buffers[cur_idx].size) {
    if (initialized) {
      cur_idx++;
    }
    if (cur_idx >= buffers.size()) {
      finished = true;
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    }
    cur_buffer.ptr = reinterpret_cast<data_ptr_t>(buffers[cur_idx].ptr);
    cur_buffer.size = static_cast<int64_t>(buffers[cur_idx].size);
    cur_buffer.pos = 0;
    initialized = true;
  }
  std::memcpy(&message_prefix, ReadData(nullptr, sizeof(message_prefix)),
              sizeof(message_prefix));
  return DecodeMessage();
}

data_ptr_t IPCBufferStreamReader::ReadData(data_ptr_t ptr, idx_t size) {
  if (cur_buffer.pos > static_cast<idx_t>(cur_buffer.size) ||
      size > static_cast<idx_t>(cur_buffer.size) - cur_buffer.pos) {
    throw IOException("Arrow IPC buffer is truncated, it ends inside a message");
  }
  data_ptr_t cur_ptr = cur_buffer.ptr + cur_buffer.pos;
  cur_buffer.pos += size;
  return cur_ptr;
}

bool IPCBufferStreamReader::DecodeHeader(idx_t message_header_size) {
  // Our Header must contain the message prefix
  header.ptr = ReadData(header.ptr, message_header_size - sizeof(message_prefix)) -
               sizeof(message_prefix);
  header.size = message_header_size;
  return DecodeHeaderBuffer(AllocatedDataView(header.ptr, header.size));
}

void IPCBufferStreamReader::DecodeBody() {
  body = IPCBuffer{};
  if (decoder->body_size_bytes > 0) {
    body.ptr = ReadData(body.ptr, decoder->body_size_bytes);
    body.size = decoder->body_size_bytes;
  }
  if (body.ptr) {
    cur_ptr = body.ptr;
    cur_size = body.size;
  } else {
    cur_ptr = nullptr;
    cur_size = 0;
  }
}

nanoarrow::UniqueBuffer IPCBufferStreamReader::GetUniqueBuffer() {
  nanoarrow::UniqueBuffer out;
  nanoarrow::BufferInitWrapped(out.get(), body, body.ptr, body.size);
  return out;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
