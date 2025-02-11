#include "ipc/stream_reader/ipc_buffer_stream_reader.hpp"

#include <iostream>

namespace duckdb {
namespace ext_nanoarrow {

IPCBufferStreamReader::IPCBufferStreamReader(vector<ArrowIPCBuffer> buffers, Allocator& allocator): IPCStreamReader(allocator), buffers(std::move(buffers)){

}

ArrowIpcMessageType IPCBufferStreamReader::ReadNextMessage() {
  if (cur_idx >= buffers.size() || finished) {
      finished = true;
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    }
    std::cout << cur_idx;
    cur_ptr = reinterpret_cast<data_ptr_t>(buffers[cur_idx].ptr);
    cur_size = static_cast<int64_t>(buffers[cur_idx].size);
    cur_idx++;
    return NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH;
}



} // namespace ext_nanoarrow
} // namespace duckdb
