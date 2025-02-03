#include "ipc/stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

IPCBufferStreamReader::IPCBufferStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle, Allocator& allocator){

}
  //! Gets the output schema, which is the file schema with projection pushdown being considered
const ArrowSchema* IPCBufferStreamReader::GetOutputSchema(){

}
bool IPCBufferStreamReader::GetNextBatch(ArrowArray* out){

}



} // namespace ext_nanoarrow
} // namespace duckdb
