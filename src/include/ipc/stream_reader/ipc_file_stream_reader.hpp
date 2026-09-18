//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/stream_reader/ipc_file_stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader/base_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! IPC File
class IPCFileStreamReader final : public IPCStreamReader {
 public:
  IPCFileStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle,
                      Allocator& allocator);

  ArrowIpcMessageType ReadNextMessage() override;

  double GetProgress();

  //! The size of the file being read, for estimating a row count without a footer
  idx_t FileSize() { return file_reader.FileSize(); }

  //! Reads the footer once, false when the file has none and the scan stays sequential
  bool TryReadFooter();
  //! The record batch blocks named by the footer, empty when there is no footer
  const vector<ArrowIpcFileBlock>& RecordBatchBlocks() const {
    return record_batch_blocks;
  }
  //! Dictionaries are decoded in stream order, so those files keep one reader
  bool HasDictionaryBlocks() const { return has_dictionary_blocks; }
  //! Reads only the record batches of these footer blocks, then reports the end
  void SetBlocks(const ArrowIpcFileBlock* begin, const ArrowIpcFileBlock* end);

 private:
  BufferedFileReader file_reader;
  AllocatedData message_header;
  shared_ptr<AllocatedData> message_body;
  //! Pipes and character devices must keep the sequential read
  bool regular_file = false;
  //! Blocks copied out of the decoder, which frees its own copy on the next message
  vector<ArrowIpcFileBlock> record_batch_blocks;
  bool has_dictionary_blocks = false;
  bool footer_read = false;
  //! The claimed blocks still to read, both null outside a block scan
  const ArrowIpcFileBlock* next_block = nullptr;
  const ArrowIpcFileBlock* end_block = nullptr;

  void EnsureInputStreamAligned();
  //! Whether the body can be read with one positional read instead of the buffered reader
  bool CanReadBodyPositionally(idx_t body_start, idx_t body_size);
  //! Reads only the buffers the projection needs, returns false to read the whole body
  bool TryReadProjectedBody(idx_t body_start, idx_t body_size);

  data_ptr_t ReadData(data_ptr_t ptr, idx_t size) override;
  static void DecodeArray(nanoarrow::ipc::UniqueDecoder& decoder, ArrowArray* out,
                          ArrowBufferView& body_view, ArrowError* error);
  bool DecodeHeader(idx_t message_header_size) override;
  void DecodeBody() override;
  nanoarrow::UniqueBuffer GetUniqueBuffer() override;
  void PopulateNames(vector<string>& names);
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
