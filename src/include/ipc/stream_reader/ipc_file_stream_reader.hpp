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

  //! Publishes the offset of each message it reaches, for progress read on another thread
  void TrackProgress(shared_ptr<atomic<idx_t>> offset);

  //! The size of the file being read, for estimating a row count without a footer
  idx_t FileSize() { return file_reader.FileSize(); }

  //! Reads the footer once, false when the file has none and the scan stays sequential
  bool TryReadFooter();
  //! The record batch blocks named by the footer, empty when there is no footer
  const vector<ArrowIpcFileBlock>& RecordBatchBlocks() const {
    return record_batch_blocks;
  }
  //! The dictionary batch blocks named by the footer
  const vector<ArrowIpcFileBlock>& DictionaryBlocks() const { return dictionary_blocks; }
  //! Decodes these dictionary blocks, which the record batch blocks need first
  void LoadDictionaries(const vector<ArrowIpcFileBlock>& blocks);
  //! Reads only the record batches of these footer blocks, then reports the end
  void SetBlocks(const ArrowIpcFileBlock* begin, const ArrowIpcFileBlock* end);
  //! Whether batch lengths can be read from the headers alone
  bool CanCountWithoutBodies();
  //! Reads the length of the next record batch and skips its body, false at the end
  bool NextBatchLength(idx_t& length);

 private:
  BufferedFileReader file_reader;
  AllocatedData message_header;
  shared_ptr<AllocatedData> message_body;
  //! Pipes and character devices must keep the sequential read
  bool positional = false;
  shared_ptr<atomic<idx_t>> progress_offset;
  vector<ArrowIpcFileBlock> record_batch_blocks;
  vector<ArrowIpcFileBlock> dictionary_blocks;
  bool footer_read = false;
  //! Whether the file starts with the magic of the file format, which has a footer
  bool file_magic = false;
  //! The claimed blocks still to read, both null outside a block scan
  const ArrowIpcFileBlock* next_block = nullptr;
  const ArrowIpcFileBlock* end_block = nullptr;
  //! Counting reads headers only, so regular files seek past the bodies
  bool skip_bodies = false;
  //! The flat index of the field whose view gives the batch length, negative when none
  int64_t count_field = -1;

  void EnsureInputStreamAligned();
  //! Whether the body can be read with one positional read instead of the buffered reader
  bool CanReadBodyPositionally(idx_t body_start, idx_t body_size);
  //! Reads a whole body, small ones through the buffer and large ones positionally
  void ReadBodyPositionally(idx_t body_start, idx_t body_size);
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
