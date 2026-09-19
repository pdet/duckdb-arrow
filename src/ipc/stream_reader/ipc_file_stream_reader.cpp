#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

#include <algorithm>

#include "duckdb/common/file_system.hpp"
#include "ipc/file_format.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! A half open byte range inside a record batch body
struct BodyRange {
  idx_t begin;
  idx_t end;
};

//! Ranges nearer than this are read together, sized for one seek on a local disk
constexpr idx_t kCoalesceGapBytes = 64 * 1024;
//! Small bodies are read whole, several reads cost more than the bytes they save
constexpr idx_t kMinBodyBytesForRanges = 1024 * 1024;
//! Reading most of the body is the whole body read with extra system calls
constexpr double kMaxProjectedFraction = 0.8;
//! Enough for the footer of most files, so reading it costs one request
constexpr idx_t kFooterReadBytes = 64 * 1024;

//! Makes the discovery decode take the uncompressed path, which only does arithmetic
struct CodecOverride {
  explicit CodecOverride(ArrowIpcDecoder& decoder)
      : decoder(decoder), saved(decoder.codec) {
    decoder.codec = NANOARROW_IPC_COMPRESSION_TYPE_NONE;
  }
  ~CodecOverride() { decoder.codec = saved; }
  ArrowIpcDecoder& decoder;
  ArrowIpcCompressionType saved;
};

//! Collects the body ranges of one decoded field, children only and never dictionaries
bool CollectFieldRanges(const ArrowArrayView& view, const_data_ptr_t base,
                        idx_t body_size, vector<BodyRange>& ranges) {
  for (int i = 0; i < NANOARROW_MAX_FIXED_BUFFERS; i++) {
    if (view.layout.buffer_type[i] == NANOARROW_BUFFER_TYPE_NONE) {
      break;
    }
    const auto& buffer = view.buffer_views[i];
    // An empty buffer carries no pointer to take an offset from
    if (buffer.size_bytes <= 0 || buffer.data.data == nullptr) {
      continue;
    }
    if (buffer.data.as_uint8 < base) {
      return false;
    }
    const auto begin = static_cast<idx_t>(buffer.data.as_uint8 - base);
    const auto size = static_cast<idx_t>(buffer.size_bytes);
    if (begin > body_size || size > body_size - begin) {
      return false;
    }
    ranges.push_back(BodyRange{begin, begin + size});
  }
  // A dictionary is decoded from its own batch body, so its buffers are not in ours
  for (int64_t i = 0; i < view.n_children; i++) {
    if (!CollectFieldRanges(*view.children[i], base, body_size, ranges)) {
      return false;
    }
  }
  return true;
}

//! Whether a field or any of its children is dictionary encoded
bool HasDictionary(const ArrowSchema& schema) {
  if (schema.dictionary) {
    return true;
  }
  for (int64_t i = 0; i < schema.n_children; i++) {
    if (HasDictionary(*schema.children[i])) {
      return true;
    }
  }
  return false;
}

//! Copies blocks out of the decoder, which frees its own copy on the next message
bool CopyFooterBlocks(const ArrowBuffer& source, idx_t file_size,
                      vector<ArrowIpcFileBlock>& blocks) {
  const auto count = static_cast<idx_t>(source.size_bytes) / sizeof(ArrowIpcFileBlock);
  blocks.resize(count);
  if (count > 0) {
    std::memcpy(blocks.data(), source.data, count * sizeof(ArrowIpcFileBlock));
  }
  // A block naming bytes outside the file would send a positional read anywhere
  for (const auto& block : blocks) {
    // Messages start aligned after the leading magic, and a scan seeks straight to them
    if (block.offset < static_cast<int64_t>(kArrowIPCFileHeaderSize) ||
        block.offset % 8 != 0 || block.metadata_length <= 0 || block.body_length < 0) {
      return false;
    }
    const auto end = static_cast<idx_t>(block.offset) +
                     static_cast<idx_t>(block.metadata_length) +
                     static_cast<idx_t>(block.body_length);
    if (end > file_size) {
      return false;
    }
  }
  return true;
}

}  // namespace
IPCFileStreamReader::IPCFileStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle,
                                         Allocator& allocator)
    : IPCStreamReader(allocator), file_reader(fs, std::move(handle)) {
  // Regular and remote files read at an offset, and asking a remote one its type opens it
  positional = FileSystem::IsRemoteFile(file_reader.handle->GetPath()) ||
               file_reader.handle->GetType() == FileType::FILE_TYPE_REGULAR;
}

bool IPCFileStreamReader::TryReadFooter() {
  if (footer_read) {
    return !record_batch_blocks.empty();
  }
  footer_read = true;
  // Only the file format has a footer, and the schema read checks its leading magic
  GetBaseSchema();
  if (!positional || !file_magic) {
    return false;
  }
  // The tail of a file is the footer size as an int32 then the bare magic
  constexpr idx_t kFooterTailSize = sizeof(int32_t) + kArrowIPCFileMagicSize;
  const auto file_size = file_reader.FileSize();
  if (file_size < kArrowIPCFileHeaderSize + kFooterTailSize) {
    return false;
  }

  // One read usually holds the whole footer, which saves a round trip on remote files
  const auto end_size = MinValue<idx_t>(file_size, kFooterReadBytes);
  // A mismatch is reported by formatting the end of the buffer, so keep a NUL past it
  auto end = allocator.Allocate(end_size + 1);
  end.get()[end_size] = 0;
  file_reader.handle->Read(end.get(), end_size, file_size - end_size);

  auto scratch = NewDuckDBArrowDecoder();
  ArrowError footer_error{};
  if (ArrowIpcDecoderPeekFooter(
          scratch.get(),
          AllocatedDataView(end.get() + end_size - kFooterTailSize, kFooterTailSize),
          &footer_error) != NANOARROW_OK) {
    return false;
  }
  // Verification adds the tail to this size in int32, so bound it in 64 bits first
  const auto footer_size = static_cast<int64_t>(scratch->header_size_bytes);
  if (footer_size <= 0 || static_cast<idx_t>(footer_size) > file_size - kFooterTailSize) {
    return false;
  }

  // Verification rejects a window that is not the footer plus the tail
  const auto window_size = static_cast<idx_t>(footer_size) + kFooterTailSize;
  // The verifier checks alignment, so the window gets its own allocation
  auto window = allocator.Allocate(window_size + 1);
  window.get()[window_size] = 0;
  if (window_size <= end_size) {
    std::memcpy(window.get(), end.get() + end_size - window_size, window_size);
  } else {
    file_reader.handle->Read(window.get(), window_size, file_size - window_size);
  }
  const auto window_view =
      AllocatedDataView(window.get(), static_cast<int64_t>(window_size));
  if (ArrowIpcDecoderVerifyFooter(scratch.get(), window_view, &footer_error) !=
          NANOARROW_OK ||
      ArrowIpcDecoderDecodeFooter(scratch.get(), window_view, &footer_error) !=
          NANOARROW_OK) {
    return false;
  }

  const auto& footer = *scratch->footer;
  if (!CopyFooterBlocks(footer.record_batch_blocks, file_size, record_batch_blocks) ||
      !CopyFooterBlocks(footer.dictionary_blocks, file_size, dictionary_blocks)) {
    record_batch_blocks.clear();
    dictionary_blocks.clear();
    return false;
  }
  return !record_batch_blocks.empty();
}

void IPCFileStreamReader::PopulateNames(vector<string>& names) {
  GetBaseSchema();
  for (int64_t i = 0; i < base_schema->n_children; i++) {
    const ArrowSchema* column = base_schema->children[i];
    if (!column->name) {
      names.push_back("");
    } else {
      names.push_back(column->name);
    }
  }
}

void IPCFileStreamReader::TrackProgress(shared_ptr<atomic<idx_t>> offset) {
  progress_offset = std::move(offset);
}

void IPCFileStreamReader::DecodeArray(nanoarrow::ipc::UniqueDecoder& decoder,
                                      ArrowArray* out, ArrowBufferView& body_view,
                                      ArrowError* error) {
  // Use the ArrowIpcSharedBuffer if we have thread safety (i.e., if this was
  // compiled with a compiler that supports C11 atomics, i.e., not gcc 4.8 or
  // MSVC)
  nanoarrow::UniqueArray array;
  THROW_NOT_OK(IOException, error,
               ArrowIpcDecoderDecodeArray(decoder.get(), body_view, -1, array.get(),
                                          NANOARROW_VALIDATION_LEVEL_FULL, error));
  ArrowArrayMove(array.get(), out);
}

nanoarrow::UniqueBuffer IPCFileStreamReader::GetUniqueBuffer() {
  return AllocatedDataToOwningBuffer(message_body);
}
bool IPCFileStreamReader::DecodeHeader(const idx_t message_header_size) {
  if (message_header.GetSize() < message_header_size) {
    message_header = allocator.Allocate(message_header_size);
  }
  // Read the message header. I believe the fact that this loops and calls
  // the file handle's Read() method with relatively small chunks will ensure that
  // an attempt to read a very large message_header_size can be cancelled. If this
  // is not the case, we might want to implement our own buffering.
  std::memcpy(message_header.get(), &message_prefix, sizeof(message_prefix));
  ReadData(message_header.get() + sizeof(message_prefix),
           message_header_size - sizeof(message_prefix));

  return DecodeHeaderBuffer(AllocatedDataView(message_header.get(), message_header_size));
}

bool IPCFileStreamReader::CanReadBodyPositionally(idx_t body_start, idx_t body_size) {
  // CanSeek answers for the file system, not for this handle, so it lets pipes through
  if (!positional) {
    return false;
  }
  // A body running past the end keeps the sequential read, which reports truncation
  const auto file_size = file_reader.FileSize();
  return body_start <= file_size && body_size <= file_size - body_start;
}

void IPCFileStreamReader::ReadBodyPositionally(idx_t body_start, idx_t body_size) {
  // A body smaller than the buffer reads through it, which also buffers the next header
  if (body_size < FILE_BUFFER_SIZE) {
    ReadData(message_body->get(), body_size);
    return;
  }
  // Reading the header usually buffered the start of the body, so copy it, not reread it
  const auto buffered =
      MinValue<idx_t>(file_reader.read_data - file_reader.offset, body_size);
  ReadData(message_body->get(), buffered);
  // One read replaces the 4 KB reads the buffered reader would issue
  file_reader.handle->Read(message_body->get() + buffered, body_size - buffered,
                           body_start + buffered);
  file_reader.Seek(body_start + body_size);
}

bool IPCFileStreamReader::TryReadProjectedBody(idx_t body_start, idx_t body_size) {
  if (!HasProjection() || body_size < kMinBodyBytesForRanges) {
    return false;
  }
  // Dictionary batches are decoded whole, and only a record batch has projected fields
  if (decoder->message_type != NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH) {
    return false;
  }
  // Unions carried an extra buffer before V5, which shifts every later buffer index
  if (decoder->metadata_version < NANOARROW_IPC_METADATA_VERSION_V5) {
    return false;
  }
  // A swap rewrites buffers into scratch space, so their addresses stop meaning offsets
  if (NeedsEndianSwap()) {
    return false;
  }

  const auto base = const_data_ptr_cast(message_body->get());
  const auto body_view = AllocatedDataView(base, static_cast<int64_t>(body_size));

  vector<BodyRange> ranges;
  {
    // Compressed buffers are found at the same offsets, only their contents differ
    CodecOverride codec_override(*decoder.get());
    for (const auto field_index : projected_fields) {
      ArrowArrayView* view = nullptr;
      if (ArrowIpcDecoderDecodeArrayViewWithDictionaries(decoder.get(), body_view,
                                                         field_index, dictionaries.get(),
                                                         &view, &error) != NANOARROW_OK) {
        return false;
      }
      if (!CollectFieldRanges(*view, base, body_size, ranges)) {
        return false;
      }
    }
  }
  if (ranges.empty()) {
    return false;
  }

  std::sort(ranges.begin(), ranges.end(),
            [](const BodyRange& a, const BodyRange& b) { return a.begin < b.begin; });
  vector<BodyRange> merged;
  for (const auto& range : ranges) {
    if (!merged.empty() && range.begin <= merged.back().end + kCoalesceGapBytes) {
      if (range.end > merged.back().end) {
        merged.back().end = range.end;
      }
    } else {
      merged.push_back(range);
    }
  }

  idx_t projected_bytes = 0;
  for (const auto& range : merged) {
    projected_bytes += range.end - range.begin;
  }
  if (static_cast<double>(projected_bytes) >
      static_cast<double>(body_size) * kMaxProjectedFraction) {
    return false;
  }

  for (const auto& range : merged) {
    file_reader.handle->Read(message_body->get() + range.begin, range.end - range.begin,
                             body_start + range.begin);
  }
  file_reader.Seek(body_start + body_size);
  return true;
}

void IPCFileStreamReader::DecodeBody() {
  message_body.reset();
  if (decoder->body_size_bytes > 0) {
    EnsureInputStreamAligned();
    // The padding belongs to the previous message, so take the offset after it
    const auto body_start = file_reader.CurrentOffset();
    const auto body_size = static_cast<idx_t>(decoder->body_size_bytes);
    message_body =
        make_shared_ptr<AllocatedData>(allocator.Allocate(decoder->body_size_bytes));

    // Seeking drops the buffer, so a body smaller than it is cheaper to read through it
    if (skip_bodies && body_size >= FILE_BUFFER_SIZE && !NeedsEndianSwap() &&
        CanReadBodyPositionally(body_start, body_size)) {
      // A swap reads the buffers it rewrites, so only an unswapped body can stay unread
      file_reader.Seek(body_start + body_size);
    } else if (CanReadBodyPositionally(body_start, body_size)) {
      if (!TryReadProjectedBody(body_start, body_size)) {
        ReadBodyPositionally(body_start, body_size);
      }
    } else {
      ReadData(message_body->get(), decoder->body_size_bytes);
    }
  }
  if (message_body) {
    cur_ptr = message_body->get();
    cur_size = static_cast<int64_t>(message_body->GetSize());
  } else {
    cur_ptr = nullptr;
    cur_size = 0;
  }
}

data_ptr_t IPCFileStreamReader::ReadData(data_ptr_t ptr, idx_t size) {
  file_reader.ReadData(ptr, size);
  return ptr;
}

bool IPCFileStreamReader::CanCountWithoutBodies() {
  GetBaseSchema();
  int64_t flat_index = 0;
  for (int64_t i = 0; i < base_schema->n_children; i++) {
    // A skipped dictionary batch leaves its fields without the values the view needs
    if (!HasDictionary(*base_schema->children[i])) {
      count_field = flat_index;
      return true;
    }
    flat_index += CountFields(base_schema->children[i]);
  }
  return false;
}

bool IPCFileStreamReader::NextBatchLength(idx_t& length) {
  if (count_field < 0 && !CanCountWithoutBodies()) {
    throw InternalException("NextBatchLength needs a field without dictionaries");
  }
  skip_bodies = true;
  ArrowIpcMessageType message_type;
  do {
    message_type =
        IPCStreamReader::ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH,
                                          NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH});
    if (message_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
      return false;
    }
  } while (message_type != NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH);

  // The view only takes offsets inside the body, so an unread body gives the length
  const auto body_view = AllocatedDataView(cur_ptr, cur_size);
  CodecOverride codec_override(*decoder.get());
  ArrowArrayView* view = nullptr;
  THROW_NOT_OK(
      IOException, &error,
      ArrowIpcDecoderDecodeArrayViewWithDictionaries(
          decoder.get(), body_view, count_field, dictionaries.get(), &view, &error));
  if (view->length < 0) {
    throw IOException("Arrow IPC record batch has a negative length");
  }
  length = static_cast<idx_t>(view->length);
  return true;
}

void IPCFileStreamReader::LoadDictionaries(const vector<ArrowIpcFileBlock>& blocks) {
  if (blocks.empty()) {
    return;
  }
  SetBlocks(blocks.data(), blocks.data() + blocks.size());
  // Only dictionary blocks are named, so the read decodes them all and reaches the end
  nanoarrow::UniqueArray none;
  if (GetNextBatch(none.get())) {
    throw IOException("Arrow IPC footer names a record batch as a dictionary block");
  }
}

void IPCFileStreamReader::SetBlocks(const ArrowIpcFileBlock* begin,
                                    const ArrowIpcFileBlock* end) {
  next_block = begin;
  end_block = end;
  finished = false;
}

ArrowIpcMessageType IPCFileStreamReader::ReadNextMessage() {
  if (next_block) {
    if (next_block == end_block) {
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    }
    // TryReadFooter checked that the block lies inside the file on an aligned offset
    file_reader.Seek(static_cast<idx_t>(next_block->offset));
    next_block++;
  }
  if (finished) {
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }
  if (progress_offset) {
    progress_offset->store(file_reader.CurrentOffset());
  }

  // If there is no more data to be read, we're done!
  idx_t message_start = file_reader.CurrentOffset();
  try {
    EnsureInputStreamAligned();
    message_start = file_reader.CurrentOffset();
    file_reader.ReadData(reinterpret_cast<data_ptr_t>(&message_prefix),
                         sizeof(message_prefix));

    // Read the embedded stream after the file header.
    if (file_reader.CurrentOffset() == kArrowIPCFileHeaderSize &&
        std::memcmp(kArrowIPCFileMagic, &message_prefix, kArrowIPCFileHeaderSize) == 0) {
      file_magic = true;
      uint32_t token;
      do {
        file_reader.ReadData(reinterpret_cast<data_ptr_t>(&token), sizeof(token));
      } while (token != kContinuationToken);
      // Read the metadata size
      message_prefix.continuation_token = kContinuationToken;
      file_reader.ReadData(reinterpret_cast<data_ptr_t>(&message_prefix.metadata_size),
                           sizeof(message_prefix.metadata_size));
    } else if (message_prefix.continuation_token != kContinuationToken) {
      throw IOException(std::string("Expected continuation token (0xFFFFFFFF) but got " +
                                    std::to_string(message_prefix.continuation_token)));
    }
  } catch (SerializationException& e) {
    // Only a stream that stops at a message boundary may omit the end of stream marker
    if (message_start < file_reader.FileSize()) {
      throw IOException("Arrow IPC stream is truncated, it ends inside a message prefix");
    }
    finished = true;
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }

  try {
    return DecodeMessage();
  } catch (SerializationException& e) {
    throw IOException("Arrow IPC stream is truncated, it ends inside a message");
  }
}

void IPCFileStreamReader::EnsureInputStreamAligned() {
  uint8_t padding[8];
  int padding_bytes = 8 - (file_reader.CurrentOffset() % 8);
  if (padding_bytes != 8) {
    file_reader.ReadData(padding, padding_bytes);
  }
  D_ASSERT((file_reader.CurrentOffset() % 8) == 0);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
