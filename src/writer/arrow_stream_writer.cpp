#include "writer/arrow_stream_writer.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "ipc/file_format.hpp"

namespace duckdb {

namespace ext_nanoarrow {

namespace {

constexpr char kTotalCompressedSize[] = "total_compressed_size";
constexpr char kTotalUncompressedSize[] = "total_uncompressed_size";

void SetSchemaMetadata(ArrowSchema* schema,
                       const vector<pair<string, string>>& metadata) {
  nanoarrow::UniqueBuffer packed;
  NANOARROW_THROW_NOT_OK(ArrowMetadataBuilderInit(packed.get(), schema->metadata));
  for (const auto& item : metadata) {
    ArrowStringView key{item.first.data(), static_cast<int64_t>(item.first.size())};
    ArrowStringView value{item.second.data(), static_cast<int64_t>(item.second.size())};
    NANOARROW_THROW_NOT_OK(ArrowMetadataBuilderSet(packed.get(), key, value));
  }
  NANOARROW_THROW_NOT_OK(
      ArrowSchemaSetMetadata(schema, reinterpret_cast<char*>(packed->data)));
}

}  // namespace

ArrowStreamWriter::ArrowStreamWriter(const ClientProperties& options_p, FileSystem& fs,
                                     const string& file_path,
                                     const vector<LogicalType>& logical_types,
                                     const ArrowSchema& schema_p,
                                     const vector<pair<string, string>>& metadata,
                                     const vector<ArrowFieldMetadata>& field_metadata,
                                     const ArrowIpcCompressionOptions& compression,
                                     bool file_format, bool size_metadata)
    : options(options_p),
      allocator(BufferAllocator::Get(*options.client_context)),
      compression(compression),
      logical_types(logical_types),
      file_format(file_format),
      size_metadata(size_metadata) {
  InitSchema(schema_p, metadata, field_metadata);
  InitOutputFile(fs, file_path);
}

void ArrowStreamWriter::InitSchema(const ArrowSchema& schema_p,
                                   const vector<pair<string, string>>& metadata,
                                   const vector<ArrowFieldMetadata>& field_metadata) {
  // Copy into a nanoarrow owned schema so the metadata set below is freed with it
  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(&schema_p, schema.get()));
  if (!metadata.empty()) {
    SetSchemaMetadata(schema.get(), metadata);
  }
  for (const auto& field : field_metadata) {
    SetSchemaMetadata(schema->children[field.column_index], field.metadata);
  }
  if (size_metadata) {
    // Reserve space for the longest totals before writing any record batches
    const auto max_size = std::to_string(NumericLimits<int64_t>::Maximum());
    SetSchemaMetadata(schema.get(), {{kTotalCompressedSize, max_size},
                                     {kTotalUncompressedSize, max_size}});
  }
}

ArrowStreamWriter::~ArrowStreamWriter() {
  // Finalize closes the handle, so an open one here means the output is incomplete
  // The async writer aborts its own unfinished output when it is destroyed
  if (!writer || !writer->handle) {
    return;
  }
  try {
    writer->handle->AbortWrite();
  } catch (...) {
  }
}

void ArrowStreamWriter::InitOutputFile(FileSystem& fs, const string& file_path) {
  auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW;
  // Only a path this COPY creates may be removed when the write is aborted
  if (!fs.FileExists(file_path) && !fs.IsPipe(file_path)) {
    flags |= FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
  }
  // Remote outputs upload parts at once through the async writer, local ones buffer
  if (size_metadata || !FileSystem::IsRemoteFile(file_path)) {
    writer = make_uniq<BufferedFileWriter>(fs, file_path.c_str(), flags);
    // WriteFooter patches offset 8 of a new regular file, fsspec needs OnDiskFile first
    if (size_metadata && (!writer->handle->OnDiskFile() ||
                          writer->handle->GetType() != FileType::FILE_TYPE_REGULAR ||
                          writer->handle->GetFileSize() != 0)) {
      throw IOException(
          "SIZE_METADATA requires a seekable local output to update the schema");
    }
  } else {
    async_writer =
        make_uniq<AsyncFileWriter>(*options.client_context, fs, file_path, flags);
  }
  if (file_format) {
    WriteBytes(const_data_ptr_cast(kArrowIPCFileMagic), kArrowIPCFileHeaderSize);
  }
}

void ArrowStreamWriter::WriteSchema() {
  auto serializer = NewSerializer();
  serializer->SerializeSchema(schema.get());
  schema_message_size = WriteMessage(*serializer).metadata_length;
  file_size = TotalWritten();
}

unique_ptr<ColumnDataCollectionSerializer> ArrowStreamWriter::NewSerializer() const {
  auto serializer = make_uniq<ColumnDataCollectionSerializer>(options, allocator,
                                                              compression, size_metadata);
  serializer->Init(schema.get(), logical_types);
  return serializer;
}

void ArrowStreamWriter::Flush(ColumnDataCollectionSerializer& serializer) {
  lock_guard<mutex> guard(lock);
  auto block = WriteMessage(serializer);
  if (file_format) {
    blocks.push_back(block);
  }
  if (size_metadata) {
    total_compressed_size += block.body_length;
    total_uncompressed_size += serializer.UncompressedBodySize();
  }
  ++row_group_count;
  file_size = TotalWritten();
}

void ArrowStreamWriter::Finalize() {
  uint8_t end_of_stream[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};
  lock_guard<mutex> guard(lock);
  WriteBytes(end_of_stream, sizeof(end_of_stream));
  if (file_format) {
    WriteFooter();
  }
  file_size = TotalWritten();
  if (async_writer) {
    async_writer->Close();
  } else {
    writer->Close();
  }
}

void ArrowStreamWriter::WriteBytes(const_data_ptr_t data, idx_t size) {
  if (async_writer) {
    async_writer->WriteData(data, size);
  } else {
    writer->WriteData(data, size);
  }
}

ArrowIpcFileBlock ArrowStreamWriter::WriteMessage(
    ColumnDataCollectionSerializer& serializer) {
  return async_writer ? serializer.Flush(*async_writer) : serializer.Flush(*writer);
}

idx_t ArrowStreamWriter::TotalWritten() const {
  return async_writer ? async_writer->GetTotalWritten() : writer->GetTotalWritten();
}

void ArrowStreamWriter::WriteFooter() {
  auto serializer = NewSerializer();
  nanoarrow::UniqueSchema footer_schema;
  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(schema.get(), footer_schema.get()));
  if (size_metadata) {
    SetSchemaMetadata(
        footer_schema.get(),
        {{kTotalCompressedSize, std::to_string(total_compressed_size)},
         {kTotalUncompressedSize, std::to_string(total_uncompressed_size)}});
    // Update the opening schema to match the footer without changing its reserved size
    serializer->SerializeSchema(footer_schema.get(), schema_message_size);
    auto opening_schema = serializer->GetHeader();
    writer->Flush();
    const auto end_offset = writer->GetTotalWritten();
    writer->handle->Write(QueryContext(), opening_schema->data,
                          NumericCast<idx_t>(opening_schema->size_bytes),
                          kArrowIPCFileHeaderSize);
    writer->handle->Seek(end_offset);
  }
  serializer->SerializeFooter(std::move(footer_schema), blocks);
  auto footer = serializer->GetHeader();
  const auto footer_size = BSwapIfBE(NumericCast<int32_t>(footer->size_bytes));
  WriteBytes(footer->data, footer->size_bytes);
  WriteBytes(const_data_ptr_cast(&footer_size), sizeof(footer_size));
  WriteBytes(const_data_ptr_cast(kArrowIPCFileMagic), kArrowIPCFileMagicSize);
}

bool ArrowStreamWriter::IsSizeMetadataKey(const string& key) {
  return key == kTotalCompressedSize || key == kTotalUncompressedSize;
}

idx_t ArrowStreamWriter::NumberOfRowGroups() const { return row_group_count; }

idx_t ArrowStreamWriter::FileSize() const { return file_size; }

}  // namespace ext_nanoarrow
}  // namespace duckdb
