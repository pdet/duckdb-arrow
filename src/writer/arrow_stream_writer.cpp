#include "writer/arrow_stream_writer.hpp"

#include <limits>

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "ipc/file_format.hpp"

namespace duckdb {

namespace ext_nanoarrow {

namespace {

constexpr char kTotalCompressedSize[] = "total_compressed_size";
constexpr char kTotalUncompressedSize[] = "total_uncompressed_size";

void AddSchemaMetadata(ArrowSchema* schema, const vector<pair<string, string>>& metadata,
                       bool replace = false) {
  if (metadata.empty()) {
    return;
  }
  nanoarrow::UniqueBuffer packed;
  NANOARROW_THROW_NOT_OK(ArrowMetadataBuilderInit(packed.get(), schema->metadata));
  for (const auto& item : metadata) {
    ArrowStringView key{item.first.data(), NumericCast<int64_t>(item.first.size())};
    ArrowStringView value{item.second.data(), NumericCast<int64_t>(item.second.size())};
    NANOARROW_THROW_NOT_OK(replace
                               ? ArrowMetadataBuilderSet(packed.get(), key, value)
                               : ArrowMetadataBuilderAppend(packed.get(), key, value));
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
                                     bool file_format, bool size_metadata)
    : options(options_p),
      allocator(BufferAllocator::Get(*options.client_context)),
      serializer(options, allocator, size_metadata),
      logical_types(logical_types),
      file_format(file_format),
      size_metadata(size_metadata) {
  InitSchema(schema_p, metadata);
  InitOutputFile(fs, file_path);
}

void ArrowStreamWriter::InitSchema(const ArrowSchema& schema_p,
                                   const vector<pair<string, string>>& metadata) {
  // Copy into a nanoarrow owned schema so the metadata set below is freed with it
  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(&schema_p, schema.get()));

  AddSchemaMetadata(schema.get(), metadata);
  if (size_metadata) {
    // Reserve space for the longest totals before writing any record batches
    const auto max_size = std::to_string(std::numeric_limits<int64_t>::max());
    AddSchemaMetadata(schema.get(), {{kTotalCompressedSize, max_size},
                                     {kTotalUncompressedSize, max_size}});
  }

  serializer.Init(schema.get(), logical_types);
}

void ArrowStreamWriter::InitOutputFile(FileSystem& fs, const string& file_path) {
  writer = make_uniq<BufferedFileWriter>(
      fs, file_path.c_str(),
      FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  if (size_metadata && (!writer->handle->OnDiskFile() || !writer->handle->CanSeek() ||
                        writer->handle->IsPipe())) {
    throw IOException(
        "SIZE_METADATA requires a seekable local output to update the schema");
  }
  if (file_format) {
    writer->WriteData(const_data_ptr_cast(kArrowIPCFileMagic), kArrowIPCFileHeaderSize);
  }
}

void ArrowStreamWriter::WriteSchema() {
  serializer.SerializeSchema(schema.get());
  schema_message_size = serializer.Flush(*writer).metadata_length;
  file_size = writer->GetTotalWritten();
}

unique_ptr<ColumnDataCollectionSerializer> ArrowStreamWriter::NewSerializer() const {
  auto serializer =
      make_uniq<ColumnDataCollectionSerializer>(options, allocator, size_metadata);
  serializer->Init(schema.get(), logical_types);
  return serializer;
}

// Encoding under the lock bounds memory to one Arrow array and one body at a time
void ArrowStreamWriter::Flush(ColumnDataCollection& buffer) {
  if (buffer.Count() == 0) {
    return;
  }
  lock_guard<mutex> guard(lock);
  serializer.Serialize(buffer);
  buffer.Reset();
  FlushInternal(serializer);
}

// DuckDB flushes prepared batches one at a time in order
void ArrowStreamWriter::Flush(ColumnDataCollectionSerializer& serializer) {
  FlushInternal(serializer);
}

void ArrowStreamWriter::FlushInternal(ColumnDataCollectionSerializer& serializer) {
  auto block = serializer.Flush(*writer);
  if (file_format) {
    blocks.push_back(block);
  }
  if (size_metadata) {
    total_compressed_size += block.body_length;
    total_uncompressed_size += serializer.UncompressedBodySize();
  }
  ++row_group_count;
  file_size = writer->GetTotalWritten();
}

void ArrowStreamWriter::Finalize() {
  uint8_t end_of_stream[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};
  writer->WriteData(end_of_stream, sizeof(end_of_stream));
  if (file_format) {
    WriteFooter();
  }
  file_size = writer->GetTotalWritten();
  writer->Close();
}

void ArrowStreamWriter::WriteFooter() {
  nanoarrow::UniqueSchema footer_schema;
  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(schema.get(), footer_schema.get()));
  if (size_metadata) {
    AddSchemaMetadata(footer_schema.get(),
                      {{kTotalCompressedSize, std::to_string(total_compressed_size)},
                       {kTotalUncompressedSize, std::to_string(total_uncompressed_size)}},
                      true);
    // Update the opening schema to match the footer without changing its reserved size
    serializer.SerializeSchema(footer_schema.get(), schema_message_size);
    auto opening_schema = serializer.GetHeader();
    writer->Flush();
    const auto end_offset = writer->GetTotalWritten();
    writer->handle->Write(QueryContext(), opening_schema->data,
                          NumericCast<idx_t>(opening_schema->size_bytes),
                          kArrowIPCFileHeaderSize);
    writer->handle->Seek(end_offset);
  }
  serializer.SerializeFooter(std::move(footer_schema), blocks);
  auto footer = serializer.GetHeader();
  auto footer_size = NumericCast<int32_t>(footer->size_bytes);
  writer->WriteData(footer->data, footer->size_bytes);
  writer->Write<int32_t>(BSwapIfBE(footer_size));
  writer->WriteData(const_data_ptr_cast(kArrowIPCFileMagic), kArrowIPCFileMagicSize);
}

bool ArrowStreamWriter::IsSizeMetadataKey(const string& key) {
  return key == kTotalCompressedSize || key == kTotalUncompressedSize;
}

idx_t ArrowStreamWriter::NumberOfRowGroups() const { return row_group_count; }

idx_t ArrowStreamWriter::FileSize() const { return file_size; }

}  // namespace ext_nanoarrow
}  // namespace duckdb
