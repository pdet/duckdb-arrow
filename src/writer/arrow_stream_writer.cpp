#include "writer/arrow_stream_writer.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "ipc/file_format.hpp"

namespace duckdb {

namespace ext_nanoarrow {

ArrowStreamWriter::ArrowStreamWriter(const ClientProperties& options_p, FileSystem& fs,
                                     const string& file_path,
                                     const vector<LogicalType>& logical_types,
                                     const ArrowSchema& schema_p,
                                     const vector<pair<string, string>>& metadata,
                                     bool file_format)
    : options(options_p),
      allocator(BufferAllocator::Get(*options.client_context)),
      serializer(options, allocator),
      logical_types(logical_types),
      file_format(file_format) {
  InitSchema(schema_p, metadata);
  InitOutputFile(fs, file_path);
}

void ArrowStreamWriter::InitSchema(const ArrowSchema& schema_p,
                                   const vector<pair<string, string>>& metadata) {
  // Copy into a nanoarrow owned schema so the metadata set below is freed with it
  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(&schema_p, schema.get()));

  if (!metadata.empty()) {
    nanoarrow::UniqueBuffer metadata_packed;
    NANOARROW_THROW_NOT_OK(
        ArrowMetadataBuilderInit(metadata_packed.get(), schema->metadata));
    ArrowStringView key{};
    ArrowStringView value{};
    for (const auto& item : metadata) {
      key = {item.first.data(), static_cast<int64_t>(item.first.size())};
      value = {item.second.data(), static_cast<int64_t>(item.second.size())};
      NANOARROW_THROW_NOT_OK(
          ArrowMetadataBuilderAppend(metadata_packed.get(), key, value));
    }

    NANOARROW_THROW_NOT_OK(ArrowSchemaSetMetadata(
        schema.get(), reinterpret_cast<char*>(metadata_packed->data)));
  }

  serializer.Init(schema.get(), logical_types);
}

void ArrowStreamWriter::InitOutputFile(FileSystem& fs, const string& file_path) {
  writer = make_uniq<BufferedFileWriter>(
      fs, file_path.c_str(),
      FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  if (file_format) {
    writer->WriteData(const_data_ptr_cast(kArrowIPCFileMagic), kArrowIPCFileHeaderSize);
  }
}

void ArrowStreamWriter::WriteSchema() {
  serializer.SerializeSchema(schema.get());
  serializer.Flush(*writer);
  file_size = writer->GetTotalWritten();
}

unique_ptr<ColumnDataCollectionSerializer> ArrowStreamWriter::NewSerializer() const {
  auto serializer = make_uniq<ColumnDataCollectionSerializer>(options, allocator);
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
  serializer.SerializeFooter(schema.get(), blocks);
  auto footer = serializer.GetHeader();
  auto footer_size = NumericCast<int32_t>(footer->size_bytes);
  writer->WriteData(footer->data, footer->size_bytes);
  writer->Write<int32_t>(BSwapIfBE(footer_size));
  writer->WriteData(const_data_ptr_cast(kArrowIPCFileMagic), kArrowIPCFileMagicSize);
}

idx_t ArrowStreamWriter::NumberOfRowGroups() const { return row_group_count; }

idx_t ArrowStreamWriter::FileSize() const { return file_size; }

}  // namespace ext_nanoarrow
}  // namespace duckdb
