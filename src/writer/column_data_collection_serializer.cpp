#include "writer/column_data_collection_serializer.hpp"

#include <utility>

#include "duckdb/common/arrow/arrow_appender.hpp"

namespace duckdb {

namespace ext_nanoarrow {

// Initialize buffer whose realloc operations go through DuckDB's memory
// accounting. Note that the Allocator must outlive the buffer (true for
// the case of this writer, but maybe not true for generic production of
// ArrowArrays whose lifetime might outlive the connection/database).
inline void InitArrowDuckBuffer(ArrowBuffer* buffer, Allocator& duck_allocator) {
  ArrowBufferInit(buffer);

  buffer->allocator.reallocate = [](ArrowBufferAllocator* allocator, uint8_t* ptr,
                                    int64_t old_size, int64_t new_size) -> uint8_t* {
    NANOARROW_DCHECK(allocator->private_data != nullptr);
    auto duck_allocator = static_cast<Allocator*>(allocator->private_data);
    if (ptr == nullptr && new_size > 0) {
      return duck_allocator->AllocateData(new_size);
    } else if (new_size == 0) {
      duck_allocator->FreeData(ptr, old_size);
      return nullptr;
    } else {
      return duck_allocator->ReallocateData(ptr, old_size, new_size);
    }
  };

  buffer->allocator.free = [](ArrowBufferAllocator* allocator, uint8_t* ptr,
                              int64_t old_size) {
    NANOARROW_DCHECK(allocator->private_data != nullptr);
    auto duck_allocator = static_cast<Allocator*>(allocator->private_data);
    duck_allocator->FreeData(ptr, old_size);
  };

  buffer->allocator.private_data = &duck_allocator;
}

ColumnDataCollectionSerializer::ColumnDataCollectionSerializer(
    ClientProperties options, Allocator& allocator,
    ArrowIpcCompressionOptions compression)
    : options(std::move(options)), allocator(allocator), compression(compression) {}

void ColumnDataCollectionSerializer::Init(const ArrowSchema* schema_p,
                                          const vector<LogicalType>& logical_types) {
  header.reset();
  body.reset();
  encoder.reset();
  chunk_view.reset();
  chunk_arrow.reset();

  InitArrowDuckBuffer(header.get(), allocator);
  InitArrowDuckBuffer(body.get(), allocator);
  NANOARROW_THROW_NOT_OK(ArrowIpcEncoderInit(encoder.get()));
  SetArrowIpcEncoderCompression(*encoder.get(), compression);
  THROW_NOT_OK(InternalException, &error,
               ArrowArrayViewInitFromSchema(chunk_view.get(), schema_p, &error));

  schema = schema_p;

  extension_types =
      ArrowTypeExtensionData::GetExtensionTypes(*options.client_context, logical_types);
}

void ColumnDataCollectionSerializer::SerializeSchema() {
  header->size_bytes = 0;
  body->size_bytes = 0;
  // Fails for types nanoarrow cannot write yet, such as string views
  THROW_NOT_OK(NotImplementedException, &error,
               ArrowIpcEncoderEncodeSchema(encoder.get(), schema, &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));
}

idx_t ColumnDataCollectionSerializer::Serialize(ArrowArray& array) {
  header->size_bytes = 0;
  body->size_bytes = 0;

  THROW_NOT_OK(duckdb::InternalException, &error,
               ArrowArrayViewSetArray(chunk_view.get(), &array, &error));
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderEncodeSimpleRecordBatch(encoder.get(), chunk_view.get(),
                                                      body.get(), &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));

  return 1;
}
idx_t ColumnDataCollectionSerializer::Serialize(DataChunk& chunk) {
  chunk_arrow.reset();
  ArrowConverter::ToArrowArray(chunk, chunk_arrow.get(), options, extension_types);
  return Serialize(*chunk_arrow.get());
}

idx_t ColumnDataCollectionSerializer::Serialize(const ColumnDataCollection& buffer) {
  header->size_bytes = 0;
  body->size_bytes = 0;
  if (buffer.Count() == 0) {
    return 0;
  }
  chunk_arrow.reset();
  ArrowAppender appender(buffer.Types(), buffer.Count(), options, extension_types);
  for (auto& chunk : buffer.Chunks()) {
    appender.Append(chunk, 0, chunk.size(), chunk.size());
  }
  ArrowArray array = appender.Finalize();
  ArrowArrayMove(&array, chunk_arrow.get());
  return Serialize(*chunk_arrow.get());
}

void ColumnDataCollectionSerializer::Flush(BufferedFileWriter& writer) {
  writer.WriteData(header->data, header->size_bytes);
  writer.WriteData(body->data, body->size_bytes);
}
nanoarrow::UniqueBuffer ColumnDataCollectionSerializer::GetHeader() {
  auto result_header = std::move(header);
  InitArrowDuckBuffer(header.get(), allocator);
  return result_header;
}
nanoarrow::UniqueBuffer ColumnDataCollectionSerializer::GetBody() {
  auto result_body = std::move(body);
  InitArrowDuckBuffer(body.get(), allocator);
  return result_body;
}
}  // namespace ext_nanoarrow
}  // namespace duckdb
