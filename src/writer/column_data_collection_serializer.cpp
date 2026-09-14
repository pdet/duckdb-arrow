#include "writer/column_data_collection_serializer.hpp"

#include <utility>

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

namespace ext_nanoarrow {

// The allocator must outlive the buffers it owns.
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

static void CheckEncodableField(const ArrowSchema& field, const char* column) {
  if (field.dictionary) {
    throw NotImplementedException(
        "Arrow IPC output does not support ENUM values in column \"%s\", cast it to "
        "VARCHAR",
        column);
  }
  ArrowSchemaView view;
  ArrowError error{};
  THROW_NOT_OK(InternalException, &error, ArrowSchemaViewInit(&view, &field, &error));
  switch (view.type) {
    case NANOARROW_TYPE_STRING_VIEW:
    case NANOARROW_TYPE_BINARY_VIEW:
    case NANOARROW_TYPE_LIST_VIEW:
    case NANOARROW_TYPE_LARGE_LIST_VIEW:
      throw NotImplementedException(
          "Arrow IPC output does not support the Arrow view type \"%s\" in column "
          "\"%s\", reset arrow_output_version or the arrow view settings",
          field.format, column);
    default:
      break;
  }
  for (int64_t i = 0; i < field.n_children; i++) {
    CheckEncodableField(*field.children[i], column);
  }
}

nanoarrow::UniqueSchema CreateArrowIpcSchema(const vector<LogicalType>& types,
                                             const vector<string>& names,
                                             ClientProperties& options) {
  nanoarrow::UniqueSchema schema;
  ArrowConverter::ToArrowSchema(schema.get(), types, names, options);
  for (int64_t i = 0; i < schema->n_children; i++) {
    CheckEncodableField(*schema->children[i], schema->children[i]->name);
  }
  return schema;
}

ColumnDataCollectionSerializer::ColumnDataCollectionSerializer(ClientProperties options,
                                                               Allocator& allocator)
    : options(std::move(options)), allocator(allocator) {}

void ColumnDataCollectionSerializer::Init(const ArrowSchema* schema,
                                          const vector<LogicalType>& logical_types) {
  header.reset();
  body.reset();
  encoder.reset();
  chunk_view.reset();

  InitArrowDuckBuffer(header.get(), allocator);
  InitArrowDuckBuffer(body.get(), allocator);
  NANOARROW_THROW_NOT_OK(ArrowIpcEncoderInit(encoder.get()));
  THROW_NOT_OK(InternalException, &error,
               ArrowArrayViewInitFromSchema(chunk_view.get(), schema, &error));

  extension_types =
      ArrowTypeExtensionData::GetExtensionTypes(*options.client_context, logical_types);
}

void ColumnDataCollectionSerializer::SerializeSchema(const ArrowSchema* schema) {
  header->size_bytes = 0;
  body->size_bytes = 0;
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderEncodeSchema(encoder.get(), schema, &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));
}

void ColumnDataCollectionSerializer::SerializeFooter(
    const ArrowSchema* schema, const vector<ArrowIpcFileBlock>& blocks) {
  header->size_bytes = 0;
  body->size_bytes = 0;
  nanoarrow::ipc::UniqueFooter footer;
  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(schema, &footer->schema));
  NANOARROW_THROW_NOT_OK(
      ArrowBufferAppend(&footer->record_batch_blocks, blocks.data(),
                        NumericCast<int64_t>(blocks.size() * sizeof(ArrowIpcFileBlock))));
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderEncodeFooter(encoder.get(), footer.get(), &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), false, header.get()));
}

idx_t ColumnDataCollectionSerializer::Serialize(ArrowAppender& appender) {
  ArrowArray finalized = appender.Finalize();
  nanoarrow::UniqueArray array(&finalized);
  header->size_bytes = 0;
  body->size_bytes = 0;

  THROW_NOT_OK(duckdb::InternalException, &error,
               ArrowArrayViewSetArray(chunk_view.get(), array.get(), &error));
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderEncodeSimpleRecordBatch(encoder.get(), chunk_view.get(),
                                                      body.get(), &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));

  return 1;
}
idx_t ColumnDataCollectionSerializer::Serialize(const ColumnDataCollection& buffer) {
  ArrowAppender appender(buffer.Types(), buffer.Count(), options, extension_types);
  for (auto& chunk : buffer.Chunks()) {
    appender.Append(chunk, 0, chunk.size(), chunk.size());
  }
  return Serialize(appender);
}

ArrowIpcFileBlock ColumnDataCollectionSerializer::Flush(BufferedFileWriter& writer) {
  ArrowIpcFileBlock block{NumericCast<int64_t>(writer.GetTotalWritten()),
                          NumericCast<int32_t>(header->size_bytes), body->size_bytes};
  writer.WriteData(header->data, header->size_bytes);
  writer.WriteData(body->data, body->size_bytes);
  return block;
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
