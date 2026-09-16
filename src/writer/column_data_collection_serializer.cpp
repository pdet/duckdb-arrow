#include "writer/column_data_collection_serializer.hpp"

#include <cstring>
#include <utility>

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

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
                                             const vector<Identifier>& names,
                                             ClientProperties& options) {
  nanoarrow::UniqueSchema schema;
  ArrowConverter::ToArrowSchema(schema.get(), types, IdentifiersToStrings(names),
                                options);
  for (int64_t i = 0; i < schema->n_children; i++) {
    CheckEncodableField(*schema->children[i], schema->children[i]->name);
  }
  return schema;
}

static bool IsWideInteger(const LogicalType& type) {
  return type.id() == LogicalTypeId::HUGEINT || type.id() == LogicalTypeId::UHUGEINT;
}

// HUGEINT and UHUGEINT are written as DECIMAL(38, 0) unless lossless conversion is on
static LogicalType ArrowIpcWriteType(const LogicalType& type,
                                     const ClientProperties& options) {
  if (options.arrow_lossless_conversion || !TypeVisitor::Contains(type, IsWideInteger)) {
    return type;
  }
  return TypeVisitor::VisitReplace(type, [](const LogicalType& child) -> LogicalType {
    return IsWideInteger(child) ? LogicalType::DECIMAL(38, 0) : child;
  });
}

ColumnDataCollectionSerializer::ColumnDataCollectionSerializer(
    ClientProperties options, Allocator& allocator,
    ArrowIpcCompressionOptions compression, bool track_body_size)
    : options(std::move(options)),
      allocator(allocator),
      compression(compression),
      track_body_size(track_body_size) {}

void ColumnDataCollectionSerializer::Init(const ArrowSchema* schema,
                                          const vector<LogicalType>& logical_types) {
  header.reset();
  body.reset();
  encoder.reset();
  chunk_view.reset();
  write_types.clear();
  casts.clear();
  cast_executor.reset();
  cast_chunk.Destroy();

  InitArrowDuckBuffer(header.get(), allocator);
  InitArrowDuckBuffer(body.get(), allocator);
  NANOARROW_THROW_NOT_OK(ArrowIpcEncoderInit(encoder.get()));
  SetArrowIpcEncoderCompression(*encoder.get(), compression);
  THROW_NOT_OK(InternalException, &error,
               ArrowArrayViewInitFromSchema(chunk_view.get(), schema, &error));

  auto& context = *options.client_context;
  for (const auto& type : logical_types) {
    write_types.push_back(ArrowIpcWriteType(type, options));
  }
  extension_types = ArrowTypeExtensionData::GetExtensionTypes(context, write_types);
  if (write_types == logical_types) {
    return;
  }
  // A checked cast rejects values the Arrow decimal128 cannot hold, unlike a raw copy
  for (idx_t i = 0; i < logical_types.size(); i++) {
    casts.push_back(BoundCastExpression::AddCastToType(
        context,
        make_uniq<BoundReferenceExpression>(schema->children[i]->name, logical_types[i],
                                            i),
        write_types[i]));
  }
  cast_executor = make_uniq<ExpressionExecutor>(context, casts);
  cast_chunk.Initialize(context, write_types);
}

void ColumnDataCollectionSerializer::SerializeSchema(const ArrowSchema* schema,
                                                     idx_t reserved_size) {
  header->size_bytes = 0;
  body->size_bytes = 0;
  THROW_NOT_OK(NotImplementedException, &error,
               ArrowIpcEncoderEncodeSchema(encoder.get(), schema, &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));
  if (reserved_size) {
    if (reserved_size < static_cast<idx_t>(header->size_bytes)) {
      throw InternalException("Arrow IPC schema does not fit in its reserved message");
    }
    // Include padding in the message length to keep record batch offsets unchanged
    auto length = BSwapIfBE(NumericCast<int32_t>(reserved_size - 2 * sizeof(int32_t)));
    NANOARROW_THROW_NOT_OK(ArrowBufferAppendFill(
        header.get(), 0, NumericCast<int64_t>(reserved_size) - header->size_bytes));
    std::memcpy(header->data + sizeof(int32_t), &length, sizeof(length));
  }
}

void ColumnDataCollectionSerializer::SerializeFooter(
    nanoarrow::UniqueSchema schema, const vector<ArrowIpcFileBlock>& blocks) {
  header->size_bytes = 0;
  body->size_bytes = 0;
  nanoarrow::ipc::UniqueFooter footer;
  ArrowSchemaMove(schema.get(), &footer->schema);
  NANOARROW_THROW_NOT_OK(
      ArrowBufferAppend(&footer->record_batch_blocks, blocks.data(),
                        NumericCast<int64_t>(blocks.size() * sizeof(ArrowIpcFileBlock))));
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderEncodeFooter(encoder.get(), footer.get(), &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), false, header.get()));
}

// Uncompressed body size, the buffers ArrowIpcEncoderCollectArray collects padded to 8
static int64_t PaddedBodySize(const ArrowArrayView& view) {
  int64_t size = 0;
  for (int64_t c = 0; c < view.n_children; c++) {
    const auto& child = *view.children[c];
    for (int64_t b = 0; b < child.array->n_buffers; b++) {
      size += AlignValue<int64_t>(child.buffer_views[b].size_bytes);
    }
    size += PaddedBodySize(child);
  }
  return size;
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
  if (track_body_size) {
    // An uncompressed body already has the padded size, so only compressed ones walk
    uncompressed_body_size = compression.type == NANOARROW_IPC_COMPRESSION_TYPE_NONE
                                 ? body->size_bytes
                                 : PaddedBodySize(*chunk_view.get());
  }
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));

  return 1;
}
idx_t ColumnDataCollectionSerializer::Serialize(const ColumnDataCollection& buffer) {
  if (buffer.Count() == 0) {
    return 0;
  }
  ArrowAppender appender(write_types, buffer.Count(), options, extension_types);
  for (auto& chunk : buffer.Chunks()) {
    auto& write_chunk = CastToWriteTypes(chunk);
    appender.Append(write_chunk, 0, write_chunk.size(), write_chunk.size());
  }
  return Serialize(appender);
}

DataChunk& ColumnDataCollectionSerializer::CastToWriteTypes(DataChunk& input) {
  if (!cast_executor) {
    return input;
  }
  cast_chunk.Reset();
  cast_executor->Execute(input, cast_chunk);
  return cast_chunk;
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
