#include "writer/column_data_collection_serializer.hpp"

#include "duckdb/common/type_visitor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <cstring>
#include <utility>

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/bswap.hpp"
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

//! Whether Arrow has no exact type for a value of this type
static bool IsLossy(const LogicalType& type) {
  return type.id() == LogicalTypeId::HUGEINT || type.id() == LogicalTypeId::UHUGEINT;
}

//! The type a value is written as, which rebuilds only the types that hold a lossy one
static LogicalType WrittenType(const LogicalType& type) {
  if (!TypeVisitor::Contains(type, IsLossy)) {
    return type;
  }
  switch (type.id()) {
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UHUGEINT:
      // The decimal128 the schema declared anyway, with a cast that checks the range
      return LogicalType::DECIMAL(38, 0);
    case LogicalTypeId::LIST:
      return LogicalType::LIST(WrittenType(ListType::GetChildType(type)));
    case LogicalTypeId::ARRAY:
      return LogicalType::ARRAY(WrittenType(ArrayType::GetChildType(type)),
                                ArrayType::GetSize(type));
    case LogicalTypeId::MAP:
      return LogicalType::MAP(WrittenType(MapType::KeyType(type)),
                              WrittenType(MapType::ValueType(type)));
    case LogicalTypeId::STRUCT:
    case LogicalTypeId::TUPLE: {
      auto children = StructType::GetChildTypes(type);
      for (auto& child : children) {
        child.second = WrittenType(child.second);
      }
      return type.id() == LogicalTypeId::STRUCT ? LogicalType::STRUCT(children)
                                                : LogicalType::TUPLE(children);
    }
    case LogicalTypeId::UNION: {
      auto members = UnionType::CopyMemberTypes(type);
      for (auto& member : members) {
        member.second = WrittenType(member.second);
      }
      return LogicalType::UNION(members);
    }
    default:
      return type;
  }
}

//! The type a column is written as, since Arrow has no exact type for some of DuckDB's
static LogicalType WriteType(const LogicalType& type, const ClientProperties& options) {
  // The lossless conversion keeps every value in an extension type
  return options.arrow_lossless_conversion ? type : WrittenType(type);
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
  vector<LogicalType> write_types;
  for (const auto& type : types) {
    write_types.push_back(WriteType(type, options));
  }
  ArrowConverter::ToArrowSchema(schema.get(), write_types, IdentifiersToStrings(names),
                                options);
  for (int64_t i = 0; i < schema->n_children; i++) {
    CheckEncodableField(*schema->children[i], schema->children[i]->name);
  }
  return schema;
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

  InitArrowDuckBuffer(header.get(), allocator);
  InitArrowDuckBuffer(body.get(), allocator);
  NANOARROW_THROW_NOT_OK(ArrowIpcEncoderInit(encoder.get()));
  SetArrowIpcEncoderCompression(*encoder.get(), compression);
  THROW_NOT_OK(InternalException, &error,
               ArrowArrayViewInitFromSchema(chunk_view.get(), schema, &error));

  write_types.clear();
  cast_expressions.clear();
  cast_executor.reset();
  for (idx_t i = 0; i < logical_types.size(); i++) {
    write_types.push_back(WriteType(logical_types[i], options));
    unique_ptr<Expression> column =
        make_uniq<BoundReferenceExpression>(logical_types[i], i);
    if (write_types[i] != logical_types[i]) {
      column = BoundCastExpression::AddCastToType(*options.client_context,
                                                  std::move(column), write_types[i]);
    }
    cast_expressions.push_back(std::move(column));
  }
  if (write_types != logical_types) {
    cast_executor =
        make_uniq<ExpressionExecutor>(*options.client_context, cast_expressions);
    cast_chunk.Destroy();
    cast_chunk.Initialize(allocator, write_types);
  }
  extension_types =
      ArrowTypeExtensionData::GetExtensionTypes(*options.client_context, write_types);
}

DataChunk& ColumnDataCollectionSerializer::Cast(DataChunk& chunk) {
  if (!cast_executor) {
    return chunk;
  }
  cast_chunk.Reset();
  cast_executor->Execute(chunk, cast_chunk);
  return cast_chunk;
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

// A zero null count lets readers take every value as valid without a bitmap
static void DropUnusedValidity(ArrowArrayView& view) {
  for (int64_t c = 0; c < view.n_children; c++) {
    auto& child = *view.children[c];
    if (child.null_count == 0 &&
        child.layout.buffer_type[0] == NANOARROW_BUFFER_TYPE_VALIDITY) {
      child.buffer_views[0].size_bytes = 0;
    }
    DropUnusedValidity(child);
  }
}

idx_t ColumnDataCollectionSerializer::Serialize(ArrowAppender& appender) {
  ArrowArray finalized = appender.Finalize();
  nanoarrow::UniqueArray array(&finalized);
  header->size_bytes = 0;
  body->size_bytes = 0;

  THROW_NOT_OK(duckdb::InternalException, &error,
               ArrowArrayViewSetArray(chunk_view.get(), array.get(), &error));
  DropUnusedValidity(*chunk_view.get());
  if (compression.type == NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
    // One exact allocation, where growing buffer by buffer doubles and keeps the peaks
    NANOARROW_THROW_NOT_OK(
        ArrowBufferReserve(body.get(), PaddedBodySize(*chunk_view.get())));
  }
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
    auto& written = Cast(chunk);
    appender.Append(written, 0, written.size(), written.size());
  }
  return Serialize(appender);
}

namespace {
//! Keeps an encoded buffer alive while the async writer writes a slice of it
class ArrowWriteBuffer final : public AsyncWriteBuffer {
 public:
  ArrowWriteBuffer(shared_ptr<nanoarrow::UniqueBuffer> buffer_p, idx_t offset, idx_t size)
      : buffer(std::move(buffer_p)), offset(offset), size(size) {}

  data_ptr_t Ptr() override { return (*buffer)->data + offset; }
  idx_t Size() const override { return size; }

 private:
  shared_ptr<nanoarrow::UniqueBuffer> buffer;
  idx_t offset;
  idx_t size;
};

//! Slices sized like parquet pages keep the async writer draining several at once
constexpr idx_t kAsyncWriteSliceBytes = 4 * 1024 * 1024;

void WriteSlices(AsyncFileWriter& writer, nanoarrow::UniqueBuffer buffer) {
  const auto size = NumericCast<idx_t>(buffer->size_bytes);
  auto shared = make_shared_ptr<nanoarrow::UniqueBuffer>(std::move(buffer));
  for (idx_t offset = 0; offset < size; offset += kAsyncWriteSliceBytes) {
    writer.WriteData(make_uniq<ArrowWriteBuffer>(
        shared, offset, MinValue<idx_t>(kAsyncWriteSliceBytes, size - offset)));
  }
}
}  // namespace

ArrowIpcFileBlock ColumnDataCollectionSerializer::Flush(AsyncFileWriter& writer) {
  ArrowIpcFileBlock block{NumericCast<int64_t>(writer.GetTotalWritten()),
                          NumericCast<int32_t>(header->size_bytes), body->size_bytes};
  // The message registers as one batch, so backpressure applies once, as parquet does
  auto batch = writer.StartBatch();
  WriteSlices(writer, GetHeader());
  if (body->size_bytes > 0) {
    auto result_body = std::move(body);
    InitArrowDuckBuffer(body.get(), allocator);
    WriteSlices(writer, std::move(result_body));
  }
  batch.Finish();
  return block;
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
