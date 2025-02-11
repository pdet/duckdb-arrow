//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// writer/column_data_collection_serializer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_properties.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"
#include "nanoarrow_errors.hpp"

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
    auto duck_allocator = reinterpret_cast<Allocator*>(allocator->private_data);
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
    auto duck_allocator = reinterpret_cast<Allocator*>(allocator->private_data);
    duck_allocator->FreeData(ptr, old_size);
  };

  buffer->allocator.private_data = &duck_allocator;
}

class ColumnDataCollectionSerializer {
 public:
  ColumnDataCollectionSerializer(ClientProperties& options, Allocator& allocator)
      : options(options), allocator(allocator) {}

  void Init(ArrowSchema* schema_p, const vector<LogicalType>& logical_types) {
    InitArrowDuckBuffer(header.get(), allocator);
    InitArrowDuckBuffer(body.get(), allocator);
    NANOARROW_THROW_NOT_OK(ArrowIpcEncoderInit(encoder.get()));
    THROW_NOT_OK(InternalException, &error,
                 ArrowArrayViewInitFromSchema(chunk_view.get(), schema_p, &error));

    schema = schema_p;

    extension_types =
        ArrowTypeExtensionData::GetExtensionTypes(*options.client_context, logical_types);
  }

  void SerializeSchema() {
    header->size_bytes = 0;
    body->size_bytes = 0;
    THROW_NOT_OK(InternalException, &error,
                 ArrowIpcEncoderEncodeSchema(encoder.get(), schema, &error));
    NANOARROW_THROW_NOT_OK(
        ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));
  }

  idx_t Serialize(DataChunk& chunk) {
    header->size_bytes = 0;
    body->size_bytes = 0;
    chunk_arrow.reset();

    ArrowConverter::ToArrowArray(chunk, chunk_arrow.get(), options, extension_types);
    THROW_NOT_OK(duckdb::InternalException, &error,
                 ArrowArrayViewSetArray(chunk_view.get(), chunk_arrow.get(), &error));
    THROW_NOT_OK(InternalException, &error,
                 ArrowIpcEncoderEncodeSimpleRecordBatch(encoder.get(), chunk_view.get(),
                                                        body.get(), &error));
    NANOARROW_THROW_NOT_OK(
        ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));

    return 1;
  }

  idx_t Serialize(const ColumnDataCollection& buffer) {
    header->size_bytes = 0;
    body->size_bytes = 0;
    if (buffer.Count() == 0) {
      return 0;
    }
    // The ArrowConverter requires all of this to be in one big DataChunk.
    // It would be better to append these one at a time using other DuckDB
    // internals like the ArrowAppender. (Possibly better would be to skip the
    // owning ArrowArray entirely and just expose an ArrowArrayView of the
    // chunk. keeping track of any owning elements that had to be allocated,
    // since that's all that is strictly required to write).
    DataChunk chunk;
    chunk.Initialize(allocator, buffer.Types(), buffer.Count());
    for (const auto& item : buffer.Chunks()) {
      chunk.Append(item, true);
    }
    return Serialize(chunk);
  }

  void Flush(BufferedFileWriter& writer) {
    writer.WriteData(header->data, header->size_bytes);
    writer.WriteData(body->data, body->size_bytes);
  }
  nanoarrow::UniqueBuffer GetHeader() {
    auto result_header = std::move(header);
    InitArrowDuckBuffer(header.get(), allocator);
    return result_header;
  }
  nanoarrow::UniqueBuffer GetBody() {
    auto result_body = std::move(body);
    InitArrowDuckBuffer(body.get(), allocator);
    return result_body;
  }

 private:
  ClientProperties options;
  Allocator& allocator;
  ArrowSchema* schema;
  unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
  nanoarrow::ipc::UniqueEncoder encoder;
  nanoarrow::UniqueArrayView chunk_view;
  nanoarrow::UniqueArray chunk_arrow;
  nanoarrow::UniqueBuffer header;
  nanoarrow::UniqueBuffer body;
  ArrowError error{};
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
