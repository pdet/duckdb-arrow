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

class ColumnDataCollectionSerializer {
 public:
  ColumnDataCollectionSerializer(ClientProperties  options, Allocator& allocator);

  void Init(const ArrowSchema* schema_p, const vector<LogicalType>& logical_types);

  void SerializeSchema();

  idx_t Serialize(DataChunk& chunk);

  idx_t Serialize(const ColumnDataCollection& buffer);

  void Flush(BufferedFileWriter& writer);

  nanoarrow::UniqueBuffer GetHeader();

  nanoarrow::UniqueBuffer GetBody();

 private:
  ClientProperties options;
  Allocator& allocator;
  const ArrowSchema* schema{};
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
