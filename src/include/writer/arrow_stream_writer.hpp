//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// writer/arrow_stream_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/main/client_context.hpp"
#include "writer/column_data_collection_serializer.hpp"

namespace duckdb {
namespace ext_nanoarrow {

struct ArrowStreamWriter {
  ArrowStreamWriter(ClientContext& context, FileSystem& fs, const string& file_path,
                    const vector<LogicalType>& logical_types,
                    const vector<string>& column_names,
                    const vector<pair<string, string>>& metadata);

  void InitSchema(const vector<LogicalType>& logical_types,
                  const vector<string>& column_names,
                  const vector<pair<string, string>>& metadata);

  void InitOutputFile(FileSystem& fs, const string& file_path);

  void WriteSchema();

  unique_ptr<ColumnDataCollectionSerializer> NewSerializer();

  void Flush(ColumnDataCollection& buffer);

  void Flush(ColumnDataCollectionSerializer& serializer);

  void Finalize() const;

  idx_t NumberOfRowGroups() const;

  idx_t FileSize() const;

 private:
  ClientProperties options;
  Allocator& allocator;
  ColumnDataCollectionSerializer serializer;
  string file_name;
  vector<LogicalType> logical_types;
  unique_ptr<BufferedFileWriter> writer;
  idx_t row_group_count{0};
  nanoarrow::UniqueSchema schema;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
