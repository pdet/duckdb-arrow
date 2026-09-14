//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// writer/arrow_stream_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"
#include "writer/column_data_collection_serializer.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Arrow IPC stream shared by threads; encoding is per thread, the lock guards the file
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

  //! Creates a per-thread serializer (own ArrowIpcEncoder) that may outlive this writer
  unique_ptr<ColumnDataCollectionSerializer> NewSerializer() const;

  //! Appends the encoded row group held by serializer to the file
  void Flush(ColumnDataCollectionSerializer& serializer);

  void Finalize();

  idx_t NumberOfRowGroups() const;

  idx_t FileSize() const;

 private:
  ClientProperties options;
  Allocator& allocator;
  string file_name;
  vector<LogicalType> logical_types;
  nanoarrow::UniqueSchema schema;
  //! Guards writer and row_group_count only; encoding happens outside of it
  mutable mutex lock;
  unique_ptr<BufferedFileWriter> writer;
  idx_t row_group_count{0};
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
