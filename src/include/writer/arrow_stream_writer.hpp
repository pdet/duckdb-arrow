//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// writer/arrow_stream_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/atomic.hpp"
#include "duckdb/main/client_context.hpp"
#include "writer/column_data_collection_serializer.hpp"

namespace duckdb {
namespace ext_nanoarrow {

struct ArrowStreamWriter {
  ArrowStreamWriter(const ClientProperties& options, FileSystem& fs,
                    const string& file_path, const vector<LogicalType>& logical_types,
                    const ArrowSchema& schema,
                    const vector<pair<string, string>>& metadata, bool file_format,
                    bool size_metadata);

  void InitSchema(const ArrowSchema& schema,
                  const vector<pair<string, string>>& metadata);

  void InitOutputFile(FileSystem& fs, const string& file_path);

  void WriteSchema();

  unique_ptr<ColumnDataCollectionSerializer> NewSerializer() const;

  void Flush(ColumnDataCollection& buffer);

  void Flush(ColumnDataCollectionSerializer& serializer);

  void Finalize();

  idx_t NumberOfRowGroups() const;

  idx_t FileSize() const;

  static bool IsSizeMetadataKey(const string& key);

 private:
  void FlushInternal(ColumnDataCollectionSerializer& serializer);
  void WriteFooter();

  ClientProperties options;
  Allocator& allocator;
  ColumnDataCollectionSerializer serializer;
  vector<LogicalType> logical_types;
  bool file_format;
  bool size_metadata;
  mutex lock;
  unique_ptr<BufferedFileWriter> writer;
  vector<ArrowIpcFileBlock> blocks;
  idx_t schema_message_size = 0;
  int64_t total_compressed_size = 0;
  int64_t total_uncompressed_size = 0;
  // Rotation checks read these while another thread may be flushing
  atomic<idx_t> row_group_count{0};
  atomic<idx_t> file_size{0};
  nanoarrow::UniqueSchema schema;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
