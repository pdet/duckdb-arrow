//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// file_scanner/arrow_file_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_factory.hpp"

#include "duckdb/common/multi_file/base_file_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! This class refers to an Arrow File Scan
class ArrowFileScan : public BaseFileReader {
 public:
  explicit ArrowFileScan(ClientContext& context, const string& file_name);

  //! Factory of this stream
  unique_ptr<ArrowIPCStreamFactory> factory;
  //! Variables to handle projection pushdown
  set<idx_t> projected_columns;

  std::vector<std::pair<idx_t, idx_t>> projection_ids;
  string GetReaderType() const override;

  const vector<string>& GetNames();
  const vector<LogicalType>& GetTypes();

 private:
  ClientContext& context;
  vector<string> names;
  vector<LogicalType> types;
};
}  // namespace ext_nanoarrow
}  // namespace duckdb
