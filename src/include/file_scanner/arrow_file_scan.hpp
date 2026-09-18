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
#include "duckdb/parallel/async_result.hpp"

namespace duckdb {
namespace ext_nanoarrow {

struct ArrowFileLocalState;

//! This class refers to an Arrow File Scan
class ArrowFileScan : public BaseFileReader {
 public:
  explicit ArrowFileScan(ClientContext& context, const string& file_name);
  //! Each scan takes a deep copy of the schema, so this one releases its own
  ~ArrowFileScan() override = default;

  //! Factory of this stream
  unique_ptr<FileIPCStreamFactory> factory;

  string GetReaderType() const override;

  const vector<string>& GetNames();
  const vector<LogicalType>& GetTypes();
  ArrowSchemaWrapper schema_root;
  ArrowTableSchema arrow_table;

  bool TryInitializeScan(ClientContext& context, GlobalTableFunctionState& gstate,
                         LocalTableFunctionState& lstate) override;
  void PrepareScan(ClientContext& context, GlobalTableFunctionState& gstate,
                   LocalTableFunctionState& lstate) override;
  AsyncResult Scan(ClientContext& context, GlobalTableFunctionState& global_state,
                   LocalTableFunctionState& local_state, DataChunk& chunk) override;
  double GetProgressInFile(ClientContext& context) override;

  shared_ptr<BaseUnionData> GetUnionData(idx_t file_idx) override;

  //! Rows estimated from the file size and the average width of a row, 0 when unknown
  idx_t EstimatedRowCount();
  //! How many scans can run on this file at once
  idx_t ClaimCount() const;

 private:
  //! A range of footer blocks that one scan reads
  struct BlockRange {
    idx_t begin;
    idx_t end;
  };

  //! Groups the footer blocks into claims, leaving none when one scan reads the file
  void PlanClaims(const vector<ArrowIpcFileBlock>& file_blocks);
  void InitializeScanData(ArrowFileLocalState& lstate, stream_factory_produce_t producer,
                          uintptr_t producer_data);
  void StartScan(ClientContext& context, ArrowFileLocalState& lstate);
  static unique_ptr<ArrowArrayStreamWrapper> ProduceBlocks(
      uintptr_t local_state, ArrowStreamParameters& parameters);

  vector<string> names;
  vector<LogicalType> types;
  //! Tells readers apart in reused scan states, where an address could repeat
  const idx_t scan_id;
  vector<ArrowIpcFileBlock> blocks;
  vector<BlockRange> claims;
  //! Handed out under the multi file lock, read without it for progress
  atomic<idx_t> next_claim{0};
};
}  // namespace ext_nanoarrow
}  // namespace duckdb
