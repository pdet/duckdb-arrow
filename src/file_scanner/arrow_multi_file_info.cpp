#include "file_scanner/arrow_multi_file_info.hpp"

#include "duckdb/common/bind_helpers.hpp"
#include "file_scanner/arrow_file_scan.hpp"
#include "ipc/stream_factory.hpp"

namespace duckdb {
namespace ext_nanoarrow {

unique_ptr<BaseFileReaderOptions> ArrowMultiFileInfo::InitializeOptions(
    ClientContext& context, optional_ptr<TableFunctionInfo> info) {
  return make_uniq<ArrowFileReaderOptions>();
}

bool ArrowMultiFileInfo::ParseCopyOption(ClientContext& context, const Identifier& key,
                                         const vector<Value>& values,
                                         BaseFileReaderOptions& options_p,
                                         vector<Identifier>& expected_names,
                                         vector<LogicalType>& expected_types) {
  // We currently do not have any options for the scanner, so we always return false
  return false;
}

unique_ptr<MultiFileReaderInterface> ArrowMultiFileInfo::CreateInterface(
    ClientContext& context) {
  return make_uniq<ArrowMultiFileInfo>();
}

bool ArrowMultiFileInfo::ParseOption(ClientContext& context, const Identifier& key,
                                     const Value& val, MultiFileOptions& file_options,
                                     BaseFileReaderOptions& options) {
  // We currently do not have any options for the scanner, so we always return false
  return false;
}

void ArrowMultiFileInfo::FinalizeCopyBind(ClientContext& context,
                                          BaseFileReaderOptions& options_p,
                                          const vector<Identifier>& expected_names,
                                          const vector<LogicalType>& expected_types) {}

struct ArrowMultiFileData final : public TableFunctionData {
  ArrowMultiFileData() = default;

  unique_ptr<FunctionData> Copy() const override {
    auto result = make_uniq<ArrowMultiFileData>();
    result->initial_file_claims = initial_file_claims;
    return std::move(result);
  }

  unique_ptr<ArrowFileScan> file_scan;
  //! How many scans the first file allows, which bounds the threads of a one file scan
  idx_t initial_file_claims = 1;
};

unique_ptr<TableFunctionData> ArrowMultiFileInfo::InitializeBindData(
    MultiFileBindData& multi_file_data, unique_ptr<BaseFileReaderOptions> options_p) {
  return make_uniq<ArrowMultiFileData>();
}

void ArrowMultiFileInfo::BindReader(ClientContext& context,
                                    vector<LogicalType>& return_types,
                                    vector<Identifier>& names,
                                    MultiFileBindData& bind_data) {
  ArrowFileReaderOptions options;
  auto& multi_file_list = *bind_data.file_list;
  if (!bind_data.file_options.union_by_name) {
    bind_data.reader_bind = bind_data.multi_file_reader->BindReader(
        context, return_types, names, *bind_data.file_list, bind_data, options,
        bind_data.file_options);

  } else {
    bind_data.reader_bind = bind_data.multi_file_reader->BindUnionReader(
        context, return_types, names, multi_file_list, bind_data, options,
        bind_data.file_options);
  }
  D_ASSERT(names.size() == return_types.size());
}

void ArrowMultiFileInfo::FinalizeBindData(MultiFileBindData& multi_file_data) {
  if (multi_file_data.initial_reader) {
    auto& bind_data = multi_file_data.bind_data->Cast<ArrowMultiFileData>();
    bind_data.initial_file_claims =
        multi_file_data.initial_reader->Cast<ArrowFileScan>().ClaimCount();
  }
}

void ArrowMultiFileInfo::GetBindInfo(const TableFunctionData& bind_data, BindInfo& info) {
}

optional_idx ArrowMultiFileInfo::MaxThreads(const MultiFileBindData& bind_data_p,
                                            const MultiFileGlobalState& global_state,
                                            FileExpandResult expand_result) {
  if (expand_result == FileExpandResult::MULTIPLE_FILES) {
    // always launch max threads if we are reading multiple files
    return {};
  }
  return bind_data_p.bind_data->Cast<ArrowMultiFileData>().initial_file_claims;
}

unique_ptr<GlobalTableFunctionState> ArrowMultiFileInfo::InitializeGlobalState(
    ClientContext& context, MultiFileBindData& bind_data,
    MultiFileGlobalState& global_state) {
  return make_uniq<ArrowFileGlobalState>(
      context, bind_data.file_list->GetTotalFileCount(), bind_data, global_state);
}

unique_ptr<LocalTableFunctionState> ArrowMultiFileInfo::InitializeLocalState(
    ClientContext& context, GlobalTableFunctionState& function_state) {
  return make_uniq<ArrowFileLocalState>();
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, GlobalTableFunctionState& gstate_p, BaseUnionData& union_data,
    const MultiFileBindData& bind_data) {
  return make_shared_ptr<ArrowFileScan>(context, union_data.GetFileName());
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, GlobalTableFunctionState& gstate_p,
    const OpenFileInfo& file_info, idx_t file_idx, const MultiFileBindData& bind_data) {
  return make_shared_ptr<ArrowFileScan>(context, file_info.path);
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, const OpenFileInfo& file, BaseFileReaderOptions& options,
    const MultiFileOptions& file_options) {
  return make_shared_ptr<ArrowFileScan>(context, file.path);
}

void ArrowMultiFileInfo::FinishReading(ClientContext& context,
                                       GlobalTableFunctionState& global_state,
                                       LocalTableFunctionState& local_state) {}

unique_ptr<NodeStatistics> ArrowMultiFileInfo::GetCardinality(
    ClientContext& context, const MultiFileBindData& bind_data, idx_t file_count) {
  // Without an estimate every Arrow scan looks like one row and joins build on it
  if (!bind_data.initial_reader) {
    return make_uniq<NodeStatistics>();
  }
  auto rows = bind_data.initial_reader->Cast<ArrowFileScan>().EstimatedRowCount();
  if (rows == 0) {
    return make_uniq<NodeStatistics>();
  }
  // The first file stands in for the rest, as the parquet reader also does
  return make_uniq<NodeStatistics>(rows * (file_count > 0 ? file_count : 1));
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
