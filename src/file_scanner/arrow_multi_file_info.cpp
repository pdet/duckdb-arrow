#include "file_scanner/arrow_multi_file_info.hpp"

#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

#include "duckdb/common/bind_helpers.hpp"
#include "file_scanner/arrow_file_scan.hpp"
#include "ipc/stream_factory.hpp"

namespace duckdb {
namespace ext_nanoarrow {

unique_ptr<BaseFileReaderOptions> ArrowMultiFileInfo::InitializeOptions(
    ClientContext& context, optional_ptr<TableFunctionInfo> info) {
  return make_uniq<ArrowFileReaderOptions>();
}

bool ArrowMultiFileInfo::ParseCopyOption(ClientContext& context, const string& key,
                                         const vector<Value>& values,
                                         BaseFileReaderOptions& options_p,
                                         vector<string>& expected_names,
                                         vector<LogicalType>& expected_types) {
  // We currently do not have any options for the scanner, so we always return false
  return false;
}

bool ArrowMultiFileInfo::ParseOption(ClientContext& context, const string& key,
                                     const Value& val, MultiFileOptions& file_options,
                                     BaseFileReaderOptions& options) {
  // We currently do not have any options for the scanner, so we always return false
  return false;
}

void ArrowMultiFileInfo::FinalizeCopyBind(ClientContext& context,
                                          BaseFileReaderOptions& options_p,
                                          const vector<string>& expected_names,
                                          const vector<LogicalType>& expected_types) {}

struct ArrowMultiFileData final : public TableFunctionData {
  ArrowMultiFileData() = default;

  unique_ptr<ArrowFileScan> file_scan;
};

unique_ptr<TableFunctionData> ArrowMultiFileInfo::InitializeBindData(
    MultiFileBindData& multi_file_data, unique_ptr<BaseFileReaderOptions> options_p) {
  return make_uniq<ArrowMultiFileData>();
}

void ArrowMultiFileInfo::BindReader(ClientContext& context,
                                    vector<LogicalType>& return_types,
                                    vector<string>& names, MultiFileBindData& bind_data) {
  ArrowFileReaderOptions options;
  auto& multi_file_list = *bind_data.file_list;
  if (!bind_data.file_options.union_by_name) {
    bind_data.reader_bind = bind_data.multi_file_reader->BindReader<ArrowMultiFileInfo>(
        context, return_types, names, *bind_data.file_list, bind_data, options,
        bind_data.file_options);
  } else {
    bind_data.reader_bind =
        bind_data.multi_file_reader->BindUnionReader<ArrowMultiFileInfo>(
            context, return_types, names, multi_file_list, bind_data, options,
            bind_data.file_options);
  }
  D_ASSERT(names.size() == return_types.size());
}

void ArrowMultiFileInfo::FinalizeBindData(MultiFileBindData& multi_file_data) {}

void ArrowMultiFileInfo::GetBindInfo(const TableFunctionData& bind_data, BindInfo& info) {
}

optional_idx ArrowMultiFileInfo::MaxThreads(const MultiFileBindData& bind_data_p,
                                            const MultiFileGlobalState& global_state,
                                            FileExpandResult expand_result) {
  if (expand_result == FileExpandResult::MULTIPLE_FILES) {
    // always launch max threads if we are reading multiple files
    return {};
  }
  // Otherwise, only one thread
  return 1;
}

struct ArrowFileGlobalState : public GlobalTableFunctionState {
 public:
  ArrowFileGlobalState(ClientContext& context_p, idx_t total_file_count,
                       const MultiFileBindData& bind_data,
                       MultiFileGlobalState& global_state)
      : global_state(global_state), context(context_p) {};

  ~ArrowFileGlobalState() override = default;

  const MultiFileGlobalState& global_state;
  ClientContext& context;
  set<idx_t> files;
};

unique_ptr<GlobalTableFunctionState> ArrowMultiFileInfo::InitializeGlobalState(
    ClientContext& context, MultiFileBindData& bind_data,
    MultiFileGlobalState& global_state) {
  return make_uniq<ArrowFileGlobalState>(
      context, bind_data.file_list->GetTotalFileCount(), bind_data, global_state);
}

//! The Arrow Local File State, basically refers to the Scan of one Arrow File
//! This is done by calling the Arrow Scan directly on one file.
struct ArrowFileLocalState : public LocalTableFunctionState {
 public:
  explicit ArrowFileLocalState(ExecutionContext& execution_context)
      : execution_context(execution_context) {};
  //! Factory Pointer
  shared_ptr<ArrowFileScan> file_scan;

  ExecutionContext& execution_context;

  //! Each local state refers to an Arrow Scan on a local file
  unique_ptr<ArrowScanFunctionData> local_arrow_function_data;
  unique_ptr<TableFunctionInitInput> init_input;
  unique_ptr<GlobalTableFunctionState> local_arrow_global_state;
  unique_ptr<LocalTableFunctionState> local_arrow_local_state;
  unique_ptr<TableFunctionInput> table_function_input;
};

unique_ptr<LocalTableFunctionState> ArrowMultiFileInfo::InitializeLocalState(
    ExecutionContext& context, GlobalTableFunctionState& function_state) {
  return make_uniq<ArrowFileLocalState>(context);
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, GlobalTableFunctionState& gstate_p, BaseUnionData& union_data,
    const MultiFileBindData& bind_data) {
  return make_shared_ptr<ArrowFileScan>(context, union_data.GetFileName());
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, GlobalTableFunctionState& gstate_p, const string& filename,
    idx_t file_idx, const MultiFileBindData& bind_data) {
  return make_shared_ptr<ArrowFileScan>(context, filename);
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, const string& filename, ArrowFileReaderOptions& options,
    const MultiFileOptions& file_options) {
  return make_shared_ptr<ArrowFileScan>(context, filename);
}

shared_ptr<BaseUnionData> ArrowMultiFileInfo::GetUnionData(
    shared_ptr<BaseFileReader> scan_p, idx_t file_idx) {
  auto& scan = scan_p->Cast<ArrowFileScan>();
  auto data = make_shared_ptr<BaseUnionData>(scan_p->GetFileName());
  if (file_idx == 0) {
    data->names = scan.GetNames();
    data->types = scan.GetTypes();
    data->reader = std::move(scan_p);
  } else {
    data->names = scan.GetNames();
    data->types = scan.GetTypes();
  }
  return data;
}

void ArrowMultiFileInfo::FinalizeReader(ClientContext& context, BaseFileReader& reader,
                                        GlobalTableFunctionState&) {}

bool ArrowMultiFileInfo::TryInitializeScan(ClientContext& context,
                                           const shared_ptr<BaseFileReader>& reader,
                                           GlobalTableFunctionState& gstate_p,
                                           LocalTableFunctionState& lstate_p) {
  auto& gstate = gstate_p.Cast<ArrowFileGlobalState>();
  auto& lstate = lstate_p.Cast<ArrowFileLocalState>();
  if (gstate.files.find(reader->file_list_idx.GetIndex()) != gstate.files.end()) {
    // TODO: We might need to reevaluate file parallelism here in the future
    return false;
  }
  gstate.files.insert(reader->file_list_idx.GetIndex());

  lstate.file_scan = shared_ptr_cast<BaseFileReader, ArrowFileScan>(reader);
  lstate.local_arrow_function_data = make_uniq<ArrowScanFunctionData>(
      &FileIPCStreamFactory::Produce,
      reinterpret_cast<uintptr_t>(lstate.file_scan->factory.get()));
  lstate.local_arrow_function_data->schema_root = lstate.file_scan->schema_root;
  lstate.local_arrow_function_data->arrow_table = lstate.file_scan->arrow_table_type;
  if (!reader->column_indexes.empty()) {
    lstate.init_input = make_uniq<TableFunctionInitInput>(
        *lstate.local_arrow_function_data, reader->column_indexes,
        gstate.global_state.projection_ids, reader->filters);
  } else {
    lstate.init_input = make_uniq<TableFunctionInitInput>(
        *lstate.local_arrow_function_data, gstate.global_state.column_indexes,
        gstate.global_state.projection_ids, reader->filters);
  }
  lstate.local_arrow_global_state =
      ArrowTableFunction::ArrowScanInitGlobal(context, *lstate.init_input);
  lstate.local_arrow_local_state =
      ArrowTableFunction::ArrowScanInitLocal(lstate.execution_context, *lstate.init_input,
                                             lstate.local_arrow_global_state.get());
  lstate.table_function_input = make_uniq<TableFunctionInput>(
      lstate.local_arrow_function_data.get(), lstate.local_arrow_local_state.get(),
      lstate.local_arrow_global_state.get());
  return true;
}

void ArrowMultiFileInfo::Scan(ClientContext& context, BaseFileReader& reader,
                              GlobalTableFunctionState& global_state,
                              LocalTableFunctionState& local_state, DataChunk& chunk) {
  auto& lstate = local_state.Cast<ArrowFileLocalState>();
  ArrowTableFunction::ArrowScanFunction(context, *lstate.table_function_input, chunk);
}

void ArrowMultiFileInfo::FinishFile(ClientContext& context,
                                    GlobalTableFunctionState& global_state,
                                    BaseFileReader& reader) {}

void ArrowMultiFileInfo::FinishReading(ClientContext& context,
                                       GlobalTableFunctionState& global_state,
                                       LocalTableFunctionState& local_state) {}

unique_ptr<NodeStatistics> ArrowMultiFileInfo::GetCardinality(
    const MultiFileBindData& bind_data, idx_t file_count) {
  // TODO: Here is where we might set statistics, for optimizations if we have them
  // e.g., cardinality from the file footer
  return make_uniq<NodeStatistics>();
}

unique_ptr<BaseStatistics> ArrowMultiFileInfo::GetStatistics(ClientContext& context,
                                                             BaseFileReader& reader,
                                                             const string& name) {
  return nullptr;
}

double ArrowMultiFileInfo::GetProgressInFile(ClientContext& context,
                                             const BaseFileReader& reader) {
  auto& file_scan = reader.Cast<ArrowFileScan>();
  if (!file_scan.factory->reader) {
    // We are done with this file
    return 100;
  }
  auto file_reader =
      reinterpret_cast<IPCFileStreamReader*>(file_scan.factory->reader.get());
  return file_reader->GetProgress();
}

void ArrowMultiFileInfo::GetVirtualColumns(ClientContext&, MultiFileBindData&,
                                           virtual_column_map_t& result) {}

}  // namespace ext_nanoarrow
}  // namespace duckdb
