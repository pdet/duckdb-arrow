#include "file_scanner/arrow_multi_file_info.hpp"
#include "duckdb/common/bind_helpers.hpp"

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
  return true;
}

bool ArrowMultiFileInfo::ParseOption(ClientContext& context, const string& key,
                                     const Value& val, MultiFileOptions& file_options,
                                     BaseFileReaderOptions& options) {
  return true;
}

void ArrowMultiFileInfo::FinalizeCopyBind(ClientContext& context,
                                          BaseFileReaderOptions& options_p,
                                          const vector<string>& expected_names,
                                          const vector<LogicalType>& expected_types) {}

struct ArrowMultifileData : public TableFunctionData {
  ArrowMultifileData() = default;

  //! The arrow stream factory (if any): this is used when automatic detection is used
  //! during binding. In this case, some CSV buffers have already been read and can be
  //! reused.
  unique_ptr<ArrowIPCStreamFactory> factory;
};

unique_ptr<TableFunctionData> ArrowMultiFileInfo::InitializeBindData(
    MultiFileBindData& multi_file_data, unique_ptr<BaseFileReaderOptions> options_p) {
  return make_uniq<TableFunctionData>();
}

void ArrowMultiFileInfo::BindReader(ClientContext& context,
                                    vector<LogicalType>& return_types,
                                    vector<string>& names, MultiFileBindData& bind_data) {
  auto& csv_data = bind_data.bind_data->Cast<ArrowMultifileData>();
  auto& multi_file_list = *bind_data.file_list;
  // auto &options = csv_data.options;
  if (!bind_data.file_options.union_by_name) {
    ArrowSchemaWrapper schema_root;
    const auto file_paths = multi_file_list.GetAllFiles();
    auto stream_factory = make_uniq<FileIPCStreamFactory>(context, file_paths[0]);
    stream_factory->InitReader();
    stream_factory->GetFileSchema(schema_root);
    ArrowTableType arrow_table_type;

    DBConfig& config = DatabaseInstance::GetDatabase(context).config;
    ArrowTableFunction::PopulateArrowTableType(config, arrow_table_type, schema_root,
                                               names, return_types);
    QueryResult::DeduplicateColumns(names);
    if (return_types.empty()) {
      throw InvalidInputException(
          "Provided table/dataframe must have at least one column");
    }
    bind_data.multi_file_reader->BindOptions(bind_data.file_options, multi_file_list,
                                             return_types, names, bind_data.reader_bind);
  } else {
    D_ASSERT(0);
  }
}

void ArrowMultiFileInfo::FinalizeBindData(MultiFileBindData& multi_file_data) {}

void ArrowMultiFileInfo::GetBindInfo(const TableFunctionData& bind_data, BindInfo& info) {
}

optional_idx ArrowMultiFileInfo::MaxThreads(const MultiFileBindData& bind_data,
                                            const MultiFileGlobalState& global_state,
                                            FileExpandResult expand_result) {
  return optional_idx();
}

struct ArrowFileGlobalState : public GlobalTableFunctionState {
 public:
  ArrowFileGlobalState(ClientContext& context_p, idx_t total_file_count,
                       const MultiFileBindData& bind_data,
                       MultiFileGlobalState& global_state)
      : global_state(global_state), context(context_p) {};

  ~ArrowFileGlobalState() override {}

  const MultiFileGlobalState& global_state;

 private:
  bool is_union;
  //! Reference to the client context that created this scan
  ClientContext& context;
  // const MultiFileBindData& bind_data;
  //! For insertion order preservation?
  atomic<idx_t> scanner_idx;
  //! Current File Index?
  atomic<idx_t> current_file;
};

unique_ptr<GlobalTableFunctionState> ArrowMultiFileInfo::InitializeGlobalState(
    ClientContext& context, MultiFileBindData& bind_data,
    MultiFileGlobalState& global_state) {
  return make_uniq<ArrowFileGlobalState>(
      context, bind_data.file_list->GetTotalFileCount(), bind_data);
}

//! The Arrow Local File State, basically refers to the Scan of one Arrow File
//! This is done by calling the Arrow Scan directly on one file.
struct ArrowFileLocalState : public LocalTableFunctionState {
 public:
  //! Factory Pointer
  unique_ptr<ArrowIPCStreamFactory> factory;

  //! Each local state refers to an Arrow Scan on a local file
  unique_ptr<ArrowScanFunctionData> local_arrow_function_data;
  unique_ptr<TableFunctionInitInput> init_input;
  unique_ptr<GlobalTableFunctionState> local_arrow_global_state;
  unique_ptr<LocalTableFunctionState> local_arrow_local_state;

  //! Projection and filter being pushed down in this file.
  ArrowStreamParameters pushdown_parameters;
};

unique_ptr<LocalTableFunctionState> ArrowMultiFileInfo::InitializeLocalState(
    ExecutionContext& context, GlobalTableFunctionState& function_state) {
  auto& arrow_global_state = function_state.Cast<ArrowFileGlobalState>();
  auto res = make_uniq<ArrowFileLocalState>();

  // Initialize all variables necessary for the ArrowTableFunction Scan
  res->factory = make_uniq<ArrowIPCStreamFactory>(BufferAllocator::Get(context.client));
  res->local_arrow_function_data = make_uniq<ArrowScanFunctionData>(
      &ArrowIPCStreamFactory::Produce, res->factory.get());
  res->init_input = make_uniq<TableFunctionInitInput>(
      *res->local_arrow_function_data, arrow_global_state.global_state.column_indexes,
      arrow_global_state.global_state.projection_ids,
      arrow_global_state.global_state.filters);
  res->local_arrow_global_state =
      ArrowTableFunction::ArrowScanInitGlobal(context.client, *res->init_input);
  res->local_arrow_local_state =
      ArrowTableFunction::ArrowScanInitLocal(context, *res->init_input, &function_state);

  return res;
}

class ArrowFileScan : public BaseFileReader {
  explicit ArrowFileScan(const string& file_name) : BaseFileReader(file_name) {}
  string GetReaderType() const override { return "ARROW"; }
};

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, GlobalTableFunctionState& gstate_p, BaseUnionData& union_data,
    const MultiFileBindData& bind_data) {
  return make_shared_ptr<ArrowFileScan>(union_data.GetFileName());
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, GlobalTableFunctionState& gstate_p, const string& filename,
    idx_t file_idx, const MultiFileBindData& bind_data) {
  return make_shared_ptr<ArrowFileScan>(filename);
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, const string& filename, CSVReaderOptions& options,
    const MultiFileOptions& file_options) {
  return make_shared_ptr<ArrowFileScan>(filename);
}

shared_ptr<BaseUnionData> ArrowMultiFileInfo::GetUnionData(
    shared_ptr<BaseFileReader> scan_p, idx_t file_idx) {
  auto& scan = scan_p->Cast<ArrowFileScan>();
  return make_shared_ptr<BaseUnionData>(scan_p->GetFileName());
  ;
}

void ArrowMultiFileInfo::FinalizeReader(ClientContext& context, BaseFileReader& reader,
                                        GlobalTableFunctionState&) {}

bool ArrowMultiFileInfo::TryInitializeScan(ClientContext& context,
                                           shared_ptr<BaseFileReader>& reader,
                                           GlobalTableFunctionState& gstate_p,
                                           LocalTableFunctionState& lstate_p) {
  auto& gstate = gstate_p.Cast<ArrowFileGlobalState>();
  auto& lstate = lstate_p.Cast<ArrowFileLocalState>();
  auto csv_reader_ptr = shared_ptr_cast<BaseFileReader, CSVFileScan>(reader);
  // gstate.FinishScan(std::move(lstate.csv_reader));
  // lstate.csv_reader = gstate.Next(csv_reader_ptr);
  if (!lstate.csv_reader) {
    // exhausted the scan
    return false;
  }
  return true;
}

void ArrowMultiFileInfo::Scan(ClientContext& context, BaseFileReader& reader,
                              GlobalTableFunctionState& global_state,
                              LocalTableFunctionState& local_state, DataChunk& chunk) {
  auto& lstate = local_state.Cast<ArrowFileLocalState>();
  ArrowScanFunction() if (lstate.csv_reader->FinishedIterator()) { return; }
  lstate.csv_reader->Flush(chunk);
}

void ArrowMultiFileInfo::FinishFile(ClientContext& context,
                                    GlobalTableFunctionState& global_state,
                                    BaseFileReader& reader) {
  auto& gstate = global_state.Cast<CSVGlobalState>();
  gstate.FinishLaunchingTasks(reader.Cast<CSVFileScan>());
}

void ArrowMultiFileInfo::FinishReading(ClientContext& context,
                                       GlobalTableFunctionState& global_state,
                                       LocalTableFunctionState& local_state) {
  auto& gstate = global_state.Cast<CSVGlobalState>();
  auto& lstate = local_state.Cast<CSVLocalState>();
  gstate.FinishScan(std::move(lstate.csv_reader));
}

unique_ptr<NodeStatistics> ArrowMultiFileInfo::GetCardinality(
    const MultiFileBindData& bind_data, idx_t file_count) {
  auto& csv_data = bind_data.bind_data->Cast<ReadCSVData>();
  // determined through the scientific method as the average amount of rows in a CSV file
  idx_t per_file_cardinality = 42;
  if (csv_data.buffer_manager && csv_data.buffer_manager->file_handle) {
    auto estimated_row_width = (bind_data.types.size() * 5);
    per_file_cardinality =
        csv_data.buffer_manager->file_handle->FileSize() / estimated_row_width;
  }
  return make_uniq<NodeStatistics>(file_count * per_file_cardinality);
}

unique_ptr<BaseStatistics> ArrowMultiFileInfo::GetStatistics(ClientContext& context,
                                                             BaseFileReader& reader,
                                                             const string& name) {
  throw InternalException("Unimplemented CSVMultiFileInfo method");
}

double ArrowMultiFileInfo::GetProgressInFile(ClientContext& context,
                                             const BaseFileReader& reader) {
  auto& csv_scan = reader.Cast<CSVFileScan>();

  auto buffer_manager = csv_scan.buffer_manager;
  if (!buffer_manager) {
    // We are done with this file, so it's 100%
    return 100.0;
  }
  double bytes_read;
  if (buffer_manager->file_handle->compression_type == FileCompressionType::GZIP ||
      buffer_manager->file_handle->compression_type == FileCompressionType::ZSTD) {
    // compressed file: we care about the progress made in the *underlying* file handle
    // the bytes read from the uncompressed file are skewed
    bytes_read = buffer_manager->file_handle->GetProgress();
  } else {
    bytes_read = static_cast<double>(csv_scan.bytes_read);
  }
  double file_progress = bytes_read / static_cast<double>(csv_scan.file_size);
  return file_progress * 100.0;
}

void ArrowMultiFileInfo::GetVirtualColumns(ClientContext&, MultiFileBindData&,
                                           virtual_column_map_t& result) {
  result.insert(
      make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
