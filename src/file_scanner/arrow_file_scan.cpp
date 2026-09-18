#include "duckdb/function/table/arrow.hpp"

#include "file_scanner/arrow_file_scan.hpp"
#include "file_scanner/arrow_multi_file_info.hpp"
#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {
struct ArrowFileLocalState;

ArrowFileScan::ArrowFileScan(ClientContext& context, const string& file_name)
    : BaseFileReader(OpenFileInfo(file_name)) {
  factory = make_uniq<FileIPCStreamFactory>(context, file_name);

  factory->InitReader();
  factory->GetFileSchema(schema_root);
  ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table,
                                               schema_root.arrow_schema);
  names = arrow_table.GetNames();
  types = arrow_table.GetTypes();
  if (types.empty()) {
    throw InvalidInputException("Provided table/dataframe must have at least one column");
  }
  columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(
      StringsToIdentifiers(names), types);
}

string ArrowFileScan::GetReaderType() const { return "ARROW"; }

const vector<string>& ArrowFileScan::GetNames() { return names; }
const vector<LogicalType>& ArrowFileScan::GetTypes() { return types; }

bool ArrowFileScan::TryInitializeScan(ClientContext& context,
                                      GlobalTableFunctionState& gstate_p,
                                      LocalTableFunctionState& lstate_p) {
  auto& gstate = gstate_p.Cast<ArrowFileGlobalState>();
  if (gstate.files.find(file_list_idx.GetIndex()) != gstate.files.end()) {
    // Return false because we don't currently support more than one thread
    // scanning a file. In the future we may be able to support this by (e.g.)
    // reading the Arrow file footer or sending a thread to read ahead to scan
    // for RecordBatch messages.
    return false;
  }
  gstate.files.insert(file_list_idx.GetIndex());
  return true;
}

void ArrowFileScan::PrepareScan(ClientContext& context,
                                GlobalTableFunctionState& gstate_p,
                                LocalTableFunctionState& lstate_p) {
  // The global lock is released before this runs, so the first batch decodes here
  auto& lstate = lstate_p.Cast<ArrowFileLocalState>();
  lstate.local_arrow_function_data = make_uniq<ArrowScanFunctionData>(
      &FileIPCStreamFactory::Produce, reinterpret_cast<uintptr_t>(factory.get()));
  // A memberwise copy shares the release pointer, so a second scan would free it twice
  NANOARROW_THROW_NOT_OK(
      ArrowSchemaDeepCopy(&schema_root.arrow_schema,
                          &lstate.local_arrow_function_data->schema_root.arrow_schema));
  lstate.local_arrow_function_data->arrow_table = arrow_table;
  // Global column indexes and projection ids do not address this file's schema
  auto local_column_indexes = column_indexes;
  if (local_column_indexes.empty()) {
    // Only virtual or constant columns are read, so any file column yields the rows
    local_column_indexes.emplace_back(0);
  }
  const vector<idx_t> no_projection_ids;
  lstate.init_input = make_uniq<TableFunctionInitInput>(*lstate.local_arrow_function_data,
                                                        std::move(local_column_indexes),
                                                        no_projection_ids, filters);
  lstate.local_arrow_global_state =
      ArrowTableFunction::ArrowScanInitGlobal(context, *lstate.init_input);
  lstate.local_arrow_local_state = ArrowTableFunction::ArrowScanInitLocalInternal(
      context, *lstate.init_input, lstate.local_arrow_global_state.get());
  lstate.table_function_input = make_uniq<TableFunctionInput>(
      lstate.local_arrow_function_data.get(), lstate.local_arrow_local_state.get(),
      lstate.local_arrow_global_state.get());
}
AsyncResult ArrowFileScan::Scan(ClientContext& context,
                                GlobalTableFunctionState& global_state,
                                LocalTableFunctionState& local_state, DataChunk& chunk) {
  auto& lstate = local_state.Cast<ArrowFileLocalState>();
  ArrowTableFunction::ArrowScanFunction(context, *lstate.table_function_input, chunk);
  if (chunk.size() == 0) {
    return SourceResultType::FINISHED;
  }
  return SourceResultType::HAVE_MORE_OUTPUT;
}

double ArrowFileScan::GetProgressInFile(ClientContext& context) {
  if (!factory->reader) {
    return 100;
  }
  auto file_reader = static_cast<IPCFileStreamReader*>(factory->reader.get());
  return file_reader->GetProgress();
}

idx_t ArrowFileScan::EstimatedRowCount() {
  if (!factory || !factory->reader) {
    return 0;
  }
  // Without reading the footer the row count is the file size over the row width
  idx_t row_width = 0;
  for (const auto& type : types) {
    auto width = GetTypeIdSize(type.InternalType());
    // A variable length value stores an offset here and its bytes elsewhere
    row_width += type.InternalType() == PhysicalType::VARCHAR ? width + 16 : width;
  }
  if (row_width == 0) {
    return 0;
  }
  auto file_size = static_cast<IPCFileStreamReader*>(factory->reader.get())->FileSize();
  return file_size / row_width;
}

shared_ptr<BaseUnionData> ArrowFileScan::GetUnionData(idx_t file_idx) {
  auto data = make_shared_ptr<BaseUnionData>(OpenFileInfo(GetFileName()));
  data->names = GetNames();
  data->types = GetTypes();
  return data;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
