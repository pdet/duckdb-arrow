
#include "duckdb/main/extension_util.hpp"
#include "ipc/stream_factory.hpp"
#include "table_function/arrow_ipc_function_data.hpp"
#include "table_function/scan_arrow_ipc.hpp"

#include "duckdb/function/table/arrow.hpp"

#include "ipc/stream_reader/stream_reader.hpp"

#include "duckdb/function/function.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
namespace duckdb {

namespace ext_nanoarrow {

struct ScanArrowIPCFunction : ArrowTableFunction {
  static unique_ptr<FunctionData> ScanArrowIPCBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {
  // Create a vector with all the buffers and their sizes
  vector<ArrowIPCBuffer> buffers;
  const auto buffer_ptr_list = ListValue::GetChildren(input.inputs[0]);
  for (auto &buffer_ptr_struct : buffer_ptr_list) {
    auto unpacked = StructValue::GetChildren(buffer_ptr_struct);
    buffers.emplace_back(unpacked[0].GetValue<uint64_t>(),unpacked[1].GetValue<uint64_t>());
  }

    auto stream_factory = make_uniq<ArrowIPCStreamFactory>(context, buffers);
    auto res = make_uniq<ArrowIPCFunctionData>(std::move(stream_factory));
    res->factory->InitReader();
    res->factory->GetFileSchema(res->schema_root);

    DBConfig& config = DatabaseInstance::GetDatabase(context).config;
    PopulateArrowTableType(config, res->arrow_table, res->schema_root,
                                               names, return_types);
    QueryResult::DeduplicateColumns(names);
    res->all_types = return_types;
    if (return_types.empty()) {
      throw InvalidInputException(
          "Provided table/dataframe must have at least one column");
    }

    return std::move(res);
}
static void ScanArrowIPCScan(ClientContext &context, TableFunctionInput &data_p,
                                              DataChunk &output) {
  if (!data_p.local_state) {

    return;
  }
  auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
  auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
  auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

  //! Out of tuples in this chunk
  if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
    if (!ArrowScanParallelStateNext(context, data_p.bind_data.get(), state,
                                    global_state)) {
      return;
    }
  }
  int64_t output_size =
      MinValue<int64_t>(STANDARD_VECTOR_SIZE,
                        state.chunk->arrow_array.length - state.chunk_offset);
  data.lines_read += output_size;
  if (global_state.CanRemoveFilterColumns()) {
    state.all_columns.Reset();
    state.all_columns.SetCardinality(output_size);
    ArrowToDuckDB(state, data.arrow_table.GetColumns(), state.all_columns,
                  data.lines_read - output_size, false);
    output.ReferenceColumns(state.all_columns, global_state.projection_ids);
  } else {
    output.SetCardinality(output_size);
    ArrowToDuckDB(state, data.arrow_table.GetColumns(), output,
                  data.lines_read - output_size, false);
  }

  output.Verify();
  state.chunk_offset += output.size();
}

  static TableFunction Function() {
     child_list_t<LogicalType> make_buffer_struct_children{
      {"ptr", LogicalType::UBIGINT}, {"size", LogicalType::UBIGINT}};
  TableFunction scan_arrow_ipc_func(
      "scan_arrow_ipc",
      {LogicalType::LIST(LogicalType::STRUCT(make_buffer_struct_children))},
      ScanArrowIPCScan,
      ScanArrowIPCBind,
      ArrowScanInitGlobal,
      ArrowScanInitLocal);

  scan_arrow_ipc_func.cardinality = ArrowScanCardinality;
  scan_arrow_ipc_func.projection_pushdown = true;
  scan_arrow_ipc_func.filter_pushdown = false;
  scan_arrow_ipc_func.filter_prune = false;

  return scan_arrow_ipc_func;
  }
};

void ScanArrowIPC::RegisterReadArrowStream(DatabaseInstance& db) {
  auto function = ScanArrowIPCFunction::Function();
  ExtensionUtil::RegisterFunction(db, function);
}

}
}