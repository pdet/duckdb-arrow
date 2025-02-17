
#include "table_function/scan_arrow_ipc.hpp"
#include "duckdb/main/extension_util.hpp"
#include "ipc/stream_factory.hpp"
#include "table_function/arrow_ipc_function_data.hpp"

#include "duckdb/function/table/arrow.hpp"

#include "ipc/stream_reader/base_stream_reader.hpp"

#include "duckdb/function/function.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
namespace duckdb {

namespace ext_nanoarrow {

struct ScanArrowIPCFunction : ArrowTableFunction {
  static unique_ptr<FunctionData> ScanArrowIPCBind(ClientContext& context,
                                                   TableFunctionBindInput& input,
                                                   vector<LogicalType>& return_types,
                                                   vector<string>& names) {
    // Create a vector with all the buffers and their sizes
    vector<ArrowIPCBuffer> buffers;
    const auto buffer_ptr_list = ListValue::GetChildren(input.inputs[0]);
    for (auto& buffer_ptr_struct : buffer_ptr_list) {
      auto unpacked = StructValue::GetChildren(buffer_ptr_struct);
      buffers.emplace_back(unpacked[0].GetValue<uint64_t>(),
                           unpacked[1].GetValue<uint64_t>());
    }

    auto stream_factory = make_uniq<BufferIPCStreamFactory>(context, buffers);
    auto res = make_uniq<ArrowIPCFunctionData>(std::move(stream_factory));
    res->factory->InitReader();
    res->factory->GetFileSchema(res->schema_root);

    DBConfig& config = DatabaseInstance::GetDatabase(context).config;
    PopulateArrowTableType(config, res->arrow_table, res->schema_root, names,
                           return_types);
    QueryResult::DeduplicateColumns(names);
    res->all_types = return_types;
    if (return_types.empty()) {
      throw InvalidInputException(
          "Provided table/dataframe must have at least one column");
    }

    return std::move(res);
  }

  static TableFunction Function() {
    child_list_t<LogicalType> make_buffer_struct_children{{"ptr", LogicalType::UBIGINT},
                                                          {"size", LogicalType::UBIGINT}};
    TableFunction scan_arrow_ipc_func(
        "scan_arrow_ipc",
        {LogicalType::LIST(LogicalType::STRUCT(make_buffer_struct_children))},
        ArrowScanFunction, ScanArrowIPCBind, ArrowScanInitGlobal, ArrowScanInitLocal);

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

}  // namespace ext_nanoarrow
}  // namespace duckdb
