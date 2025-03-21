#include "table_function/read_arrow.hpp"

#include <inttypes.h>

#include "zstd.h"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "ipc/stream_factory.hpp"
#include "ipc/stream_reader/base_stream_reader.hpp"
#include "nanoarrow_errors.hpp"
#include "table_function/arrow_ipc_function_data.hpp"

// read_arrow() implementation
//
// This version uses the ArrowIpcDecoder directly. instead of nanoarrow's
// ArrowArrayStream wrapper. This lets it use DuckDB's allocator at the
// expense of a bit more verbosity. Because we can apply the projection
// it reduces some of the verbosity of the actual DuckDB part (although the
// ArrayStreamReader from nanoarrow could support a projection, which
// would handle that too).
//
// I like this version better than the simpler one, and there are more parts
// that could get optimized here (whereas with the array stream version you
// don't have much control).

namespace duckdb {

namespace ext_nanoarrow {

struct ReadArrowStream : ArrowTableFunction {
  // Define the function. Unlike arrow_scan(), which takes integer pointers
  // as arguments, we keep the factory alive by making it a member of the bind
  // data (instead of as a Python object whose ownership is kept alive via the
  // DependencyItem mechanism).
  static TableFunction Function() {
    TableFunction fn("read_arrow", {LogicalType::VARCHAR}, ArrowScanFunction, Bind,
                     ArrowScanInitGlobal, ArrowScanInitLocal);
    fn.cardinality = ArrowScanCardinality;
    fn.projection_pushdown = true;
    fn.filter_pushdown = false;
    fn.filter_prune = false;
    return fn;
  }

  static unique_ptr<TableRef> ScanReplacement(ClientContext& context,
                                              ReplacementScanInput& input,
                                              optional_ptr<ReplacementScanData> data) {
    auto table_name = ReplacementScan::GetFullPath(input);
    if (!ReplacementScan::CanReplace(table_name, {"arrows", "arrow"})) {
      return nullptr;
    }

    auto table_function = make_uniq<TableFunctionRef>();
    vector<unique_ptr<ParsedExpression>> children;
    auto table_name_expr = make_uniq<ConstantExpression>(Value(table_name));
    children.push_back(std::move(table_name_expr));
    auto function_expr = make_uniq<FunctionExpression>("read_arrow", std::move(children));
    table_function->function = std::move(function_expr);

    if (!FileSystem::HasGlob(table_name)) {
      auto& fs = FileSystem::GetFileSystem(context);
      table_function->alias = fs.ExtractBaseName(table_name);
    }

    return std::move(table_function);
  }

  // Our Bind() function is different from the arrow_scan because our input
  // is a filename (and their input is three pointer addresses).
  static unique_ptr<FunctionData> Bind(ClientContext& context,
                                       TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types,
                                       vector<string>& names) {
    return BindInternal(context, input.inputs[0].GetValue<string>(), return_types, names);
  }

  static unique_ptr<FunctionData> BindCopy(ClientContext& context, CopyInfo& info,
                                           vector<string>& expected_names,
                                           vector<LogicalType>& expected_types) {
    return BindInternal(context, info.file_path, expected_types, expected_names);
  }

  static unique_ptr<FunctionData> BindInternal(ClientContext& context, std::string src,
                                               vector<LogicalType>& return_types,
                                               vector<string>& names) {
    auto stream_factory = make_uniq<FileIPCStreamFactory>(context, src);
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
};

unique_ptr<FunctionData> ReadArrowStreamBindCopy(ClientContext& context, CopyInfo& info,
                                                 vector<string>& expected_names,
                                                 vector<LogicalType>& expected_types) {
  return ReadArrowStream::BindCopy(context, info, expected_names, expected_types);
}

TableFunction ReadArrowStreamFunction() { return ReadArrowStream::Function(); }

void RegisterReadArrowStream(DatabaseInstance& db) {
  auto function = ReadArrowStream::Function();
  ExtensionUtil::RegisterFunction(db, function);
  // So we can accept a list of paths as well e.g., ['file_1.arrow','file_2.arrow']
  function.arguments = {LogicalType::LIST(LogicalType::VARCHAR)};
  ExtensionUtil::RegisterFunction(db, function);
  auto& config = DBConfig::GetConfig(db);
  config.replacement_scans.emplace_back(ReadArrowStream::ScanReplacement);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
