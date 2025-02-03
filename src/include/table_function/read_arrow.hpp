//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// table_function/read_arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {
namespace ext_nanoarrow {

// Needed to define the copy function
unique_ptr<FunctionData> ReadArrowStreamBindCopy(ClientContext& context, CopyInfo& info,
                                                 vector<string>& expected_names,
                                                 vector<LogicalType>& expected_types);

TableFunction ReadArrowStreamFunction();

void RegisterReadArrowStream(DatabaseInstance& db);

}  // namespace ext_nanoarrow
}  // namespace duckdb
