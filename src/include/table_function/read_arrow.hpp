//===----------------------------------------------------------------------===//
//                         DuckDB - arrow
//
// table_function/read_arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {
namespace ext_arrow {

TableFunction ReadArrowStreamFunction();

void RegisterReadArrowStream(DatabaseInstance& db);

}  // namespace ext_arrow
}  // namespace duckdb
