//===----------------------------------------------------------------------===//
//                         DuckDB - arrow
//
// write_arrow_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/function/copy_function.hpp"

namespace duckdb {
namespace ext_arrow {

void RegisterArrowStreamCopyFunction(DatabaseInstance& db);

}  // namespace ext_arrow
}  // namespace duckdb
