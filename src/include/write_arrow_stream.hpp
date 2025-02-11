//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// write_arrow_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/function/copy_function.hpp"

namespace duckdb {
namespace ext_nanoarrow {

void RegisterArrowStreamCopyFunction(DatabaseInstance& db);

}  // namespace ext_nanoarrow
}  // namespace duckdb
