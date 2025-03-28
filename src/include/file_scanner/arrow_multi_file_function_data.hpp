//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// file_scanner/arrow_multi_file_function_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file_reader_function.hpp"

namespace duckdb {
namespace ext_nanoarrow {

struct ArrowMultiFileFunctionData : public TableFunctionData {};

}  // namespace ext_nanoarrow
}  // namespace duckdb
