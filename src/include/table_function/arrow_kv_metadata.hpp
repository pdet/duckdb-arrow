//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// table_function/arrow_kv_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

void RegisterArrowKvMetadata(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
