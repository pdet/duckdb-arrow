//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// nanoarrow_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/database.hpp"

namespace duckdb {

class NanoarrowExtension : public Extension {
 public:
  void Load(DuckDB& db) override;
  std::string Name() override;
  std::string Version() const override;
};

}  // namespace duckdb
