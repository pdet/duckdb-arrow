//===----------------------------------------------------------------------===//
//                         DuckDB - arrow
//
// arrow_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/database.hpp"

namespace duckdb {

class ArrowExtension : public Extension {
 public:
  void Load(DuckDB& db) override;
  std::string Name() override;
  std::string Version() const override;
};

}  // namespace duckdb
