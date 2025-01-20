#define DUCKDB_EXTENSION_MAIN

#include "nanoarrow_extension.hpp"

#include <string>

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "nanoarrow/nanoarrow.hpp"

#include "read_arrow.hpp"
#include "write_arrow_stream.hpp"

namespace duckdb {

namespace {

struct NanoarrowVersion {
  static void Register(DatabaseInstance& db) {
    auto fn = ScalarFunction("nanoarrow_version", {}, LogicalType::VARCHAR, ExecuteFn);
    ExtensionUtil::RegisterFunction(db, fn);
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    result.SetValue(0, StringVector::AddString(result, ArrowNanoarrowVersion()));
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
};

void LoadInternal(DatabaseInstance& db) {
  NanoarrowVersion::Register(db);
  ext_nanoarrow::RegisterReadArrowStream(db);
  ext_nanoarrow::RegisterArrowStreamCopyFunction(db);
}

}  // namespace

void NanoarrowExtension::Load(DuckDB& db) { LoadInternal(*db.instance); }
std::string NanoarrowExtension::Name() { return "nanoarrow"; }

std::string NanoarrowExtension::Version() const {
#ifdef EXT_VERSION_NANOARROW
  return EXT_VERSION_NANOARROW;
#else
  return "";
#endif
}

}  // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void nanoarrow_init(duckdb::DatabaseInstance& db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::NanoarrowExtension>();
}

DUCKDB_EXTENSION_API const char* nanoarrow_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
