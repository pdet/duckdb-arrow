#define DUCKDB_EXTENSION_MAIN

#include "arrow_extension.hpp"

#include <string>
#include "writer/to_arrow_ipc.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "nanoarrow/nanoarrow.hpp"

#include "table_function/read_arrow.hpp"
#include "table_function/scan_arrow_ipc.hpp"
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
  ext_arrow::RegisterReadArrowStream(db);
  ext_arrow::RegisterArrowStreamCopyFunction(db);

  ext_arrow::ScanArrowIPC::RegisterReadArrowStream(db);
  ext_arrow::ToArrowIPCFunction::RegisterToIPCFunction(db);
}

}  // namespace

void ArrowExtension::Load(DuckDB& db) { LoadInternal(*db.instance); }
std::string ArrowExtension::Name() { return "arrow"; }

std::string ArrowExtension::Version() const {
#ifdef EXT_VERSION_NANOARROW
  return EXT_VERSION_NANOARROW;
#else
  return "";
#endif
}

}  // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void arrow_init(duckdb::DatabaseInstance& db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::ArrowExtension>();
}

DUCKDB_EXTENSION_API const char* arrow_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
