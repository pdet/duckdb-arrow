#define DUCKDB_EXTENSION_MAIN

#include "nanoarrow_extension.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void NanoarrowScalarFun(DataChunk& args, ExpressionState& state, Vector& result) {
  auto& name_vector = args.data[0];
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result, "Nanoarrow " + name.GetString() + " 🐥");
        ;
      });
}

inline void NanoarrowOpenSSLVersionScalarFun(DataChunk& args, ExpressionState& state,
                                             Vector& result) {
  auto& name_vector = args.data[0];
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result, "Nanoarrow " + name.GetString() +
                                                   ", my linked OpenSSL version is " +
                                                   OPENSSL_VERSION_TEXT);
        ;
      });
}

static void LoadInternal(DatabaseInstance& instance) {
  // Register a scalar function
  auto nanoarrow_scalar_function = ScalarFunction(
      "nanoarrow", {LogicalType::VARCHAR}, LogicalType::VARCHAR, NanoarrowScalarFun);
  ExtensionUtil::RegisterFunction(instance, nanoarrow_scalar_function);

  // Register another scalar function
  auto nanoarrow_openssl_version_scalar_function =
      ScalarFunction("nanoarrow_openssl_version", {LogicalType::VARCHAR},
                     LogicalType::VARCHAR, NanoarrowOpenSSLVersionScalarFun);
  ExtensionUtil::RegisterFunction(instance, nanoarrow_openssl_version_scalar_function);
}

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
