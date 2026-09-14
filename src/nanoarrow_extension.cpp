#include "nanoarrow_extension.hpp"

#include <string>
#include "writer/to_arrow_ipc.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "nanoarrow/nanoarrow.hpp"

#include "table_function/arrow_kv_metadata.hpp"
#include "table_function/read_arrow.hpp"
#include "table_function/scan_arrow_ipc.hpp"
#include "write_arrow_stream.hpp"

namespace duckdb {

namespace {

struct NanoarrowVersion {
  static void Register(ExtensionLoader& loader) {
    auto fn = ScalarFunction("nanoarrow_version", {}, LogicalType::VARCHAR, ExecuteFn);
    loader.RegisterFunction(fn);
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    result.SetValue(0, StringVector::AddString(result, ArrowNanoarrowVersion()));
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
};

void LoadInternal(ExtensionLoader& loader) {
  NanoarrowVersion::Register(loader);
  ext_nanoarrow::RegisterReadArrowStream(loader);
  ext_nanoarrow::RegisterArrowKvMetadata(loader);
  ext_nanoarrow::RegisterArrowStreamCopyFunction(loader);

  ext_nanoarrow::ScanArrowIPC::RegisterReadArrowStream(loader);
  ext_nanoarrow::ToArrowIPCFunction::RegisterToIPCFunction(loader);
}

}  // namespace

void NanoarrowExtension::Load(ExtensionLoader& loader) { LoadInternal(loader); }

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
DUCKDB_CPP_EXTENSION_ENTRY(nanoarrow, loader) { duckdb::LoadInternal(loader); }
}
