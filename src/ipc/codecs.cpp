#include "ipc/codecs.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "nanoarrow_errors.hpp"

namespace duckdb {
namespace ext_nanoarrow {

static ArrowIpcCompressionType ParseArrowIpcCompressionType(const string& name) {
  // Accept the spellings DuckDB users know from other COPY formats on top of nanoarrow's
  auto lname = StringUtil::Lower(name);
  if (lname == "uncompressed") {
    lname = "none";
  } else if (lname == "lz4_frame") {
    lname = "lz4";
  }

  ArrowIpcCompressionType type;
  ArrowError error{};
  if (ArrowIpcCompressionTypeFromString(lname.c_str(), &type, &error) != NANOARROW_OK) {
    throw BinderException(
        "Unsupported compression type \"%s\" for Arrow IPC, expected one of "
        "'uncompressed', 'zstd' or 'lz4'",
        name);
  }
  return type;
}

bool ArrowIpcCompressionOptions::TrySetOption(const string& name, const Value& value) {
  if (StringUtil::CIEquals(name, "compression")) {
    type = ParseArrowIpcCompressionType(value.ToString());
  } else if (StringUtil::CIEquals(name, "compression_level")) {
    level = value.GetValue<int64_t>();
    level_set = true;
  } else {
    return false;
  }
  return true;
}

void ArrowIpcCompressionOptions::Validate() const {
  if (!level_set) {
    return;
  }
  if (type == NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
    throw BinderException(
        "COMPRESSION_LEVEL requires COMPRESSION to be set to 'zstd' or 'lz4'");
  }

  // nanoarrow checks the level too but only once the encoder exists, binding is earlier
  int min_level;
  int max_level;
  NANOARROW_THROW_NOT_OK(ArrowIpcGetCompressionLevelRange(type, &min_level, &max_level));
  if (level < min_level || level > max_level) {
    throw BinderException("Compression level for %s must be between %d and %d",
                          ArrowIpcCompressionTypeToString(type), min_level, max_level);
  }
}

nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder() {
  // nanoarrow adds its serial decompressor on first use, a threaded one could be set here
  nanoarrow::ipc::UniqueDecoder decoder;
  NANOARROW_THROW_NOT_OK(ArrowIpcDecoderInit(decoder.get()));
  return decoder;
}

void SetArrowIpcEncoderCompression(ArrowIpcEncoder& encoder,
                                   const ArrowIpcCompressionOptions& options) {
  // Installs nanoarrow's serial compressor, ArrowIpcEncoderSetCompressor takes others
  ArrowError error{};
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderSetCompression(&encoder, options.type,
                                             static_cast<int>(options.level), &error));
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
