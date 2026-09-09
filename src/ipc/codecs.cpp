#include "ipc/codecs.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "nanoarrow_errors.hpp"

namespace duckdb {
namespace ext_nanoarrow {

ArrowIpcCompressionType ParseArrowIpcCompressionType(const string& name) {
  // nanoarrow knows the codecs by their canonical names; accept a few spellings that
  // DuckDB users expect from other COPY formats on top of those
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

void ValidateArrowIpcCompressionLevel(ArrowIpcCompressionType type, int64_t level) {
  if (type == NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
    throw BinderException(
        "COMPRESSION_LEVEL requires COMPRESSION to be set to 'zstd' or 'lz4'");
  }

  // The encoder rejects out-of-range levels too, but only once the writer is created;
  // checking here reports the problem while binding the COPY statement
  int min_level;
  int max_level;
  NANOARROW_THROW_NOT_OK(ArrowIpcGetCompressionLevelRange(type, &min_level, &max_level));
  if (level < min_level || level > max_level) {
    throw BinderException("Compression level for %s must be between %d and %d",
                          ArrowIpcCompressionTypeToString(type), min_level, max_level);
  }
}

nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder() {
  // The decoder creates nanoarrow's serial decompressor on first use. A threaded
  // decompressor could parallelize batches with many columns.
  nanoarrow::ipc::UniqueDecoder decoder;
  NANOARROW_THROW_NOT_OK(ArrowIpcDecoderInit(decoder.get()));
  return decoder;
}

void SetArrowIpcEncoderCompression(ArrowIpcEncoder& encoder,
                                   const ArrowIpcCompressionOptions& options) {
  // Likewise the encoder creates nanoarrow's serial compressor on first use
  ArrowError error{};
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderSetCompression(&encoder, options.type,
                                             static_cast<int>(options.level), &error));
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
