#include "ipc/codecs.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
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

namespace {

// Count the original buffers before nanoarrow adds compression prefixes and padding
struct CountingCompressor {
  ArrowIpcCompressor inner{};
  int64_t& uncompressed_size;

  explicit CountingCompressor(int64_t& uncompressed_size)
      : uncompressed_size(uncompressed_size) {}
  ~CountingCompressor() {
    if (inner.release) {
      inner.release(&inner);
    }
  }
};

}  // namespace

void SetArrowIpcEncoderCompression(ArrowIpcEncoder& encoder,
                                   const ArrowIpcCompressionOptions& options,
                                   int64_t* uncompressed_size) {
  // Installs nanoarrow's serial compressor, ArrowIpcEncoderSetCompressor takes others
  ArrowError error{};
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderSetCompression(&encoder, options.type,
                                             static_cast<int>(options.level), &error));
  if (!uncompressed_size || options.type == NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
    return;
  }

  auto state = make_uniq<CountingCompressor>(*uncompressed_size);
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialCompressor(&state->inner, options.type,
                                                  static_cast<int>(options.level)));
  ArrowIpcCompressor counting{};
  counting.compression_type = options.type;
  counting.private_data = state.get();
  counting.compress_add = [](ArrowIpcCompressor* compressor, ArrowBufferView src,
                             ArrowBuffer* dst, ArrowError* error) {
    auto& state = *static_cast<CountingCompressor*>(compressor->private_data);
    const auto result = state.inner.compress_add(&state.inner, src, dst, error);
    if (result == NANOARROW_OK) {
      state.uncompressed_size += AlignValue<int64_t>(src.size_bytes);
    }
    return result;
  };
  counting.compress_wait = [](ArrowIpcCompressor* compressor, int64_t timeout_ms,
                              ArrowError* error) {
    auto& state = *static_cast<CountingCompressor*>(compressor->private_data);
    return state.inner.compress_wait(&state.inner, timeout_ms, error);
  };
  counting.release = [](ArrowIpcCompressor* compressor) {
    delete static_cast<CountingCompressor*>(compressor->private_data);
    compressor->release = nullptr;
  };
  NANOARROW_THROW_NOT_OK(ArrowIpcEncoderSetCompressor(&encoder, &counting));
  state.release();
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
