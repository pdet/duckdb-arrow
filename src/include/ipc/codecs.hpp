//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/codecs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! How the bodies of RecordBatch messages are compressed when writing Arrow IPC
struct ArrowIpcCompressionOptions {
  ArrowIpcCompressionType type = NANOARROW_IPC_COMPRESSION_TYPE_NONE;
  //! Codec-specific compression level, passed to nanoarrow as-is
  bool level_set = false;
  int64_t level = NANOARROW_IPC_COMPRESSION_LEVEL_DEFAULT;
};

//! Parses the value of the COMPRESSION copy option (case-insensitive): 'uncompressed',
//! 'none', 'zstd', 'lz4' or 'lz4_frame'. Throws a BinderException for anything else.
ArrowIpcCompressionType ParseArrowIpcCompressionType(const string& name);

//! Throws a BinderException if level is out of range for the compression type
void ValidateArrowIpcCompressionLevel(ArrowIpcCompressionType type, int64_t level);

//! Creates an IPC decoder. nanoarrow decompresses zstd and lz4 RecordBatch bodies itself.
nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder();

//! Configures an IPC encoder to compress the RecordBatch bodies it encodes. Throws if
//! this build of nanoarrow does not support the codec.
void SetArrowIpcEncoderCompression(ArrowIpcEncoder& encoder,
                                   const ArrowIpcCompressionOptions& options);

}  // namespace ext_nanoarrow
}  // namespace duckdb
