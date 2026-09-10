//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/codecs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! How the bodies of RecordBatch messages are compressed when writing Arrow IPC
struct ArrowIpcCompressionOptions {
  ArrowIpcCompressionType type = NANOARROW_IPC_COMPRESSION_TYPE_NONE;
  //! Codec specific level passed to nanoarrow unchanged
  int64_t level = NANOARROW_IPC_COMPRESSION_LEVEL_DEFAULT;
  bool level_set = false;

  //! Applies a COMPRESSION or COMPRESSION_LEVEL option, returns false for any other name
  bool TrySetOption(const string& name, const Value& value);
  //! Throws a BinderException if the level does not fit the codec, call once all are set
  void Validate() const;
};

//! Creates an IPC decoder, nanoarrow decompresses zstd and lz4 bodies itself
nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder();

//! Makes an IPC encoder compress bodies, throws if this nanoarrow build lacks the codec
void SetArrowIpcEncoderCompression(ArrowIpcEncoder& encoder,
                                   const ArrowIpcCompressionOptions& options);

}  // namespace ext_nanoarrow
}  // namespace duckdb
