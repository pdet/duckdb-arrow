#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {
namespace ext_nanoarrow {

// Files start with the magic padded to eight bytes and end with the bare magic
constexpr char kArrowIPCFileMagic[] = "ARROW1\0";
constexpr idx_t kArrowIPCFileMagicSize = 6;
constexpr idx_t kArrowIPCFileHeaderSize = 8;
static_assert(sizeof(kArrowIPCFileMagic) == kArrowIPCFileHeaderSize,
              "padded magic must be eight bytes");

}  // namespace ext_nanoarrow
}  // namespace duckdb
