#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {
namespace ext_nanoarrow {

constexpr char kArrowIPCFileMagic[] = "ARROW1\0";
constexpr idx_t kArrowIPCFileMagicSize = sizeof(kArrowIPCFileMagic) - 2;
constexpr idx_t kArrowIPCFileHeaderSize = sizeof(kArrowIPCFileMagic);

}  // namespace ext_nanoarrow
}  // namespace duckdb
