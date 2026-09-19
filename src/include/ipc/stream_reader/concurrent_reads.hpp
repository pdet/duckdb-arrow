//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/stream_reader/concurrent_reads.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! A read of the file into memory
struct FileRead {
  data_ptr_t target;
  idx_t size;
  idx_t location;
};

//! Schedules each read as a task of the executor, which runs on the executor's pool
void ScheduleReads(TaskExecutor& executor, FileHandle& handle,
                   const vector<FileRead>& reads);
//! Runs the reads at the same time on the async pool, so they wait one round trip
void ReadConcurrently(TaskScheduler& scheduler, FileHandle& handle,
                      const vector<FileRead>& reads);

}  // namespace ext_nanoarrow
}  // namespace duckdb
