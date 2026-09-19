#include "ipc/stream_reader/concurrent_reads.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! Runs one read on the executor's pool
class FileReadTask : public BaseExecutorTask {
 public:
  FileReadTask(TaskExecutor& executor, FileHandle& handle, const FileRead& read)
      : BaseExecutorTask(executor), handle(handle), read(read) {}

  void ExecuteTask() override { handle.Read(read.target, read.size, read.location); }

 private:
  FileHandle& handle;
  FileRead read;
};

}  // namespace

void ScheduleReads(TaskExecutor& executor, FileHandle& handle,
                   const vector<FileRead>& reads) {
  for (const auto& read : reads) {
    executor.ScheduleTask(make_uniq<FileReadTask>(executor, handle, read));
  }
}

void ReadConcurrently(TaskScheduler& scheduler, FileHandle& handle,
                      const vector<FileRead>& reads) {
  TaskExecutor executor(scheduler, TaskSchedulerType::ASYNC);
  ScheduleReads(executor, handle, reads);
  executor.WorkOnTasks();
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
