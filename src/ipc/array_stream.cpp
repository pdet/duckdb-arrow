#include "ipc/array_stream.hpp"

namespace duckdb {
namespace ext_nanoarrow {

IpcArrayStream::IpcArrayStream(unique_ptr<IPCStreamReader> reader)
    : reader(std::move(reader)) {}

IPCStreamReader& IpcArrayStream::Reader() const { return *reader; }

void IpcArrayStream::ToArrayStream(ArrowArrayStream* stream) {
  nanoarrow::ArrayStreamFactory<IpcArrayStream>::InitArrayStream(
      new IpcArrayStream(std::move(reader)), stream);
}

int IpcArrayStream::GetSchema(ArrowSchema* schema) {
  return Wrap([&]() {
    NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(reader->GetOutputSchema(), schema));
  });
}

int IpcArrayStream::GetNext(ArrowArray* array) {
  return Wrap([&]() { reader->GetNextBatch(array); });
}

const char* IpcArrayStream::GetLastError() const { return last_msg.c_str(); }

}  // namespace ext_nanoarrow
}  // namespace duckdb
