#include "ipc/array_stream.hpp"

namespace duckdb {
namespace ext_nanoarrow {

IpcArrayStream::IpcArrayStream(unique_ptr<IPCStreamReader> reader)
    : owned_reader(std::move(reader)), reader(owned_reader.get()) {}

IpcArrayStream::IpcArrayStream(IPCStreamReader& borrowed_reader)
    : reader(&borrowed_reader) {}

IPCStreamReader& IpcArrayStream::Reader() const { return *reader; }

void IpcArrayStream::ToArrayStream(ArrowArrayStream* stream) {
  auto private_data = owned_reader ? new IpcArrayStream(std::move(owned_reader))
                                   : new IpcArrayStream(*reader);
  nanoarrow::ArrayStreamFactory<IpcArrayStream>::InitArrayStream(private_data, stream);
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
