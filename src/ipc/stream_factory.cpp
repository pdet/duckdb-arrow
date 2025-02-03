#include "ipc/stream_factory.hpp"
#include "ipc/stream_reader/ipc_buffer_stream_reader.hpp"
#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {
ArrowIPCStreamFactory::ArrowIPCStreamFactory(ClientContext& context,
                                           std::string src_string)
      : fs(FileSystem::GetFileSystem(context)),
        allocator(BufferAllocator::Get(context)),
        src_string(std::move(src_string)) {}

ArrowIPCStreamFactory::ArrowIPCStreamFactory(ClientContext& context, vector<ArrowIPCBuffer> buffers): fs(FileSystem::GetFileSystem(context)),
        allocator(BufferAllocator::Get(context)),
        buffers(std::move(buffers)) {
}

unique_ptr<ArrowArrayStreamWrapper> ArrowIPCStreamFactory::Produce(
     uintptr_t factory_ptr,  ArrowStreamParameters& parameters) {
    auto factory = static_cast<ArrowIPCStreamFactory*>(
        reinterpret_cast<void*>(factory_ptr));

    if (!factory->reader) {
      throw InternalException("IpcStreamReader was not initialized or was already moved");
    }

    if (!parameters.projected_columns.columns.empty()) {
      factory->reader->SetColumnProjection(parameters.projected_columns.columns);
    }

    auto out = make_uniq<ArrowArrayStreamWrapper>();
    IpcArrayStream(std::move(factory->reader)).ToArrayStream(&out->arrow_array_stream);
    return out;
  }

  void ArrowIPCStreamFactory::GetFileSchema(ArrowSchemaWrapper& schema) const {
    if (!reader) {
      throw InternalException("IpcStreamReader is no longer valid");
    }

    NANOARROW_THROW_NOT_OK(
        ArrowSchemaDeepCopy(reader->GetBaseSchema(), &schema.arrow_schema));
  }


  void ArrowIPCStreamFactory::InitReader() {
    if (reader) {
      throw InternalException("ArrowArrayStream or IpcStreamReader already initialized");
    }
    // FIXME: bit hacky, clean this up
    D_ASSERT(src_string.empty() || buffers.empty());
    if (!src_string.empty()) {
        unique_ptr<FileHandle> handle = fs.OpenFile(src_string, FileOpenFlags::FILE_FLAGS_READ);
      reader = make_uniq<IPCFileStreamReader>(fs, std::move(handle), allocator);
    } else {
      reader = make_uniq<IPCBufferStreamReader>( buffers, allocator);
    }
  }

} // namespace ext_nanoarrow
} // namespace duckdb

