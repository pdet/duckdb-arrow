#include "ipc/stream_factory.hpp"

namespace duckdb {
namespace ext_nanoarrow {
ArrowIpcArrowArrayStreamFactory::ArrowIpcArrowArrayStreamFactory(ClientContext& context,
                                           std::string  src_string)
      : fs(FileSystem::GetFileSystem(context)),
        allocator(BufferAllocator::Get(context)),
        src_string(std::move(src_string)) {};

unique_ptr<ArrowArrayStreamWrapper> ArrowIpcArrowArrayStreamFactory::Produce(
     uintptr_t factory_ptr,  ArrowStreamParameters& parameters) {
    auto factory = static_cast<ArrowIpcArrowArrayStreamFactory*>(
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

  void ArrowIpcArrowArrayStreamFactory::GetFileSchema(ArrowSchemaWrapper& schema) const {
    if (!reader) {
      throw InternalException("IpcStreamReader is no longer valid");
    }

    NANOARROW_THROW_NOT_OK(
        ArrowSchemaDeepCopy(reader->GetFileSchema(), &schema.arrow_schema));
  }


  void ArrowIpcArrowArrayStreamFactory::InitReader() {
    if (reader) {
      throw InternalException("ArrowArrayStream or IpcStreamReader already initialized");
    }

    unique_ptr<FileHandle> handle =
        fs.OpenFile(src_string, FileOpenFlags::FILE_FLAGS_READ);
    reader = make_uniq<IpcStreamReader>(fs, std::move(handle), allocator);
  }

};
}
