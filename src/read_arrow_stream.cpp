#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#define _DUCKDB_NANOARROW_THROW_NOT_OK_IMPL(NAME, ExceptionCls, ERROR_PTR, EXPR,      \
                                            EXPR_STR)                                 \
  do {                                                                                \
    const int NAME = (EXPR);                                                          \
    if (NAME) {                                                                       \
      throw ExceptionCls(std::string(EXPR_STR) + std::string(" failed with errno ") + \
                         std::to_string(NAME) + std::string(": ") +                   \
                         std::string((ERROR_PTR)->message));                          \
    }                                                                                 \
  } while (0)

#define THROW_NOT_OK(ExceptionCls, ERROR_PTR, EXPR)                                     \
  _DUCKDB_NANOARROW_THROW_NOT_OK_IMPL(_NANOARROW_MAKE_NAME(errno_status_, __COUNTER__), \
                                      ExceptionCls, ERROR_PTR, EXPR, #EXPR)

namespace duckdb {

namespace ext_nanoarrow {

namespace {

inline void InitDuckDBInputStream(unique_ptr<FileHandle> handle,
                                  ArrowIpcInputStream* out);

class ArrowIpcArrowArrayStreamFactory {
 public:
  explicit ArrowIpcArrowArrayStreamFactory(ClientContext& context,
                                           const std::string& src_string)
      : fs(FileSystem::GetFileSystem(context)),
        allocator(BufferAllocator::Get(context)),
        src_string(src_string){};

  // Called once when initializing Scan States
  static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr,
                                                     ArrowStreamParameters& parameters) {
    auto factory = static_cast<ArrowIpcArrowArrayStreamFactory*>(
        reinterpret_cast<void*>(factory_ptr));

    if (!factory->stream->release) {
      throw InternalException("ArrowArrayStream was not initialized");
    }

    auto out = make_uniq<ArrowArrayStreamWrapper>();
    ArrowArrayStreamMove(factory->stream.get(), &out->arrow_array_stream);

    return out;
  }

  // Get the schema of the arrow object
  void GetSchema(ArrowSchemaWrapper& schema) {
    if (!stream->release) {
      throw InternalException("ArrowArrayStream was released by another thread/library");
    }

    THROW_NOT_OK(InternalException, &error,
                 ArrowArrayStreamGetSchema(stream.get(), &schema.arrow_schema, &error));
  }

  void InitStream() {
    if (stream->release) {
      throw InternalException("ArrowArrayStream is already initialized");
    }

    unique_ptr<FileHandle> handle =
        fs.OpenFile(src_string, FileOpenFlags::FILE_FLAGS_READ);

    nanoarrow::ipc::UniqueInputStream input_stream;
    InitDuckDBInputStream(std::move(handle), input_stream.get());

    NANOARROW_THROW_NOT_OK(
        ArrowIpcArrayStreamReaderInit(stream.get(), input_stream.get(), nullptr));
  }

  FileSystem& fs;
  // Not currently used; however, the nanoarrow stream implementation should
  // accept an ArrowBufferAllocator so that we can plug this in (or we should
  // wrap the ArrowIpcDecoder ourselves)
  Allocator& allocator;
  std::string src_string;
  nanoarrow::UniqueArrayStream stream;
  ArrowError error{};
};

struct ReadArrowStream {
  static void Register(DatabaseInstance& db) {
    TableFunction fn("read_arrow_stream", {LogicalType::VARCHAR}, Scan, Bind,
                     ArrowTableFunction::ArrowScanInitGlobal,
                     ArrowTableFunction::ArrowScanInitLocal);
    fn.projection_pushdown = false;
    fn.filter_pushdown = false;
    fn.filter_prune = false;
    ExtensionUtil::RegisterFunction(db, fn);
  }

  struct Data : public ArrowScanFunctionData {
    Data(std::unique_ptr<ArrowIpcArrowArrayStreamFactory> factory)
        : ArrowScanFunctionData(ArrowIpcArrowArrayStreamFactory::Produce,
                                reinterpret_cast<uintptr_t>(factory.get())),
          factory(std::move(factory)) {}
    std::unique_ptr<ArrowIpcArrowArrayStreamFactory> factory;
  };

  static unique_ptr<FunctionData> Bind(ClientContext& context,
                                       TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types,
                                       vector<string>& names) {
    std::string src = input.inputs[0].GetValue<string>();

    auto stream_factory = make_uniq<ArrowIpcArrowArrayStreamFactory>(context, src);
    auto res = make_uniq<Data>(std::move(stream_factory));
    res->factory->InitStream();
    res->factory->GetSchema(res->schema_root);

    ArrowTableFunction::PopulateArrowTableType(res->arrow_table, res->schema_root, names,
                                               return_types);
    QueryResult::DeduplicateColumns(names);
    res->all_types = return_types;
    if (return_types.empty()) {
      throw InvalidInputException(
          "Provided table/dataframe must have at least one column");
    }

    return unique_ptr<FunctionData>(res.release());
  }

  static void Scan(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    ArrowTableFunction::ArrowScanFunction(context, input, output);
  }
};

struct DuckDBArrowInputStream {
  unique_ptr<FileHandle> handle;

  static ArrowErrorCode Read(ArrowIpcInputStream* stream, uint8_t* buf,
                             int64_t buf_size_bytes, int64_t* size_read_out,
                             ArrowError* error) {
    try {
      auto private_data = reinterpret_cast<DuckDBArrowInputStream*>(stream->private_data);
      *size_read_out = private_data->handle->Read(buf, buf_size_bytes);
      return NANOARROW_OK;
    } catch (std::exception& e) {
      ArrowErrorSet(error, "Uncaught exception in DuckDBArrowInputStream::Read(): %s",
                    e.what());
      return EIO;
    }
  }

  static void Release(ArrowIpcInputStream* stream) {
    auto private_data = reinterpret_cast<DuckDBArrowInputStream*>(stream->private_data);
    private_data->handle->Close();
    delete private_data;
  }
};

inline void InitDuckDBInputStream(unique_ptr<FileHandle> handle,
                                  ArrowIpcInputStream* out) {
  out->private_data = new DuckDBArrowInputStream{std::move(handle)};
  out->read = &DuckDBArrowInputStream::Read;
  out->release = &DuckDBArrowInputStream::Release;
}

}  // namespace

void RegisterReadArrowStream(DatabaseInstance& db) { ReadArrowStream::Register(db); }

}  // namespace ext_nanoarrow
}  // namespace duckdb
