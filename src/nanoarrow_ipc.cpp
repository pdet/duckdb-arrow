#include "duckdb/common/file_opener.hpp"
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
  explicit ArrowIpcArrowArrayStreamFactory(const std::string& src_string)
      : src_string(src_string){};

  // Should be only called once when initializing Scan States
  static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr,
                                                     ArrowStreamParameters& parameters) {
    auto factory = static_cast<ArrowIpcArrowArrayStreamFactory*>(
        reinterpret_cast<void*>(factory_ptr));

    nanoarrow::ipc::UniqueInputStream input_stream;
    // InitDuckDBInputStream(unique_ptr<FileHandle> handle, input_stream.get());
    auto out = make_uniq<ArrowArrayStreamWrapper>();
    NANOARROW_THROW_NOT_OK(ArrowIpcArrayStreamReaderInit(&out->arrow_array_stream, input_stream.get(), nullptr));
    return out;
  }

  // Get the schema of the arrow object
  static void GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper& schema) {
    auto factory = static_cast<ArrowIpcArrowArrayStreamFactory*>(
        reinterpret_cast<void*>(factory_ptr));
    if (!factory->stream->release) {
      throw InternalException("ArrowArrayStream was released by another thread/library");
    }

    THROW_NOT_OK(InternalException, &factory->error,
                 ArrowArrayStreamGetSchema(factory->stream.get(), &schema.arrow_schema,
                                           &factory->error));
  }

  static void InitScan(const string& name) {}

  // Source string (e.g., filename)
  std::string src_string;
  nanoarrow::UniqueArrayStream stream;
  ArrowError error{};
};

struct ReadArrow {
  static void Register(DatabaseInstance& db) {
    TableFunction fn("read_arrow_stream", {LogicalType::VARCHAR}, Scan, Bind,
                     ArrowTableFunction::ArrowScanInitGlobal,
                     ArrowTableFunction::ArrowScanInitLocal);
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
    auto stream_factory = make_uniq<ArrowIpcArrowArrayStreamFactory>(src);

    // Actually get schema

    auto data = make_uniq<Data>(std::move(stream_factory));

    return data;
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
  }
};

inline void InitDuckDBInputStream(unique_ptr<FileHandle> handle,
                                  ArrowIpcInputStream* out) {
  out->private_data = new DuckDBArrowInputStream{std::move(handle)};
  out->read = &DuckDBArrowInputStream::Read;
  out->release = &DuckDBArrowInputStream::Release;
}

}  // namespace

}  // namespace ext_nanoarrow
}  // namespace duckdb
