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

// read_arrow_stream() implementation
//
// This currently uses the "easy" IPC Reader route, which wraps
// an ArrowIpcInputStream (wrapper around a FileHandle) with an
// ArrowArrayStream implementation. This works but involves copying
// quite a lot of DuckDB's internals and doesn't use DuckDB's allocator,
// Really this should use the ArrowIpcEncoder() and implement the various
// pieces of the scan specific to Arrow IPC.
//
// DuckDB could improve this process by making it easier
// to build a file scanner around an ArrowArrayStream; nanoarrow could make
// this easier by allowing the ArrowIpcArrayStreamReader to plug in an
// ArrowBufferAllocator().

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

// Initializes our ArrowIpcInputStream wrapper from DuckDB's file
// abstraction. This lets us use their filesystems and any plugins that
// add them (like httpfs).
inline void InitDuckDBInputStream(unique_ptr<FileHandle> handle,
                                  ArrowIpcInputStream* out);

// This Factory is a type invented by DuckDB. Notably, the Produce()
// function pointer is passed to the constructor of the ArrowScanFunctionData
// constructor (which we wrap).
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

    THROW_NOT_OK(IOException, &error,
                 ArrowArrayStreamGetSchema(stream.get(), &schema.arrow_schema, &error));
  }

  // Opens the file, wraps it in the ArrowIpcInputStream, and wraps it in
  // the ArrowArrayStream reader.
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
  // Register the function. Unlike arrow_scan(), which takes integer pointers
  // as arguments, we keep the factory alive by making it a member of the bind
  // data (instead of as a Python object whose ownership is kept alive via the
  // DependencyItem mechanism).
  static void Register(DatabaseInstance& db) {
    TableFunction fn("read_arrow_stream", {LogicalType::VARCHAR}, Scan, Bind,
                     ArrowTableFunction::ArrowScanInitGlobal,
                     ArrowTableFunction::ArrowScanInitLocal);
    fn.get_batch_index = GetBatchIndex;
    fn.cardinality = Cardinality;
    fn.projection_pushdown = true;
    fn.filter_pushdown = false;
    fn.filter_prune = false;
    ExtensionUtil::RegisterFunction(db, fn);
  }

  // Our FunctionData is the same as the ArrowScanFunctionData except we extend it
  // it to keep the ArrowIpcArrowArrayStreamFactory alive.
  struct Data : public ArrowScanFunctionData {
    Data(std::unique_ptr<ArrowIpcArrowArrayStreamFactory> factory)
        : ArrowScanFunctionData(ArrowIpcArrowArrayStreamFactory::Produce,
                                reinterpret_cast<uintptr_t>(factory.get())),
          factory(std::move(factory)) {}
    std::unique_ptr<ArrowIpcArrowArrayStreamFactory> factory;
  };

  // Our Bind() function is differenct from the arrow_scan because our input
  // is a filename (and their input is three pointer addresses).
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

  // This is almost the same as ArrowTableFunction::Scan() except we need to pass
  // arrow_scan_is_projected = false to ArrowToDuckDB(). It's a bit unfortunate
  // we have to copy this much (although the spatial extension also copies this
  // as it does something vaguely similar).
  static void Scan(ClientContext& context, TableFunctionInput& data_p,
                   DataChunk& output) {
    if (!data_p.local_state) {
      return;
    }
    auto& data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();  // FIXME
    auto& state = data_p.local_state->Cast<ArrowScanLocalState>();
    auto& global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

    //! Out of tuples in this chunk
    if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
      if (!ArrowTableFunction::ArrowScanParallelStateNext(context, data_p.bind_data.get(),
                                                          state, global_state)) {
        return;
      }
    }
    auto output_size = MinValue<idx_t>(
        STANDARD_VECTOR_SIZE,
        NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
    data.lines_read += output_size;
    if (global_state.CanRemoveFilterColumns()) {
      state.all_columns.Reset();
      state.all_columns.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(),
                                        state.all_columns, data.lines_read - output_size,
                                        false);
      output.ReferenceColumns(state.all_columns, global_state.projection_ids);
    } else {
      output.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), output,
                                        data.lines_read - output_size, false);
    }

    output.Verify();
    state.chunk_offset += output.size();
  }

  // Identical to the ArrowTableFunction, but that version is marked protected
  static idx_t GetBatchIndex(ClientContext& context, const FunctionData* bind_data_p,
                             LocalTableFunctionState* local_state,
                             GlobalTableFunctionState* global_state) {
    auto& state = local_state->Cast<ArrowScanLocalState>();
    return state.batch_index;
  }

  // Identical to the ArrowTableFunction, but that version is marked protected
  static unique_ptr<NodeStatistics> Cardinality(ClientContext& context,
                                                const FunctionData* data) {
    return make_uniq<NodeStatistics>();
  }
};

// Implementation of the ArrowIpcInputStream wrapper around DuckDB's input stream
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
