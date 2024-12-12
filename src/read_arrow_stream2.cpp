#include "duckdb/common/radix.hpp"
#include "read_arrow_stream.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "nanoarrow_errors.hpp"

// read_arrow_stream() implementation
//
// This currently uses the "easy" IPC Reader route, which wraps
// an ArrowIpcInputStream (wrapper around a FileHandle) with an
// ArrowArrayStream implementation. This works but involves copying
// quite a lot of DuckDB's internals and doesn't use DuckDB's allocator,
// Really this should use the ArrowIpcEncoder() and implement the various
// pieces of the scan specific to Arrow IPC.
//
// This version is based on the Python scanner; the ArrowArrayStreamWrapper
// was discovered towards the end of writing this. We probably do want the
// version based on the Python scanner (and when we support Arrow files,
// this will make a bit more sense, since we'll have a queue of record batch
// file offsets instead of an indeterminate stream.
//
// DuckDB could improve this process by making it easier
// to build an efficient file scanner around an ArrowArrayStream;
// nanoarrow could make this easier by allowing the ArrowIpcArrayStreamReader
// to plug in an ArrowBufferAllocator().

namespace duckdb {

namespace ext_nanoarrow {

namespace {

struct ArrowIpcMessagePrefix {
  uint32_t continuation_token;
  int32_t metadata_size;
};

class IpcStreamReader {
 public:
  IpcStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle, Allocator& allocator)
      : file_reader(fs, std::move(handle)), allocator(allocator) {
    NANOARROW_THROW_NOT_OK(ArrowIpcDecoderInit(decoder.get()));
  }

  const ArrowSchema* GetFileSchema() {
    if (file_schema->release) {
      return file_schema.get();
    }

    ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_SCHEMA}, /*end_of_stream_ok*/ false);
    THROW_NOT_OK(IOException, &error,
                 ArrowIpcDecoderDecodeSchema(decoder.get(), file_schema.get(), &error));
    return file_schema.get();
  }

  bool HasProjection() { return !projected_fields.empty(); }

  const ArrowSchema* GetOuputSchema() {
    if (HasProjection()) {
      return projected_schema.get();
    } else {
      return file_schema.get();
    }
  }

  void SetColumnProjection(vector<string> column_names) {
    if (column_names.size() == 0) {
      throw InternalException("Can't request zero fields projected from IpcStreamReader");
    }

    GetFileSchema();

    nanoarrow::UniqueSchema projected_schema;
    ArrowSchemaInit(projected_schema.get());
    NANOARROW_THROW_NOT_OK(ArrowSchemaSetTypeStruct(
        projected_schema.get(), UnsafeNumericCast<int64_t>(column_names.size())));

    // The ArrowArray builder needs the flattened field index, which we need to
    // keep track of.
    unordered_map<string, pair<idx_t, const ArrowSchema*>> name_to_flat_field_map;

    // Duplicate column names are in theory fine as long as they are not queried,
    // so we need to make a list of them to check.
    unordered_set<string> duplicate_column_names;

    // Loop over columns to build the field map
    idx_t field_count = 0;
    for (int64_t i = 0; i < file_schema->n_children; i++) {
      const ArrowSchema* column = file_schema->children[i];
      string name;
      if (!column->name) {
        name = "";
      } else {
        name = column->name;
      }

      if (name_to_flat_field_map.find(name) != name_to_flat_field_map.end()) {
        duplicate_column_names.insert(name);
      }

      name_to_flat_field_map.insert({name, {field_count, column}});

      field_count += CountFields(column);
    }

    // Loop over projected column names to build the projection information
    int64_t output_column_index = 0;
    for (const auto& column_name : column_names) {
      if (duplicate_column_names.find(column_name) != duplicate_column_names.end()) {
        throw InternalException(string("Field '") + column_name +
                                "' refers to a duplicate column name in IPC file schema");
      }

      auto field_id_item = name_to_flat_field_map.find(column_name);
      if (field_id_item == name_to_flat_field_map.end()) {
        throw InternalException(string("Field '") + column_name +
                                "' does not exist in IPC file schema");
      }

      // Record the flat field index for this column
      projected_fields.push_back(field_id_item->second.first);

      // Record the Schema for this column
      NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(
          field_id_item->second.second, projected_schema->children[output_column_index]));

      ++output_column_index;
    }
  }

 private:
  nanoarrow::ipc::UniqueDecoder decoder{};
  bool finished{false};
  BufferedFileReader file_reader;
  Allocator& allocator;
  ArrowError error{};

  ArrowIpcMessagePrefix message_prefix{};
  AllocatedData message_header;
  shared_ptr<AllocatedData> message_body;

  nanoarrow::UniqueSchema file_schema;
  nanoarrow::UniqueSchema projected_schema;
  vector<idx_t> projected_fields;

  ArrowIpcMessageType ReadNextMessage(vector<ArrowIpcMessageType> expected_types,
                                      bool end_of_stream_ok = true) {
    ArrowIpcMessageType actual_type = ReadNextMessage();
    if (end_of_stream_ok && actual_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
      return actual_type;
    }

    for (const auto expected_type : expected_types) {
      if (expected_type == actual_type) {
        return actual_type;
      }
    }

    std::stringstream expected_types_label;
    for (size_t i = 0; i < expected_types.size(); i++) {
      if (i > 0) {
        expected_types_label << " or ";
      }

      expected_types_label << MessageTypeString(expected_types[i]);
    }

    string actual_type_label;
    if (actual_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
      actual_type_label = "end of stream";
    } else {
      actual_type_label = MessageTypeString(actual_type);
    }

    throw IOException(string("Expected ") + expected_types_label.str() +
                      " Arrow IPC message but got " + actual_type_label);
  }

  ArrowIpcMessageType ReadNextMessage() {
    if (finished || file_reader.Finished()) {
      finished = true;
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    }

    // If there is no more data to be read, we're done!
    try {
      EnsureInputStreamAligned();
      file_reader.ReadData(reinterpret_cast<data_ptr_t>(&message_prefix),
                           sizeof(message_prefix));
    } catch (SerializationException& e) {
      finished = true;
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    }

    if (message_prefix.continuation_token != kContinuationToken) {
      throw IOException(std::string("Expected continuation token (0xFFFFFFFF) but got " +
                                    std::to_string(message_prefix.continuation_token)));
    }

    idx_t metadata_size;
    if (!Radix::IsLittleEndian()) {
      metadata_size = static_cast<int32_t>(BSWAP32(message_prefix.metadata_size));
    } else {
      metadata_size = message_prefix.metadata_size;
    }

    if (metadata_size < 0) {
      throw IOException(std::string("Expected metadata size >= 0 but got " +
                                    std::to_string(metadata_size)));
    }

    // Ensure we have enough space to read the header
    idx_t message_header_size = metadata_size + sizeof(message_prefix);
    if (message_header.GetSize() < message_header_size) {
      message_header = allocator.Allocate(message_header_size);
    }

    // Read the message header. I believe the fact that this loops and calls
    // the file handle's Read() method with relatively small chunks will ensure that
    // an attempt to read a very large message_header_size can be cancelled. If this
    // is not the case, we might want to implement our own buffering.
    std::memcpy(message_header.get(), &message_prefix, sizeof(message_prefix));
    file_reader.ReadData(message_header.get() + sizeof(message_prefix),
                         message_prefix.metadata_size);

    THROW_NOT_OK(IOException, &error,
                 ArrowIpcDecoderDecodeHeader(decoder.get(),
                                             AllocatedDataView(message_header), &error));

    if (decoder->body_size_bytes > 0) {
      EnsureInputStreamAligned();
      message_body =
          make_shared_ptr<AllocatedData>(allocator.Allocate(decoder->body_size_bytes));

      // Again, this is possibly a long running Read() call for a large body.
      // We could possibly be smarter about how we do this, particularly if we
      // are reading a small portion of the input from a seekable file.
      file_reader.ReadData(message_body->get(), decoder->body_size_bytes);
    }

    return decoder->message_type;
  }

  void EnsureInputStreamAligned() {
    uint8_t padding[8];
    int padding_bytes = 8 - (file_reader.CurrentOffset() % 8);
    if (padding_bytes != 8) {
      file_reader.ReadData(padding, padding_bytes);
    }

    D_ASSERT((file_reader.CurrentOffset() % 8) == 0);
  }

  static idx_t CountFields(const ArrowSchema* schema) {
    idx_t n_fields = 1;
    for (int64_t i = 0; i < schema->n_children; i++) {
      n_fields += CountFields(schema->children[i]);
    }
    return n_fields;
  }

  static ArrowBufferView AllocatedDataView(const AllocatedData& data) {
    ArrowBufferView view;
    view.data.data = data.get();
    view.size_bytes = UnsafeNumericCast<int64_t>(data.GetSize());
    return view;
  }

  static const char* MessageTypeString(ArrowIpcMessageType message_type) {
    switch (message_type) {
      case NANOARROW_IPC_MESSAGE_TYPE_SCHEMA:
        return "Schema";
      case NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH:
        return "RecordBatch";
      case NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH:
        return "DictionaryBatch";
      case NANOARROW_IPC_MESSAGE_TYPE_TENSOR:
        return "Tensor";
      case NANOARROW_IPC_MESSAGE_TYPE_SPARSE_TENSOR:
        return "SparseTensor";
      case NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED:
        return "Uninitialized";
      default:
        return "";
    }
  }

  static constexpr uint32_t kContinuationToken = 0xFFFFFFFF;
};

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
        src_string(src_string) {};

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

struct ReadArrowStream2 {
  // Define the function. Unlike arrow_scan(), which takes integer pointers
  // as arguments, we keep the factory alive by making it a member of the bind
  // data (instead of as a Python object whose ownership is kept alive via the
  // DependencyItem mechanism).
  static TableFunction Function() {
    TableFunction fn("read_arrow_stream2", {LogicalType::VARCHAR}, Scan, Bind,
                     ArrowTableFunction::ArrowScanInitGlobal,
                     ArrowTableFunction::ArrowScanInitLocal);
    fn.cardinality = Cardinality;
    fn.projection_pushdown = true;
    fn.filter_pushdown = false;
    fn.filter_prune = false;
    return fn;
  }

  static unique_ptr<TableRef> ScanReplacement(ClientContext& context,
                                              ReplacementScanInput& input,
                                              optional_ptr<ReplacementScanData> data) {
    auto table_name = ReplacementScan::GetFullPath(input);
    if (!ReplacementScan::CanReplace(table_name, {"arrows2"})) {
      return nullptr;
    }

    auto table_function = make_uniq<TableFunctionRef>();
    vector<unique_ptr<ParsedExpression>> children;
    auto table_name_expr = make_uniq<ConstantExpression>(Value(table_name));
    children.push_back(std::move(table_name_expr));
    auto function_expr =
        make_uniq<FunctionExpression>("read_arrow_stream2", std::move(children));
    table_function->function = std::move(function_expr);

    if (!FileSystem::HasGlob(table_name)) {
      auto& fs = FileSystem::GetFileSystem(context);
      table_function->alias = fs.ExtractBaseName(table_name);
    }

    return std::move(table_function);
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
    return BindInternal(context, input.inputs[0].GetValue<string>(), return_types, names);
  }

  static unique_ptr<FunctionData> BindCopy(ClientContext& context, CopyInfo& info,
                                           vector<string>& expected_names,
                                           vector<LogicalType>& expected_types) {
    return BindInternal(context, info.file_path, expected_types, expected_names);
  }

  static unique_ptr<FunctionData> BindInternal(ClientContext& context, std::string src,
                                               vector<LogicalType>& return_types,
                                               vector<string>& names) {
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

unique_ptr<FunctionData> ReadArrowStream2BindCopy(ClientContext& context, CopyInfo& info,
                                                  vector<string>& expected_names,
                                                  vector<LogicalType>& expected_types) {
  return ReadArrowStream2::BindCopy(context, info, expected_names, expected_types);
}

TableFunction ReadArrowStream2Function() { return ReadArrowStream2::Function(); }

void RegisterReadArrowStream2(DatabaseInstance& db) {
  ExtensionUtil::RegisterFunction(db, ReadArrowStream2::Function());
  auto& config = DBConfig::GetConfig(db);
  config.replacement_scans.emplace_back(ReadArrowStream2::ScanReplacement);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
