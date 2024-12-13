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

  const ArrowSchema* GetOutputSchema() {
    if (HasProjection()) {
      return projected_schema.get();
    } else {
      return GetFileSchema();
    }
  }

  void PopulateNames(vector<string>& names) {
    GetFileSchema();

    for (int64_t i = 0; i < file_schema->n_children; i++) {
      const ArrowSchema* column = file_schema->children[i];
      if (!column->name) {
        names.push_back("");
      } else {
        names.push_back(column->name);
      }
    }
  }

  bool GetNextBatch(ArrowArray* out) {
    // When nanoarrow supports dictionary batches, we'd accept either a
    // RecordBatch or DictionaryBatch message, recording the dictionary batch
    // (or possibly ignoring it if it is for a field that we don't care about),
    // but looping until we end up with a RecordBatch in the decoder.
    ArrowIpcMessageType message_type =
        ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH});
    if (message_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
      out->release = nullptr;
      return false;
    }

    nanoarrow::UniqueArray array;
    ArrowBufferView body_view = AllocatedDataView(*message_body);
    // Can use this to get around the no-custom-allocator thing in a sec
    // THROW_NOT_OK(InternalException, &error,
    //              ArrowArrayInitFromSchema(array.get(), GetOutputSchema(), &error));
    // ArrowIpcDecoderDecodeArrayView() + nanoarrow::BufferInit<> to borrow buffers from
    // the message_body.

    if (HasProjection()) {
      NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(array.get(), NANOARROW_TYPE_STRUCT));
      NANOARROW_THROW_NOT_OK(
          ArrowArrayAllocateChildren(array.get(), GetOutputSchema()->n_children));

      for (int64_t i = 0; i < array->n_children; i++) {
        THROW_NOT_OK(InternalException, &error,
                     ArrowIpcDecoderDecodeArray(
                         decoder.get(), body_view, projected_fields[i],
                         array->children[i], NANOARROW_VALIDATION_LEVEL_DEFAULT, &error));
      }
    } else {
      THROW_NOT_OK(
          InternalException, &error,
          ArrowIpcDecoderDecodeArray(decoder.get(), body_view, -1, array.get(),
                                     NANOARROW_VALIDATION_LEVEL_DEFAULT, &error));
    }

    ArrowArrayMove(array.get(), out);
    return true;
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
    unordered_map<string, pair<int64_t, const ArrowSchema*>> name_to_flat_field_map;

    // Duplicate column names are in theory fine as long as they are not queried,
    // so we need to make a list of them to check.
    unordered_set<string> duplicate_column_names;

    // Loop over columns to build the field map
    int64_t field_count = 0;
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
  vector<int64_t> projected_fields;

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

  static int64_t CountFields(const ArrowSchema* schema) {
    int64_t n_fields = 1;
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

class IpcArrayStream {
 public:
  IpcArrayStream(unique_ptr<IpcStreamReader> reader) : reader(std::move(reader)) {}

  IpcStreamReader& Reader() { return *reader; }

  void ToArrayStream(ArrowArrayStream* stream) {
    nanoarrow::ArrayStreamFactory<IpcArrayStream>::InitArrayStream(
        new IpcArrayStream(std::move(reader)), stream);
  }

  int GetSchema(ArrowSchema* schema) {
    return Wrap([&]() {
      NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(reader->GetOutputSchema(), schema));
    });
  }

  int GetNext(ArrowArray* array) {
    return Wrap([&]() { reader->GetNextBatch(array); });
  }

  const char* GetLastError() { return last_msg.c_str(); }

 private:
  unique_ptr<IpcStreamReader> reader;
  string last_msg;

  template <typename Func>
  int Wrap(Func&& func) {
    try {
      func();
      return NANOARROW_OK;
    } catch (IOException& e) {
      last_msg = std::string("IOException: ") + e.what();
      return EIO;
    } catch (InternalException& e) {
      last_msg = std::string("InternalException: ") + e.what();
      return EINVAL;
    } catch (nanoarrow::Exception& e) {
      last_msg = std::string("nanoarrow::Exception: ") + e.what();
      // Could probably find a way to pass on this code, usually ENOMEM
      return ENOMEM;
    } catch (std::exception& e) {
      last_msg = e.what();
      return EINVAL;
    }
  }
};

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

    if (!factory->reader) {
      throw InternalException("IpcStreamReader was not initialized or was already moved");
    }

    factory->reader->SetColumnProjection(parameters.projected_columns.columns);

    auto out = make_uniq<ArrowArrayStreamWrapper>();
    IpcArrayStream(std::move(factory->reader)).ToArrayStream(&out->arrow_array_stream);
    return out;
  }

  // Get the schema of the arrow object
  void GetFileSchema(ArrowSchemaWrapper& schema) {
    if (!reader) {
      throw InternalException("IpcStreamReader is no longer valid");
    }

    NANOARROW_THROW_NOT_OK(
        ArrowSchemaDeepCopy(reader->GetFileSchema(), &schema.arrow_schema));
  }

  // Opens the file, wraps it in the ArrowIpcInputStream, and wraps it in
  // the ArrowArrayStream reader.
  void InitReader() {
    if (reader) {
      throw InternalException("ArrowArrayStream or IpcStreamReader already initialized");
    }

    unique_ptr<FileHandle> handle =
        fs.OpenFile(src_string, FileOpenFlags::FILE_FLAGS_READ);
    reader = make_uniq<IpcStreamReader>(fs, std::move(handle), allocator);
  }

  FileSystem& fs;
  Allocator& allocator;
  std::string src_string;
  unique_ptr<IpcStreamReader> reader;
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
        make_uniq<FunctionExpression>("read_arrow_stream", std::move(children));
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

  // Our Bind() function is different from the arrow_scan because our input
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
    res->factory->InitReader();
    res->factory->GetFileSchema(res->schema_root);

    ArrowTableFunction::PopulateArrowTableType(res->arrow_table, res->schema_root, names,
                                               return_types);
    QueryResult::DeduplicateColumns(names);
    res->all_types = return_types;
    if (return_types.empty()) {
      throw InvalidInputException(
          "Provided table/dataframe must have at least one column");
    }

    return std::move(res);
  }

  static void Scan(ClientContext& context, TableFunctionInput& data_p,
                   DataChunk& output) {
    ArrowTableFunction::ArrowScanFunction(context, data_p, output);
  }

  // Identical to the ArrowTableFunction, but that version is marked protected
  static unique_ptr<NodeStatistics> Cardinality(ClientContext& context,
                                                const FunctionData* data) {
    return make_uniq<NodeStatistics>();
  }
};

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
