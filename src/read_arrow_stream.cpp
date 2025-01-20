#include "read_arrow_stream.hpp"

#include <inttypes.h>

#include <zstd.h>

#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "nanoarrow_errors.hpp"

// read_arrow_stream() implementation
//
// Same results as read_arrow_stream(), but this version uses the
// ArrowIpcDecoder directly. This lets it use DuckDB's allocator at the
// expense of a bit more verbosity. Because we can apply the projection
// it reduces some of the verbosity of the actual DuckDB part (although the
// ArrayStreamReader from nanoarrow could support a projection, which
// would handle that too).
//
// I like this version better than the simpler one, and there are more parts
// that could get optimized here (whereas with the array stream version you
// don't have much control).

namespace duckdb {

namespace ext_nanoarrow {

namespace {

struct ArrowIpcMessagePrefix {
  uint32_t continuation_token;
  int32_t metadata_size;
};

nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder();

class IpcStreamReader {
 public:
  IpcStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle, Allocator& allocator)
      : decoder(NewDuckDBArrowDecoder()),
        file_reader(fs, std::move(handle)),
        allocator(allocator) {}

  const ArrowSchema* GetFileSchema() {
    if (file_schema->release) {
      return file_schema.get();
    }

    ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_SCHEMA}, /*end_of_stream_ok*/ false);

    if (decoder->feature_flags & NANOARROW_IPC_FEATURE_DICTIONARY_REPLACEMENT) {
      throw IOException("This stream uses unsupported feature DICTIONARY_REPLACEMENT");
    }

    // Decode the schema
    THROW_NOT_OK(IOException, &error,
                 ArrowIpcDecoderDecodeSchema(decoder.get(), file_schema.get(), &error));

    // Set up the decoder to decode batches
    THROW_NOT_OK(InternalException, &error,
                 ArrowIpcDecoderSetEndianness(decoder.get(), decoder->endianness));
    THROW_NOT_OK(InternalException, &error,
                 ArrowIpcDecoderSetSchema(decoder.get(), file_schema.get(), &error));

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

    // There is a way to actually get zero copy here...probably the ArrowIpcSharedBuffer
    // approach is easiest but we can also work with the array view directly and borrow
    // the requisite buffers using nanoarrow::BufferInit<> to add references to the
    // shared_ptr<AllocatedData>.

    if (HasProjection()) {
      NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(array.get(), NANOARROW_TYPE_STRUCT));
      NANOARROW_THROW_NOT_OK(
          ArrowArrayAllocateChildren(array.get(), GetOutputSchema()->n_children));

      for (int64_t i = 0; i < array->n_children; i++) {
        THROW_NOT_OK(InternalException, &error,
                     ArrowIpcDecoderDecodeArray(decoder.get(), body_view,
                                                projected_fields[i], array->children[i],
                                                NANOARROW_VALIDATION_LEVEL_FULL, &error));
      }

      D_ASSERT(array->n_children > 0);
      array->length = array->children[0]->length;
      array->null_count = 0;
    } else {
      THROW_NOT_OK(InternalException, &error,
                   ArrowIpcDecoderDecodeArray(decoder.get(), body_view, -1, array.get(),
                                              NANOARROW_VALIDATION_LEVEL_FULL, &error));
    }

    ArrowArrayMove(array.get(), out);
    return true;
  }

  void SetColumnProjection(vector<string> column_names) {
    if (column_names.size() == 0) {
      throw InternalException("Can't request zero fields projected from IpcStreamReader");
    }

    // Ensure we have a file schema to work with
    GetFileSchema();

    nanoarrow::UniqueSchema schema;
    ArrowSchemaInit(schema.get());
    NANOARROW_THROW_NOT_OK(ArrowSchemaSetTypeStruct(
        schema.get(), UnsafeNumericCast<int64_t>(column_names.size())));

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
      NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(field_id_item->second.second,
                                                 schema->children[output_column_index]));

      ++output_column_index;
    }

    projected_schema = std::move(schema);
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

    // If we're at the beginning of the read and we see the Arrow file format
    // header bytes, skip them and try to read the stream anyway. This works because
    // there's a full stream within an Arrow file (including the EOS indicator, which
    // is key to success. This EOS indicator is unfortunately missing in Rust releases
    // prior to ~September 2024).
    //
    // When we support dictionary encoding we will possibly need to seek to the footer
    // here, parse that, and maybe lazily seek and read dictionaries for if/when they are
    // required.
    if (file_reader.CurrentOffset() == 8 &&
        std::memcmp("ARROW1\0\0", &message_prefix, 8) == 0) {
      return ReadNextMessage();
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

    ArrowErrorCode decode_header_status = ArrowIpcDecoderDecodeHeader(
        decoder.get(), AllocatedDataView(message_header), &error);
    if (decode_header_status == ENODATA) {
      finished = true;
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    } else {
      THROW_NOT_OK(IOException, &error, decode_header_status);
    }

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

    if (!parameters.projected_columns.columns.empty()) {
      factory->reader->SetColumnProjection(parameters.projected_columns.columns);
    }

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

struct ReadArrowStream {
  // Define the function. Unlike arrow_scan(), which takes integer pointers
  // as arguments, we keep the factory alive by making it a member of the bind
  // data (instead of as a Python object whose ownership is kept alive via the
  // DependencyItem mechanism).
  static TableFunction Function() {
    TableFunction fn("read_arrow_stream", {LogicalType::VARCHAR}, Scan, Bind,
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
    if (!ReplacementScan::CanReplace(table_name, {"arrows", "arrow"})) {
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

    DBConfig& config = DatabaseInstance::GetDatabase(context).config;
    ArrowTableFunction::PopulateArrowTableType(config, res->arrow_table, res->schema_root,
                                               names, return_types);
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

// A version of ArrowDecompressZstd that uses DuckDB's C++ namespaceified
// zstd.h header that doesn't work with a C compiler
static ArrowErrorCode DuckDBDecompressZstd(struct ArrowBufferView src, uint8_t* dst,
                                           int64_t dst_size, struct ArrowError* error) {
  size_t code = duckdb_zstd::ZSTD_decompress((void*)dst, (size_t)dst_size, src.data.data,
                                             src.size_bytes);
  if (duckdb_zstd::ZSTD_isError(code)) {
    ArrowErrorSet(error,
                  "ZSTD_decompress([buffer with %" PRId64
                  " bytes] -> [buffer with %" PRId64 " bytes]) failed with error '%s'",
                  src.size_bytes, dst_size, duckdb_zstd::ZSTD_getErrorName(code));
    return EIO;
  }

  if (dst_size != (int64_t)code) {
    ArrowErrorSet(error,
                  "Expected decompressed size of %" PRId64 " bytes but got %" PRId64
                  " bytes",
                  dst_size, (int64_t)code);
    return EIO;
  }

  return NANOARROW_OK;
}

// Create an ArrowIpcDecoder() with the appropriate decompressor set.
// We could also define a decompressor that uses threads to parellelize
// decompression for batches with many columns.
nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder() {
  nanoarrow::ipc::UniqueDecompressor decompressor;
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressor(decompressor.get()));
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressorSetFunction(
      decompressor.get(), NANOARROW_IPC_COMPRESSION_TYPE_ZSTD, DuckDBDecompressZstd));

  nanoarrow::ipc::UniqueDecoder decoder;
  NANOARROW_THROW_NOT_OK(ArrowIpcDecoderInit(decoder.get()));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcDecoderSetDecompressor(decoder.get(), decompressor.get()));
  // Bug in nanoarrow!
  decompressor->release = nullptr;
  return decoder;
}

}  // namespace

unique_ptr<FunctionData> ReadArrowStreamBindCopy(ClientContext& context, CopyInfo& info,
                                                 vector<string>& expected_names,
                                                 vector<LogicalType>& expected_types) {
  return ReadArrowStream::BindCopy(context, info, expected_names, expected_types);
}

TableFunction ReadArrowStreamFunction() { return ReadArrowStream::Function(); }

void RegisterReadArrowStream(DatabaseInstance& db) {
  ExtensionUtil::RegisterFunction(db, ReadArrowStream::Function());
  auto& config = DBConfig::GetConfig(db);
  config.replacement_scans.emplace_back(ReadArrowStream::ScanReplacement);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
