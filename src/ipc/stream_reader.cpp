#include "ipc/stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {


  IpcFileStreamReader::IpcFileStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle, Allocator& allocator)
      : decoder(NewDuckDBArrowDecoder()),
        file_reader(fs, std::move(handle)),
        allocator(allocator) {}

  const ArrowSchema* IpcFileStreamReader::GetBaseSchema() {
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

  bool IpcFileStreamReader::HasProjection() const { return !projected_fields.empty(); }

  const ArrowSchema* IpcFileStreamReader::GetOutputSchema() {
    if (HasProjection()) {
      return projected_schema.get();
    } else {
      return GetBaseSchema();
    }
  }

  void IpcFileStreamReader::PopulateNames(vector<string>& names) {
    GetBaseSchema();

    for (int64_t i = 0; i < file_schema->n_children; i++) {
      const ArrowSchema* column = file_schema->children[i];
      if (!column->name) {
        names.push_back("");
      } else {
        names.push_back(column->name);
      }
    }
  }

void IpcFileStreamReader::DecodeArray(nanoarrow::ipc::UniqueDecoder &decoder, ArrowArray* out,  ArrowBufferView& body_view, ArrowError *error) {
    // Use the ArrowIpcSharedBuffer if we have thread safety (i.e., if this was
    // compiled with a compiler that supports C11 atomics, i.e., not gcc 4.8 or
    // MSVC)
    nanoarrow::UniqueArray array;
    THROW_NOT_OK(InternalException, error,
    ArrowIpcDecoderDecodeArray(decoder.get(), body_view, -1, array.get(),
    NANOARROW_VALIDATION_LEVEL_FULL, error));
    ArrowArrayMove(array.get(), out);
  }

  bool IpcFileStreamReader::GetNextBatch(ArrowArray* out) {
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

    // Use the ArrowIpcSharedBuffer if we have thread safety (i.e., if this was
    // compiled with a compiler that supports C11 atomics, i.e., not gcc 4.8 or
    // MSVC)
    bool thread_safe_shared = ArrowIpcSharedBufferIsThreadSafe();
    struct ArrowBufferView body_view = AllocatedDataView(*message_body);
    nanoarrow::UniqueBuffer body_shared = AllocatedDataToOwningBuffer(message_body);
    UniqueSharedBuffer shared;
    NANOARROW_THROW_NOT_OK(ArrowIpcSharedBufferInit(&shared.data, body_shared.get()));

    nanoarrow::UniqueArray array;
    if (HasProjection()) {
      NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(array.get(), NANOARROW_TYPE_STRUCT));
      NANOARROW_THROW_NOT_OK(
          ArrowArrayAllocateChildren(array.get(), GetOutputSchema()->n_children));

      if (thread_safe_shared) {
        for (int64_t i = 0; i < array->n_children; i++) {
          THROW_NOT_OK(InternalException, &error,
                       ArrowIpcDecoderDecodeArrayFromShared(
                           decoder.get(), &shared.data, projected_fields[i],
                           array->children[i], NANOARROW_VALIDATION_LEVEL_FULL, &error));
        }
      } else {
        for (int64_t i = 0; i < array->n_children; i++) {
          THROW_NOT_OK(InternalException, &error,
                       ArrowIpcDecoderDecodeArray(
                           decoder.get(), body_view, projected_fields[i],
                           array->children[i], NANOARROW_VALIDATION_LEVEL_FULL, &error));
        }
      }

      D_ASSERT(array->n_children > 0);
      array->length = array->children[0]->length;
      array->null_count = 0;
    } else if (thread_safe_shared) {
      THROW_NOT_OK(InternalException, &error,
                   ArrowIpcDecoderDecodeArrayFromShared(
                       decoder.get(), &shared.data, -1, array.get(),
                       NANOARROW_VALIDATION_LEVEL_FULL, &error));
    } else {
      THROW_NOT_OK(InternalException, &error,
                   ArrowIpcDecoderDecodeArray(decoder.get(), body_view, -1, array.get(),
                                              NANOARROW_VALIDATION_LEVEL_FULL, &error));
    }

    ArrowArrayMove(array.get(), out);
    return true;
  }

  void IpcFileStreamReader::SetColumnProjection(const vector<string>& column_names) {
    if (column_names.empty()) {
      throw InternalException("Can't request zero fields projected from IpcStreamReader");
    }

    // Ensure we have a file schema to work with
    GetBaseSchema();

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

  ArrowIpcMessageType IpcFileStreamReader::ReadNextMessage(vector<ArrowIpcMessageType> expected_types,
                                      bool end_of_stream_ok) {
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

    ArrowIpcMessageType IpcFileStreamReader::ReadNextMessage() {
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

  void IpcFileStreamReader::EnsureInputStreamAligned() {
    uint8_t padding[8];
    int padding_bytes = 8 - (file_reader.CurrentOffset() % 8);
    if (padding_bytes != 8) {
      file_reader.ReadData(padding, padding_bytes);
    }

    D_ASSERT((file_reader.CurrentOffset() % 8) == 0);
  }

  int64_t IpcFileStreamReader::CountFields(const ArrowSchema* schema) {
    int64_t n_fields = 1;
    for (int64_t i = 0; i < schema->n_children; i++) {
      n_fields += CountFields(schema->children[i]);
    }
    return n_fields;
  }

  ArrowBufferView IpcFileStreamReader::AllocatedDataView(const AllocatedData& data) {
    ArrowBufferView view;
    view.data.data = data.get();
    view.size_bytes = UnsafeNumericCast<int64_t>(data.GetSize());
    return view;
  }

  nanoarrow::UniqueBuffer IpcFileStreamReader::AllocatedDataToOwningBuffer(
      shared_ptr<AllocatedData> data) {
    nanoarrow::UniqueBuffer out;
    nanoarrow::BufferInitWrapped(out.get(), data, data->get(),
                                 UnsafeNumericCast<int64_t>(data->GetSize()));
    return out;
  }

  const char* IpcFileStreamReader::MessageTypeString(ArrowIpcMessageType message_type) {
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

};
}
