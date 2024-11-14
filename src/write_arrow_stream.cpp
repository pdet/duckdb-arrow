
#include "write_arrow_stream.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "nanoarrow/nanoarrow_ipc.hpp"

#include "nanoarrow_errors.hpp"
#include "read_arrow_stream.hpp"

namespace duckdb {

namespace ext_nanoarrow {

namespace {

// Initialize buffer whose realloc operations go through DuckDB's memory
// accounting. Note that the Allocator must outlive the buffer (true for
// the case of this writer, but maybe not true for generic production of
// ArrowArrays whose lifetime might outlive the connection/database).
void InitArrowDuckBuffer(ArrowBuffer* buffer, Allocator& duck_allocator) {
  ArrowBufferInit(buffer);

  buffer->allocator.reallocate = [](ArrowBufferAllocator* allocator, uint8_t* ptr,
                                    int64_t old_size, int64_t new_size) -> uint8_t* {
    NANOARROW_DCHECK(allocator->private_data != nullptr);
    auto duck_allocator = reinterpret_cast<Allocator*>(allocator->private_data);
    if (ptr == nullptr && new_size > 0) {
      return duck_allocator->AllocateData(new_size);
    } else if (new_size == 0) {
      duck_allocator->FreeData(ptr, old_size);
      return nullptr;
    } else {
      return duck_allocator->ReallocateData(ptr, old_size, new_size);
    }
  };

  buffer->allocator.free = [](ArrowBufferAllocator* allocator, uint8_t* ptr,
                              int64_t old_size) {
    NANOARROW_DCHECK(allocator->private_data != nullptr);
    auto duck_allocator = reinterpret_cast<Allocator*>(allocator->private_data);
    duck_allocator->FreeData(ptr, old_size);
  };

  buffer->allocator.private_data = &duck_allocator;
}

struct ArrowStreamWriter {
  ArrowStreamWriter(ClientContext& context, FileSystem& fs, const string& file_path,
                    const vector<LogicalType>& logical_types,
                    const vector<string>& column_names,
                    const vector<pair<string, string>>& metadata)
      : options(context.GetClientProperties()), allocator(BufferAllocator::Get(context)) {
    InitArrowDuckBuffer(header.get(), allocator);
    InitArrowDuckBuffer(body.get(), allocator);

    InitializeSchema(logical_types, column_names, metadata);
    InitializeOutputFile(fs, file_path);
    InitializeEncoderAndWriteSchema();
  }

  // Probably should do this conversion at a higher level and reuse for all outputs
  void InitializeSchema(const vector<LogicalType>& logical_types,
                        const vector<string>& column_names,
                        const vector<pair<string, string>>& metadata) {
    nanoarrow::UniqueSchema tmp_schema;
    converter.ToArrowSchema(tmp_schema.get(), logical_types, column_names, options);

    if (metadata.empty()) {
      ArrowSchemaMove(tmp_schema.get(), schema.get());
    } else {
      nanoarrow::UniqueBuffer metadata_packed;
      NANOARROW_THROW_NOT_OK(
          ArrowMetadataBuilderInit(metadata_packed.get(), tmp_schema->metadata));
      ArrowStringView key;
      ArrowStringView value;
      for (const auto& item : metadata) {
        key = {item.first.data(), static_cast<int64_t>(item.first.size())};
        key = {item.second.data(), static_cast<int64_t>(item.second.size())};
        NANOARROW_THROW_NOT_OK(
            ArrowMetadataBuilderAppend(metadata_packed.get(), key, value));
      }

      NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(tmp_schema.get(), schema.get()));
      NANOARROW_THROW_NOT_OK(ArrowSchemaSetMetadata(
          schema.get(), reinterpret_cast<char*>(metadata_packed->data)));
    }
  }

  void InitializeOutputFile(FileSystem& fs, const string& file_path) {
    writer = make_uniq<BufferedFileWriter>(
        fs, file_path.c_str(),
        FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  }

  void InitializeEncoderAndWriteSchema() {
    THROW_NOT_OK(InternalException, &error,
                 ArrowArrayViewInitFromSchema(chunk_view.get(), schema.get(), &error));
    NANOARROW_THROW_NOT_OK(ArrowIpcEncoderInit(encoder.get()));
    THROW_NOT_OK(InternalException, &error,
                 ArrowIpcEncoderEncodeSchema(encoder.get(), schema.get(), &error));
    NANOARROW_THROW_NOT_OK(
        ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));

    writer->WriteData(header->data, header->size_bytes);
  }

  void Flush(ColumnDataCollection& buffer) {
    if (buffer.ChunkCount() == 0) {
      return;
    }

    // The ArrowConverter requires all of this to be in one big DataChunk.
    // It would be better to append these one at a time using other DuckDB
    // internals like the ArrowAppender. (Possibly better would be to skip the
    // owning ArrowArray entirely and just expose an ArrowArrayView of the
    // chunk. keeping track of any owning elements that had to be allocated,
    // since that's all that is strictly required to write).
    DataChunk chunk;
    chunk.Initialize(allocator, buffer.Types(), buffer.Count());
    for (const auto& item : buffer.Chunks()) {
      chunk.Append(item, true);
    }

    chunk_arrow.reset();
    converter.ToArrowArray(chunk, chunk_arrow.get(), options);
    THROW_NOT_OK(InternalException, &error,
                 ArrowArrayViewSetArray(chunk_view.get(), chunk_arrow.get(), &error));

    // It would be nice to be able to flush this straight to the output file
    // rather than buffer the entire output in memory (again).
    ArrowIpcEncoderEncodeSimpleRecordBatch(encoder.get(), chunk_view.get(), body.get(),
                                           &error);
    header->size_bytes = 0;
    ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get());

    writer->WriteData(header->data, header->size_bytes);
    writer->WriteData(body->data, body->size_bytes);
    ++row_group_count;
  }

  void Finalize() {
    uint8_t end_of_stream[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};
    writer->WriteData(end_of_stream, sizeof(end_of_stream));
    writer->Close();
  }

  idx_t NumberOfRowGroups() { return row_group_count; }

  idx_t FileSize() { return writer->GetTotalWritten(); }

 private:
  ClientProperties options;
  Allocator& allocator;
  nanoarrow::ipc::UniqueEncoder encoder;
  ArrowConverter converter;
  unique_ptr<BufferedFileWriter> writer;
  idx_t row_group_count{0};
  nanoarrow::UniqueSchema schema;
  nanoarrow::UniqueArrayView chunk_view;
  nanoarrow::UniqueArray chunk_arrow;
  nanoarrow::UniqueBuffer header;
  nanoarrow::UniqueBuffer body;
  ArrowError error{};
};

struct ArrowWriteBindData : public TableFunctionData {
  vector<LogicalType> sql_types;
  vector<string> column_names;
  vector<pair<string, string>> kv_metadata;
  // Storage::ROW_GROUP_SIZE is not defined in at least one CI job
  idx_t row_group_size = STANDARD_ROW_GROUPS_SIZE;
  optional_idx row_groups_per_file;
  static constexpr const idx_t BYTES_PER_ROW = 1024;
  idx_t row_group_size_bytes;
};

struct ArrowWriteGlobalState : public GlobalFunctionData {
  unique_ptr<ArrowStreamWriter> writer;
};

struct ArrowWriteLocalState : public LocalFunctionData {
  explicit ArrowWriteLocalState(ClientContext& context, const vector<LogicalType>& types)
      : buffer(context, types, ColumnDataAllocatorType::HYBRID) {
    buffer.InitializeAppend(append_state);
  }

  ColumnDataCollection buffer;
  ColumnDataAppendState append_state;
};

unique_ptr<FunctionData> ArrowWriteBind(ClientContext& context,
                                        CopyFunctionBindInput& input,
                                        const vector<string>& names,
                                        const vector<LogicalType>& sql_types) {
  D_ASSERT(names.size() == sql_types.size());
  auto bind_data = make_uniq<ArrowWriteBindData>();
  bool row_group_size_bytes_set = false;

  for (auto& option : input.info.options) {
    const auto loption = StringUtil::Lower(option.first);
    if (option.second.size() != 1) {
      // All Arrow write options require exactly one argument
      throw BinderException("%s requires exactly one argument",
                            StringUtil::Upper(loption));
    }

    if (loption == "row_group_size" || loption == "chunk_size") {
      bind_data->row_group_size = option.second[0].GetValue<uint64_t>();
    } else if (loption == "row_group_size_bytes") {
      auto roption = option.second[0];
      if (roption.GetTypeMutable().id() == LogicalTypeId::VARCHAR) {
        bind_data->row_group_size_bytes = DBConfig::ParseMemoryLimit(roption.ToString());
      } else {
        bind_data->row_group_size_bytes = option.second[0].GetValue<uint64_t>();
      }
      row_group_size_bytes_set = true;
    } else if (loption == "row_groups_per_file") {
      bind_data->row_groups_per_file = option.second[0].GetValue<uint64_t>();
    } else if (loption == "kv_metadata") {
      auto& kv_struct = option.second[0];
      auto& kv_struct_type = kv_struct.type();
      if (kv_struct_type.id() != LogicalTypeId::STRUCT) {
        throw BinderException("Expected kv_metadata argument to be a STRUCT");
      }
      auto values = StructValue::GetChildren(kv_struct);
      for (idx_t i = 0; i < values.size(); i++) {
        auto value = values[i];
        auto key = StructType::GetChildName(kv_struct_type, i);
        // If the value is a blob, write the raw blob bytes
        // otherwise, cast to string
        if (value.type().id() == LogicalTypeId::BLOB) {
          bind_data->kv_metadata.emplace_back(key, StringValue::Get(value));
        } else {
          bind_data->kv_metadata.emplace_back(key, value.ToString());
        }
      }
    }
  }

  if (row_group_size_bytes_set) {
    if (DBConfig::GetConfig(context).options.preserve_insertion_order) {
      throw BinderException(
          "ROW_GROUP_SIZE_BYTES does not work while preserving insertion order. Use "
          "\"SET preserve_insertion_order=false;\" to disable preserving insertion "
          "order.");
    }
  } else {
    // We always set a max row group size bytes so we don't use too much memory
    bind_data->row_group_size_bytes =
        bind_data->row_group_size * ArrowWriteBindData::BYTES_PER_ROW;
  }

  bind_data->sql_types = sql_types;
  bind_data->column_names = names;

  return std::move(bind_data);
}

unique_ptr<GlobalFunctionData> ArrowWriteInitializeGlobal(ClientContext& context,
                                                          FunctionData& bind_data,
                                                          const string& file_path) {
  auto global_state = make_uniq<ArrowWriteGlobalState>();
  auto& arrow_bind = bind_data.Cast<ArrowWriteBindData>();

  auto& fs = FileSystem::GetFileSystem(context);
  global_state->writer =
      make_uniq<ArrowStreamWriter>(context, fs, file_path, arrow_bind.sql_types,
                                   arrow_bind.column_names, arrow_bind.kv_metadata);
  return std::move(global_state);
}

void ArrowWriteSink(ExecutionContext& context, FunctionData& bind_data_p,
                    GlobalFunctionData& gstate, LocalFunctionData& lstate,
                    DataChunk& input) {
  auto& bind_data = bind_data_p.Cast<ArrowWriteBindData>();
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();
  auto& local_state = lstate.Cast<ArrowWriteLocalState>();

  // append data to the local (buffered) chunk collection
  local_state.buffer.Append(local_state.append_state, input);

  if (local_state.buffer.Count() >= bind_data.row_group_size ||
      local_state.buffer.SizeInBytes() >= bind_data.row_group_size_bytes) {
    // if the chunk collection exceeds a certain size (rows/bytes) we flush it to the
    // Arrow file
    local_state.append_state.current_chunk_state.handles.clear();
    global_state.writer->Flush(local_state.buffer);
    local_state.buffer.InitializeAppend(local_state.append_state);
  }
}

void ArrowWriteCombine(ExecutionContext& context, FunctionData& bind_data,
                       GlobalFunctionData& gstate, LocalFunctionData& lstate) {
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();
  auto& local_state = lstate.Cast<ArrowWriteLocalState>();
  // flush any data left in the local state to the file
  global_state.writer->Flush(local_state.buffer);
}

void ArrowWriteFinalize(ClientContext& context, FunctionData& bind_data,
                        GlobalFunctionData& gstate) {
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();
  // finalize: write any additional metadata to the file here
  global_state.writer->Finalize();
}

unique_ptr<LocalFunctionData> ArrowWriteInitializeLocal(ExecutionContext& context,
                                                        FunctionData& bind_data_p) {
  auto& bind_data = bind_data_p.Cast<ArrowWriteBindData>();
  return make_uniq<ArrowWriteLocalState>(context.client, bind_data.sql_types);
}

CopyFunctionExecutionMode ArrowWriteExecutionMode(bool preserve_insertion_order,
                                                  bool supports_batch_index) {
  if (!preserve_insertion_order) {
    return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
  }
  if (supports_batch_index) {
    return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
  }
  return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

idx_t ArrowWriteDesiredBatchSize(ClientContext& context, FunctionData& bind_data_p) {
  auto& bind_data = bind_data_p.Cast<ArrowWriteBindData>();
  return bind_data.row_group_size;
}

bool ArrowWriteRotateFiles(FunctionData& bind_data_p,
                           const optional_idx& file_size_bytes) {
  auto& bind_data = bind_data_p.Cast<ArrowWriteBindData>();
  return file_size_bytes.IsValid() || bind_data.row_groups_per_file.IsValid();
}

bool ArrowWriteRotateNextFile(GlobalFunctionData& gstate, FunctionData& bind_data_p,
                              const optional_idx& file_size_bytes) {
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();
  auto& bind_data = bind_data_p.Cast<ArrowWriteBindData>();
  if (file_size_bytes.IsValid() &&
      global_state.writer->FileSize() > file_size_bytes.GetIndex()) {
    return true;
  }

  if (bind_data.row_groups_per_file.IsValid() &&
      global_state.writer->NumberOfRowGroups() >=
          bind_data.row_groups_per_file.GetIndex()) {
    return true;
  }
  return false;
}

void ArrowWriteFlushBatch(ClientContext& context, FunctionData& bind_data,
                          GlobalFunctionData& gstate, PreparedBatchData& batch_p) {
  // auto &global_state = gstate.Cast<ArrowWriteGlobalState>();
  // auto &batch = batch_p.Cast<ArrowWriteBatchData>();
  // global_state.writer->FlushRowGroup(batch.prepared_row_group);
}

}  // namespace

void RegisterArrowStreamCopyFunction(DatabaseInstance& db) {
  CopyFunction function("arrows");
  // function.copy_to_select = ArrowWriteSelect;
  function.copy_to_bind = ArrowWriteBind;
  function.copy_to_initialize_global = ArrowWriteInitializeGlobal;
  function.copy_to_initialize_local = ArrowWriteInitializeLocal;
  function.copy_to_sink = ArrowWriteSink;
  function.copy_to_combine = ArrowWriteCombine;
  function.copy_to_finalize = ArrowWriteFinalize;
  function.execution_mode = ArrowWriteExecutionMode;
  function.copy_from_bind = ReadArrowStreamBindCopy;
  function.copy_from_function = ReadArrowStreamFunction();
  // function.prepare_batch = ArrowWritePrepareBatch;
  function.flush_batch = ArrowWriteFlushBatch;
  function.desired_batch_size = ArrowWriteDesiredBatchSize;
  function.rotate_files = ArrowWriteRotateFiles;
  function.rotate_next_file = ArrowWriteRotateNextFile;

  function.extension = "arrows";
  ExtensionUtil::RegisterFunction(db, function);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
