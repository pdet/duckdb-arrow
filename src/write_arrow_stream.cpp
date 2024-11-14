#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "read_arrow_stream.hpp"

namespace duckdb {

namespace ext_nanoarrow {

namespace {

struct ArrowStreamWriter {
  ArrowStreamWriter(ClientContext& context, FileSystem& fs, const string& file_path,
                    vector<LogicalType> logical_types, vector<string> column_names,
                    vector<pair<string, string>> metadata) {}

  void Flush(ColumnDataCollection& buffer) {}

  void Finalize() {}

  idx_t NumberOfRowGroups() { return 0; }

  idx_t FileSize() { return 0; }
};

struct ArrowWriteBindData : public TableFunctionData {
  vector<LogicalType> sql_types;
  vector<string> column_names;
  vector<pair<string, string>> kv_metadata;
  idx_t row_group_size = Storage::ROW_GROUP_SIZE;
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

  return bind_data;
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
