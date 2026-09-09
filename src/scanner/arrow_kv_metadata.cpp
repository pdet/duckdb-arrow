#include "table_function/arrow_kv_metadata.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/table_function.hpp"
#include "ipc/stream_factory.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

struct ArrowKvMetadataRow {
  Value field_path;
  string key;
  string value;
};

struct ArrowKvMetadataBindData : public TableFunctionData {
  shared_ptr<MultiFileList> files;
};

struct ArrowKvMetadataGlobalState : public GlobalTableFunctionState {
  explicit ArrowKvMetadataGlobalState(idx_t file_count) : file_count(file_count) {}
  idx_t MaxThreads() const override { return file_count; }

  mutex lock;
  MultiFileListScanData scan;
  idx_t file_count;
};

struct ArrowKvMetadataLocalState : public LocalTableFunctionState {
  string file_name;
  vector<ArrowKvMetadataRow> rows;
  idx_t next_row = 0;
};

// Collects the metadata of a schema node and of its children, addressed by field path
void CollectMetadata(const ArrowSchema* schema, vector<Value>& path,
                     vector<ArrowKvMetadataRow>& rows) {
  auto field_path = path.empty() ? Value(LogicalType::LIST(LogicalType::VARCHAR))
                                 : Value::LIST(LogicalType::VARCHAR, path);
  ArrowMetadataReader reader;
  NANOARROW_THROW_NOT_OK(ArrowMetadataReaderInit(&reader, schema->metadata));
  while (reader.remaining_keys > 0) {
    ArrowStringView key;
    ArrowStringView value;
    NANOARROW_THROW_NOT_OK(ArrowMetadataReaderRead(&reader, &key, &value));
    rows.push_back({field_path, string(key.data, key.size_bytes),
                    string(value.data, value.size_bytes)});
  }
  for (int64_t i = 0; i < schema->n_children; i++) {
    auto child = schema->children[i];
    string name = child->name ? child->name : "";
    Utf8Proc::MakeValid(&name[0], name.size());
    path.emplace_back(name);
    CollectMetadata(child, path, rows);
    path.pop_back();
  }
}

unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                              vector<LogicalType>& return_types, vector<string>& names) {
  auto result = make_uniq<ArrowKvMetadataBindData>();
  auto multi_file_reader = MultiFileReader::CreateDefault("arrow_kv_metadata");
  result->files = multi_file_reader->CreateFileList(context, input.inputs[0]);
  names = {"file_name", "field_path", "key", "value"};
  return_types = {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR),
                  LogicalType::BLOB, LogicalType::BLOB};
  return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context,
                                                TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<ArrowKvMetadataBindData>();
  auto result =
      make_uniq<ArrowKvMetadataGlobalState>(bind_data.files->GetTotalFileCount());
  bind_data.files->InitializeScan(result->scan);
  return std::move(result);
}

unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context,
                                              TableFunctionInitInput& input,
                                              GlobalTableFunctionState* global_state) {
  return make_uniq<ArrowKvMetadataLocalState>();
}

void Function(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
  auto& bind_data = input.bind_data->Cast<ArrowKvMetadataBindData>();
  auto& global_state = input.global_state->Cast<ArrowKvMetadataGlobalState>();
  auto& state = input.local_state->Cast<ArrowKvMetadataLocalState>();
  while (state.next_row == state.rows.size()) {
    OpenFileInfo file;
    {
      lock_guard<mutex> guard(global_state.lock);
      if (!bind_data.files->Scan(global_state.scan, file)) {
        return;
      }
    }
    state.file_name = file.path;
    state.rows.clear();
    state.next_row = 0;
    FileIPCStreamFactory factory(context, file.path);
    factory.InitReader();
    vector<Value> path;
    CollectMetadata(factory.reader->GetBaseSchema(), path, state.rows);
  }
  idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.next_row);
  output.data[0].Reference(Value(state.file_name));
  auto keys = FlatVector::GetData<string_t>(output.data[2]);
  auto values = FlatVector::GetData<string_t>(output.data[3]);
  for (idx_t i = 0; i < count; i++) {
    auto& row = state.rows[state.next_row + i];
    output.SetValue(1, i, row.field_path);
    keys[i] = StringVector::AddStringOrBlob(output.data[2], row.key);
    values[i] = StringVector::AddStringOrBlob(output.data[3], row.value);
  }
  state.next_row += count;
  output.SetCardinality(count);
}

}  // namespace

void RegisterArrowKvMetadata(ExtensionLoader& loader) {
  TableFunction function("arrow_kv_metadata", {LogicalType::VARCHAR}, Function, Bind,
                         InitGlobal, InitLocal);
  loader.RegisterFunction(MultiFileReader::CreateFunctionSet(function));
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
