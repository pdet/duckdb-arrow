#include "writer/to_arrow_ipc.hpp"

#include "duckdb/main/extension_util.hpp"

#include "writer/column_data_collection_serializer.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace ext_nanoarrow {

struct ToArrowIpcFunctionData : public TableFunctionData {
  ToArrowIpcFunctionData() {}
  ArrowSchema schema;
  vector<LogicalType> logical_types;
  idx_t chunk_size;
};

struct ToArrowIpcGlobalState : public GlobalTableFunctionState {
  ToArrowIpcGlobalState() : sent_schema(false) {}
  atomic<bool> sent_schema;
  mutex lock;
};

struct ToArrowIpcLocalState : public LocalTableFunctionState {
  // unique_ptr<ArrowAppender> appender;
  unique_ptr<ColumnDataCollectionSerializer> serializer;
  idx_t current_count = 0;
  bool checked_schema = false;
};

unique_ptr<LocalTableFunctionState>
ToArrowIPCFunction::InitLocal(ExecutionContext &context,
                              TableFunctionInitInput &input,
                              GlobalTableFunctionState *global_state) {
  auto local_state = make_uniq<ToArrowIpcLocalState>();
  auto properties = context.client.GetClientProperties();
  local_state->serializer = make_uniq<ColumnDataCollectionSerializer>(properties, BufferAllocator::Get(context.client));
  return local_state;
}

unique_ptr<GlobalTableFunctionState>
ToArrowIPCFunction::InitGlobal(ClientContext &context,
                               TableFunctionInitInput &input) {
  return make_uniq<ToArrowIpcGlobalState>();
}

unique_ptr<FunctionData>
ToArrowIPCFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                         vector<LogicalType> &return_types,
                         vector<string> &names) {
  auto result = make_uniq<ToArrowIpcFunctionData>();

  result->chunk_size = DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;

  // Set return schema
  return_types.emplace_back(LogicalType::BLOB);
  names.emplace_back("ipc");
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("header");

  // Create the Arrow schema
  auto properties = context.GetClientProperties();
  result->logical_types = input.input_table_types;
  ArrowConverter::ToArrowSchema(&result->schema, input.input_table_types,
                                input.input_table_names, properties);
  return std::move(result);
}

OperatorResultType ToArrowIPCFunction::Function(ExecutionContext &context,
                                                TableFunctionInput &data_p,
                                                DataChunk &input,
                                                DataChunk &output) {
  nanoarrow::UniqueBuffer arrow_serialized_ipc_buffer;
  auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;
  auto &local_state = (ToArrowIpcLocalState &)*data_p.local_state;
  auto &global_state = (ToArrowIpcGlobalState &)*data_p.global_state;

  bool sending_schema = false;

  bool caching_disabled = !PhysicalOperator::OperatorCachingAllowed(context);

  if (!local_state.checked_schema) {
    if (!global_state.sent_schema) {
      lock_guard<mutex> init_lock(global_state.lock);
      if (!global_state.sent_schema) {
        // This run will send the schema, other threads can just send the
        // buffers
        global_state.sent_schema = true;
        sending_schema = true;
      }
    }
    local_state.checked_schema = true;
  }

  if (sending_schema) {
    local_state.serializer->Init(&data.schema, data.logical_types);
    local_state.serializer->SerializeSchema();
    arrow_serialized_ipc_buffer = local_state.serializer->GetHeader();
    output.data[1].SetValue(0, Value::BOOLEAN(true));
  } else {
    // if (!local_state.appender) {
    //   local_state.appender = make_uniq<ArrowAppender>(input.GetTypes(), data.chunk_size,
    //                                context.client.GetClientProperties(),
    //                                ArrowTypeExtensionData::GetExtensionTypes(
    //                                    context.client, input.GetTypes()));
    // }

    // Append input chunk
    // local_state.appender->Append(input, 0, input.size(), input.size());
    local_state.current_count += input.size();

    // If chunk size is reached, we can flush to IPC blob
    if (caching_disabled || local_state.current_count >= data.chunk_size) {
      // Construct record batch from DataChunk
      // ArrowArray arr = local_state.appender->Finalize();
      // input.Print();
      local_state.serializer->Serialize(input);
      arrow_serialized_ipc_buffer = local_state.serializer->GetBody();

      // Reset appender
      // local_state.appender.reset();
      local_state.current_count = 0;

      output.data[1].SetValue(0, Value::BOOLEAN(false));
    } else {
      return OperatorResultType::NEED_MORE_INPUT;
    }
  }

  // TODO clean up
  auto ptr = reinterpret_cast<const char *>(arrow_serialized_ipc_buffer->data);
  auto len = arrow_serialized_ipc_buffer->size_bytes;
  auto wrapped_buffer =
      make_buffer<ArrowStringVectorBuffer>(std::move(arrow_serialized_ipc_buffer));
  auto &vector = output.data[0];
  StringVector::AddBuffer(vector, wrapped_buffer);
  auto data_ptr = (string_t *)vector.GetData();
  *data_ptr = string_t(ptr,len);
  output.SetCardinality(1);

  if (sending_schema) {
    return OperatorResultType::HAVE_MORE_OUTPUT;
  } else {
    return OperatorResultType::NEED_MORE_INPUT;
  }
}

OperatorFinalizeResultType ToArrowIPCFunction::FunctionFinal(
    ExecutionContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;
  auto &local_state = (ToArrowIpcLocalState &)*data_p.local_state;
  // std::shared_ptr<arrow::Buffer> arrow_serialized_ipc_buffer;

  // // TODO clean up
  // if (local_state.appender) {
  //   ArrowArray arr = local_state.appender->Finalize();
  //   auto record_batch =
  //       arrow::ImportRecordBatch(&arr, data.schema).ValueOrDie();
  //
  //   // Serialize recordbatch
  //   auto options = arrow::ipc::IpcWriteOptions::Defaults();
  //   auto result = arrow::ipc::SerializeRecordBatch(*record_batch, options);
  //   arrow_serialized_ipc_buffer = result.ValueOrDie();
  //
  //   auto wrapped_buffer =
  //       make_buffer<ArrowStringVectorBuffer>(arrow_serialized_ipc_buffer);
  //   auto &vector = output.data[0];
  //   StringVector::AddBuffer(vector, wrapped_buffer);
  //   auto data_ptr = (string_t *)vector.GetData();
  //   *data_ptr = string_t((const char *)arrow_serialized_ipc_buffer->data(),
  //                        arrow_serialized_ipc_buffer->size());
  //   output.SetCardinality(1);
  //   local_state.appender.reset();
  //   output.data[1].SetValue(0, Value::BOOLEAN(0));
  // }

  return OperatorFinalizeResultType::FINISHED;
}

TableFunction ToArrowIPCFunction::GetFunction() {
  TableFunction fun("to_arrow_ipc", {LogicalType::TABLE}, nullptr,
                    Bind, InitGlobal,
                    InitLocal);
  fun.in_out_function = Function;
  fun.in_out_function_final = FunctionFinal;
  return fun;
}

void ToArrowIPCFunction::RegisterToIPCFunction(DatabaseInstance& db) {
  const auto function = GetFunction();
  ExtensionUtil::RegisterFunction(db, function);
}
}  // namespace ext_nanoarrow
}  // namespace duckdb