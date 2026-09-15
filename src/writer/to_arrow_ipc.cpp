#include "writer/to_arrow_ipc.hpp"

#include "ipc/codecs.hpp"
#include "writer/column_data_collection_serializer.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

namespace ext_nanoarrow {

struct ToArrowIpcFunctionData : public TableFunctionData {
  ToArrowIpcFunctionData() = default;
  ClientProperties options;
  ArrowIpcCompressionOptions compression;
  bool emit_eos = false;
  nanoarrow::UniqueSchema schema;
  vector<LogicalType> logical_types;
  const idx_t chunk_size = ToArrowIPCFunction::DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;
};

class LogicalArrowIpcStream : public LogicalExtensionOperator {
 public:
  LogicalArrowIpcStream(unique_ptr<LogicalOperator> child, Value schema_p,
                        bool emit_eos_p)
      : schema(std::move(schema_p)), emit_eos(emit_eos_p) {
    children.push_back(std::move(child));
  }

  vector<ColumnBinding> GetColumnBindings() override {
    return children[0]->GetColumnBindings();
  }

  string GetName() const override { return "ARROW_IPC_STREAM"; }

  bool SupportSerialization() const override { return false; }

  PhysicalOperator& CreatePlan(ClientContext& context,
                               PhysicalPlanGenerator& planner) override {
    ArenaLinkedList<reference<PhysicalOperator>> streams(planner.ArenaRef());
    streams.push_back(MessagePlan(planner, schema, true));
    streams.push_back(planner.CreatePlan(*children[0]));
    if (emit_eos) {
      const uint8_t eos[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};
      streams.push_back(MessagePlan(planner, Value::BLOB(eos, sizeof(eos)), false));
    }
    // Keep framing outside the batch pipelines and preserve its order
    return planner.Make<PhysicalUnion>(types, streams, estimated_cardinality, false);
  }

 protected:
  void ResolveTypes() override { types = children[0]->types; }

 private:
  PhysicalOperator& MessagePlan(PhysicalPlanGenerator& planner, Value message,
                                bool header) {
    vector<unique_ptr<Expression>> expressions;
    expressions.push_back(make_uniq<BoundConstantExpression>(std::move(message)));
    expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(header)));
    auto& projection = planner.Make<PhysicalProjection>(types, std::move(expressions), 1);
    projection.children.push_back(
        planner.Make<PhysicalDummyScan>(vector<LogicalType>{LogicalType::INTEGER}, 1));
    return projection;
  }

  Value schema;
  bool emit_eos;
};

struct ToArrowIpcLocalState : public LocalTableFunctionState {
  unique_ptr<ArrowAppender> appender;
  unique_ptr<ColumnDataCollectionSerializer> serializer;
  idx_t current_count = 0;
};

unique_ptr<LocalTableFunctionState> ToArrowIPCFunction::InitLocal(
    ExecutionContext& context, TableFunctionInitInput& input,
    GlobalTableFunctionState* global_state) {
  auto local_state = make_uniq<ToArrowIpcLocalState>();
  auto& data = input.bind_data->Cast<ToArrowIpcFunctionData>();
  local_state->serializer = make_uniq<ColumnDataCollectionSerializer>(
      data.options, BufferAllocator::Get(context.client), data.compression);
  local_state->serializer->Init(data.schema.get(), data.logical_types);
  return std::move(local_state);
}

unique_ptr<GlobalTableFunctionState> ToArrowIPCFunction::InitGlobal(
    ClientContext& context, TableFunctionInitInput& input) {
  return make_uniq<GlobalTableFunctionState>();
}

unique_ptr<FunctionData> ToArrowIPCFunction::Bind(ClientContext& context,
                                                  TableFunctionBindInput& input,
                                                  vector<LogicalType>& return_types,
                                                  vector<string>& names) {
  auto result = make_uniq<ToArrowIpcFunctionData>();
  for (auto& kv : input.named_parameters) {
    if (kv.second.IsNull()) {
      throw BinderException("Cannot use NULL as function argument");
    }
    if (StringUtil::CIEquals(kv.first, "emit_eos")) {
      result->emit_eos = BooleanValue::Get(kv.second);
    } else {
      result->compression.TrySetOption(kv.first, kv.second);
    }
  }
  result->compression.Validate();

  return_types.emplace_back(LogicalType::BLOB);
  names.emplace_back("ipc");
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("header");

  result->options = context.GetClientProperties();
  result->logical_types = input.input_table_types;
  result->schema = CreateArrowIpcSchema(input.input_table_types, input.input_table_names,
                                        result->options);
  return std::move(result);
}

unique_ptr<LogicalOperator> ToArrowIPCFunction::BindOperator(
    ClientContext& context, TableFunctionBindInput& input, idx_t bind_index,
    vector<string>& names) {
  vector<LogicalType> types;
  auto bind_data = Bind(context, input, types, names);
  auto& data = bind_data->Cast<ToArrowIpcFunctionData>();
  ColumnDataCollectionSerializer serializer(data.options, BufferAllocator::Get(context));
  serializer.Init(data.schema.get(), data.logical_types);
  serializer.SerializeSchema(data.schema.get());
  auto schema = serializer.GetHeader();
  auto schema_value = Value::BLOB(schema->data, schema->size_bytes);

  const auto& aliases = input.ref.column_name_alias;
  for (idx_t i = 0; i < MinValue<idx_t>(names.size(), aliases.size()); i++) {
    names[i] = aliases[i];
  }
  bool with_ordinality = input.ref.with_ordinality == OrdinalityType::WITH_ORDINALITY;
  auto get_index = with_ordinality ? input.binder->GenerateTableIndex() : bind_index;
  auto function = input.table_function;
  function.bind_operator = nullptr;
  auto get =
      make_uniq<LogicalGet>(get_index, function, std::move(bind_data), types, names);
  get->input_table_types = input.input_table_types;
  get->input_table_names = input.input_table_names;
  for (idx_t i = 0; i < types.size(); i++) {
    get->AddColumnId(i);
  }
  auto stream = make_uniq<LogicalArrowIpcStream>(std::move(get), std::move(schema_value),
                                                 data.emit_eos);
  if (!with_ordinality) {
    return std::move(stream);
  }

  auto window_index = input.binder->GenerateTableIndex();
  auto window = make_uniq<LogicalWindow>(window_index);
  auto row_number = make_uniq<BoundWindowExpression>(
      ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
  row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
  row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
  window->expressions.push_back(std::move(row_number));
  window->children.push_back(std::move(stream));

  vector<unique_ptr<Expression>> expressions;
  for (idx_t i = 0; i < types.size(); i++) {
    expressions.push_back(
        make_uniq<BoundColumnRefExpression>(types[i], ColumnBinding(get_index, i)));
  }
  expressions.push_back(make_uniq<BoundColumnRefExpression>(
      LogicalType::BIGINT, ColumnBinding(window_index, 0)));
  auto projection = make_uniq<LogicalProjection>(bind_index, std::move(expressions));
  projection->children.push_back(std::move(window));

  string ordinality_name = "ordinality";
  case_insensitive_set_t used_names(names.begin(), names.end());
  for (idx_t suffix = 1; used_names.count(ordinality_name); suffix++) {
    ordinality_name = "ordinality_" + to_string(suffix);
  }
  names.push_back(aliases.size() > types.size() ? aliases[types.size()]
                                                : ordinality_name);
  return std::move(projection);
}

void SerializeArray(const ToArrowIpcLocalState& local_state,
                    nanoarrow::UniqueBuffer& arrow_serialized_ipc_buffer) {
  local_state.serializer->Serialize(*local_state.appender);
  arrow_serialized_ipc_buffer = local_state.serializer->GetHeader();
  auto body = local_state.serializer->GetBody();
  NANOARROW_THROW_NOT_OK(
      ArrowBufferAppend(arrow_serialized_ipc_buffer.get(), body->data, body->size_bytes));
}

void InsertMessageToChunk(nanoarrow::UniqueBuffer& arrow_serialized_ipc_buffer,
                          DataChunk& output) {
  const auto ptr = reinterpret_cast<const char*>(arrow_serialized_ipc_buffer->data);
  const auto len = arrow_serialized_ipc_buffer->size_bytes;
  const auto wrapped_buffer =
      make_buffer<ArrowStringVectorBuffer>(std::move(arrow_serialized_ipc_buffer));
  auto& vector = output.data[0];
  StringVector::AddBuffer(vector, wrapped_buffer);
  const auto data_ptr = reinterpret_cast<string_t*>(vector.GetData());
  *data_ptr = string_t(ptr, len);
  output.SetCardinality(1);
  output.Verify();
}

OperatorResultType ToArrowIPCFunction::Function(ExecutionContext& context,
                                                TableFunctionInput& data_p,
                                                DataChunk& input, DataChunk& output) {
  nanoarrow::UniqueBuffer arrow_serialized_ipc_buffer;
  auto& data = data_p.bind_data->Cast<ToArrowIpcFunctionData>();
  auto& local_state = data_p.local_state->Cast<ToArrowIpcLocalState>();

  if (!local_state.appender) {
    local_state.appender = make_uniq<ArrowAppender>(
        input.GetTypes(), data.chunk_size, data.options,
        ArrowTypeExtensionData::GetExtensionTypes(context.client, input.GetTypes()));
  }

  local_state.appender->Append(input, 0, input.size(), input.size());
  local_state.current_count += input.size();

  bool caching_disabled =
      PhysicalOperator::SelectOperatorCachingMode(context) == OperatorCachingMode::NONE;
  if (!caching_disabled && local_state.current_count < data.chunk_size) {
    return OperatorResultType::NEED_MORE_INPUT;
  }

  SerializeArray(local_state, arrow_serialized_ipc_buffer);
  local_state.appender.reset();
  local_state.current_count = 0;
  output.data[1].SetValue(0, Value::BOOLEAN(false));
  InsertMessageToChunk(arrow_serialized_ipc_buffer, output);
  return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType ToArrowIPCFunction::FunctionFinal(ExecutionContext& context,
                                                             TableFunctionInput& data_p,
                                                             DataChunk& output) {
  auto& local_state = data_p.local_state->Cast<ToArrowIpcLocalState>();

  if (local_state.appender) {
    nanoarrow::UniqueBuffer arrow_serialized_ipc_buffer;
    SerializeArray(local_state, arrow_serialized_ipc_buffer);
    InsertMessageToChunk(arrow_serialized_ipc_buffer, output);

    output.data[1].SetValue(0, Value::BOOLEAN(false));
  }

  return OperatorFinalizeResultType::FINISHED;
}

TableFunction ToArrowIPCFunction::GetFunction() {
  TableFunction fun("to_arrow_ipc", {LogicalType::TABLE}, nullptr, Bind, InitGlobal,
                    InitLocal);
  fun.bind_operator = BindOperator;
  fun.in_out_function = Function;
  fun.in_out_function_final = FunctionFinal;
  fun.named_parameters["compression"] = LogicalType::VARCHAR;
  fun.named_parameters["compression_level"] = LogicalType::BIGINT;
  fun.named_parameters["emit_eos"] = LogicalType::BOOLEAN;
  return fun;
}

void ToArrowIPCFunction::RegisterToIPCFunction(ExtensionLoader& loader) {
  const auto function = GetFunction();
  loader.RegisterFunction(function);
}
}  // namespace ext_nanoarrow
}  // namespace duckdb
