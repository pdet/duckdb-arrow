//
// #include "duckdb/function/function.hpp"
// #include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
// #include "duckdb/function/table_function.hpp"
// #include "duckdb/main/config.hpp"
// namespace duckdb {
//
// namespace ext_nanoarrow {
// //! This function receives a list of structs [{ptr:size}] with the encoded buffers
// unique_ptr<FunctionData> ArrowScanBind(
//     ClientContext &context, TableFunctionBindInput &input,
//     vector<LogicalType> &return_types, vector<string> &names) {
//   auto stream_decoder = make_uniq<BufferingArrowIPCStreamDecoder>();
//
//   // Decode buffer ptr list
//   auto buffer_ptr_list = ListValue::GetChildren(input.inputs[0]);
//   for (auto &buffer_ptr_struct : buffer_ptr_list) {
//     auto unpacked = StructValue::GetChildren(buffer_ptr_struct);
//     uint64_t ptr = unpacked[0].GetValue<uint64_t>();
//     uint64_t size = unpacked[1].GetValue<uint64_t>();
//
//     // Feed stream into decoder
//     auto res = stream_decoder->Consume((const uint8_t *)ptr, size);
//
//     if (!res.ok()) {
//       throw IOException("Invalid IPC stream");
//     }
//   }
//
//   if (!stream_decoder->buffer()->is_eos()) {
//     throw IOException(
//         "IPC buffers passed to arrow scan should contain entire stream");
//   }
//
//   // These are the params I need to produce from the ipc buffers using the
//   // WebDB.cc code
//   auto stream_factory_ptr = (uintptr_t)&stream_decoder->buffer();
//   auto stream_factory_produce =
//       (stream_factory_produce_t)&ArrowIPCStreamBufferReader::CreateStream;
//   auto stream_factory_get_schema =
//       (stream_factory_get_schema_t)&ArrowIPCStreamBufferReader::GetSchema;
//
//   auto res = make_uniq<ArrowIPCScanFunctionData>(stream_factory_produce,
//                                                  stream_factory_ptr);
//
//   // Store decoder
//   res->stream_decoder = std::move(stream_decoder);
//
//   // TODO Everything below this is identical to the bind in
//   // duckdb/src/function/table/arrow.cpp
//   auto &data = *res;
//   stream_factory_get_schema((ArrowArrayStream *)stream_factory_ptr,
//                             data.schema_root.arrow_schema);
//   for (idx_t col_idx = 0;
//        col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++) {
//     auto &schema = *data.schema_root.arrow_schema.children[col_idx];
//     if (!schema.release) {
//       throw InvalidInputException("arrow_scan: released schema passed");
//     }
//     auto arrow_type =
//        ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);
//
//     if (schema.dictionary) {
//       auto dictionary_type = ArrowType::GetArrowLogicalType(
//           DBConfig::GetConfig(context), *schema.dictionary);
//       return_types.emplace_back(dictionary_type->GetDuckType());
//       arrow_type->SetDictionary(std::move(dictionary_type));
//     } else {
//       return_types.emplace_back(arrow_type->GetDuckType());
//     }
//     res->arrow_table.AddColumn(col_idx, std::move(arrow_type));
//     auto format = string(schema.format);
//     auto name = string(schema.name);
//     if (name.empty()) {
//       name = string("v") + to_string(col_idx);
//     }
//     names.push_back(name);
//   }
//   QueryResult::DeduplicateColumns(names);
//   return std::move(res);
// }
//
// }
// }