#include "file_scanner/arrow_file_scan.hpp"

namespace duckdb {
namespace ext_nanoarrow {

ArrowFileScan::ArrowFileScan(ClientContext& context, const string& file_name)
    : BaseFileReader(file_name), context(context) {
  factory = make_uniq<FileIPCStreamFactory>(context, file_name);
  ArrowSchemaWrapper schema_root;
  factory->InitReader();
  factory->GetFileSchema(schema_root);
  ArrowTableType arrow_table_type;
  DBConfig& config = DatabaseInstance::GetDatabase(context).config;
  ArrowTableFunction::PopulateArrowTableType(config, arrow_table_type, schema_root, names,
                                             types);
  QueryResult::DeduplicateColumns(names);
  if (types.empty()) {
    throw InvalidInputException("Provided table/dataframe must have at least one column");
  }
}

string ArrowFileScan::GetReaderType() const { return "ARROW"; }

const vector<string>& ArrowFileScan::GetNames() { return names; }
const vector<LogicalType>& ArrowFileScan::GetTypes() { return types; }

}  // namespace ext_nanoarrow
}  // namespace duckdb
