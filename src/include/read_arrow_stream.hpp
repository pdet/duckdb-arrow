
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace ext_nanoarrow {

// Needed to define the copy function
unique_ptr<FunctionData> ReadArrowStreamBindCopy(ClientContext& context, CopyInfo& info,
                                                 vector<string>& expected_names,
                                                 vector<LogicalType>& expected_types);

TableFunction ReadArrowStreamFunction();

void RegisterReadArrowStream(DatabaseInstance& db);

}  // namespace ext_nanoarrow
}  // namespace duckdb
