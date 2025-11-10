#define DUCKDB_EXTENSION_MAIN

#include "nsv_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

extern "C" {
#include "nsv.h"
}

namespace duckdb {

struct NSVBindData : public TableFunctionData {
  string filename;
  vector<string> names;
  vector<LogicalType> types;
  CNsvResult *data;

  ~NSVBindData() {
    if (data) {
      nsv_free_result(data);
    }
  }
};

struct NSVScanState : public GlobalTableFunctionState {
  idx_t row = 0;
};

unique_ptr<FunctionData> NSVBind(ClientContext &ctx,
                                 TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types,
                                 vector<string> &names) {
  auto result = make_uniq<NSVBindData>();
  result->filename = input.inputs[0].GetValue<string>();
  result->data = nsv_parse_file(result->filename.c_str());

  if (result->data->error) {
    string error_msg = result->data->error;
    throw InvalidInputException(error_msg);
  }

  if (result->data->nrows == 0) {
    throw InvalidInputException("Empty NSV file");
  }

  // Use first row as column names if available, otherwise generate names
  idx_t ncols = result->data->ncols[0];
  if (result->data->nrows > 0) {
    for (idx_t i = 0; i < ncols; i++) {
      char *cell = result->data->rows[0][i];
      if (cell && strlen(cell) > 0) {
        result->names.push_back(string(cell));
      } else {
        result->names.push_back("col" + to_string(i));
      }
      result->types.push_back(LogicalType::VARCHAR);
    }
  }

  names = result->names;
  return_types = result->types;
  return std::move(result);
}

unique_ptr<GlobalTableFunctionState> NSVInit(ClientContext &ctx,
                                             TableFunctionInitInput &input) {
  return make_uniq<NSVScanState>();
}

void NSVScan(ClientContext &ctx, TableFunctionInput &input, DataChunk &output) {
  auto &bind = input.bind_data->Cast<NSVBindData>();
  auto &state = input.global_state->Cast<NSVScanState>();

  // Skip header row (row 0 contains column names)
  if (state.row == 0) {
    state.row = 1;
  }

  idx_t count =
      std::min((idx_t)STANDARD_VECTOR_SIZE, bind.data->nrows - state.row);
  if (count == 0) {
    output.SetCardinality(0);
    return;
  }

  for (idx_t col = 0; col < output.ColumnCount(); col++) {
    auto &vec = output.data[col];
    for (idx_t i = 0; i < count; i++) {
      idx_t row_idx = state.row + i;
      if (col < bind.data->ncols[row_idx]) {
        char *cell = bind.data->rows[row_idx][col];
        vec.SetValue(i, cell ? Value(string(cell)) : Value());
      } else {
        vec.SetValue(i, Value());
      }
    }
  }

  output.SetCardinality(count);
  state.row += count;
}

static void LoadInternal(ExtensionLoader &loader) {
  TableFunction read_nsv_func("read_nsv", {LogicalType::VARCHAR}, NSVScan,
                              NSVBind);
  read_nsv_func.init_global = NSVInit;
  loader.RegisterFunction(read_nsv_func);
}

void NsvExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

std::string NsvExtension::Name() { return "nsv"; }

std::string NsvExtension::Version() const {
#ifdef EXT_VERSION_NSV
  return EXT_VERSION_NSV;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(nsv, loader) { duckdb::LoadInternal(loader); }
}
