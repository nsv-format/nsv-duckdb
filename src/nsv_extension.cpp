#define DUCKDB_EXTENSION_MAIN

#include "nsv_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

extern "C" {
#include "nsv.h"
}

namespace duckdb {

// Type candidates in order of priority (most specific to least specific)
// Based on DuckDB CSV sniffer approach
static const vector<LogicalType> TYPE_CANDIDATES = {
    LogicalType::BOOLEAN, LogicalType::BIGINT,    LogicalType::DOUBLE,
    LogicalType::DATE,    LogicalType::TIMESTAMP,
    LogicalType::VARCHAR // Fallback - always succeeds
};

struct NSVBindData : public TableFunctionData {
  string filename;
  vector<string> names;
  vector<LogicalType> types;
  CNsvResult *data;
  bool all_varchar;

  ~NSVBindData() {
    if (data) {
      nsv_free_result(data);
    }
  }
};

struct NSVScanState : public GlobalTableFunctionState {
  idx_t row = 0;
};

// Try to detect the best type for a column by sampling values
static LogicalType DetectColumnType(ClientContext &ctx, CNsvResult *data,
                                    idx_t col_idx, idx_t sample_size) {
  // Sample rows starting from row 1 (skip header)
  idx_t start_row = 1;
  idx_t end_row = std::min(data->nrows, start_row + sample_size);

  for (const auto &candidate_type : TYPE_CANDIDATES) {
    if (candidate_type == LogicalType::VARCHAR) {
      // VARCHAR always succeeds
      return LogicalType::VARCHAR;
    }

    bool all_cast_ok = true;
    bool has_non_null = false;

    for (idx_t row = start_row; row < end_row && all_cast_ok; row++) {
      // Check if column exists in this row
      if (col_idx >= data->ncols[row]) {
        continue; // Treat as NULL, which casts to anything
      }

      char *cell = data->rows[row][col_idx];
      if (!cell || strlen(cell) == 0) {
        continue; // NULL/empty casts to anything
      }

      has_non_null = true;

      // Try casting the string value to the candidate type
      string cell_str(cell);
      Value str_val(cell_str);
      Value result_val;
      string error_msg;

      if (!str_val.TryCastAs(ctx, candidate_type, result_val, &error_msg,
                             true)) { // strict=true to prevent truncation
        all_cast_ok = false;
      }
    }

    // Only accept this type if we successfully cast all non-null values
    // and we had at least one non-null value to test
    if (all_cast_ok && has_non_null) {
      return candidate_type;
    }
  }

  return LogicalType::VARCHAR;
}

unique_ptr<FunctionData> NSVBind(ClientContext &ctx,
                                 TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types,
                                 vector<string> &names) {
  auto result = make_uniq<NSVBindData>();
  result->filename = input.inputs[0].GetValue<string>();

  // Check for all_varchar option
  result->all_varchar = false;
  auto all_varchar_entry = input.named_parameters.find("all_varchar");
  if (all_varchar_entry != input.named_parameters.end()) {
    result->all_varchar = all_varchar_entry->second.GetValue<bool>();
  }

  // Parse the NSV file
  result->data = nsv_parse_file(result->filename.c_str());

  if (result->data->error) {
    string error_msg = result->data->error;
    throw InvalidInputException(error_msg);
  }

  if (result->data->nrows == 0) {
    throw InvalidInputException("Empty NSV file");
  }

  // Use first row as column names
  idx_t ncols = result->data->ncols[0];
  for (idx_t i = 0; i < ncols; i++) {
    char *cell = result->data->rows[0][i];
    if (cell && strlen(cell) > 0) {
      result->names.push_back(string(cell));
    } else {
      result->names.push_back("col" + to_string(i));
    }

    // Detect type for this column
    if (result->all_varchar) {
      result->types.push_back(LogicalType::VARCHAR);
    } else {
      // Sample up to 1000 rows for type detection
      LogicalType detected = DetectColumnType(ctx, result->data, i, 1000);
      result->types.push_back(detected);
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
    const auto &target_type = bind.types[col];

    for (idx_t i = 0; i < count; i++) {
      idx_t row_idx = state.row + i;

      // Handle missing columns in ragged rows
      if (col >= bind.data->ncols[row_idx]) {
        vec.SetValue(i, Value());
        continue;
      }

      char *cell = bind.data->rows[row_idx][col];
      if (!cell || strlen(cell) == 0) {
        vec.SetValue(i, Value());
        continue;
      }

      // Create string value and cast to target type
      string cell_str(cell);
      Value str_val(cell_str);

      if (target_type == LogicalType::VARCHAR) {
        vec.SetValue(i, str_val);
      } else {
        Value result_val;
        string error_msg;
        if (str_val.TryCastAs(ctx, target_type, result_val, &error_msg,
                              false)) {
          vec.SetValue(i, result_val);
        } else {
          // Cast failed - this shouldn't happen if detection worked correctly,
          // but fall back to NULL for safety
          vec.SetValue(i, Value());
        }
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

  // Add named parameters
  read_nsv_func.named_parameters["all_varchar"] = LogicalType::BOOLEAN;

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
