#define DUCKDB_EXTENSION_MAIN

#include "nsv_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

#include "nsv_ffi.h"

namespace duckdb {

// ── Type detection ──────────────────────────────────────────────────

static const vector<LogicalType> TYPE_CANDIDATES = {
    LogicalType::BOOLEAN, LogicalType::BIGINT,    LogicalType::DOUBLE,
    LogicalType::DATE,    LogicalType::TIMESTAMP,
    LogicalType::VARCHAR // fallback — always succeeds
};

static LogicalType DetectColumnType(ClientContext &ctx, NsvHandle *data,
                                    idx_t col_idx, idx_t start_row,
                                    idx_t sample_size) {
  idx_t nrows = nsv_row_count(data);
  idx_t end_row = MinValue<idx_t>(nrows, start_row + sample_size);

  for (const auto &candidate : TYPE_CANDIDATES) {
    if (candidate == LogicalType::VARCHAR) {
      return LogicalType::VARCHAR;
    }

    bool all_ok = true;
    bool has_value = false;

    for (idx_t row = start_row; row < end_row && all_ok; row++) {
      size_t cell_len = 0;
      const char *cell = nsv_cell(data, row, col_idx, &cell_len);
      if (!cell || cell_len == 0) {
        continue; // NULL / empty → casts to anything
      }
      has_value = true;

      Value str_val(string(cell, cell_len));
      Value result_val;
      string error_msg;
      if (!str_val.TryCastAs(ctx, candidate, result_val, &error_msg, true)) {
        all_ok = false;
      }
    }

    if (all_ok && has_value) {
      return candidate;
    }
  }

  return LogicalType::VARCHAR;
}

// ── read_nsv ────────────────────────────────────────────────────────

struct NSVBindData : public TableFunctionData {
  string filename;
  vector<string> names;
  vector<LogicalType> types;
  NsvHandle *handle = nullptr;
  //! Raw file bytes kept for nsv_decode_projected at scan init.
  string raw_buffer;
  bool all_varchar = false;

  ~NSVBindData() {
    if (handle) {
      nsv_free(handle);
    }
  }
};

struct NSVScanState : public GlobalTableFunctionState {
  idx_t current_row = 0;
  //! Maps scanned column index → source column index (from projection
  //! pushdown). When filter pushdown adds extra columns, this includes both
  //! output and filter-only columns.
  vector<column_t> column_ids;
  //! Maps output column position → index into column_ids.
  //! Empty when there are no filter-only columns (i.e. all scanned columns
  //! appear in the output).
  vector<idx_t> projection_ids;
  //! Projected handle — pre-decoded, only requested columns.
  ProjectedNsvHandle *projected = nullptr;
  //! Pushed-down filters. Keys are indices into column_ids (not source column
  //! indices) — DuckDB remaps them in CreateTableFilterSet.
  unique_ptr<TableFilterSet> filters;

  ~NSVScanState() {
    if (projected) {
      nsv_projected_free(projected);
    }
  }
};

static unique_ptr<FunctionData> NSVBind(ClientContext &ctx,
                                        TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types,
                                        vector<string> &names) {
  auto result = make_uniq<NSVBindData>();
  result->filename = input.inputs[0].GetValue<string>();

  // Named parameters
  auto it = input.named_parameters.find("all_varchar");
  if (it != input.named_parameters.end()) {
    result->all_varchar = it->second.GetValue<bool>();
  }

  // Read file via DuckDB's filesystem (supports local, HTTP, S3, etc.)
  auto &fs = FileSystem::GetFileSystem(ctx);
  auto file_handle = fs.OpenFile(result->filename, FileFlags::FILE_FLAGS_READ);
  auto file_size = fs.GetFileSize(*file_handle);

  result->raw_buffer.resize(file_size);
  fs.Read(*file_handle, (void *)result->raw_buffer.data(), file_size);

  // Eager decode via Rust FFI — headers + type sniffing
  result->handle =
      nsv_decode(reinterpret_cast<const uint8_t *>(result->raw_buffer.data()),
                 result->raw_buffer.size());
  if (!result->handle) {
    throw InvalidInputException("Failed to parse NSV file: %s",
                                result->filename);
  }

  idx_t nrows = nsv_row_count(result->handle);
  if (nrows == 0) {
    throw InvalidInputException("Empty NSV file: %s", result->filename);
  }

  // Row 0 = column headers
  idx_t ncols = nsv_col_count(result->handle, 0);
  for (idx_t i = 0; i < ncols; i++) {
    size_t cell_len = 0;
    const char *cell = nsv_cell(result->handle, 0, i, &cell_len);
    if (cell && cell_len > 0) {
      result->names.emplace_back(cell, cell_len);
    } else {
      result->names.push_back("col" + to_string(i));
    }

    if (result->all_varchar) {
      result->types.push_back(LogicalType::VARCHAR);
    } else {
      // Sample up to 1000 data rows (starting at row 1)
      auto detected = DetectColumnType(ctx, result->handle, i, 1, 1000);
      result->types.push_back(detected);
    }
  }

  names = result->names;
  return_types = result->types;
  return std::move(result);
}

static unique_ptr<GlobalTableFunctionState>
NSVInit(ClientContext &ctx, TableFunctionInitInput &input) {
  auto state = make_uniq<NSVScanState>();
  state->column_ids = input.column_ids;
  state->projection_ids = input.projection_ids;

  // Capture pushed-down filters (if any).
  if (input.filters) {
    state->filters = input.filters->Copy();
  }

  // Only use projected decode when a strict subset of columns is requested.
  // SELECT * populates column_ids with all columns, so re-decoding would
  // just duplicate the eager handle from bind — skip it.
  auto &bind = input.bind_data->Cast<NSVBindData>();
  idx_t ncols = bind.names.size();

  if (!bind.raw_buffer.empty() && !state->column_ids.empty() &&
      state->column_ids.size() < ncols) {
    vector<size_t> col_indices;
    col_indices.reserve(state->column_ids.size());
    for (auto &cid : state->column_ids) {
      col_indices.push_back(static_cast<size_t>(cid));
    }
    state->projected = nsv_decode_projected(
        reinterpret_cast<const uint8_t *>(bind.raw_buffer.data()),
        bind.raw_buffer.size(), col_indices.data(), col_indices.size());
  }

  return std::move(state);
}

// ── Filter evaluation ────────────────────────────────────────────────

//! Evaluate a single TableFilter against a (possibly NULL) Value.
//! Returns true if the row passes (should be emitted).
static bool EvaluateFilter(const TableFilter &filter, const Value &val) {
  switch (filter.filter_type) {
  case TableFilterType::IS_NULL:
    return val.IsNull();
  case TableFilterType::IS_NOT_NULL:
    return !val.IsNull();
  case TableFilterType::CONSTANT_COMPARISON: {
    if (val.IsNull()) {
      return false;
    }
    auto &const_filter = filter.Cast<ConstantFilter>();
    return const_filter.Compare(val);
  }
  case TableFilterType::IN_FILTER: {
    if (val.IsNull()) {
      return false;
    }
    auto &in_filter = filter.Cast<InFilter>();
    for (auto &candidate : in_filter.values) {
      if (Value::NotDistinctFrom(val, candidate)) {
        return true;
      }
    }
    return false;
  }
  case TableFilterType::CONJUNCTION_AND: {
    auto &conj = filter.Cast<ConjunctionAndFilter>();
    for (auto &child : conj.child_filters) {
      if (!EvaluateFilter(*child, val)) {
        return false;
      }
    }
    return true;
  }
  case TableFilterType::CONJUNCTION_OR: {
    auto &conj = filter.Cast<ConjunctionOrFilter>();
    for (auto &child : conj.child_filters) {
      if (EvaluateFilter(*child, val)) {
        return true;
      }
    }
    return false;
  }
  default:
    // Unsupported filter type — conservatively pass the row through.
    return true;
  }
}

// ── Scan ─────────────────────────────────────────────────────────────

//! Read a cell from either the projected or full handle, cast it to the target
//! type, and return the resulting Value (NULL on empty/missing/cast failure).
static Value ReadAndCastCell(ClientContext &ctx, const NSVBindData &bind,
                             const NSVScanState &state, idx_t row_idx,
                             idx_t scan_col) {
  bool use_projected = (state.projected != nullptr);
  idx_t src_col = state.column_ids[scan_col];
  const auto &target_type = bind.types[src_col];

  size_t cell_len = 0;
  const char *cell;

  if (use_projected) {
    cell = nsv_projected_cell(state.projected, row_idx, scan_col, &cell_len);
  } else {
    if (src_col >= nsv_col_count(bind.handle, row_idx)) {
      return Value();
    }
    cell = nsv_cell(bind.handle, row_idx, src_col, &cell_len);
  }

  if (!cell || cell_len == 0) {
    return Value();
  }

  Value str_val(string(cell, cell_len));
  if (target_type == LogicalType::VARCHAR) {
    return str_val;
  }

  Value result_val;
  string error_msg;
  if (str_val.TryCastAs(ctx, target_type, result_val, &error_msg, false)) {
    return result_val;
  }
  return Value();
}

static void NSVScan(ClientContext &ctx, TableFunctionInput &input,
                    DataChunk &output) {
  auto &bind = input.bind_data->Cast<NSVBindData>();
  auto &state = input.global_state->Cast<NSVScanState>();

  // Skip header row
  if (state.current_row == 0) {
    state.current_row = 1;
  }

  bool use_projected = (state.projected != nullptr);
  idx_t total_rows = use_projected ? nsv_projected_row_count(state.projected)
                                   : nsv_row_count(bind.handle);

  bool has_filters = state.filters && !state.filters->filters.empty();

  // Determine how many of the scanned columns go to output.
  // When projection_ids is non-empty, only those columns are output;
  // the rest are filter-only columns that we scan but don't emit.
  idx_t output_col_count = output.ColumnCount();

  idx_t emitted = 0;
  while (emitted < STANDARD_VECTOR_SIZE && state.current_row < total_rows) {
    idx_t row_idx = state.current_row++;

    // Fast path: no filters — read and emit directly.
    if (!has_filters) {
      for (idx_t out_col = 0; out_col < output_col_count; out_col++) {
        // Map output column to scanned column index.
        idx_t scan_col = state.projection_ids.empty()
                             ? out_col
                             : state.projection_ids[out_col];
        output.data[out_col].SetValue(
            emitted, ReadAndCastCell(ctx, bind, state, row_idx, scan_col));
      }
      emitted++;
      continue;
    }

    // Evaluate filters. Filter keys are indices into column_ids (not source
    // column indices) — this is how DuckDB's CreateTableFilterSet remaps them.
    bool passes = true;
    for (auto &filter_entry : state.filters->filters) {
      idx_t scan_col = filter_entry.first;

      Value val = ReadAndCastCell(ctx, bind, state, row_idx, scan_col);
      if (!EvaluateFilter(*filter_entry.second, val)) {
        passes = false;
        break;
      }
    }

    if (!passes) {
      continue;
    }

    // Row passes filters — emit to output.
    for (idx_t out_col = 0; out_col < output_col_count; out_col++) {
      idx_t scan_col = state.projection_ids.empty()
                           ? out_col
                           : state.projection_ids[out_col];
      output.data[out_col].SetValue(
          emitted, ReadAndCastCell(ctx, bind, state, row_idx, scan_col));
    }
    emitted++;
  }

  output.SetCardinality(emitted);
}

// ── write_nsv (COPY TO) ────────────────────────────────────────────

struct NSVWriteBindData : public TableFunctionData {
  vector<string> names;
  vector<LogicalType> types;
  bool write_header = true;
};

struct NSVWriteGlobalState : public GlobalFunctionData {
  string filename;
  unique_ptr<FileHandle> file_handle;
  NsvEncoder *encoder = nullptr;
  bool header_written = false;

  ~NSVWriteGlobalState() {
    // Encoder should have been finished in Finalize, but safety net.
    if (encoder) {
      uint8_t *buf = nullptr;
      size_t len = 0;
      nsv_encoder_finish(encoder, &buf, &len);
      if (buf) {
        nsv_free_buf(buf, len);
      }
      encoder = nullptr;
    }
  }
};

struct NSVWriteLocalState : public LocalFunctionData {};

static unique_ptr<FunctionData> NSVWriteBind(ClientContext &,
                                             CopyFunctionBindInput &input,
                                             const vector<string> &names,
                                             const vector<LogicalType> &types) {
  auto result = make_uniq<NSVWriteBindData>();
  result->names = names;
  result->types = types;

  auto it = input.info.options.find("header");
  if (it != input.info.options.end()) {
    result->write_header = it->second[0].GetValue<bool>();
  }

  return std::move(result);
}

static unique_ptr<GlobalFunctionData>
NSVWriteInitGlobal(ClientContext &ctx, FunctionData &bind_data,
                   const string &filename) {
  auto result = make_uniq<NSVWriteGlobalState>();
  result->filename = filename;
  auto &fs = FileSystem::GetFileSystem(ctx);
  result->file_handle =
      fs.OpenFile(filename, FileFlags::FILE_FLAGS_WRITE |
                                FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  result->encoder = nsv_encoder_new();
  return std::move(result);
}

static unique_ptr<LocalFunctionData> NSVWriteInitLocal(ExecutionContext &,
                                                       FunctionData &) {
  return make_uniq<NSVWriteLocalState>();
}

static void NSVWriteSink(ExecutionContext &, FunctionData &bind_data,
                         GlobalFunctionData &gstate, LocalFunctionData &,
                         DataChunk &input) {
  auto &bind = bind_data.Cast<NSVWriteBindData>();
  auto &state = gstate.Cast<NSVWriteGlobalState>();

  // Write header row on first call
  if (!state.header_written && bind.write_header) {
    for (auto &name : bind.names) {
      nsv_encoder_push_cell(state.encoder,
                            reinterpret_cast<const uint8_t *>(name.data()),
                            name.size());
    }
    nsv_encoder_end_row(state.encoder);
    state.header_written = true;
  }

  idx_t count = input.size();
  for (idx_t row = 0; row < count; row++) {
    for (idx_t col = 0; col < input.ColumnCount(); col++) {
      auto val = input.GetValue(col, row);
      if (val.IsNull()) {
        nsv_encoder_push_null(state.encoder);
      } else {
        auto str = val.ToString();
        nsv_encoder_push_cell(state.encoder,
                              reinterpret_cast<const uint8_t *>(str.data()),
                              str.size());
      }
    }
    nsv_encoder_end_row(state.encoder);
  }
}

static void NSVWriteCombine(ExecutionContext &, FunctionData &,
                            GlobalFunctionData &, LocalFunctionData &) {
  // single-threaded write, nothing to combine
}

static void NSVWriteFinalize(ClientContext &ctx, FunctionData &,
                             GlobalFunctionData &gstate) {
  auto &state = gstate.Cast<NSVWriteGlobalState>();
  if (!state.encoder) {
    return;
  }

  uint8_t *buf = nullptr;
  size_t len = 0;
  nsv_encoder_finish(state.encoder, &buf, &len);
  state.encoder = nullptr;

  if (buf && len > 0) {
    auto &fs = FileSystem::GetFileSystem(ctx);
    fs.Write(*state.file_handle, (void *)buf, len);
    nsv_free_buf(buf, len);
  }
}

// ── Extension registration ──────────────────────────────────────────

static void LoadInternal(ExtensionLoader &loader) {
  // read_nsv table function with projection + filter pushdown
  TableFunction read_nsv("read_nsv", {LogicalType::VARCHAR}, NSVScan, NSVBind);
  read_nsv.init_global = NSVInit;
  read_nsv.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
  read_nsv.projection_pushdown = true;
  read_nsv.filter_pushdown = true;
  loader.RegisterFunction(read_nsv);

  // COPY TO ... (FORMAT nsv)
  CopyFunction nsv_copy("nsv");
  nsv_copy.copy_to_bind = NSVWriteBind;
  nsv_copy.copy_to_initialize_global = NSVWriteInitGlobal;
  nsv_copy.copy_to_initialize_local = NSVWriteInitLocal;
  nsv_copy.copy_to_sink = NSVWriteSink;
  nsv_copy.copy_to_combine = NSVWriteCombine;
  nsv_copy.copy_to_finalize = NSVWriteFinalize;
  nsv_copy.extension = "nsv";
  loader.RegisterFunction(nsv_copy);
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
