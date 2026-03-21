#define DUCKDB_EXTENSION_MAIN

#include "nsv_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "duckdb/common/operator/cast_operators.hpp"

#include "nsv_ffi.h"

#include <atomic>
#include <cstring>

namespace duckdb {

// ── Type detection ──────────────────────────────────────────────────

static const vector<LogicalType> TYPE_CANDIDATES = {
    LogicalType::BOOLEAN, LogicalType::BIGINT,    LogicalType::DOUBLE,
    LogicalType::DATE,    LogicalType::TIMESTAMP,
    LogicalType::VARCHAR // fallback — always succeeds
};

static LogicalType DetectColumnType(ClientContext &ctx, SampleHandle *data,
                                    idx_t col_idx, idx_t start_row,
                                    idx_t sample_size) {
  idx_t nrows = nsv_sample_row_count(data);
  idx_t end_row = MinValue<idx_t>(nrows, start_row + sample_size);

  for (const auto &candidate : TYPE_CANDIDATES) {
    if (candidate == LogicalType::VARCHAR) {
      return LogicalType::VARCHAR;
    }

    bool all_ok = true;
    bool has_value = false;

    for (idx_t row = start_row; row < end_row && all_ok; row++) {
      size_t cell_len = 0;
      const char *cell = nsv_sample_cell(data, row, col_idx, &cell_len);
      if (!cell || cell_len == 0) {
        continue;
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
  //! Sample handle for header/type sniffing (first 1002 rows).
  SampleHandle *sample = nullptr;
  //! Raw file bytes — kept alive for inline parsing during scan.
  string raw_buffer;
  //! Row offsets built in C++ during bind. offsets[i] = start of row i.
  //! offsets[num_rows] = sentinel (end of input).
  vector<size_t> row_offsets;
  bool all_varchar = false;

  ~NSVBindData() {
    if (sample) {
      nsv_sample_free(sample);
    }
  }
};

struct NSVGlobalState : public GlobalTableFunctionState {
  //! Total number of rows (from bind data's row_offsets).
  idx_t total_rows = 0;
  //! Atomic counter for parallel scan — next row to process.
  //! Starts at 1 (row 0 is the header).
  std::atomic<idx_t> next_row{1};
  //! Maps output column index → source column index.
  vector<column_t> column_ids;

  idx_t MaxThreads() const override {
    if (total_rows <= 1) {
      return 1;
    }
    idx_t data_rows = total_rows - 1;
    return (data_rows + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
  }
};

struct NSVLocalState : public LocalTableFunctionState {
  //! Precomputed col_map: col_map[source_col] = output_col, or -1 if not projected.
  vector<int32_t> col_map;
  //! Max source column index we care about (skip parsing beyond this).
  idx_t max_source_col = 0;

  //! Precomputed type tag per OUTPUT column. Avoids LogicalType comparisons in inner loop.
  enum ColType : uint8_t {
    COL_VARCHAR = 0,
    COL_BIGINT = 1,
    COL_DOUBLE = 2,
    COL_BOOLEAN = 3,
    COL_DATE = 4,
    COL_TIMESTAMP = 5,
    COL_OTHER = 6
  };
  vector<ColType> col_types;
};

static unique_ptr<FunctionData> NSVBind(ClientContext &ctx,
                                        TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types,
                                        vector<string> &names) {
  auto result = make_uniq<NSVBindData>();
  result->filename = input.inputs[0].GetValue<string>();

  auto it = input.named_parameters.find("all_varchar");
  if (it != input.named_parameters.end()) {
    result->all_varchar = it->second.GetValue<bool>();
  }

  auto &fs = FileSystem::GetFileSystem(ctx);
  auto file_handle = fs.OpenFile(result->filename, FileFlags::FILE_FLAGS_READ);
  auto file_size = fs.GetFileSize(*file_handle);

  result->raw_buffer.resize(file_size);
  fs.Read(*file_handle, (void *)result->raw_buffer.data(), file_size);

  // Build row offsets using Rust FFI (SIMD-accelerated memchr::memmem).
  // Done at bind time while data is still hot in cache from the file read.
  {
    RowIndex *ri = nsv_build_row_index(
        reinterpret_cast<const uint8_t *>(result->raw_buffer.data()),
        result->raw_buffer.size());
    if (ri) {
      size_t nrows = nsv_row_index_count(ri);
      const size_t *offsets = nsv_row_index_offsets(ri);
      result->row_offsets.assign(offsets, offsets + nrows + 1);
      nsv_row_index_free(ri);
    }
  }

  // Sample decode: header + up to 1000 data rows for type sniffing.
  result->sample = nsv_decode_sample(
      reinterpret_cast<const uint8_t *>(result->raw_buffer.data()),
      result->raw_buffer.size(), 1002);
  if (!result->sample) {
    throw InvalidInputException("Failed to parse NSV file: %s",
                                result->filename);
  }

  idx_t nrows = nsv_sample_row_count(result->sample);
  if (nrows == 0) {
    throw InvalidInputException("Empty NSV file: %s", result->filename);
  }

  idx_t ncols = nsv_sample_col_count(result->sample, 0);
  for (idx_t i = 0; i < ncols; i++) {
    size_t cell_len = 0;
    const char *cell = nsv_sample_cell(result->sample, 0, i, &cell_len);
    if (cell && cell_len > 0) {
      result->names.emplace_back(cell, cell_len);
    } else {
      result->names.push_back("col" + to_string(i));
    }

    if (result->all_varchar) {
      result->types.push_back(LogicalType::VARCHAR);
    } else {
      auto detected = DetectColumnType(ctx, result->sample, i, 1, 1000);
      result->types.push_back(detected);
    }
  }

  names = result->names;
  return_types = result->types;
  return std::move(result);
}

static unique_ptr<GlobalTableFunctionState>
NSVInitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
  auto state = make_uniq<NSVGlobalState>();
  state->column_ids = input.column_ids;

  auto &bind = input.bind_data->Cast<NSVBindData>();

  if (!bind.row_offsets.empty()) {
    state->total_rows = bind.row_offsets.size() - 1; // excluding sentinel
  }

  return std::move(state);
}

static unique_ptr<LocalTableFunctionState>
NSVInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
             GlobalTableFunctionState *global_state) {
  auto state = make_uniq<NSVLocalState>();
  auto &gstate = global_state->Cast<NSVGlobalState>();
  auto &bind = input.bind_data->Cast<NSVBindData>();
  idx_t ncols = bind.names.size();

  // Build col_map: source_col → output_col (or -1)
  state->col_map.resize(ncols, -1);
  state->max_source_col = 0;
  for (idx_t out_col = 0; out_col < gstate.column_ids.size(); out_col++) {
    auto src_col = gstate.column_ids[out_col];
    if (src_col < ncols) {
      state->col_map[src_col] = static_cast<int32_t>(out_col);
      if (src_col > state->max_source_col) {
        state->max_source_col = src_col;
      }
    }
  }

  // Precompute type tags per output column — avoid LogicalType comparisons in inner loop.
  state->col_types.resize(gstate.column_ids.size());
  for (idx_t out_col = 0; out_col < gstate.column_ids.size(); out_col++) {
    auto src_col = gstate.column_ids[out_col];
    if (src_col < bind.types.size()) {
      auto id = bind.types[src_col].id();
      switch (id) {
      case LogicalTypeId::VARCHAR:
        state->col_types[out_col] = NSVLocalState::COL_VARCHAR;
        break;
      case LogicalTypeId::BIGINT:
        state->col_types[out_col] = NSVLocalState::COL_BIGINT;
        break;
      case LogicalTypeId::DOUBLE:
        state->col_types[out_col] = NSVLocalState::COL_DOUBLE;
        break;
      case LogicalTypeId::BOOLEAN:
        state->col_types[out_col] = NSVLocalState::COL_BOOLEAN;
        break;
      case LogicalTypeId::DATE:
        state->col_types[out_col] = NSVLocalState::COL_DATE;
        break;
      case LogicalTypeId::TIMESTAMP:
        state->col_types[out_col] = NSVLocalState::COL_TIMESTAMP;
        break;
      default:
        state->col_types[out_col] = NSVLocalState::COL_OTHER;
        break;
      }
    } else {
      state->col_types[out_col] = NSVLocalState::COL_VARCHAR;
    }
  }

  return std::move(state);
}

// ── Inline NSV cell parsing ──────────────────────────────────────────
//
// Parse cells directly from raw bytes. No Rust FFI calls per cell.
// For VARCHAR: check for backslash via memchr — skip unescape if clean.
// For typed: raw bytes are valid for casting (no escapes possible).

/// Unescape an NSV cell entirely in C++.
/// Returns (ptr, len) of the unescaped data.
/// If no backslash found, returns the original (ptr, len) — zero-copy.
static inline std::pair<const char *, size_t>
UnescapeCell(const uint8_t *cell_ptr, size_t cell_len,
             vector<char> &unescape_buf) {
  // Fast path: scan for backslash
  const void *bs = memchr(cell_ptr, '\\', cell_len);
  if (!bs) {
    // No backslash — cell is clean, return raw pointer.
    return {reinterpret_cast<const char *>(cell_ptr), cell_len};
  }

  // Slow path: unescape in C++ (left-to-right character consumption).
  unescape_buf.clear();
  unescape_buf.reserve(cell_len);
  for (size_t i = 0; i < cell_len; i++) {
    if (cell_ptr[i] == '\\' && i + 1 < cell_len) {
      uint8_t next = cell_ptr[i + 1];
      if (next == 'n') {
        unescape_buf.push_back('\n');
      } else if (next == '\\') {
        unescape_buf.push_back('\\');
      } else {
        // Unrecognized escape: pass through with backslash
        unescape_buf.push_back('\\');
        unescape_buf.push_back(static_cast<char>(next));
      }
      i++; // consume the next char
    } else {
      unescape_buf.push_back(static_cast<char>(cell_ptr[i]));
    }
  }
  return {unescape_buf.data(), unescape_buf.size()};
}

static void NSVScan(ClientContext &ctx, TableFunctionInput &input,
                    DataChunk &output) {
  auto &bind = input.bind_data->Cast<NSVBindData>();
  auto &gstate = input.global_state->Cast<NSVGlobalState>();
  auto &lstate = input.local_state->Cast<NSVLocalState>();

  if (gstate.total_rows <= 1) {
    output.SetCardinality(0);
    return;
  }

  // Atomically grab a range of rows.
  idx_t start_row =
      gstate.next_row.fetch_add(STANDARD_VECTOR_SIZE, std::memory_order_relaxed);
  if (start_row >= gstate.total_rows) {
    output.SetCardinality(0);
    return;
  }

  idx_t count =
      MinValue<idx_t>(STANDARD_VECTOR_SIZE, gstate.total_rows - start_row);

  const auto *offsets = bind.row_offsets.data();
  const auto *raw =
      reinterpret_cast<const uint8_t *>(bind.raw_buffer.data());

  idx_t num_out_cols = gstate.column_ids.size();

  // ── Pass 1: Parse cell boundaries into a flat offset table ────────
  // cell_offsets[i * num_out_cols + out_col] = {start, len} for each projected cell.
  // This pass reads the raw data sequentially (read-friendly).
  struct CellPos {
    uint32_t start;
    uint32_t len;
  };
  // Stack-allocate for small chunks, heap for large.
  vector<CellPos> cell_positions(count * num_out_cols);

  for (idx_t i = 0; i < count; i++) {
    idx_t row_idx = start_row + i;
    size_t row_start = offsets[row_idx];
    size_t row_end = offsets[row_idx + 1];
    if (row_idx + 1 < gstate.total_rows) {
      row_end -= 2;
    }

    idx_t col_idx = 0;
    size_t pos = row_start;
    CellPos *row_cells = &cell_positions[i * num_out_cols];

    // Initialize all projected cells to empty (NULL)
    for (idx_t c = 0; c < num_out_cols; c++) {
      row_cells[c] = {0, 0};
    }

    while (pos < row_end) {
      size_t cell_start = pos;
      while (pos < row_end && raw[pos] != '\n') {
        pos++;
      }
      size_t cell_len = pos - cell_start;
      if (pos < row_end) {
        pos++;
      }

      if (col_idx < lstate.col_map.size()) {
        int32_t out_col = lstate.col_map[col_idx];
        if (out_col >= 0) {
          row_cells[out_col] = {static_cast<uint32_t>(cell_start),
                                static_cast<uint32_t>(cell_len)};
        }
      }

      col_idx++;
      if (col_idx > lstate.max_source_col) {
        break;
      }
    }
  }

  // ── Pass 2: Write each column sequentially (write-friendly) ───────
  // Each column's vector data is written in one contiguous sweep,
  // keeping the write target hot in L1/L2 cache.
  vector<char> unescape_buf;

  for (idx_t out_col = 0; out_col < num_out_cols; out_col++) {
    auto &vec = output.data[out_col];
    auto col_type = lstate.col_types[out_col];
    auto &validity = FlatVector::Validity(vec);

    switch (col_type) {
    case NSVLocalState::COL_VARCHAR: {
      auto str_data = FlatVector::GetData<string_t>(vec);
      for (idx_t i = 0; i < count; i++) {
        auto &cp = cell_positions[i * num_out_cols + out_col];
        if (cp.len == 0) {
          validity.SetInvalid(i);
        } else {
          const void *bs = memchr(raw + cp.start, '\\', cp.len);
          if (!bs) {
            str_data[i] = string_t(
                reinterpret_cast<const char *>(raw + cp.start),
                static_cast<uint32_t>(cp.len));
          } else {
            auto [ptr, len] =
                UnescapeCell(raw + cp.start, cp.len, unescape_buf);
            str_data[i] = StringVector::AddString(vec, ptr, len);
          }
        }
      }
      break;
    }
    case NSVLocalState::COL_BIGINT: {
      auto typed_data = FlatVector::GetData<int64_t>(vec);
      for (idx_t i = 0; i < count; i++) {
        auto &cp = cell_positions[i * num_out_cols + out_col];
        if (cp.len == 0) {
          validity.SetInvalid(i);
        } else {
          string_t sv(reinterpret_cast<const char *>(raw + cp.start),
                      static_cast<uint32_t>(cp.len));
          if (!TryCast::Operation(sv, typed_data[i], false)) {
            validity.SetInvalid(i);
          }
        }
      }
      break;
    }
    case NSVLocalState::COL_DOUBLE: {
      auto typed_data = FlatVector::GetData<double>(vec);
      for (idx_t i = 0; i < count; i++) {
        auto &cp = cell_positions[i * num_out_cols + out_col];
        if (cp.len == 0) {
          validity.SetInvalid(i);
        } else {
          string_t sv(reinterpret_cast<const char *>(raw + cp.start),
                      static_cast<uint32_t>(cp.len));
          if (!TryCast::Operation(sv, typed_data[i], false)) {
            validity.SetInvalid(i);
          }
        }
      }
      break;
    }
    case NSVLocalState::COL_BOOLEAN: {
      auto typed_data = FlatVector::GetData<bool>(vec);
      for (idx_t i = 0; i < count; i++) {
        auto &cp = cell_positions[i * num_out_cols + out_col];
        if (cp.len == 0) {
          validity.SetInvalid(i);
        } else {
          string_t sv(reinterpret_cast<const char *>(raw + cp.start),
                      static_cast<uint32_t>(cp.len));
          if (!TryCast::Operation(sv, typed_data[i], false)) {
            validity.SetInvalid(i);
          }
        }
      }
      break;
    }
    case NSVLocalState::COL_DATE: {
      auto typed_data = FlatVector::GetData<date_t>(vec);
      for (idx_t i = 0; i < count; i++) {
        auto &cp = cell_positions[i * num_out_cols + out_col];
        if (cp.len == 0) {
          validity.SetInvalid(i);
        } else {
          string_t sv(reinterpret_cast<const char *>(raw + cp.start),
                      static_cast<uint32_t>(cp.len));
          if (!TryCast::Operation(sv, typed_data[i], false)) {
            validity.SetInvalid(i);
          }
        }
      }
      break;
    }
    case NSVLocalState::COL_TIMESTAMP: {
      auto typed_data = FlatVector::GetData<timestamp_t>(vec);
      for (idx_t i = 0; i < count; i++) {
        auto &cp = cell_positions[i * num_out_cols + out_col];
        if (cp.len == 0) {
          validity.SetInvalid(i);
        } else {
          string_t sv(reinterpret_cast<const char *>(raw + cp.start),
                      static_cast<uint32_t>(cp.len));
          if (!TryCast::Operation(sv, typed_data[i], false)) {
            validity.SetInvalid(i);
          }
        }
      }
      break;
    }
    default: {
      auto src_col = gstate.column_ids[out_col];
      for (idx_t i = 0; i < count; i++) {
        auto &cp = cell_positions[i * num_out_cols + out_col];
        if (cp.len == 0) {
          validity.SetInvalid(i);
        } else {
          Value str_v(string(reinterpret_cast<const char *>(raw + cp.start), cp.len));
          Value result_v;
          string error_msg;
          if (str_v.TryCastAs(ctx, bind.types[src_col], result_v, &error_msg, false)) {
            vec.SetValue(i, result_v);
          } else {
            validity.SetInvalid(i);
          }
        }
      }
      break;
    }
    }
  }

  output.SetCardinality(count);
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
                            GlobalFunctionData &, LocalFunctionData &) {}

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
  // read_nsv table function with parallel scan + projection pushdown
  TableFunction read_nsv("read_nsv", {LogicalType::VARCHAR}, NSVScan, NSVBind);
  read_nsv.init_global = NSVInitGlobal;
  read_nsv.init_local = NSVInitLocal;
  read_nsv.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
  read_nsv.projection_pushdown = true;
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
