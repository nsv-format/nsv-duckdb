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

#include "duckdb/parallel/task_scheduler.hpp"

#include "nsv_ffi.h"

#include <atomic>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

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

// ── Chunk boundary helpers ──────────────────────────────────────────

//! Find the Nth \n\n boundary starting from `from`.
static size_t FindNthRowBoundary(const uint8_t *buf, size_t buf_len,
                                 size_t from, size_t n) {
  size_t found = 0;
  for (size_t i = from; i + 1 < buf_len; i++) {
    if (buf[i] == '\n' && buf[i + 1] == '\n') {
      found++;
      if (found == n) {
        return i + 2;
      }
    }
  }
  return buf_len;
}

//! Find the next \n\n boundary at or after `from`.
static size_t FindNextRowBoundary(const uint8_t *buf, size_t buf_len,
                                  size_t from) {
  return FindNthRowBoundary(buf, buf_len, from, 1);
}

// ── read_nsv ────────────────────────────────────────────────────────

struct NSVBindData : public TableFunctionData {
  string filename;
  vector<string> names;
  vector<LogicalType> types;
  //! File data pointer and size.
  const uint8_t *file_data = nullptr;
  size_t file_size = 0;
#ifndef _WIN32
  //! If mmap'd: fd and mmap pointer for cleanup.
  int mmap_fd = -1;
  void *mmap_ptr = nullptr;
#endif
  //! If read into memory: owned buffer (fallback for non-local/Windows files).
  string read_buffer;
  //! Byte offset where data rows begin (past header row).
  size_t data_start_offset = 0;
  bool all_varchar = false;
  bool has_header = true;

  ~NSVBindData() {
#ifndef _WIN32
    if (mmap_ptr && mmap_ptr != MAP_FAILED) {
      munmap(mmap_ptr, file_size);
    }
    if (mmap_fd >= 0) {
      close(mmap_fd);
    }
#endif
  }
};

struct NSVGlobalState : public GlobalTableFunctionState {
  //! Maps output column index → source column index.
  vector<column_t> column_ids;
  //! Per-column projection indices for nsv_decode_flat.
  vector<size_t> col_indices;
  //! Per-column unescape flags (1 = VARCHAR, needs unescape).
  vector<uint8_t> needs_unescape;
  //! Work units: byte ranges [start, end) in the raw buffer.
  vector<pair<size_t, size_t>> ranges;
  //! Next range to hand out.
  std::atomic<idx_t> next_range{0};

  idx_t MaxThreads() const override { return ranges.size(); }
};

struct NSVLocalState : public LocalTableFunctionState {
  //! Flat arrays sized for STANDARD_VECTOR_SIZE rows.
  vector<size_t> offsets;
  vector<size_t> lengths;
  //! Scratch buffer for unescaped cells (from last decode call).
  NsvScratchBuf *scratch = nullptr;
  //! Number of projected columns.
  idx_t num_cols = 0;
  //! Current byte position within the assigned range.
  size_t byte_pos = 0;
  size_t range_end = 0;
  bool exhausted = true;

  ~NSVLocalState() {
    if (scratch) {
      nsv_scratch_free(scratch);
    }
  }
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

  auto hdr_it = input.named_parameters.find("header");
  if (hdr_it != input.named_parameters.end()) {
    result->has_header = hdr_it->second.GetValue<bool>();
  }

  // Try mmap for local files (avoids kernel→userspace copy).
  bool use_mmap = false;
#ifndef _WIN32
  {
    int fd = open(result->filename.c_str(), O_RDONLY);
    if (fd >= 0) {
      struct stat st;
      if (fstat(fd, &st) == 0 && st.st_size > 0) {
        void *mapped = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped != MAP_FAILED) {
          madvise(mapped, st.st_size, MADV_SEQUENTIAL);
          result->mmap_fd = fd;
          result->mmap_ptr = mapped;
          result->file_data = reinterpret_cast<const uint8_t *>(mapped);
          result->file_size = static_cast<size_t>(st.st_size);
          use_mmap = true;
        } else {
          close(fd);
        }
      } else {
        close(fd);
      }
    }
  }
#endif

  if (!use_mmap) {
    auto &fs = FileSystem::GetFileSystem(ctx);
    auto file_handle =
        fs.OpenFile(result->filename, FileFlags::FILE_FLAGS_READ);
    auto file_size = fs.GetFileSize(*file_handle);
    result->read_buffer.resize(file_size);
    fs.Read(*file_handle, (void *)result->read_buffer.data(), file_size);
    result->file_data =
        reinterpret_cast<const uint8_t *>(result->read_buffer.data());
    result->file_size = result->read_buffer.size();
  }

  auto *buf = result->file_data;
  size_t buf_len = result->file_size;

  // Decode header + up to 1000 sample rows for type sniffing.
  size_t sample_end = FindNthRowBoundary(buf, buf_len, 0, 1001);
  SampleHandle *sample = nsv_decode_sample(buf, sample_end, 1002);
  if (!sample) {
    throw InvalidInputException("Failed to parse NSV file: %s",
                                result->filename);
  }

  idx_t nrows = nsv_sample_row_count(sample);
  if (nrows == 0) {
    nsv_sample_free(sample);
    throw InvalidInputException("Empty NSV file: %s", result->filename);
  }

  idx_t ncols = nsv_sample_col_count(sample, 0);
  idx_t data_start_row;

  if (result->has_header) {
    // Row 0 = column headers; data starts after first row boundary.
    result->data_start_offset = FindNextRowBoundary(buf, buf_len, 0);
    data_start_row = 1;
    for (idx_t i = 0; i < ncols; i++) {
      size_t cell_len = 0;
      const char *cell = nsv_sample_cell(sample, 0, i, &cell_len);
      if (cell && cell_len > 0) {
        result->names.emplace_back(cell, cell_len);
      } else {
        result->names.push_back("col" + to_string(i));
      }
    }
  } else {
    // No header: data starts at byte 0; generate column0, column1, ...
    result->data_start_offset = 0;
    data_start_row = 0;
    for (idx_t i = 0; i < ncols; i++) {
      result->names.push_back("column" + to_string(i));
    }
  }

  for (idx_t i = 0; i < ncols; i++) {
    if (result->all_varchar) {
      result->types.push_back(LogicalType::VARCHAR);
    } else {
      auto detected = DetectColumnType(ctx, sample, i, data_start_row, 1000);
      result->types.push_back(detected);
    }
  }

  nsv_sample_free(sample);

  names = result->names;
  return_types = result->types;
  return std::move(result);
}

static unique_ptr<GlobalTableFunctionState>
NSVInitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
  auto state = make_uniq<NSVGlobalState>();
  state->column_ids = input.column_ids;

  auto &bind = input.bind_data->Cast<NSVBindData>();

  // Build projection info for nsv_decode_flat.
  state->col_indices.reserve(state->column_ids.size());
  state->needs_unescape.reserve(state->column_ids.size());
  for (auto &cid : state->column_ids) {
    state->col_indices.push_back(static_cast<size_t>(cid));
    state->needs_unescape.push_back(
        bind.types[cid] == LogicalType::VARCHAR ? 1 : 0);
  }

  // Split data region into ~2MB ranges at \n\n boundaries.
  auto *buf = bind.file_data;
  size_t buf_len = bind.file_size;
  size_t data_start = bind.data_start_offset;
  size_t data_len = buf_len - data_start;

  idx_t num_threads = TaskScheduler::GetScheduler(ctx).NumberOfThreads();
  const size_t TARGET_RANGE_BYTES = 2 * 1024 * 1024;
  idx_t num_ranges = MaxValue<idx_t>(
      num_threads * 4, static_cast<idx_t>(data_len / TARGET_RANGE_BYTES));
  num_ranges = MaxValue<idx_t>(1, MinValue<idx_t>(num_ranges, data_len / 4096));
  size_t range_size = data_len / num_ranges;

  state->ranges.reserve(num_ranges);
  size_t pos = data_start;
  for (idx_t i = 1; i < num_ranges; i++) {
    size_t nominal = data_start + i * range_size;
    if (nominal >= buf_len)
      break;
    size_t boundary = FindNextRowBoundary(buf, buf_len, nominal);
    if (boundary < buf_len && boundary > pos) {
      state->ranges.emplace_back(pos, boundary);
      pos = boundary;
    }
  }
  if (pos < buf_len) {
    state->ranges.emplace_back(pos, buf_len);
  }

  return std::move(state);
}

static unique_ptr<LocalTableFunctionState>
NSVInitLocal(ExecutionContext &, TableFunctionInitInput &,
             GlobalTableFunctionState *) {
  return make_uniq<NSVLocalState>();
}

static void NSVScan(ClientContext &ctx, TableFunctionInput &input,
                    DataChunk &output) {
  auto &bind = input.bind_data->Cast<NSVBindData>();
  auto &gstate = input.global_state->Cast<NSVGlobalState>();
  auto &lstate = input.local_state->Cast<NSVLocalState>();

  auto *file_buf = bind.file_data;
  idx_t nc = static_cast<idx_t>(gstate.col_indices.size());

  // Ensure flat arrays are allocated (once).
  size_t cap = STANDARD_VECTOR_SIZE * nc;
  if (lstate.offsets.size() < cap) {
    lstate.offsets.resize(cap);
    lstate.lengths.resize(cap);
    lstate.num_cols = nc;
  }

  // Grab ranges until we get data or run out.
  for (;;) {
    if (lstate.exhausted || lstate.byte_pos >= lstate.range_end) {
      idx_t range_idx = gstate.next_range.fetch_add(1);
      if (range_idx >= static_cast<idx_t>(gstate.ranges.size())) {
        output.SetCardinality(0);
        return;
      }
      lstate.byte_pos = gstate.ranges[range_idx].first;
      lstate.range_end = gstate.ranges[range_idx].second;
      lstate.exhausted = false;
    }

    // Free previous scratch.
    if (lstate.scratch) {
      nsv_scratch_free(lstate.scratch);
      lstate.scratch = nullptr;
    }

    // Decode up to STANDARD_VECTOR_SIZE rows via Rust FFI.
    size_t chunk_len = lstate.range_end - lstate.byte_pos;
    NsvScratchBuf *scratch = nullptr;
    size_t bytes_consumed = 0;
    size_t decoded = nsv_decode_flat(
        file_buf + lstate.byte_pos, chunk_len, lstate.byte_pos,
        gstate.col_indices.data(), nc, gstate.needs_unescape.data(),
        lstate.offsets.data(), lstate.lengths.data(), STANDARD_VECTOR_SIZE,
        &scratch, &bytes_consumed);

    lstate.scratch = scratch;
    lstate.byte_pos += bytes_consumed;

    if (decoded > 0) {
      idx_t count = static_cast<idx_t>(decoded);
      const uint8_t *scratch_ptr = scratch ? nsv_scratch_ptr(scratch) : nullptr;

      for (idx_t out_col = 0; out_col < output.ColumnCount(); out_col++) {
        idx_t src_col = gstate.column_ids[out_col];
        auto &vec = output.data[out_col];
        const auto &target_type = bind.types[src_col];

        if (target_type == LogicalType::VARCHAR) {
          // VARCHAR: write strings directly into the output vector.
          auto str_data = FlatVector::GetData<string_t>(vec);
          auto &validity = FlatVector::Validity(vec);

          for (idx_t i = 0; i < count; i++) {
            size_t idx = i * nc + out_col;
            size_t off = lstate.offsets[idx];
            size_t len = lstate.lengths[idx];
            if (len == 0) {
              validity.SetInvalid(i);
            } else {
              const char *cell;
              if (off & NSV_SCRATCH_BIT) {
                cell = reinterpret_cast<const char *>(scratch_ptr +
                                                      (off & ~NSV_SCRATCH_BIT));
              } else {
                cell = reinterpret_cast<const char *>(file_buf + off);
              }
              str_data[i] = StringVector::AddString(vec, cell, len);
            }
          }
        } else {
          // Typed columns: populate VARCHAR vector, then batch-cast.
          Vector str_vec(LogicalType::VARCHAR, count);
          auto str_data = FlatVector::GetData<string_t>(str_vec);
          auto &str_validity = FlatVector::Validity(str_vec);

          for (idx_t i = 0; i < count; i++) {
            size_t idx = i * nc + out_col;
            size_t off = lstate.offsets[idx];
            size_t len = lstate.lengths[idx];
            if (len == 0) {
              str_validity.SetInvalid(i);
            } else {
              const char *cell;
              if (off & NSV_SCRATCH_BIT) {
                cell = reinterpret_cast<const char *>(scratch_ptr +
                                                      (off & ~NSV_SCRATCH_BIT));
              } else {
                cell = reinterpret_cast<const char *>(file_buf + off);
              }
              str_data[i] = StringVector::AddString(str_vec, cell, len);
            }
          }

          string error_msg;
          VectorOperations::TryCast(ctx, str_vec, vec, count, &error_msg,
                                    false);
        }
      }

      output.SetCardinality(count);
      return;
    }

    // decoded == 0: range exhausted, loop to grab next range.
    lstate.exhausted = true;
  }
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
  bool header_written = false;
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
  return std::move(result);
}

static unique_ptr<LocalFunctionData> NSVWriteInitLocal(ExecutionContext &,
                                                       FunctionData &) {
  return make_uniq<NSVWriteLocalState>();
}

static void NSVWriteSink(ExecutionContext &context, FunctionData &bind_data,
                         GlobalFunctionData &gstate, LocalFunctionData &,
                         DataChunk &input) {
  auto &bind = bind_data.Cast<NSVWriteBindData>();
  auto &state = gstate.Cast<NSVWriteGlobalState>();

  auto &fs = FileSystem::GetFileSystem(context.client);

  if (!state.header_written && bind.write_header) {
    NsvEncoder *enc = nsv_encoder_new();
    for (auto &name : bind.names) {
      nsv_encoder_push_cell(enc, reinterpret_cast<const uint8_t *>(name.data()),
                            name.size());
    }
    nsv_encoder_end_row(enc);
    uint8_t *hdr = nullptr;
    size_t hdr_len = 0;
    nsv_encoder_finish(enc, &hdr, &hdr_len);
    if (hdr && hdr_len > 0) {
      fs.Write(*state.file_handle, (void *)hdr, hdr_len);
      nsv_free_buf(hdr, hdr_len);
    }
    state.header_written = true;
  }

  idx_t count = input.size();
  idx_t ncols = input.ColumnCount();

  // Batch-cast all non-VARCHAR columns to VARCHAR.
  vector<Vector> cast_vectors;
  cast_vectors.reserve(ncols);
  for (idx_t col = 0; col < ncols; col++) {
    if (bind.types[col] == LogicalType::VARCHAR) {
      cast_vectors.emplace_back(LogicalType::VARCHAR); // placeholder
    } else {
      Vector target(LogicalType::VARCHAR, count);
      VectorOperations::Cast(context.client, input.data[col], target, count);
      cast_vectors.push_back(std::move(target));
    }
  }

  // Build column-major arrays for nsv_write_chunk.
  vector<const uint8_t *> cell_ptrs(ncols * count);
  vector<size_t> cell_lens(ncols * count);
  vector<uint8_t> null_masks(ncols * count);

  for (idx_t col = 0; col < ncols; col++) {
    auto &vec = (bind.types[col] == LogicalType::VARCHAR) ? input.data[col]
                                                          : cast_vectors[col];
    vec.Flatten(count);
    auto str_data = FlatVector::GetData<string_t>(vec);
    auto &validity = FlatVector::Validity(vec);

    for (idx_t row = 0; row < count; row++) {
      idx_t idx = col * count + row;
      if (!validity.RowIsValid(row)) {
        cell_ptrs[idx] = nullptr;
        cell_lens[idx] = 0;
        null_masks[idx] = 1;
      } else {
        cell_ptrs[idx] =
            reinterpret_cast<const uint8_t *>(str_data[row].GetData());
        cell_lens[idx] = str_data[row].GetSize();
        null_masks[idx] = 0;
      }
    }
  }

  uint8_t *out = nullptr;
  size_t out_len = 0;
  nsv_write_chunk(cell_ptrs.data(), cell_lens.data(), null_masks.data(), count,
                  ncols, &out, &out_len);
  if (out && out_len > 0) {
    fs.Write(*state.file_handle, (void *)out, out_len);
    nsv_free_buf(out, out_len);
  }
}

static void NSVWriteCombine(ExecutionContext &, FunctionData &,
                            GlobalFunctionData &, LocalFunctionData &) {}

static void NSVWriteFinalize(ClientContext &, FunctionData &,
                             GlobalFunctionData &) {}

// ── Extension registration ──────────────────────────────────────────

static void LoadInternal(ExtensionLoader &loader) {
  // read_nsv table function with projection pushdown + parallel scan
  TableFunction read_nsv("read_nsv", {LogicalType::VARCHAR}, NSVScan, NSVBind);
  read_nsv.init_global = NSVInitGlobal;
  read_nsv.init_local = NSVInitLocal;
  read_nsv.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
  read_nsv.named_parameters["header"] = LogicalType::BOOLEAN;
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
