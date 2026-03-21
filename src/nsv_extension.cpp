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
#include "duckdb/parallel/task_scheduler.hpp"

#include "nsv_ffi.h"

#include <atomic>
#include <cstring>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace duckdb {

// ── Type detection ──────────────────────────────────────────────────

static const vector<LogicalType> TYPE_CANDIDATES = {
    LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::DOUBLE, LogicalType::DATE, LogicalType::TIMESTAMP,
    LogicalType::VARCHAR // fallback — always succeeds
};

static LogicalType DetectColumnType(ClientContext &ctx, SampleHandle *data, idx_t col_idx, idx_t start_row,
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

//! Find the next \n\n boundary at or after `from`. Returns `buf_len` if none.
static size_t FindNextRowBoundary(const uint8_t *buf, size_t buf_len, size_t from) {
	for (size_t i = from; i + 1 < buf_len; i++) {
		if (buf[i] == '\n' && buf[i + 1] == '\n') {
			return i + 2; // byte after \n\n
		}
	}
	return buf_len;
}

//! Find the Nth \n\n boundary starting from `from`.
static size_t FindNthRowBoundary(const uint8_t *buf, size_t buf_len, size_t from, size_t n) {
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

// ── Cell position struct (used in scan) ─────────────────────────────

/// Cell position (byte offset + length in the raw buffer).
struct CellPos {
	uint32_t start;
	uint32_t len;
};

// ── read_nsv ────────────────────────────────────────────────────────

struct NSVBindData : public TableFunctionData {
	string filename;
	vector<string> names;
	vector<LogicalType> types;
	//! File data pointer and size (valid for lifetime of bind data).
	const uint8_t *file_data = nullptr;
	size_t file_size = 0;
#ifndef _WIN32
	//! If mmap'd: fd and mmap pointer for cleanup.
	int mmap_fd = -1;
	void *mmap_ptr = nullptr;
#endif
	//! If read into memory: owned buffer (fallback for non-local files).
	string read_buffer;
	//! Byte offset where data rows begin (past header row's \n\n).
	size_t data_start_offset = 0;
	bool all_varchar = false;

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
	//! Work units: byte ranges [start, end) in the raw buffer.
	vector<pair<size_t, size_t>> ranges;
	//! Next range to hand out.
	std::atomic<idx_t> next_range {0};

	idx_t MaxThreads() const override {
		return ranges.size();
	}
};

struct NSVLocalState : public LocalTableFunctionState {
	//! Precomputed col_map: col_map[source_col] = output_col, or -1.
	vector<int32_t> col_map;
	//! Max source column index we care about.
	idx_t max_source_col = 0;

	//! Precomputed type tag per OUTPUT column.
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

	//! Current byte position within the assigned range.
	size_t byte_pos = 0;
	size_t range_end = 0;
	bool exhausted = true;

	//! Reusable cell position buffer (allocated once per local state).
	vector<CellPos> cell_buf;
	//! Reusable unescape buffer.
	vector<char> unescape_buf;
};

static unique_ptr<FunctionData> NSVBind(ClientContext &ctx, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<NSVBindData>();
	result->filename = input.inputs[0].GetValue<string>();

	auto it = input.named_parameters.find("all_varchar");
	if (it != input.named_parameters.end()) {
		result->all_varchar = it->second.GetValue<bool>();
	}

	// Try mmap for local files (avoids kernel→userspace copy).
	// On Windows, fall back to DuckDB's filesystem.
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
		// Fallback: read via DuckDB's filesystem (supports HTTP, S3, etc.)
		auto &fs = FileSystem::GetFileSystem(ctx);
		auto file_handle = fs.OpenFile(result->filename, FileFlags::FILE_FLAGS_READ);
		auto file_size = fs.GetFileSize(*file_handle);
		result->read_buffer.resize(file_size);
		fs.Read(*file_handle, (void *)result->read_buffer.data(), file_size);
		result->file_data = reinterpret_cast<const uint8_t *>(result->read_buffer.data());
		result->file_size = result->read_buffer.size();
	}

	auto *buf = result->file_data;
	size_t buf_len = result->file_size;

	// Find byte range covering header + up to 1000 sample rows for type sniffing.
	size_t sample_end = FindNthRowBoundary(buf, buf_len, 0, 1001);

	// Sample decode via Rust FFI (only the prefix).
	SampleHandle *sample = nsv_decode_sample(buf, sample_end, 1002);
	if (!sample) {
		throw InvalidInputException("Failed to parse NSV file: %s", result->filename);
	}

	idx_t nrows = nsv_sample_row_count(sample);
	if (nrows == 0) {
		nsv_sample_free(sample);
		throw InvalidInputException("Empty NSV file: %s", result->filename);
	}

	// Data starts after the header row's \n\n boundary.
	result->data_start_offset = FindNextRowBoundary(buf, buf_len, 0);

	// Row 0 = column headers.
	idx_t ncols = nsv_sample_col_count(sample, 0);
	for (idx_t i = 0; i < ncols; i++) {
		size_t cell_len = 0;
		const char *cell = nsv_sample_cell(sample, 0, i, &cell_len);
		if (cell && cell_len > 0) {
			result->names.emplace_back(cell, cell_len);
		} else {
			result->names.push_back("col" + to_string(i));
		}

		if (result->all_varchar) {
			result->types.push_back(LogicalType::VARCHAR);
		} else {
			auto detected = DetectColumnType(ctx, sample, i, 1, 1000);
			result->types.push_back(detected);
		}
	}

	nsv_sample_free(sample);

	names = result->names;
	return_types = result->types;
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> NSVInitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
	auto state = make_uniq<NSVGlobalState>();
	state->column_ids = input.column_ids;

	auto &bind = input.bind_data->Cast<NSVBindData>();

	auto *buf = bind.file_data;
	size_t buf_len = bind.file_size;
	size_t data_start = bind.data_start_offset;
	size_t data_len = buf_len - data_start;

	// Split data region into ~2MB ranges at \n\n boundaries.
	idx_t num_threads = TaskScheduler::GetScheduler(ctx).NumberOfThreads();
	const size_t TARGET_RANGE_BYTES = 2 * 1024 * 1024;
	idx_t num_ranges = MaxValue<idx_t>(num_threads * 4, static_cast<idx_t>(data_len / TARGET_RANGE_BYTES));
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
	// Last range goes to end of buffer.
	if (pos < buf_len) {
		state->ranges.emplace_back(pos, buf_len);
	}

	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> NSVInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
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

	// Precompute type tags per output column.
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

/// Unescape an NSV cell entirely in C++.
static inline std::pair<const char *, size_t> UnescapeCell(const uint8_t *cell_ptr, size_t cell_len,
                                                           vector<char> &unescape_buf) {
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
				unescape_buf.push_back('\\');
				unescape_buf.push_back(static_cast<char>(next));
			}
			i++;
		} else {
			unescape_buf.push_back(static_cast<char>(cell_ptr[i]));
		}
	}
	return {unescape_buf.data(), unescape_buf.size()};
}

/// Phase 1: Scan raw bytes at [pos, end) and extract cell boundaries.
/// Fills row_cells[] in row-major order (row * num_out_cols + out_col).
/// Returns the number of rows parsed (up to max_rows).
/// Updates `pos` to the byte after the last consumed row boundary.
static idx_t ScanCellBoundaries(const uint8_t *raw, size_t &pos, size_t end, idx_t max_rows, idx_t num_out_cols,
                                const vector<int32_t> &col_map, idx_t max_source_col,
                                const vector<column_t> &column_ids, CellPos *row_cells) {
	idx_t row_count = 0;
	idx_t col_idx = 0;
	bool row_has_cells = false;

	// Zero-initialize the first row.
	memset(row_cells, 0, num_out_cols * sizeof(CellPos));

	while (pos < end && row_count < max_rows) {
		// Scan for next \n using SIMD-accelerated memchr.
		size_t cell_start = pos;
		const void *nl = memchr(raw + pos, '\n', end - pos);
		if (nl) {
			pos = static_cast<size_t>(reinterpret_cast<const uint8_t *>(nl) - raw);
		} else {
			pos = end;
		}

		if (pos >= end) {
			// Ran out of data mid-row.
			size_t cell_len = pos - cell_start;
			if (cell_len > 0 && col_idx < col_map.size()) {
				int32_t out_col = col_map[col_idx];
				if (out_col >= 0) {
					row_cells[row_count * num_out_cols + out_col] = {static_cast<uint32_t>(cell_start),
					                                                 static_cast<uint32_t>(cell_len)};
				}
				col_idx++;
				row_has_cells = true;
			}
			break;
		}

		size_t cell_len = pos - cell_start;
		pos++; // consume the \n

		if (cell_len > 0) {
			// Non-empty cell
			if (col_idx < col_map.size()) {
				int32_t out_col = col_map[col_idx];
				if (out_col >= 0) {
					row_cells[row_count * num_out_cols + out_col] = {static_cast<uint32_t>(cell_start),
					                                                 static_cast<uint32_t>(cell_len)};
				}
			}
			col_idx++;
			row_has_cells = true;
			// Skip remaining columns we don't need.
			if (col_idx > max_source_col && pos < end && raw[pos] != '\n') {
				// Fast-forward to next \n\n row boundary.
				while (pos + 1 < end) {
					if (raw[pos] == '\n' && raw[pos + 1] == '\n') {
						break;
					}
					pos++;
				}
			}
		} else {
			// Empty segment (\n\n) = row boundary
			if (row_has_cells) {
				row_count++;
				if (row_count < max_rows) {
					memset(row_cells + row_count * num_out_cols, 0, num_out_cols * sizeof(CellPos));
				}
			}
			col_idx = 0;
			row_has_cells = false;
		}
	}

	// Finalize trailing row (no final \n\n at end of range).
	if (row_has_cells && row_count < max_rows) {
		row_count++;
	}

	return row_count;
}

/// Phase 2: Write cell data to output vectors from pre-scanned boundaries.
static void WriteCells(const uint8_t *raw, idx_t count, idx_t num_out_cols, const CellPos *row_cells, DataChunk &output,
                       NSVLocalState &lstate, NSVGlobalState &gstate, const NSVBindData &bind, ClientContext &ctx,
                       vector<char> &unescape_buf) {
	for (idx_t out_col = 0; out_col < num_out_cols; out_col++) {
		auto &vec = output.data[out_col];
		auto col_type = lstate.col_types[out_col];
		auto &validity = FlatVector::Validity(vec);

		switch (col_type) {
		case NSVLocalState::COL_VARCHAR: {
			auto str_data = FlatVector::GetData<string_t>(vec);
			for (idx_t i = 0; i < count; i++) {
				auto &cp = row_cells[i * num_out_cols + out_col];
				if (cp.len == 0) {
					validity.SetInvalid(i);
				} else {
					const void *bs = memchr(raw + cp.start, '\\', cp.len);
					if (!bs) {
						str_data[i] =
						    string_t(reinterpret_cast<const char *>(raw + cp.start), static_cast<uint32_t>(cp.len));
					} else {
						auto [ptr, len] = UnescapeCell(raw + cp.start, cp.len, unescape_buf);
						str_data[i] = StringVector::AddString(vec, ptr, len);
					}
				}
			}
			break;
		}
		case NSVLocalState::COL_BIGINT: {
			auto typed_data = FlatVector::GetData<int64_t>(vec);
			for (idx_t i = 0; i < count; i++) {
				auto &cp = row_cells[i * num_out_cols + out_col];
				if (cp.len == 0) {
					validity.SetInvalid(i);
				} else {
					string_t sv(reinterpret_cast<const char *>(raw + cp.start), static_cast<uint32_t>(cp.len));
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
				auto &cp = row_cells[i * num_out_cols + out_col];
				if (cp.len == 0) {
					validity.SetInvalid(i);
				} else {
					string_t sv(reinterpret_cast<const char *>(raw + cp.start), static_cast<uint32_t>(cp.len));
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
				auto &cp = row_cells[i * num_out_cols + out_col];
				if (cp.len == 0) {
					validity.SetInvalid(i);
				} else {
					string_t sv(reinterpret_cast<const char *>(raw + cp.start), static_cast<uint32_t>(cp.len));
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
				auto &cp = row_cells[i * num_out_cols + out_col];
				if (cp.len == 0) {
					validity.SetInvalid(i);
				} else {
					string_t sv(reinterpret_cast<const char *>(raw + cp.start), static_cast<uint32_t>(cp.len));
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
				auto &cp = row_cells[i * num_out_cols + out_col];
				if (cp.len == 0) {
					validity.SetInvalid(i);
				} else {
					string_t sv(reinterpret_cast<const char *>(raw + cp.start), static_cast<uint32_t>(cp.len));
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
				auto &cp = row_cells[i * num_out_cols + out_col];
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
}

static void NSVScan(ClientContext &ctx, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->Cast<NSVBindData>();
	auto &gstate = input.global_state->Cast<NSVGlobalState>();
	auto &lstate = input.local_state->Cast<NSVLocalState>();

	auto *raw = bind.file_data;
	idx_t num_out_cols = static_cast<idx_t>(gstate.column_ids.size());

	// Ensure cell buffer is allocated (once per local state).
	size_t cap = STANDARD_VECTOR_SIZE * num_out_cols;
	if (lstate.cell_buf.size() < cap) {
		lstate.cell_buf.resize(cap);
	}

	// Grab ranges until we get data or run out.
	for (;;) {
		// Need a new range?
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

		// Phase 1: Scan cell boundaries (tight loop, auto-vectorizable).
		size_t pos = lstate.byte_pos;
		idx_t count = ScanCellBoundaries(raw, pos, lstate.range_end, STANDARD_VECTOR_SIZE, num_out_cols, lstate.col_map,
		                                 lstate.max_source_col, gstate.column_ids, lstate.cell_buf.data());
		lstate.byte_pos = pos;

		if (count > 0) {
			// Phase 2: Write cells to output vectors (column-at-a-time).
			WriteCells(raw, count, num_out_cols, lstate.cell_buf.data(), output, lstate, gstate, bind, ctx,
			           lstate.unescape_buf);
			output.SetCardinality(count);
			return;
		}

		// Range exhausted with 0 rows — loop to grab next range.
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

static unique_ptr<FunctionData> NSVWriteBind(ClientContext &, CopyFunctionBindInput &input, const vector<string> &names,
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

static unique_ptr<GlobalFunctionData> NSVWriteInitGlobal(ClientContext &ctx, FunctionData &bind_data,
                                                         const string &filename) {
	auto result = make_uniq<NSVWriteGlobalState>();
	result->filename = filename;
	auto &fs = FileSystem::GetFileSystem(ctx);
	result->file_handle = fs.OpenFile(filename, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	result->encoder = nsv_encoder_new();
	return std::move(result);
}

static unique_ptr<LocalFunctionData> NSVWriteInitLocal(ExecutionContext &, FunctionData &) {
	return make_uniq<NSVWriteLocalState>();
}

static void NSVWriteSink(ExecutionContext &, FunctionData &bind_data, GlobalFunctionData &gstate, LocalFunctionData &,
                         DataChunk &input) {
	auto &bind = bind_data.Cast<NSVWriteBindData>();
	auto &state = gstate.Cast<NSVWriteGlobalState>();

	if (!state.header_written && bind.write_header) {
		for (auto &name : bind.names) {
			nsv_encoder_push_cell(state.encoder, reinterpret_cast<const uint8_t *>(name.data()), name.size());
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
				nsv_encoder_push_cell(state.encoder, reinterpret_cast<const uint8_t *>(str.data()), str.size());
			}
		}
		nsv_encoder_end_row(state.encoder);
	}
}

static void NSVWriteCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

static void NSVWriteFinalize(ClientContext &ctx, FunctionData &, GlobalFunctionData &gstate) {
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

void NsvExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string NsvExtension::Name() {
	return "nsv";
}

std::string NsvExtension::Version() const {
#ifdef EXT_VERSION_NSV
	return EXT_VERSION_NSV;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(nsv, loader) {
	duckdb::LoadInternal(loader);
}
}
