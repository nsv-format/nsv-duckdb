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

#include "nsv_ffi.h"

namespace duckdb {

// ── Type detection ──────────────────────────────────────────────────

static const vector<LogicalType> TYPE_CANDIDATES = {
    LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::DOUBLE, LogicalType::DATE, LogicalType::TIMESTAMP,
    LogicalType::VARCHAR // fallback — always succeeds
};

static LogicalType DetectColumnType(ClientContext &ctx, NsvHandle *data, idx_t col_idx, idx_t start_row,
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
	//! Maps output column index → source column index (from projection pushdown).
	vector<column_t> column_ids;
	//! Projected handle — pre-decoded, only requested columns.
	ProjectedNsvHandle *projected = nullptr;

	~NSVScanState() {
		if (projected) {
			nsv_projected_free(projected);
		}
	}
};

static unique_ptr<FunctionData> NSVBind(ClientContext &ctx, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
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
	    nsv_decode(reinterpret_cast<const uint8_t *>(result->raw_buffer.data()), result->raw_buffer.size());
	if (!result->handle) {
		throw InvalidInputException("Failed to parse NSV file: %s", result->filename);
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

static unique_ptr<GlobalTableFunctionState> NSVInit(ClientContext &ctx, TableFunctionInitInput &input) {
	auto state = make_uniq<NSVScanState>();
	state->column_ids = input.column_ids;

	// Build a projected handle from the raw buffer using the column_ids
	// from projection pushdown.  Single-pass decode of selected columns only.
	auto &bind = input.bind_data->Cast<NSVBindData>();

	if (!bind.raw_buffer.empty() && !state->column_ids.empty()) {
		vector<size_t> col_indices;
		col_indices.reserve(state->column_ids.size());
		for (auto &cid : state->column_ids) {
			col_indices.push_back(static_cast<size_t>(cid));
		}
		state->projected = nsv_decode_projected(reinterpret_cast<const uint8_t *>(bind.raw_buffer.data()),
		                                        bind.raw_buffer.size(), col_indices.data(), col_indices.size());
	}

	return std::move(state);
}

static void NSVScan(ClientContext &ctx, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->Cast<NSVBindData>();
	auto &state = input.global_state->Cast<NSVScanState>();

	// Skip header row
	if (state.current_row == 0) {
		state.current_row = 1;
	}

	// Use projected handle if available, otherwise fall back to eager handle.
	bool use_projected = (state.projected != nullptr);
	idx_t total_rows = use_projected ? nsv_projected_row_count(state.projected) : nsv_row_count(bind.handle);
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, total_rows - state.current_row);
	if (count == 0) {
		output.SetCardinality(0);
		return;
	}

	for (idx_t out_col = 0; out_col < output.ColumnCount(); out_col++) {
		idx_t src_col = state.column_ids[out_col];
		auto &vec = output.data[out_col];
		const auto &target_type = bind.types[src_col];

		for (idx_t i = 0; i < count; i++) {
			idx_t row_idx = state.current_row + i;

			size_t cell_len = 0;
			const char *cell;

			if (use_projected) {
				// proj_col = out_col (projected handle stores columns in output order)
				cell = nsv_projected_cell(state.projected, row_idx, out_col, &cell_len);
			} else {
				// Fallback: direct access by source column
				if (src_col >= nsv_col_count(bind.handle, row_idx)) {
					vec.SetValue(i, Value());
					continue;
				}
				cell = nsv_cell(bind.handle, row_idx, src_col, &cell_len);
			}

			if (!cell || cell_len == 0) {
				vec.SetValue(i, Value());
				continue;
			}

			Value str_val(string(cell, cell_len));
			if (target_type == LogicalType::VARCHAR) {
				vec.SetValue(i, str_val);
			} else {
				Value result_val;
				string error_msg;
				if (str_val.TryCastAs(ctx, target_type, result_val, &error_msg, false)) {
					vec.SetValue(i, result_val);
				} else {
					vec.SetValue(i, Value());
				}
			}
		}
	}

	output.SetCardinality(count);
	state.current_row += count;
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

	// Write header row on first call
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
	// single-threaded write, nothing to combine
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
	// read_nsv table function with projection pushdown
	TableFunction read_nsv("read_nsv", {LogicalType::VARCHAR}, NSVScan, NSVBind);
	read_nsv.init_global = NSVInit;
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
