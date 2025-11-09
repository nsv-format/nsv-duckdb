#define DUCKDB_EXTENSION_MAIN

#include "nsv_extension.hpp"
#include "nsv_ffi.h"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <fstream>
#include <sstream>

namespace duckdb {

// Global state for reading NSV
struct ReadNSVData : public TableFunctionData {
	string filename;
	vector<string> column_names;
	vector<LogicalType> return_types;
	NsvData *parsed_data = nullptr;
	idx_t current_row = 1; // Skip header row

	~ReadNSVData() {
		if (parsed_data) {
			nsv_free(parsed_data);
		}
	}
};

// Bind function - called once to set up the scan
static unique_ptr<FunctionData> ReadNSVBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ReadNSVData>();

	// Get filename
	result->filename = StringValue::Get(input.inputs[0]);

	// Read file
	std::ifstream file(result->filename);
	if (!file.is_open()) {
		throw IOException("Cannot open file: " + result->filename);
	}

	std::stringstream buffer;
	buffer << file.rdbuf();
	string content = buffer.str();

	// Parse with Rust
	result->parsed_data = nsv_parse(content.c_str());
	if (!result->parsed_data) {
		throw IOException("Failed to parse NSV file");
	}

	size_t row_count = nsv_row_count(result->parsed_data);
	if (row_count == 0) {
		throw IOException("Empty NSV file");
	}

	// First row is header
	size_t col_count = nsv_col_count(result->parsed_data, 0);
	for (size_t i = 0; i < col_count; i++) {
		char *cell = nsv_get_cell(result->parsed_data, 0, i);
		if (cell) {
			result->column_names.push_back(string(cell));
			nsv_free_string(cell);
		} else {
			result->column_names.push_back("col" + std::to_string(i));
		}
	}

	// All columns are VARCHAR for now (can enhance with type detection later)
	for (size_t i = 0; i < col_count; i++) {
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back(result->column_names[i]);
	}

	result->return_types = return_types;
	return std::move(result);
}

// Init function - called once per thread
static unique_ptr<GlobalTableFunctionState> ReadNSVInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlobalTableFunctionState>();
}

// Scan function - called to read data chunks
static void ReadNSVScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ReadNSVData>();

	idx_t row_count = nsv_row_count(data.parsed_data);
	idx_t col_count = data.column_names.size();

	idx_t count = 0;
	while (data.current_row < row_count && count < STANDARD_VECTOR_SIZE) {
		// Check if row has correct column count
		size_t this_col_count = nsv_col_count(data.parsed_data, data.current_row);

		for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
			auto &vec = output.data[col_idx];

			if (col_idx < this_col_count) {
				char *cell = nsv_get_cell(data.parsed_data, data.current_row, col_idx);
				if (cell) {
					auto str_val = StringVector::AddString(vec, string(cell));
					FlatVector::GetData<string_t>(vec)[count] = str_val;
					nsv_free_string(cell);
				} else {
					FlatVector::SetNull(vec, count, true);
				}
			} else {
				// Missing column - set to NULL
				FlatVector::SetNull(vec, count, true);
			}
		}

		count++;
		data.current_row++;
	}

	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register read_nsv table function
	TableFunction read_nsv_func("read_nsv", {LogicalType::VARCHAR}, ReadNSVScan, ReadNSVBind, ReadNSVInit);
	read_nsv_func.name = "read_nsv";
	ExtensionUtil::RegisterFunction(loader, read_nsv_func);
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
	return "0.0.1";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(nsv, loader) {
	duckdb::LoadInternal(loader);
}
}
