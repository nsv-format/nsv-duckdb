// Simple test of the Rust FFI
#include "src/include/nsv_ffi.h"
#include <stdio.h>
#include <string.h>

int main() {
    // Test data
    const char *nsv_data = "name\nage\ncity\n\nAlice\n30\nNYC\n\nBob\n25\nSF\n";

    printf("Parsing NSV data...\n");
    NsvData *data = nsv_parse(nsv_data);

    if (!data) {
        printf("Failed to parse!\n");
        return 1;
    }

    size_t rows = nsv_row_count(data);
    printf("Rows: %zu\n", rows);

    for (size_t r = 0; r < rows; r++) {
        size_t cols = nsv_col_count(data, r);
        printf("Row %zu (%zu columns): ", r, cols);

        for (size_t c = 0; c < cols; c++) {
            char *cell = nsv_get_cell(data, r, c);
            if (cell) {
                printf("'%s' ", cell);
                nsv_free_string(cell);
            }
        }
        printf("\n");
    }

    nsv_free(data);
    printf("Success!\n");
    return 0;
}
