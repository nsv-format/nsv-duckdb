#ifndef NSV_FFI_H
#define NSV_FFI_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

// Opaque handle to parsed NSV data
typedef struct NsvData NsvData;

// Parse NSV string and return handle
NsvData *nsv_parse(const char *input);

// Get number of rows
size_t nsv_row_count(const NsvData *data);

// Get number of columns in a specific row
size_t nsv_col_count(const NsvData *data, size_t row);

// Get cell value as C string (must be freed with nsv_free_string)
char *nsv_get_cell(const NsvData *data, size_t row, size_t col);

// Free string returned by nsv_get_cell
void nsv_free_string(char *s);

// Free NsvData
void nsv_free(NsvData *data);

// Encode data to NSV format (must be freed with nsv_free_string)
char *nsv_encode(const NsvData *data);

#ifdef __cplusplus
}
#endif

#endif // NSV_FFI_H
