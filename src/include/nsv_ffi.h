/* nsv_ffi.h — C interface to the nsv Rust library */
#ifndef NSV_FFI_H
#define NSV_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Eager reading (full decode) ─────────────────────────────────── */

/* Opaque handle to decoded NSV data (all cells eagerly unescaped). */
typedef struct NsvHandle NsvHandle;

/* Decode `len` bytes at `ptr` into an NSV handle.
 * The input need not be null-terminated.
 * Returns NULL on null input. Caller must free with nsv_free(). */
NsvHandle *nsv_decode(const uint8_t *ptr, size_t len);

/* Number of rows in the decoded data. */
size_t nsv_row_count(const NsvHandle *h);

/* Number of cells in row `row`. */
size_t nsv_col_count(const NsvHandle *h, size_t row);

/* Pointer to the cell string at (row, col).
 * Sets *out_len to the byte length (excluding any null terminator).
 * Returns NULL if out of bounds.
 * The pointer is valid until nsv_free(h). */
const char *nsv_cell(const NsvHandle *h, size_t row, size_t col, size_t *out_len);

/* Free a handle returned by nsv_decode(). */
void nsv_free(NsvHandle *h);

/* ── Projected reading (only requested columns) ──────────────────── */

/* Opaque handle for projected NSV data.
 * Cells are pre-decoded; pointers are stable until nsv_projected_free(). */
typedef struct ProjectedNsvHandle ProjectedNsvHandle;

/* Single-pass decode of selected columns only.
 * col_indices is an array of num_cols 0-based column indices.
 * Returns NULL on null input. Caller must free with nsv_projected_free(). */
ProjectedNsvHandle *nsv_decode_projected(const uint8_t *ptr, size_t len, const size_t *col_indices, size_t num_cols);

/* Number of rows in the projected data. */
size_t nsv_projected_row_count(const ProjectedNsvHandle *h);

/* Return pre-decoded cell at (row, proj_col).
 * proj_col is the index into the projected columns array (0-based),
 * NOT the original column index.
 * Pointer is stable until nsv_projected_free(). */
const char *nsv_projected_cell(const ProjectedNsvHandle *h, size_t row, size_t proj_col, size_t *out_len);

/* Free a handle returned by nsv_decode_projected(). */
void nsv_projected_free(ProjectedNsvHandle *h);

/* ── Writing ─────────────────────────────────────────────────────── */

/* Opaque encoder handle. */
typedef struct NsvEncoder NsvEncoder;

/* Create a new encoder. Caller must finish with nsv_encoder_finish(). */
NsvEncoder *nsv_encoder_new(void);

/* Append a cell (ptr, len) to the current row. */
void nsv_encoder_push_cell(NsvEncoder *enc, const uint8_t *ptr, size_t len);

/* Append a NULL cell (encoded as the empty string in NSV). */
void nsv_encoder_push_null(NsvEncoder *enc);

/* Terminate the current row. */
void nsv_encoder_end_row(NsvEncoder *enc);

/* Finish encoding, write the result to *out_ptr / *out_len.
 * Consumes (frees) the encoder.
 * Caller must free the buffer with nsv_free_buf(). */
void nsv_encoder_finish(NsvEncoder *enc, uint8_t **out_ptr, size_t *out_len);

/* Free a buffer returned by nsv_encoder_finish(). */
void nsv_free_buf(uint8_t *ptr, size_t len);

/* ── Metadata ────────────────────────────────────────────────────── */

/* Return the nsv library version as a C string.
 * Caller must free with nsv_free_string(). */
char *nsv_version(void);

/* Free a C string returned by nsv_version(). */
void nsv_free_string(char *s);

#ifdef __cplusplus
}
#endif

#endif /* NSV_FFI_H */
