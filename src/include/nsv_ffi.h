/* nsv_ffi.h — C interface to the nsv Rust library (0.0.8+) */
#ifndef NSV_FFI_H
#define NSV_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Reading ─────────────────────────────────────────────────────── */

/* Opaque handle to decoded NSV data. */
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
const char *nsv_cell(const NsvHandle *h, size_t row, size_t col,
                     size_t *out_len);

/* Free a handle returned by nsv_decode(). */
void nsv_free(NsvHandle *h);

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
