/* nsv_ffi.h — C interface to the nsv Rust library */
#ifndef NSV_FFI_H
#define NSV_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Sample decode (bind-time) ────────────────────────────────────── */

typedef struct SampleHandle SampleHandle;

SampleHandle *nsv_decode_sample(const uint8_t *ptr, size_t len,
                                size_t max_rows);
size_t nsv_sample_row_count(const SampleHandle *h);
size_t nsv_sample_col_count(const SampleHandle *h, size_t row);
const char *nsv_sample_cell(const SampleHandle *h, size_t row, size_t col,
                            size_t *out_len);
void nsv_sample_free(SampleHandle *h);

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

#ifdef __cplusplus
}
#endif

#endif /* NSV_FFI_H */
