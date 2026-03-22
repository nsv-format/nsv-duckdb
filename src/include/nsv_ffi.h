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

/* ── Flat-buffer decode (zero-allocation scan) ───────────────────── */

/* High bit flag: when set on an offset, the cell data lives in the scratch
 * buffer rather than in the original input buffer. */
#define NSV_SCRATCH_BIT ((size_t)1 << (sizeof(size_t) * 8 - 1))

/* Opaque scratch buffer for unescaped cell data. */
typedef struct NsvScratchBuf NsvScratchBuf;

/* Get a pointer to the scratch buffer's data. */
const uint8_t *nsv_scratch_ptr(const NsvScratchBuf *buf);

/* Free a scratch buffer. */
void nsv_scratch_free(NsvScratchBuf *buf);

/* Decode a chunk of NSV into caller-provided flat arrays.
 *
 * out_offsets and out_lengths are row-major: cell (r, c) is at index
 * r * num_cols + c.  Caller must allocate max_rows * num_cols entries.
 *
 * For raw cells, offset is the byte position in the original file buffer.
 * For cells that were unescaped, offset has NSV_SCRATCH_BIT set and the
 * remaining bits are the offset into the scratch buffer.
 *
 * Returns the number of rows actually decoded (<= max_rows).
 * *out_scratch receives a handle that must be freed with nsv_scratch_free(). */
size_t nsv_decode_flat(
    const uint8_t *ptr, size_t len,
    size_t input_base_offset,
    const size_t *col_indices, size_t num_cols,
    const uint8_t *needs_unescape,
    size_t *out_offsets, size_t *out_lengths,
    size_t max_rows,
    NsvScratchBuf **out_scratch,
    size_t *out_bytes_consumed);

/* ── Writing ─────────────────────────────────────────────────────── */

typedef struct NsvEncoder NsvEncoder;

NsvEncoder *nsv_encoder_new(void);
void nsv_encoder_push_cell(NsvEncoder *enc, const uint8_t *ptr, size_t len);
void nsv_encoder_push_null(NsvEncoder *enc);
void nsv_encoder_end_row(NsvEncoder *enc);
void nsv_encoder_finish(NsvEncoder *enc, uint8_t **out_ptr, size_t *out_len);

void nsv_write_chunk(const uint8_t *const *cell_ptrs, const size_t *cell_lens,
                     const uint8_t *null_masks, size_t nrows, size_t ncols,
                     uint8_t **out_ptr, size_t *out_len);

void nsv_free_buf(uint8_t *ptr, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* NSV_FFI_H */
