//! FFI bridge between the nsv crate and the DuckDB C++ extension.
//!
//! Two API surfaces:
//! - `SampleHandle` — eager decode of a prefix (header + sample rows) for type sniffing.
//! - `nsv_decode_flat` — zero-allocation flat-buffer decode (scan-time, hot path).
//!
//! Memory model:
//! - `nsv_decode_sample` returns an owned `*mut SampleHandle`; free with `nsv_sample_free`.
//! - `nsv_decode_flat` writes into caller-provided arrays; unescaped cells go into a
//!   `NsvScratchBuf` that the caller frees with `nsv_scratch_free`.

use std::ffi::CString;
use std::os::raw::c_char;

// ── Sample decode (bind-time: header + type sniffing) ───────────────

pub struct SampleHandle {
    data: Vec<Vec<Vec<u8>>>,
}

#[no_mangle]
pub extern "C" fn nsv_decode_sample(
    ptr: *const u8,
    len: usize,
    max_rows: usize,
) -> *mut SampleHandle {
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    let input = unsafe { std::slice::from_raw_parts(ptr, len) };
    let mut reader = nsv::Reader::new(input);
    let mut data = Vec::new();
    while data.len() < max_rows {
        match reader.next_row() {
            Ok(Some(row)) => data.push(row),
            _ => break,
        }
    }
    Box::into_raw(Box::new(SampleHandle { data }))
}

#[no_mangle]
pub extern "C" fn nsv_sample_row_count(handle: *const SampleHandle) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { &*handle }.data.len()
}

#[no_mangle]
pub extern "C" fn nsv_sample_col_count(handle: *const SampleHandle, row: usize) -> usize {
    if handle.is_null() {
        return 0;
    }
    let h = unsafe { &*handle };
    h.data.get(row).map_or(0, |r| r.len())
}

#[no_mangle]
pub extern "C" fn nsv_sample_cell(
    handle: *const SampleHandle,
    row: usize,
    col: usize,
    out_len: *mut usize,
) -> *const c_char {
    if handle.is_null() {
        return std::ptr::null();
    }
    let h = unsafe { &*handle };
    match h.data.get(row).and_then(|r| r.get(col)) {
        Some(cell) => {
            if !out_len.is_null() {
                unsafe { *out_len = cell.len() };
            }
            cell.as_ptr() as *const c_char
        }
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn nsv_sample_free(handle: *mut SampleHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

// ── Flat-buffer decode (zero-allocation scan) ──────────────────────
//
// Writes cell locations into caller-provided flat arrays. For cells
// that need unescaping, the unescaped bytes go into a scratch buffer.
//
// Cell reference encoding in (offsets, lengths):
//   - Raw cell: offset = byte position in the original file buffer
//   - Escaped cell: offset = position in scratch buffer | SCRATCH_BIT
//   - Empty / missing cell: offset = 0, length = 0

const SCRATCH_BIT: usize = 1 << (usize::BITS - 1);

/// Scratch buffer for unescaped cell data.
pub struct NsvScratchBuf {
    data: Vec<u8>,
}

#[no_mangle]
pub extern "C" fn nsv_scratch_ptr(buf: *const NsvScratchBuf) -> *const u8 {
    if buf.is_null() {
        return std::ptr::null();
    }
    unsafe { &*buf }.data.as_ptr()
}

#[no_mangle]
pub extern "C" fn nsv_scratch_free(buf: *mut NsvScratchBuf) {
    if !buf.is_null() {
        unsafe { drop(Box::from_raw(buf)) };
    }
}

/// Build a column-map: col_map[original_col] = projected_index (or usize::MAX to skip).
fn build_col_map(columns: &[usize]) -> (Vec<usize>, usize) {
    let max_col = columns.iter().copied().max().unwrap_or(0);
    let mut col_map = vec![usize::MAX; max_col + 1];
    for (proj_idx, &orig_col) in columns.iter().enumerate() {
        col_map[orig_col] = proj_idx;
    }
    (col_map, max_col)
}

/// Decode a chunk of NSV into caller-provided flat arrays.
///
/// # Arguments
/// - `ptr`, `len`: input bytes (a range within the full file buffer)
/// - `input_base_offset`: byte offset of `ptr` within the full file buffer
/// - `col_indices`, `num_cols`: which original columns to project
/// - `needs_unescape`: per-projected-column flag (1 = VARCHAR, do unescape)
/// - `out_offsets`, `out_lengths`: flat arrays of size `max_rows * num_cols`
/// - `max_rows`: capacity of the output arrays
/// - `out_scratch`: receives a scratch buffer handle (caller frees)
/// - `out_bytes_consumed`: receives bytes consumed from input
///
/// Returns the number of rows decoded (<= max_rows).
#[no_mangle]
pub extern "C" fn nsv_decode_flat(
    ptr: *const u8,
    len: usize,
    input_base_offset: usize,
    col_indices: *const usize,
    num_cols: usize,
    needs_unescape: *const u8,
    out_offsets: *mut usize,
    out_lengths: *mut usize,
    max_rows: usize,
    out_scratch: *mut *mut NsvScratchBuf,
    out_bytes_consumed: *mut usize,
) -> usize {
    if ptr.is_null()
        || col_indices.is_null()
        || needs_unescape.is_null()
        || out_offsets.is_null()
        || out_lengths.is_null()
        || num_cols == 0
        || max_rows == 0
    {
        return 0;
    }

    let input = unsafe { std::slice::from_raw_parts(ptr, len) };
    let columns = unsafe { std::slice::from_raw_parts(col_indices, num_cols) };
    let unescape_flags = unsafe { std::slice::from_raw_parts(needs_unescape, num_cols) };
    let offsets = unsafe { std::slice::from_raw_parts_mut(out_offsets, max_rows * num_cols) };
    let lengths = unsafe { std::slice::from_raw_parts_mut(out_lengths, max_rows * num_cols) };

    let (col_map, max_col) = build_col_map(columns);

    let mut scratch = Vec::with_capacity(4096);
    let mut row_count: usize = 0;
    let mut col_idx: usize = 0;
    let mut start: usize = 0;
    let mut row_has_cells = false;
    let mut bytes_consumed: usize = 0;

    // Zero-initialize first row.
    for c in 0..num_cols {
        offsets[c] = 0;
        lengths[c] = 0;
    }

    for pos in 0..len {
        if input[pos] == b'\n' {
            if pos > start {
                // Non-empty cell
                if col_idx <= max_col && row_count < max_rows {
                    if let Some(&proj_idx) = col_map.get(col_idx) {
                        if proj_idx != usize::MAX {
                            let base = row_count * num_cols + proj_idx;
                            if unescape_flags[proj_idx] != 0 {
                                match nsv::unescape_bytes(&input[start..pos]) {
                                    std::borrow::Cow::Borrowed(_) => {
                                        offsets[base] = input_base_offset + start;
                                        lengths[base] = pos - start;
                                    }
                                    std::borrow::Cow::Owned(unescaped) => {
                                        let scratch_start = scratch.len();
                                        let ulen = unescaped.len();
                                        scratch.extend_from_slice(&unescaped);
                                        offsets[base] = scratch_start | SCRATCH_BIT;
                                        lengths[base] = ulen;
                                    }
                                }
                            } else {
                                offsets[base] = input_base_offset + start;
                                lengths[base] = pos - start;
                            }
                        }
                    }
                }
                col_idx += 1;
                row_has_cells = true;
            } else {
                // Empty cell = row boundary (\n\n)
                if row_has_cells {
                    row_count += 1;
                    bytes_consumed = pos + 1;
                    if row_count >= max_rows {
                        break;
                    }
                    // Zero-initialize next row.
                    let base = row_count * num_cols;
                    for c in 0..num_cols {
                        offsets[base + c] = 0;
                        lengths[base + c] = 0;
                    }
                }
                col_idx = 0;
                row_has_cells = false;
            }
            start = pos + 1;
        }
    }

    // Handle trailing data (no final \n\n).
    if row_count < max_rows && start < len {
        if col_idx <= max_col {
            if let Some(&proj_idx) = col_map.get(col_idx) {
                if proj_idx != usize::MAX {
                    let base = row_count * num_cols + proj_idx;
                    if unescape_flags[proj_idx] != 0 {
                        match nsv::unescape_bytes(&input[start..]) {
                            std::borrow::Cow::Borrowed(_) => {
                                offsets[base] = input_base_offset + start;
                                lengths[base] = len - start;
                            }
                            std::borrow::Cow::Owned(unescaped) => {
                                let scratch_start = scratch.len();
                                let ulen = unescaped.len();
                                scratch.extend_from_slice(&unescaped);
                                offsets[base] = scratch_start | SCRATCH_BIT;
                                lengths[base] = ulen;
                            }
                        }
                    } else {
                        offsets[base] = input_base_offset + start;
                        lengths[base] = len - start;
                    }
                }
            }
        }
        row_has_cells = true;
    }

    if row_count < max_rows && row_has_cells {
        row_count += 1;
        bytes_consumed = len;
    }

    if !out_bytes_consumed.is_null() {
        unsafe { *out_bytes_consumed = bytes_consumed };
    }

    if !out_scratch.is_null() {
        unsafe {
            *out_scratch = Box::into_raw(Box::new(NsvScratchBuf { data: scratch }));
        }
    }

    row_count
}

// ── Encoding (COPY TO) ─────────────────────────────────────────────

pub struct NsvEncoder {
    writer: nsv::Writer<Vec<u8>>,
    current_row: Vec<Vec<u8>>,
}

#[no_mangle]
pub extern "C" fn nsv_encoder_new() -> *mut NsvEncoder {
    Box::into_raw(Box::new(NsvEncoder {
        writer: nsv::Writer::new(Vec::new()),
        current_row: Vec::new(),
    }))
}

#[no_mangle]
pub extern "C" fn nsv_encoder_push_cell(enc: *mut NsvEncoder, ptr: *const u8, len: usize) {
    if enc.is_null() {
        return;
    }
    let e = unsafe { &mut *enc };
    if ptr.is_null() {
        e.current_row.push(Vec::new());
    } else {
        let cell = unsafe { std::slice::from_raw_parts(ptr, len) };
        e.current_row.push(cell.to_vec());
    }
}

#[no_mangle]
pub extern "C" fn nsv_encoder_push_null(enc: *mut NsvEncoder) {
    if enc.is_null() {
        return;
    }
    let e = unsafe { &mut *enc };
    e.current_row.push(Vec::new());
}

#[no_mangle]
pub extern "C" fn nsv_encoder_end_row(enc: *mut NsvEncoder) {
    if enc.is_null() {
        return;
    }
    let e = unsafe { &mut *enc };
    let row = std::mem::take(&mut e.current_row);
    let _ = e.writer.write_row(&row);
}

#[no_mangle]
pub extern "C" fn nsv_encoder_finish(
    enc: *mut NsvEncoder,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) {
    if enc.is_null() {
        return;
    }
    let mut e = unsafe { Box::from_raw(enc) };
    if !e.current_row.is_empty() {
        let row = std::mem::take(&mut e.current_row);
        let _ = e.writer.write_row(&row);
    }
    let buf = e.writer.into_inner();
    let len = buf.len();
    let boxed = buf.into_boxed_slice();
    let ptr = Box::into_raw(boxed) as *mut u8;
    if !out_ptr.is_null() {
        unsafe { *out_ptr = ptr };
    }
    if !out_len.is_null() {
        unsafe { *out_len = len };
    }
}

// ── Column-major chunk write ────────────────────────────────────────

#[no_mangle]
pub extern "C" fn nsv_write_chunk(
    cell_ptrs: *const *const u8,
    cell_lens: *const usize,
    null_masks: *const u8,
    nrows: usize,
    ncols: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) {
    if cell_ptrs.is_null()
        || cell_lens.is_null()
        || null_masks.is_null()
        || out_ptr.is_null()
        || out_len.is_null()
        || nrows == 0
        || ncols == 0
    {
        if !out_ptr.is_null() {
            unsafe { *out_ptr = std::ptr::null_mut() };
        }
        if !out_len.is_null() {
            unsafe { *out_len = 0 };
        }
        return;
    }

    let ptrs = unsafe { std::slice::from_raw_parts(cell_ptrs, ncols * nrows) };
    let lens = unsafe { std::slice::from_raw_parts(cell_lens, ncols * nrows) };
    let nulls = unsafe { std::slice::from_raw_parts(null_masks, ncols * nrows) };

    let mut escaped: Vec<std::borrow::Cow<'_, [u8]>> = Vec::with_capacity(ncols * nrows);
    for idx in 0..ncols * nrows {
        if nulls[idx] != 0 {
            escaped.push(std::borrow::Cow::Borrowed(b""));
        } else {
            let cell = unsafe { std::slice::from_raw_parts(ptrs[idx], lens[idx]) };
            escaped.push(nsv::escape_bytes(cell));
        }
    }

    let total_cell_bytes: usize = escaped.iter().map(|c| c.len()).sum();
    let mut buf = Vec::with_capacity(total_cell_bytes + ncols * nrows + nrows);

    for row in 0..nrows {
        for col in 0..ncols {
            let idx = col * nrows + row;
            buf.extend_from_slice(&escaped[idx]);
            buf.push(b'\n');
        }
        buf.push(b'\n');
    }

    let len = buf.len();
    let boxed = buf.into_boxed_slice();
    let ptr = Box::into_raw(boxed) as *mut u8;
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
}

/// Free a buffer returned by `nsv_encoder_finish` or `nsv_write_chunk`.
#[no_mangle]
pub extern "C" fn nsv_free_buf(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len));
        };
    }
}

/// Return the nsv library version as a C string. Caller must free with `nsv_free_string`.
#[no_mangle]
pub extern "C" fn nsv_version() -> *mut c_char {
    CString::new(nsv::VERSION).unwrap().into_raw()
}

/// Free a C string returned by `nsv_version`.
#[no_mangle]
pub extern "C" fn nsv_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_decode() {
        let input = b"name\nage\n\nAlice\n30\n\nBob\n25\n\n";
        let handle = nsv_decode_sample(input.as_ptr(), input.len(), 100);
        assert!(!handle.is_null());
        assert_eq!(nsv_sample_row_count(handle), 3);
        assert_eq!(nsv_sample_col_count(handle, 0), 2);

        let mut len = 0usize;
        let cell = nsv_sample_cell(handle, 0, 0, &mut len);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"name");

        let cell = nsv_sample_cell(handle, 1, 0, &mut len);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"Alice");

        nsv_sample_free(handle);
    }

    #[test]
    fn test_null_safety() {
        assert!(nsv_decode_sample(std::ptr::null(), 0, 100).is_null());
        assert_eq!(nsv_sample_row_count(std::ptr::null()), 0);
        nsv_sample_free(std::ptr::null_mut());
    }

    #[test]
    fn test_flat_decode_basic() {
        let input = b"name\nage\n\nAlice\n30\n\nBob\n25\n\n";
        let cols: [usize; 2] = [0, 1];
        let needs_unescape: [u8; 2] = [1, 0];
        let max_rows = 10;
        let mut offsets = vec![0usize; max_rows * 2];
        let mut lengths = vec![0usize; max_rows * 2];
        let mut scratch: *mut NsvScratchBuf = std::ptr::null_mut();
        let mut consumed: usize = 0;

        let rows = nsv_decode_flat(
            input.as_ptr(),
            input.len(),
            0,
            cols.as_ptr(),
            2,
            needs_unescape.as_ptr(),
            offsets.as_mut_ptr(),
            lengths.as_mut_ptr(),
            max_rows,
            &mut scratch,
            &mut consumed,
        );

        assert_eq!(rows, 3);
        assert_eq!(consumed, input.len());
        assert_eq!(lengths[0], 4); // "name"
        assert_eq!(lengths[1], 3); // "age"
        assert_eq!(lengths[2], 5); // "Alice"
        assert_eq!(lengths[3], 2); // "30"

        if !scratch.is_null() {
            nsv_scratch_free(scratch);
        }
    }

    #[test]
    fn test_flat_decode_max_rows() {
        let input = b"h\n\na\n\nb\n\nc\n\n";
        let cols: [usize; 1] = [0];
        let needs_unescape: [u8; 1] = [0];
        let max_rows = 2;
        let mut offsets = vec![0usize; max_rows];
        let mut lengths = vec![0usize; max_rows];
        let mut scratch: *mut NsvScratchBuf = std::ptr::null_mut();
        let mut consumed: usize = 0;

        let rows = nsv_decode_flat(
            input.as_ptr(),
            input.len(),
            0,
            cols.as_ptr(),
            1,
            needs_unescape.as_ptr(),
            offsets.as_mut_ptr(),
            lengths.as_mut_ptr(),
            max_rows,
            &mut scratch,
            &mut consumed,
        );
        assert_eq!(rows, 2);
        if !scratch.is_null() {
            nsv_scratch_free(scratch);
            scratch = std::ptr::null_mut();
        }

        // Resume from consumed offset
        let rows2 = nsv_decode_flat(
            unsafe { input.as_ptr().add(consumed) },
            input.len() - consumed,
            consumed,
            cols.as_ptr(),
            1,
            needs_unescape.as_ptr(),
            offsets.as_mut_ptr(),
            lengths.as_mut_ptr(),
            max_rows,
            &mut scratch,
            &mut consumed,
        );
        assert_eq!(rows2, 2);
        if !scratch.is_null() {
            nsv_scratch_free(scratch);
        }
    }

    #[test]
    fn test_flat_decode_unescape() {
        let input = b"line1\\nline2\n\n";
        let cols: [usize; 1] = [0];
        let needs_unescape: [u8; 1] = [1];
        let max_rows = 10;
        let mut offsets = vec![0usize; max_rows];
        let mut lengths = vec![0usize; max_rows];
        let mut scratch: *mut NsvScratchBuf = std::ptr::null_mut();
        let mut consumed: usize = 0;

        let rows = nsv_decode_flat(
            input.as_ptr(),
            input.len(),
            0,
            cols.as_ptr(),
            1,
            needs_unescape.as_ptr(),
            offsets.as_mut_ptr(),
            lengths.as_mut_ptr(),
            max_rows,
            &mut scratch,
            &mut consumed,
        );
        assert_eq!(rows, 1);
        assert!(offsets[0] & SCRATCH_BIT != 0, "should use scratch buffer");
        assert_eq!(lengths[0], 11); // "line1\nline2"

        if !scratch.is_null() {
            let scratch_data = nsv_scratch_ptr(scratch);
            let off = offsets[0] & !SCRATCH_BIT;
            let s = unsafe { std::slice::from_raw_parts(scratch_data.add(off), lengths[0]) };
            assert_eq!(s, b"line1\nline2");
            nsv_scratch_free(scratch);
        }
    }

    #[test]
    fn test_encode_roundtrip() {
        let enc = nsv_encoder_new();
        nsv_encoder_push_cell(enc, b"name".as_ptr(), 4);
        nsv_encoder_push_cell(enc, b"age".as_ptr(), 3);
        nsv_encoder_end_row(enc);
        nsv_encoder_push_cell(enc, b"Alice".as_ptr(), 5);
        nsv_encoder_push_cell(enc, b"30".as_ptr(), 2);
        nsv_encoder_end_row(enc);

        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        nsv_encoder_finish(enc, &mut out_ptr, &mut out_len);
        assert!(!out_ptr.is_null());
        let bytes = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(bytes, b"name\nage\n\nAlice\n30\n\n");
        nsv_free_buf(out_ptr, out_len);
    }
}
