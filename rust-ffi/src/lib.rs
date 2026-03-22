//! FFI bridge between nsv and the DuckDB C++ extension.
//!
//! Two handle types:
//! - `NsvHandle` — full eager decode (bind-time headers + type sniffing).
//! - `ProjectedNsvHandle` — single-pass decode of selected columns only (scan-time).
//!
//! Memory model:
//! - `nsv_decode` returns an owned `*mut NsvHandle` that must be freed with `nsv_free`.
//! - `nsv_cell` returns a pointer into the handle's internal storage — valid until `nsv_free`.
//!   For cells that didn't need unescaping, this points directly into the input buffer (zero-copy).
//!   For cells that did need unescaping, this points into the Cow's owned allocation.
//! - `nsv_encode` returns a malloc'd C string that must be freed with `nsv_free_string`.

use std::borrow::Cow;
use std::ffi::CString;
use std::os::raw::c_char;

/// Opaque handle holding decoded NSV data with zero-copy cells.
///
/// Owns the input buffer and stores decoded cells as `Cow<[u8]>`.
/// Borrowed cells point directly into `input` (zero-copy);
/// owned cells hold their own allocation (only when unescaping was needed).
pub struct NsvHandle {
    /// Pinned copy of the input bytes. Cow::Borrowed cells reference this.
    _input: Box<[u8]>,
    /// Decoded data. Lifetimes are tied to `_input` via unsafe transmute.
    data: Vec<Vec<Cow<'static, [u8]>>>,
}

/// Opaque handle for projected (column-subset) decode.
pub struct ProjectedNsvHandle {
    _input: Box<[u8]>,
    data: Vec<Vec<Cow<'static, [u8]>>>,
}

/// Decode a byte buffer into an NSV handle.
///
/// `ptr` must point to `len` readable bytes (not necessarily null-terminated).
/// Returns null on null input.
#[no_mangle]
pub extern "C" fn nsv_decode(ptr: *const u8, len: usize) -> *mut NsvHandle {
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };

    // Copy input into a stable heap allocation that won't move.
    let input: Box<[u8]> = bytes.into();
    let input_ref: &[u8] = &input;

    // SAFETY: `input` is heap-allocated and pinned inside NsvHandle.
    // The Cow::Borrowed variants point into `input`, which lives as long as
    // the NsvHandle. We transmute the lifetime to 'static to store them
    // in the struct. The pointers remain valid until NsvHandle is dropped.
    let data: Vec<Vec<Cow<'_, [u8]>>> = nsv::decode_bytes(input_ref);
    let data: Vec<Vec<Cow<'static, [u8]>>> = unsafe { std::mem::transmute(data) };

    Box::into_raw(Box::new(NsvHandle { _input: input, data }))
}

/// Number of rows in the decoded data.
#[no_mangle]
pub extern "C" fn nsv_row_count(handle: *const NsvHandle) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).data.len() }
}

/// Number of cells in `row`.
#[no_mangle]
pub extern "C" fn nsv_col_count(handle: *const NsvHandle, row: usize) -> usize {
    if handle.is_null() {
        return 0;
    }
    let h = unsafe { &*handle };
    h.data.get(row).map_or(0, |r| r.len())
}

/// Pointer to the cell bytes at `(row, col)`.
///
/// Returns null if out of bounds. The returned pointer is valid until
/// `nsv_free(handle)`. For cells without escape sequences, this points
/// directly into the input buffer (zero-copy). For escaped cells, it
/// points into the Cow's owned allocation.
///
/// `out_len` receives the byte length.
#[no_mangle]
pub extern "C" fn nsv_cell(
    handle: *const NsvHandle,
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
            let bytes: &[u8] = cell;
            if !out_len.is_null() {
                unsafe { *out_len = bytes.len() };
            }
            bytes.as_ptr() as *const c_char
        }
        None => std::ptr::null(),
    }
}

/// Free a handle returned by `nsv_decode`.
#[no_mangle]
pub extern "C" fn nsv_free(handle: *mut NsvHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

// ── Projected decode (scan-time) ────────────────────────────────────

#[no_mangle]
pub extern "C" fn nsv_decode_projected(
    ptr: *const u8, len: usize, col_indices: *const usize, num_cols: usize,
) -> *mut ProjectedNsvHandle {
    if ptr.is_null() || col_indices.is_null() || num_cols == 0 {
        return std::ptr::null_mut();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let columns = unsafe { std::slice::from_raw_parts(col_indices, num_cols) };

    let input: Box<[u8]> = bytes.into();
    let input_ref: &[u8] = &input;

    // SAFETY: same as nsv_decode — Cow borrows from `input` which is pinned
    // inside the handle.
    let data: Vec<Vec<Cow<'_, [u8]>>> = nsv::decode_bytes_projected(input_ref, columns);
    let data: Vec<Vec<Cow<'static, [u8]>>> = unsafe { std::mem::transmute(data) };

    Box::into_raw(Box::new(ProjectedNsvHandle { _input: input, data }))
}

#[no_mangle]
pub extern "C" fn nsv_projected_row_count(handle: *const ProjectedNsvHandle) -> usize {
    if handle.is_null() { return 0; }
    unsafe { &*handle }.data.len()
}

#[no_mangle]
pub extern "C" fn nsv_projected_cell(
    handle: *const ProjectedNsvHandle, row: usize, proj_col: usize, out_len: *mut usize,
) -> *const c_char {
    if handle.is_null() { return std::ptr::null(); }
    let h = unsafe { &*handle };
    match h.data.get(row).and_then(|r| r.get(proj_col)) {
        Some(cell) => {
            let bytes: &[u8] = cell;
            if !out_len.is_null() { unsafe { *out_len = bytes.len() }; }
            bytes.as_ptr() as *const c_char
        }
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn nsv_projected_free(handle: *mut ProjectedNsvHandle) {
    if !handle.is_null() { unsafe { drop(Box::from_raw(handle)) }; }
}

/// Streaming NSV encoder backed by `nsv::Writer`.
///
/// Usage from C:
/// 1. `nsv_encoder_new()` → encoder handle
/// 2. `nsv_encoder_push_cell(enc, ptr, len)` for each cell
/// 3. `nsv_encoder_end_row(enc)` at the end of each row
/// 4. `nsv_encoder_finish(enc, &out_ptr, &out_len)` → transfers ownership of the buffer
/// 5. `nsv_free_buf(out_ptr)` when done
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
    if enc.is_null() || ptr.is_null() {
        return;
    }
    let e = unsafe { &mut *enc };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    e.current_row.push(bytes.to_vec());
}

/// Push a NULL cell (empty string in NSV).
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

/// Finish encoding. Writes the output pointer and length.
/// The encoder is consumed (freed). Caller must free the buffer with `nsv_free_buf`.
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
    // Flush any pending row
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

// ── Column-major chunk write (TEMPORARY — belongs in nsv crate) ─────
//
// Takes column-major cell data (as DuckDB provides it), escapes each cell
// via nsv::escape_bytes (Cow::Borrowed when clean, i.e. no copy), then
// writes row-major NSV output by transposing the escaped references.

/// Write a chunk of rows from column-major cell arrays.
///
/// `cell_ptrs[col * nrows + row]` = pointer to cell bytes
/// `cell_lens[col * nrows + row]` = length of cell bytes
/// `null_masks[col * nrows + row]` = 1 if NULL, 0 otherwise
///
/// Returns an owned buffer containing the NSV output.
/// Caller must free with `nsv_free_buf`.
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
    if cell_ptrs.is_null() || cell_lens.is_null() || null_masks.is_null()
        || out_ptr.is_null() || out_len.is_null()
        || nrows == 0 || ncols == 0
    {
        if !out_ptr.is_null() { unsafe { *out_ptr = std::ptr::null_mut() }; }
        if !out_len.is_null() { unsafe { *out_len = 0 }; }
        return;
    }

    let ptrs = unsafe { std::slice::from_raw_parts(cell_ptrs, ncols * nrows) };
    let lens = unsafe { std::slice::from_raw_parts(cell_lens, ncols * nrows) };
    let nulls = unsafe { std::slice::from_raw_parts(null_masks, ncols * nrows) };

    // Phase 1: column-at-a-time escape. For each cell, escape_bytes returns
    // Cow::Borrowed (zero-copy) when no \n or \\ is present.
    let mut escaped: Vec<std::borrow::Cow<'_, [u8]>> = Vec::with_capacity(ncols * nrows);
    for idx in 0..ncols * nrows {
        if nulls[idx] != 0 {
            escaped.push(std::borrow::Cow::Borrowed(b""));
        } else {
            let cell = unsafe { std::slice::from_raw_parts(ptrs[idx], lens[idx]) };
            escaped.push(nsv::escape_bytes(cell));
        }
    }

    // Phase 2: transpose — write row-major output from column-major escaped data.
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
    fn test_decode_roundtrip() {
        let input = b"name\nage\n\nAlice\n30\n\nBob\n25\n\n";
        let handle = nsv_decode(input.as_ptr(), input.len());
        assert!(!handle.is_null());

        assert_eq!(nsv_row_count(handle), 3);
        assert_eq!(nsv_col_count(handle, 0), 2);
        assert_eq!(nsv_col_count(handle, 1), 2);

        let mut len = 0usize;
        let cell = nsv_cell(handle, 0, 0, &mut len as *mut usize);
        assert!(!cell.is_null());
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"name");

        let cell = nsv_cell(handle, 1, 0, &mut len as *mut usize);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"Alice");

        nsv_free(handle);
    }

    #[test]
    fn test_projected_decode() {
        let input = b"c0\nc1\nc2\nc3\n\na\nb\nc\nd\n\ne\nf\ng\nh\n\n";
        let cols: [usize; 2] = [0, 2];
        let handle = nsv_decode_projected(input.as_ptr(), input.len(), cols.as_ptr(), cols.len());
        assert!(!handle.is_null());
        assert_eq!(nsv_projected_row_count(handle), 3);
        let mut len = 0usize;
        let cell = nsv_projected_cell(handle, 1, 0, &mut len);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"a");
        let cell = nsv_projected_cell(handle, 1, 1, &mut len);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"c");
        nsv_projected_free(handle);
    }

    #[test]
    fn test_projected_matches_full() {
        let input = b"a\n\\\nb\n\n\\\nc\n\\\n\nLine 1\\nLine 2\nBackslash: \\\\\n\n";
        let full = nsv_decode(input.as_ptr(), input.len());
        let nrows = nsv_row_count(full);
        let cols: [usize; 3] = [0, 1, 2];
        let proj = nsv_decode_projected(input.as_ptr(), input.len(), cols.as_ptr(), cols.len());
        assert_eq!(nsv_projected_row_count(proj), nrows);
        for row in 0..nrows {
            for col in 0..nsv_col_count(full, row) {
                let mut flen = 0usize;
                let mut plen = 0usize;
                let fcell = nsv_cell(full, row, col, &mut flen);
                let pcell = nsv_projected_cell(proj, row, col, &mut plen);
                assert_eq!(flen, plen, "row={} col={}", row, col);
                if flen > 0 {
                    let fs = unsafe { std::slice::from_raw_parts(fcell as *const u8, flen) };
                    let ps = unsafe { std::slice::from_raw_parts(pcell as *const u8, plen) };
                    assert_eq!(fs, ps, "row={} col={}", row, col);
                }
            }
        }
        nsv_free(full);
        nsv_projected_free(proj);
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

    #[test]
    fn test_null_safety() {
        assert!(nsv_decode(std::ptr::null(), 0).is_null());
        assert_eq!(nsv_row_count(std::ptr::null()), 0);
        assert_eq!(nsv_col_count(std::ptr::null(), 0), 0);
        assert!(nsv_cell(std::ptr::null(), 0, 0, std::ptr::null_mut()).is_null());
        nsv_free(std::ptr::null_mut()); // should not crash
        assert!(nsv_decode_projected(std::ptr::null(), 0, std::ptr::null(), 0).is_null());
        assert_eq!(nsv_projected_row_count(std::ptr::null()), 0);
        assert!(nsv_projected_cell(std::ptr::null(), 0, 0, std::ptr::null_mut()).is_null());
        nsv_projected_free(std::ptr::null_mut());
    }

    #[test]
    fn test_zero_copy_clean_cells() {
        // Clean cells (no escaping) should borrow directly from input — verify
        // the pointer falls within the handle's input buffer range.
        let input = b"hello\nworld\n\nfoo\nbar\n\n";
        let handle = nsv_decode(input.as_ptr(), input.len());
        assert!(!handle.is_null());
        let h = unsafe { &*handle };

        // Check that borrowed cells' data pointers fall within _input's range.
        let input_start = h._input.as_ptr() as usize;
        let input_end = input_start + h._input.len();

        for row in &h.data {
            for cell in row {
                match cell {
                    Cow::Borrowed(b) => {
                        let cell_ptr = b.as_ptr() as usize;
                        assert!(cell_ptr >= input_start && cell_ptr < input_end,
                            "Borrowed cell should point into input buffer");
                    }
                    Cow::Owned(_) => {
                        panic!("Clean cell should be Cow::Borrowed, not Owned");
                    }
                }
            }
        }

        nsv_free(handle);
    }

    #[test]
    fn test_escaped_cells_are_owned() {
        // Cells that required unescaping should be Cow::Owned.
        let input = b"line1\\nline2\n\n";
        let handle = nsv_decode(input.as_ptr(), input.len());
        assert!(!handle.is_null());
        let h = unsafe { &*handle };

        let cell = &h.data[0][0];
        assert!(matches!(cell, Cow::Owned(_)), "Escaped cell should be Cow::Owned");

        // Verify the unescaped content is correct
        let mut len = 0usize;
        let ptr = nsv_cell(handle, 0, 0, &mut len);
        let s = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
        assert_eq!(s, b"line1\nline2");

        nsv_free(handle);
    }
}
