//! FFI bridge between nsv and the DuckDB C++ extension.
//!
//! Design: opaque handle, accessor-based. No file I/O here —
//! the caller (C++ side) reads the file and passes a byte buffer.
//!
//! Two decoding modes:
//! - `nsv_decode` — eager: decodes ALL cells up front (original API).
//! - `nsv_decode_lazy` — lazy: builds a structural index only. Cells are
//!   unescaped on demand via `nsv_lazy_cell`, enabling column projection
//!   to skip unescaping for columns the query doesn't need.
//!
//! Memory model:
//! - `nsv_decode` returns an owned `*mut NsvHandle` that must be freed with `nsv_free`.
//! - `nsv_cell` returns a pointer into the handle's internal storage — valid until `nsv_free`.
//! - `nsv_decode_lazy` returns `*mut LazyNsvHandle` — freed with `nsv_lazy_free`.
//! - `nsv_lazy_cell` returns a pointer valid until the next `nsv_lazy_cell` call on the
//!   same handle, or until `nsv_lazy_free`.

use std::ffi::CString;
use std::os::raw::c_char;

// ── Eager decode (original API, unchanged) ──────────────────────────

/// Opaque handle holding decoded NSV data.
pub struct NsvHandle {
    data: Vec<Vec<String>>,
}

/// Decode a byte buffer into an NSV handle (eager — all cells unescaped).
#[no_mangle]
pub extern "C" fn nsv_decode(ptr: *const u8, len: usize) -> *mut NsvHandle {
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let data = nsv::decode_bytes(bytes)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| {
                    String::from_utf8(cell)
                        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
                })
                .collect()
        })
        .collect();

    Box::into_raw(Box::new(NsvHandle { data }))
}

#[no_mangle]
pub extern "C" fn nsv_row_count(handle: *const NsvHandle) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).data.len() }
}

#[no_mangle]
pub extern "C" fn nsv_col_count(handle: *const NsvHandle, row: usize) -> usize {
    if handle.is_null() {
        return 0;
    }
    let h = unsafe { &*handle };
    h.data.get(row).map_or(0, |r| r.len())
}

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
            if !out_len.is_null() {
                unsafe { *out_len = cell.len() };
            }
            cell.as_ptr() as *const c_char
        }
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn nsv_free(handle: *mut NsvHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

// ── Lazy decode (new — column projection) ───────────────────────────

/// Opaque handle for lazily-decoded NSV data.
///
/// Stores the raw input bytes and a structural index (cell byte ranges).
/// Cells are only unescaped when accessed via `nsv_lazy_cell`.  A scratch
/// buffer is reused across calls to avoid per-cell allocation.
pub struct LazyNsvHandle {
    /// Owned copy of the raw input.
    input: Vec<u8>,
    /// Structural index: rows → cell spans.
    rows: Vec<Vec<nsv::CellSpan>>,
    /// Scratch buffer for the most recently unescaped cell.
    /// The pointer returned by `nsv_lazy_cell` points into this buffer
    /// and is valid until the next `nsv_lazy_cell` call or `nsv_lazy_free`.
    scratch: Vec<u8>,
}

/// Decode a byte buffer lazily — builds structural index without unescaping.
///
/// Returns null on null input.  Caller must free with `nsv_lazy_free`.
#[no_mangle]
pub extern "C" fn nsv_decode_lazy(ptr: *const u8, len: usize) -> *mut LazyNsvHandle {
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let input = bytes.to_vec();

    // CellSpan offsets are absolute byte positions within the input buffer.
    // decode_lazy borrows input, but into_rows() extracts the index as owned
    // data — the offsets remain valid against our owned `input` copy.
    let rows = nsv::decode_lazy(&input).into_rows();

    Box::into_raw(Box::new(LazyNsvHandle {
        input,
        rows,
        scratch: Vec::new(),
    }))
}

/// Number of rows in the lazily-decoded data.
#[no_mangle]
pub extern "C" fn nsv_lazy_row_count(handle: *const LazyNsvHandle) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).rows.len() }
}

/// Number of cells in `row`.
#[no_mangle]
pub extern "C" fn nsv_lazy_col_count(handle: *const LazyNsvHandle, row: usize) -> usize {
    if handle.is_null() {
        return 0;
    }
    let h = unsafe { &*handle };
    h.rows.get(row).map_or(0, |r| r.len())
}

/// Unescape and return the cell at `(row, col)`.
///
/// The returned pointer is valid until the next `nsv_lazy_cell` call on the
/// same handle, or until `nsv_lazy_free`.  `out_len` receives the byte
/// length (excluding null terminator).  Returns null if out of bounds.
///
/// SAFETY: this function takes `*mut` because it mutates the internal
/// scratch buffer.  Must not be called concurrently on the same handle.
#[no_mangle]
pub extern "C" fn nsv_lazy_cell(
    handle: *mut LazyNsvHandle,
    row: usize,
    col: usize,
    out_len: *mut usize,
) -> *const c_char {
    if handle.is_null() {
        return std::ptr::null();
    }
    let h = unsafe { &mut *handle };
    let span = match h.rows.get(row).and_then(|r| r.get(col)) {
        Some(s) => s,
        None => return std::ptr::null(),
    };

    // Unescape into scratch buffer
    h.scratch = nsv::unescape_bytes(&h.input[span.start..span.end]);
    // Best-effort UTF-8: replace invalid sequences
    // (DuckDB operates on strings; lossy is preferable to rejecting)
    let s = String::from_utf8_lossy(&h.scratch);
    if let std::borrow::Cow::Owned(owned) = s {
        h.scratch = owned.into_bytes();
    }

    if !out_len.is_null() {
        unsafe { *out_len = h.scratch.len() };
    }
    h.scratch.as_ptr() as *const c_char
}

/// Free a lazy handle returned by `nsv_decode_lazy`.
#[no_mangle]
pub extern "C" fn nsv_lazy_free(handle: *mut LazyNsvHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

// ── Encoder (unchanged) ─────────────────────────────────────────────

pub struct NsvEncoder {
    rows: Vec<Vec<String>>,
    current_row: Vec<String>,
}

#[no_mangle]
pub extern "C" fn nsv_encoder_new() -> *mut NsvEncoder {
    Box::into_raw(Box::new(NsvEncoder {
        rows: Vec::new(),
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
    let s = String::from_utf8(bytes.to_vec())
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    e.current_row.push(s);
}

#[no_mangle]
pub extern "C" fn nsv_encoder_push_null(enc: *mut NsvEncoder) {
    if enc.is_null() {
        return;
    }
    let e = unsafe { &mut *enc };
    e.current_row.push(String::new());
}

#[no_mangle]
pub extern "C" fn nsv_encoder_end_row(enc: *mut NsvEncoder) {
    if enc.is_null() {
        return;
    }
    let e = unsafe { &mut *enc };
    let row = std::mem::take(&mut e.current_row);
    e.rows.push(row);
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
        e.rows.push(row);
    }
    let encoded = nsv::encode(&e.rows);
    let bytes = encoded.into_bytes();
    let len = bytes.len();
    let boxed = bytes.into_boxed_slice();
    let ptr = Box::into_raw(boxed) as *mut u8;
    if !out_ptr.is_null() {
        unsafe { *out_ptr = ptr };
    }
    if !out_len.is_null() {
        unsafe { *out_len = len };
    }
}

#[no_mangle]
pub extern "C" fn nsv_free_buf(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len));
        };
    }
}

// ── Metadata ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn nsv_version() -> *mut c_char {
    CString::new(nsv::VERSION).unwrap().into_raw()
}

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
        nsv_free(std::ptr::null_mut());
    }

    #[test]
    fn test_lazy_decode_roundtrip() {
        let input = b"name\nage\n\nAlice\n30\n\nBob\n25\n\n";
        let handle = nsv_decode_lazy(input.as_ptr(), input.len());
        assert!(!handle.is_null());

        assert_eq!(nsv_lazy_row_count(handle), 3);
        assert_eq!(nsv_lazy_col_count(handle, 0), 2);
        assert_eq!(nsv_lazy_col_count(handle, 1), 2);

        let mut len = 0usize;
        let cell = nsv_lazy_cell(handle, 0, 0, &mut len as *mut usize);
        assert!(!cell.is_null());
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"name");

        let cell = nsv_lazy_cell(handle, 1, 0, &mut len as *mut usize);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"Alice");

        let cell = nsv_lazy_cell(handle, 2, 1, &mut len as *mut usize);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"25");

        nsv_lazy_free(handle);
    }

    #[test]
    fn test_lazy_matches_eager() {
        let input = b"a\n\\\nb\n\n\\\nc\n\\\n\nLine 1\\nLine 2\nBackslash: \\\\\n\n";
        let eager = nsv_decode(input.as_ptr(), input.len());
        let lazy = nsv_decode_lazy(input.as_ptr(), input.len());

        let nrows = nsv_row_count(eager);
        assert_eq!(nrows, nsv_lazy_row_count(lazy));

        for row in 0..nrows {
            let ncols = nsv_col_count(eager, row);
            assert_eq!(ncols, nsv_lazy_col_count(lazy, row));

            for col in 0..ncols {
                let mut elen = 0usize;
                let mut llen = 0usize;
                let ecell = nsv_cell(eager, row, col, &mut elen);
                let lcell = nsv_lazy_cell(lazy, row, col, &mut llen);

                assert_eq!(elen, llen, "row={} col={}", row, col);
                let es = unsafe { std::slice::from_raw_parts(ecell as *const u8, elen) };
                let ls = unsafe { std::slice::from_raw_parts(lcell as *const u8, llen) };
                assert_eq!(es, ls, "row={} col={}", row, col);
            }
        }

        nsv_free(eager);
        nsv_lazy_free(lazy);
    }

    #[test]
    fn test_lazy_null_safety() {
        assert!(nsv_decode_lazy(std::ptr::null(), 0).is_null());
        assert_eq!(nsv_lazy_row_count(std::ptr::null()), 0);
        assert_eq!(nsv_lazy_col_count(std::ptr::null(), 0), 0);
        assert!(nsv_lazy_cell(std::ptr::null_mut(), 0, 0, std::ptr::null_mut()).is_null());
        nsv_lazy_free(std::ptr::null_mut());
    }

    #[test]
    fn test_lazy_out_of_bounds() {
        let input = b"a\nb\n\n";
        let handle = nsv_decode_lazy(input.as_ptr(), input.len());

        let mut len = 0usize;
        assert!(nsv_lazy_cell(handle, 0, 5, &mut len).is_null());
        assert!(nsv_lazy_cell(handle, 99, 0, &mut len).is_null());

        nsv_lazy_free(handle);
    }
}
