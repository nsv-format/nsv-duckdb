//! FFI bridge between nsv and the DuckDB C++ extension.
//!
//! Three decoding modes:
//!
//! - `nsv_decode` — eager: decodes ALL cells up front.
//! - `nsv_decode_lazy` — lazy: structural index only, cells unescaped on
//!   demand.  Used at bind time for header/type sniffing.
//! - `nsv_decode_projected` — projected: single-pass decode of selected
//!   columns only.  Cells are pre-decoded; pointers are stable until free.

use std::ffi::CString;
use std::os::raw::c_char;

// ── Eager decode ────────────────────────────────────────────────────

pub struct NsvHandle {
    data: Vec<Vec<String>>,
}

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

// ── Lazy decode (bind-time header/type sniffing) ────────────────────

pub struct LazyNsvHandle {
    input: Vec<u8>,
    rows: Vec<Vec<nsv::CellSpan>>,
    scratch: Vec<u8>,
}

#[no_mangle]
pub extern "C" fn nsv_decode_lazy(ptr: *const u8, len: usize) -> *mut LazyNsvHandle {
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let input = bytes.to_vec();
    let rows = nsv::decode_lazy(&input).into_rows();
    Box::into_raw(Box::new(LazyNsvHandle {
        input,
        rows,
        scratch: Vec::new(),
    }))
}

#[no_mangle]
pub extern "C" fn nsv_lazy_row_count(handle: *const LazyNsvHandle) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).rows.len() }
}

#[no_mangle]
pub extern "C" fn nsv_lazy_col_count(handle: *const LazyNsvHandle, row: usize) -> usize {
    if handle.is_null() {
        return 0;
    }
    let h = unsafe { &*handle };
    h.rows.get(row).map_or(0, |r| r.len())
}

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
    h.scratch = nsv::unescape_bytes(&h.input[span.start..span.end]);
    let s = String::from_utf8_lossy(&h.scratch);
    if let std::borrow::Cow::Owned(owned) = s {
        h.scratch = owned.into_bytes();
    }
    if !out_len.is_null() {
        unsafe { *out_len = h.scratch.len() };
    }
    h.scratch.as_ptr() as *const c_char
}

/// Pointer to the raw input bytes owned by the lazy handle.
#[no_mangle]
pub extern "C" fn nsv_lazy_input_ptr(handle: *const LazyNsvHandle) -> *const u8 {
    if handle.is_null() {
        return std::ptr::null();
    }
    unsafe { (*handle).input.as_ptr() }
}

/// Length of the raw input bytes owned by the lazy handle.
#[no_mangle]
pub extern "C" fn nsv_lazy_input_len(handle: *const LazyNsvHandle) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).input.len() }
}

#[no_mangle]
pub extern "C" fn nsv_lazy_free(handle: *mut LazyNsvHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

// ── Projected decode (pre-decoded cells, stable pointers) ───────────

/// Pre-decoded projected data.  Cells are already unescaped and UTF-8
/// validated.  Pointers returned by `nsv_projected_cell` are stable
/// until `nsv_projected_free`.
pub struct ProjectedNsvHandle {
    data: Vec<Vec<String>>,
}

/// Single-pass decode of selected columns only.
///
/// `col_indices` is an array of `num_cols` 0-based column indices.
/// Caller must free with `nsv_projected_free`.
#[no_mangle]
pub extern "C" fn nsv_decode_projected(
    ptr: *const u8,
    len: usize,
    col_indices: *const usize,
    num_cols: usize,
) -> *mut ProjectedNsvHandle {
    if ptr.is_null() || col_indices.is_null() || num_cols == 0 {
        return std::ptr::null_mut();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let columns = unsafe { std::slice::from_raw_parts(col_indices, num_cols) };

    let data = nsv::decode_bytes_projected(bytes, columns)
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

    Box::into_raw(Box::new(ProjectedNsvHandle { data }))
}

#[no_mangle]
pub extern "C" fn nsv_projected_row_count(handle: *const ProjectedNsvHandle) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).data.len() }
}

/// Return the pre-decoded cell at `(row, proj_col)`.
///
/// `proj_col` is the index into the projected columns array (0-based),
/// NOT the original column index.  Pointer is stable until free.
#[no_mangle]
pub extern "C" fn nsv_projected_cell(
    handle: *const ProjectedNsvHandle,
    row: usize,
    proj_col: usize,
    out_len: *mut usize,
) -> *const c_char {
    if handle.is_null() {
        return std::ptr::null();
    }
    let h = unsafe { &*handle };
    match h.data.get(row).and_then(|r| r.get(proj_col)) {
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
pub extern "C" fn nsv_projected_free(handle: *mut ProjectedNsvHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

// ── Encoder ─────────────────────────────────────────────────────────

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

        let mut len = 0usize;
        let cell = nsv_cell(handle, 0, 0, &mut len as *mut usize);
        assert!(!cell.is_null());
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"name");

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

        let mut len = 0usize;
        let cell = nsv_lazy_cell(handle, 0, 0, &mut len as *mut usize);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"name");

        let cell = nsv_lazy_cell(handle, 1, 0, &mut len as *mut usize);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"Alice");

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
    fn test_projected_decode() {
        let input = b"c0\nc1\nc2\nc3\n\na\nb\nc\nd\n\ne\nf\ng\nh\n\n";
        let cols: [usize; 2] = [0, 2];
        let handle = nsv_decode_projected(
            input.as_ptr(),
            input.len(),
            cols.as_ptr(),
            cols.len(),
        );
        assert!(!handle.is_null());
        assert_eq!(nsv_projected_row_count(handle), 3);

        let mut len = 0usize;
        let cell = nsv_projected_cell(handle, 0, 0, &mut len);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"c0");

        let cell = nsv_projected_cell(handle, 0, 1, &mut len);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"c2");

        let cell = nsv_projected_cell(handle, 1, 0, &mut len);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"a");

        let cell = nsv_projected_cell(handle, 1, 1, &mut len);
        let s = unsafe { std::slice::from_raw_parts(cell as *const u8, len) };
        assert_eq!(s, b"c");

        nsv_projected_free(handle);
    }

    #[test]
    fn test_projected_matches_lazy() {
        let input = b"a\n\\\nb\n\n\\\nc\n\\\n\nLine 1\\nLine 2\nBackslash: \\\\\n\n";
        let lazy = nsv_decode_lazy(input.as_ptr(), input.len());
        let nrows = nsv_lazy_row_count(lazy);

        let cols: [usize; 3] = [0, 1, 2];
        let proj = nsv_decode_projected(
            input.as_ptr(),
            input.len(),
            cols.as_ptr(),
            cols.len(),
        );
        assert_eq!(nsv_projected_row_count(proj), nrows);

        for row in 0..nrows {
            for col in 0..nsv_lazy_col_count(lazy, row) {
                let mut llen = 0usize;
                let mut plen = 0usize;
                let lcell = nsv_lazy_cell(lazy, row, col, &mut llen);
                let pcell = nsv_projected_cell(proj, row, col, &mut plen);
                assert_eq!(llen, plen, "row={} col={}", row, col);
                if llen > 0 {
                    let ls = unsafe { std::slice::from_raw_parts(lcell as *const u8, llen) };
                    let ps = unsafe { std::slice::from_raw_parts(pcell as *const u8, plen) };
                    assert_eq!(ls, ps, "row={} col={}", row, col);
                }
            }
        }

        nsv_lazy_free(lazy);
        nsv_projected_free(proj);
    }

    #[test]
    fn test_projected_null_safety() {
        assert!(nsv_decode_projected(std::ptr::null(), 0, std::ptr::null(), 0).is_null());
        assert_eq!(nsv_projected_row_count(std::ptr::null()), 0);
        assert!(nsv_projected_cell(std::ptr::null(), 0, 0, std::ptr::null_mut()).is_null());
        nsv_projected_free(std::ptr::null_mut());
    }
}
