//! FFI bridge between nsv 0.0.8 and the DuckDB C++ extension.
//!
//! Design: opaque handle, accessor-based. No file I/O here —
//! the caller (C++ side) reads the file and passes a byte buffer.
//!
//! Memory model:
//! - `nsv_decode` returns an owned `*mut NsvHandle` that must be freed with `nsv_free`.
//! - `nsv_cell` returns a pointer into the handle's internal storage — valid until `nsv_free`.
//! - `nsv_encode` returns a malloc'd C string that must be freed with `nsv_free_string`.

use std::ffi::CString;
use std::os::raw::c_char;

/// Opaque handle holding decoded NSV data.
///
/// Stores the raw bytes of each cell contiguously, with a null terminator
/// appended so that the C side can treat each cell as a C string directly
/// (via `nsv_cell`), avoiding per-access allocation.
pub struct NsvHandle {
    /// Decoded data as `Vec<Vec<String>>`.
    data: Vec<Vec<String>>,
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
    let data = nsv::decode_bytes(bytes)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| {
                    // Best-effort UTF-8. DuckDB operates on strings, so lossy is
                    // preferable to rejecting the whole file.
                    String::from_utf8(cell).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
                })
                .collect()
        })
        .collect();

    Box::into_raw(Box::new(NsvHandle { data }))
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

/// Pointer to the cell string at `(row, col)`.
///
/// Returns null if out of bounds. The returned pointer is valid until
/// `nsv_free(handle)`. The string is null-terminated (it's a Rust `String`
/// whose backing allocation we expose directly — we append a '\0' below).
///
/// `out_len` receives the byte length (excluding null terminator).
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

/// Free a handle returned by `nsv_decode`.
#[no_mangle]
pub extern "C" fn nsv_free(handle: *mut NsvHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

/// Encode a seqseq (built cell-by-cell from C) into an NSV byte buffer.
///
/// Usage from C:
/// 1. `nsv_encoder_new()` → encoder handle
/// 2. `nsv_encoder_push_cell(enc, ptr, len)` for each cell
/// 3. `nsv_encoder_end_row(enc)` at the end of each row
/// 4. `nsv_encoder_finish(enc, &out_ptr, &out_len)` → transfers ownership of the buffer
/// 5. `nsv_free_buf(out_ptr)` when done
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

/// Push a NULL cell (empty string in NSV).
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

/// Free a buffer returned by `nsv_encoder_finish`.
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
    }
}
