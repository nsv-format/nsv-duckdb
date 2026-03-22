//! FFI bridge between nsv and the DuckDB C++ extension.
//!
//! Handle types:
//! - `SampleHandle` — decode a prefix (header + sample rows) for type sniffing.
//! - `NsvEncoder` — build NSV output cell-by-cell (COPY TO).
//!
//! Memory model:
//! - Decode functions return owned handles that must be freed with the matching free function.
//! - Cell pointers are valid until the handle is freed. For cells without escape sequences,
//!   they point directly into the input buffer (zero-copy via Cow::Borrowed).

use std::borrow::Cow;
use std::os::raw::c_char;

// ── Sample decode (bind-time: header + type sniffing) ───────────────

fn decode_sample(input: &[u8], max_rows: usize) -> Vec<Vec<Cow<'_, [u8]>>> {
    let mut data = Vec::new();
    let mut row: Vec<Cow<'_, [u8]>> = Vec::new();
    let mut start = 0;

    for (pos, &b) in input.iter().enumerate() {
        if b == b'\n' {
            if pos > start {
                row.push(nsv::unescape_bytes(&input[start..pos]));
            } else {
                data.push(row);
                row = Vec::new();
                if data.len() >= max_rows {
                    return data;
                }
            }
            start = pos + 1;
        }
    }

    if start < input.len() {
        row.push(nsv::unescape_bytes(&input[start..]));
    }
    if !row.is_empty() {
        data.push(row);
    }

    data
}

pub struct SampleHandle {
    _input: Box<[u8]>,
    data: Vec<Vec<Cow<'static, [u8]>>>,
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
    let input: Box<[u8]> = unsafe { std::slice::from_raw_parts(ptr, len) }.into();
    let data = decode_sample(&input, max_rows);
    let data: Vec<Vec<Cow<'static, [u8]>>> = unsafe { std::mem::transmute(data) };
    Box::into_raw(Box::new(SampleHandle { _input: input, data }))
}

#[no_mangle]
pub extern "C" fn nsv_sample_row_count(handle: *const SampleHandle) -> usize {
    if handle.is_null() { return 0; }
    unsafe { &*handle }.data.len()
}

#[no_mangle]
pub extern "C" fn nsv_sample_col_count(handle: *const SampleHandle, row: usize) -> usize {
    if handle.is_null() { return 0; }
    let h = unsafe { &*handle };
    h.data.get(row).map_or(0, |r| r.len())
}

#[no_mangle]
pub extern "C" fn nsv_sample_cell(
    handle: *const SampleHandle, row: usize, col: usize, out_len: *mut usize,
) -> *const c_char {
    if handle.is_null() { return std::ptr::null(); }
    let h = unsafe { &*handle };
    match h.data.get(row).and_then(|r| r.get(col)) {
        Some(cell) => {
            let bytes: &[u8] = cell;
            if !out_len.is_null() { unsafe { *out_len = bytes.len() }; }
            bytes.as_ptr() as *const c_char
        }
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn nsv_sample_free(handle: *mut SampleHandle) {
    if !handle.is_null() { unsafe { drop(Box::from_raw(handle)) }; }
}

// ── Encoding (COPY TO) ─────────────────────────────────────────────

/// Encode a seqseq (built cell-by-cell from C) into an NSV byte buffer.
///
/// Usage from C:
/// 1. `nsv_encoder_new()` → encoder handle
/// 2. `nsv_encoder_push_cell(enc, ptr, len)` for each cell
/// 3. `nsv_encoder_end_row(enc)` after each row
/// 4. `nsv_encoder_finish(enc, &out_ptr, &out_len)` → caller owns the buffer
///
/// The encoder handle is consumed by `nsv_encoder_finish`.
pub struct NsvEncoder {
    rows: Vec<Vec<Vec<u8>>>,
    current_row: Vec<Vec<u8>>,
    has_null: Vec<bool>,
}

#[no_mangle]
pub extern "C" fn nsv_encoder_new() -> *mut NsvEncoder {
    Box::into_raw(Box::new(NsvEncoder {
        rows: Vec::new(),
        current_row: Vec::new(),
        has_null: Vec::new(),
    }))
}

#[no_mangle]
pub extern "C" fn nsv_encoder_push_cell(
    enc: *mut NsvEncoder,
    ptr: *const u8,
    len: usize,
) {
    if enc.is_null() { return; }
    let e = unsafe { &mut *enc };
    if ptr.is_null() {
        e.current_row.push(Vec::new());
        e.has_null.push(true);
    } else {
        let cell = unsafe { std::slice::from_raw_parts(ptr, len) };
        e.current_row.push(cell.to_vec());
        e.has_null.push(false);
    }
}

#[no_mangle]
pub extern "C" fn nsv_encoder_push_null(enc: *mut NsvEncoder) {
    if enc.is_null() { return; }
    let e = unsafe { &mut *enc };
    e.current_row.push(Vec::new());
    e.has_null.push(true);
}

#[no_mangle]
pub extern "C" fn nsv_encoder_end_row(enc: *mut NsvEncoder) {
    if enc.is_null() { return; }
    let e = unsafe { &mut *enc };
    let row = std::mem::take(&mut e.current_row);
    e.rows.push(row);
    e.has_null.clear();
}

#[no_mangle]
pub extern "C" fn nsv_encoder_finish(
    enc: *mut NsvEncoder,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) {
    if enc.is_null() { return; }
    let e = unsafe { Box::from_raw(enc) };

    let encoded = nsv::encode_bytes(&e.rows);

    if !out_ptr.is_null() && !out_len.is_null() {
        let len = encoded.len();
        let boxed = encoded.into_boxed_slice();
        let ptr = Box::into_raw(boxed) as *mut u8;
        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }
    }
}

/// Free a buffer returned by `nsv_encoder_finish`.
#[no_mangle]
pub extern "C" fn nsv_free_buf(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len));
        }
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
    fn test_sample_max_rows() {
        let input = b"a\n\nb\n\nc\n\nd\n\n";
        let handle = nsv_decode_sample(input.as_ptr(), input.len(), 2);
        assert_eq!(nsv_sample_row_count(handle), 2);
        nsv_sample_free(handle);
    }

    #[test]
    fn test_encode_roundtrip() {
        let enc = nsv_encoder_new();
        nsv_encoder_push_cell(enc, b"hello".as_ptr(), 5);
        nsv_encoder_push_cell(enc, b"world".as_ptr(), 5);
        nsv_encoder_end_row(enc);
        nsv_encoder_push_cell(enc, b"foo".as_ptr(), 3);
        nsv_encoder_push_cell(enc, b"bar".as_ptr(), 3);
        nsv_encoder_end_row(enc);

        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        nsv_encoder_finish(enc, &mut out_ptr, &mut out_len);
        assert!(!out_ptr.is_null());
        assert!(out_len > 0);

        let output = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(output, b"hello\nworld\n\nfoo\nbar\n\n");
        nsv_free_buf(out_ptr, out_len);
    }

    #[test]
    fn test_null_safety() {
        assert!(nsv_decode_sample(std::ptr::null(), 0, 100).is_null());
        assert_eq!(nsv_sample_row_count(std::ptr::null()), 0);
        nsv_sample_free(std::ptr::null_mut());
    }
}
