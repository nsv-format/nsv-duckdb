//! FFI bridge between the nsv crate and the DuckDB C++ extension.
//!
//! Thin wrappers around `nsv::Reader` and `nsv::Writer`. No parsing logic here.
//!
//! Handle types:
//! - `SampleHandle` — decode a prefix (header + sample rows) for type sniffing.
//! - `NsvEncoder` — streaming NSV output for COPY TO.

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
    // write_row handles escaping and \n separators
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
    let e = unsafe { Box::from_raw(enc) };
    let buf = e.writer.into_inner();

    if !out_ptr.is_null() && !out_len.is_null() {
        let len = buf.len();
        let boxed = buf.into_boxed_slice();
        let ptr = Box::into_raw(boxed) as *mut u8;
        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }
    }
}

// ── Column-major chunk write (TEMPORARY — belongs in nsv crate) ─────
//
// Takes column-major cell data (as DuckDB provides it), escapes each cell
// via nsv::escape_bytes (Cow::Borrowed when clean, i.e. no copy), then
// writes row-major NSV output by transposing the escaped references.
//
// This avoids per-cell FFI calls and lets escape_bytes skip cells that
// need no escaping (the common case for numbers, dates, short strings
// without \n or \\).

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
    // Estimate output size: sum of all cell lengths + 2 bytes per cell (\n) + 1 per row (\n).
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
