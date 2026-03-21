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

use memchr::memmem;
use rayon::prelude::*;

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

// ── Zero-copy projected decode ──────────────────────────────────────
//
// For typed (non-VARCHAR) columns: stores (offset, length) into the input
// buffer. No unescape, no allocation.
// For VARCHAR columns: unescaped via Cow (borrowed when clean, owned when escaped).
// Parallel scanning for inputs > 64KB.

const PARALLEL_THRESHOLD: usize = 64 * 1024;

enum CellRef {
    Raw(usize, usize),
    Unescaped(Vec<u8>),
}

pub struct ZeroCopyHandle {
    input: Vec<u8>,
    cells: Vec<Vec<CellRef>>,
}

fn build_col_map(columns: &[usize]) -> (Vec<usize>, usize) {
    let max_col = columns.iter().copied().max().unwrap_or(0);
    let mut col_map = vec![usize::MAX; max_col + 1];
    for (proj_idx, &orig_col) in columns.iter().enumerate() {
        col_map[orig_col] = proj_idx;
    }
    (col_map, max_col)
}

fn decode_zerocopy_sequential(
    input: &[u8],
    base_offset: usize,
    columns: &[usize],
    skip_unescape: &[bool],
) -> Vec<Vec<CellRef>> {
    let (col_map, max_col) = build_col_map(columns);
    let stride = columns.len();
    let mut data: Vec<Vec<CellRef>> = Vec::new();
    let mut row: Vec<CellRef> = (0..stride).map(|_| CellRef::Raw(0, 0)).collect();
    let mut col_idx: usize = 0;
    let mut start = 0;
    let mut row_has_cells = false;

    for (pos, &b) in input.iter().enumerate() {
        if b == b'\n' {
            if pos > start {
                if col_idx <= max_col {
                    if let Some(&proj_idx) = col_map.get(col_idx) {
                        if proj_idx != usize::MAX {
                            if skip_unescape[proj_idx] {
                                row[proj_idx] = CellRef::Raw(base_offset + start, pos - start);
                            } else {
                                row[proj_idx] = CellRef::Unescaped(
                                    nsv::unescape_bytes(&input[start..pos]).into_owned(),
                                );
                            }
                        }
                    }
                }
                col_idx += 1;
                row_has_cells = true;
            } else {
                if row_has_cells || !data.is_empty() || col_idx == 0 {
                    data.push(row);
                    row = (0..stride).map(|_| CellRef::Raw(0, 0)).collect();
                }
                col_idx = 0;
                row_has_cells = false;
            }
            start = pos + 1;
        }
    }

    if start < input.len() {
        if col_idx <= max_col {
            if let Some(&proj_idx) = col_map.get(col_idx) {
                if proj_idx != usize::MAX {
                    let cell_len = input.len() - start;
                    if skip_unescape[proj_idx] {
                        row[proj_idx] = CellRef::Raw(base_offset + start, cell_len);
                    } else {
                        row[proj_idx] = CellRef::Unescaped(
                            nsv::unescape_bytes(&input[start..]).into_owned(),
                        );
                    }
                }
            }
        }
        row_has_cells = true;
    }

    if row_has_cells {
        data.push(row);
    }

    data
}

fn decode_zerocopy_parallel(
    input: &[u8],
    columns: &[usize],
    skip_unescape: &[bool],
) -> Vec<Vec<CellRef>> {
    let num_threads = rayon::current_num_threads();
    let chunk_size = input.len() / num_threads;

    if chunk_size == 0 {
        return decode_zerocopy_sequential(input, 0, columns, skip_unescape);
    }

    let finder = memmem::Finder::new(b"\n\n");
    let mut splits = Vec::with_capacity(num_threads + 1);
    splits.push(0usize);

    for i in 1..num_threads {
        let nominal = i * chunk_size;
        if let Some(offset) = finder.find(&input[nominal..]) {
            let split = nominal + offset + 2;
            if split < input.len() {
                splits.push(split);
            }
        }
    }
    splits.push(input.len());
    splits.dedup();

    if splits.len() <= 2 {
        return decode_zerocopy_sequential(input, 0, columns, skip_unescape);
    }

    let chunks: Vec<(usize, &[u8])> = splits
        .windows(2)
        .map(|w| (w[0], &input[w[0]..w[1]]))
        .collect();

    let chunk_results: Vec<Vec<Vec<CellRef>>> = chunks
        .par_iter()
        .map(|&(base_offset, chunk)| {
            decode_zerocopy_sequential(chunk, base_offset, columns, skip_unescape)
        })
        .collect();

    let total_rows: usize = chunk_results.iter().map(|r| r.len()).sum();
    let mut result = Vec::with_capacity(total_rows);
    for chunk_rows in chunk_results {
        result.extend(chunk_rows);
    }
    result
}

#[no_mangle]
pub extern "C" fn nsv_decode_zerocopy(
    ptr: *const u8,
    len: usize,
    col_indices: *const usize,
    num_cols: usize,
    skip_unescape_flags: *const u8,
) -> *mut ZeroCopyHandle {
    if ptr.is_null() || col_indices.is_null() || num_cols == 0 || skip_unescape_flags.is_null() {
        return std::ptr::null_mut();
    }
    let input = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    let columns = unsafe { std::slice::from_raw_parts(col_indices, num_cols) };
    let raw_flags = unsafe { std::slice::from_raw_parts(skip_unescape_flags, num_cols) };
    let skip: Vec<bool> = raw_flags.iter().map(|&f| f != 0).collect();

    let cells = if input.len() < PARALLEL_THRESHOLD {
        decode_zerocopy_sequential(&input, 0, columns, &skip)
    } else {
        decode_zerocopy_parallel(&input, columns, &skip)
    };

    Box::into_raw(Box::new(ZeroCopyHandle { input, cells }))
}

#[no_mangle]
pub extern "C" fn nsv_zerocopy_row_count(handle: *const ZeroCopyHandle) -> usize {
    if handle.is_null() { return 0; }
    unsafe { &*handle }.cells.len()
}

#[no_mangle]
pub extern "C" fn nsv_zerocopy_cell(
    handle: *const ZeroCopyHandle, row: usize, proj_col: usize, out_len: *mut usize,
) -> *const c_char {
    if handle.is_null() { return std::ptr::null(); }
    let h = unsafe { &*handle };
    match h.cells.get(row).and_then(|r| r.get(proj_col)) {
        Some(cell) => {
            let (ptr, len) = match cell {
                CellRef::Raw(offset, length) => {
                    (h.input[*offset..].as_ptr(), *length)
                }
                CellRef::Unescaped(bytes) => {
                    (bytes.as_ptr(), bytes.len())
                }
            };
            if !out_len.is_null() { unsafe { *out_len = len }; }
            ptr as *const c_char
        }
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn nsv_zerocopy_free(handle: *mut ZeroCopyHandle) {
    if !handle.is_null() { unsafe { drop(Box::from_raw(handle)) }; }
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
