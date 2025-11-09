//! FFI interface for NSV parsing using nsv crate from crates.io

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

/// Opaque handle to parsed NSV data
pub struct NsvData {
    rows: Vec<Vec<String>>,
}

/// Parse NSV string and return handle
#[no_mangle]
pub unsafe extern "C" fn nsv_parse(input: *const c_char) -> *mut NsvData {
    if input.is_null() {
        return ptr::null_mut();
    }

    let c_str = CStr::from_ptr(input);
    let input_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let rows = nsv::loads(input_str);
    let data = Box::new(NsvData { rows });
    Box::into_raw(data)
}

/// Get number of rows
#[no_mangle]
pub unsafe extern "C" fn nsv_row_count(data: *const NsvData) -> usize {
    if data.is_null() {
        return 0;
    }
    (*data).rows.len()
}

/// Get number of columns in a specific row
#[no_mangle]
pub unsafe extern "C" fn nsv_col_count(data: *const NsvData, row: usize) -> usize {
    if data.is_null() {
        return 0;
    }
    let data_ref = &*data;
    if row >= data_ref.rows.len() {
        return 0;
    }
    data_ref.rows[row].len()
}

/// Get cell value as C string
#[no_mangle]
pub unsafe extern "C" fn nsv_get_cell(
    data: *const NsvData,
    row: usize,
    col: usize,
) -> *mut c_char {
    if data.is_null() {
        return ptr::null_mut();
    }

    let data_ref = &*data;
    if row >= data_ref.rows.len() || col >= data_ref.rows[row].len() {
        return ptr::null_mut();
    }

    let cell = &data_ref.rows[row][col];
    match CString::new(cell.as_str()) {
        Ok(c_string) => c_string.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Free string returned by nsv_get_cell
#[no_mangle]
pub unsafe extern "C" fn nsv_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

/// Free NsvData
#[no_mangle]
pub unsafe extern "C" fn nsv_free(data: *mut NsvData) {
    if !data.is_null() {
        drop(Box::from_raw(data));
    }
}

/// Encode data to NSV format
#[no_mangle]
pub unsafe extern "C" fn nsv_encode(data: *const NsvData) -> *mut c_char {
    if data.is_null() {
        return ptr::null_mut();
    }

    let data_ref = &*data;
    let encoded = nsv::dumps(&data_ref.rows);

    match CString::new(encoded) {
        Ok(c_string) => c_string.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}
