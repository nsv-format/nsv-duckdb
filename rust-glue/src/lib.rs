use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

#[repr(C)]
pub struct CNsvResult {
    pub rows: *mut *mut *mut c_char,
    pub nrows: usize,
    pub ncols: *mut usize,
    pub error: *mut c_char,
}

#[no_mangle]
pub extern "C" fn nsv_parse_file(filename: *const c_char) -> *mut CNsvResult {
    let filename_str = unsafe {
        match CStr::from_ptr(filename).to_str() {
            Ok(s) => s,
            Err(e) => {
                let err = CString::new(format!("Invalid UTF-8: {}", e)).unwrap();
                return Box::into_raw(Box::new(CNsvResult {
                    rows: ptr::null_mut(),
                    nrows: 0,
                    ncols: ptr::null_mut(),
                    error: err.into_raw(),
                }));
            }
        }
    };

    let content = match std::fs::read_to_string(filename_str) {
        Ok(c) => c,
        Err(e) => {
            let err = CString::new(format!("Error reading file: {}", e)).unwrap();
            return Box::into_raw(Box::new(CNsvResult {
                rows: ptr::null_mut(),
                nrows: 0,
                ncols: ptr::null_mut(),
                error: err.into_raw(),
            }));
        }
    };

    let data = nsv::loads(&content);
    let nrows = data.len();

    let mut rows_vec: Vec<*mut *mut c_char> = Vec::with_capacity(nrows);
    let mut ncols_vec: Vec<usize> = Vec::with_capacity(nrows);

    for row in data {
        let ncols = row.len();
        ncols_vec.push(ncols);
        let mut row_vec: Vec<*mut c_char> = Vec::with_capacity(ncols);
        for cell in row {
            row_vec.push(CString::new(cell).unwrap().into_raw());
        }
        rows_vec.push(row_vec.as_mut_ptr());
        std::mem::forget(row_vec);
    }

    let result = Box::into_raw(Box::new(CNsvResult {
        rows: rows_vec.as_mut_ptr(),
        nrows,
        ncols: ncols_vec.as_mut_ptr(),
        error: ptr::null_mut(),
    }));
    std::mem::forget(rows_vec);
    std::mem::forget(ncols_vec);
    result
}

#[no_mangle]
pub extern "C" fn nsv_free_result(result: *mut CNsvResult) {
    if result.is_null() {
        return;
    }

    unsafe {
        let result = Box::from_raw(result);

        if !result.error.is_null() {
            let _ = CString::from_raw(result.error);
            return;
        }

        // Free memory properly
        if !result.rows.is_null() && !result.ncols.is_null() {
            let rows_slice = std::slice::from_raw_parts_mut(result.rows, result.nrows);
            let ncols_slice = std::slice::from_raw_parts(result.ncols, result.nrows);

            for (row_ptr, &ncols) in rows_slice.iter_mut().zip(ncols_slice) {
                if !row_ptr.is_null() {
                    let row = std::slice::from_raw_parts_mut(*row_ptr, ncols);
                    for cell_ptr in row {
                        if !cell_ptr.is_null() {
                            let _ = CString::from_raw(*cell_ptr);
                        }
                    }
                }
            }
        }
    }
}
