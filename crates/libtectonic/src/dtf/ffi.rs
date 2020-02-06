extern crate libc;

use std::{mem, slice, ptr};
use std::ffi::{CStr, CString};

use std::io::Cursor;

use crate::dtf::{
    update::{
        Update,
        UpdateVecConvert
    },
    file_format::{
        decode,
        decode_buffer,
    },
};
use crate::storage::filetype::parse_kaiko_csv_to_dtf_inner;
use libc::{c_char, c_uchar};

/// struct containting a pointer to an array of `Update` and length of slice
#[repr(C)]
pub struct Slice {
    ptr: *mut Update,
    len: usize,
}

/// Takes a pointer to a string from C and copies it into a Rust-owned `CString`.
unsafe fn ptr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, ()> {
    if ptr.is_null() { return Err(()) }
    CStr::from_ptr(ptr).to_str().map_err(|_| ())
}

/// Convert all the `Update`s in DTF file to CSV
///     returns a C char pointer
#[no_mangle]
pub extern fn read_dtf_to_csv(fname: *const c_char) -> *mut c_char {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let ups = decode(fname, None).unwrap();
    let data = ups.to_csv();

    let ret = String::from(data);
    let c_str_song = CString::new(ret).unwrap();
    c_str_song.into_raw()
}

/// Convert at most `num` `Update`s in DTF file to CSV
///     returns a C char pointer
#[no_mangle]
pub extern fn read_dtf_to_csv_with_limit(fname: *const c_char, num: u32) -> *mut c_char {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let ups = decode(fname, Some(num)).unwrap();
    let data = ups.to_csv();

    let ret = String::from(data);
    let c_str_song = CString::new(ret).unwrap();
    c_str_song.into_raw()
}

/// Convert all the Updates in DTF file to an array
///     returns a Slice
#[no_mangle]
pub extern fn read_dtf_to_arr(fname: *const c_char) -> Slice {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let mut ups = decode(fname, None).unwrap();

    let p = ups.as_mut_ptr();
    let len = ups.len();

    // so that no destructor is run on our vector
    mem::forget(ups);

    Slice { ptr: p, len: len }
}

/// Convert at most `num` Updates in DTF file to an array
///     returns a Slice
#[no_mangle]
pub extern fn read_dtf_to_arr_with_limit(fname: *const c_char, num: u32) -> Slice {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let mut ups = decode(fname, Some(num)).unwrap();

    let p = ups.as_mut_ptr();
    let len = ups.len();

    // so that no destructor is run on our vector
    mem::forget(ups);

    Slice { ptr: p, len: len }
}


/// This is for converting kaiko csv data into DTF.
///
/// Given an output filename and a string containing input CSV to parse, parses the CSV into DTF and
/// writes it to the output file.  If the file exists, the data will be appended.
///
/// If an error occurs, will return a pointer to a string containing the error message.  If the process
/// completes successfully, will return `nullptr`.
#[no_mangle]
pub unsafe extern "C" fn parse_kaiko_csv_to_dtf(
    symbol: *const c_char,
    fname: *const c_char,
    csv_str: *const c_char
) -> *const c_char {
    let symbol = match ptr_to_str(symbol) {
        Ok(symbol) => symbol,
        Err(()) => { return CString::new("Symbol was invalid.").unwrap().into_raw() },
    };
    let fname = match ptr_to_str(fname) {
        Ok(fname) => fname,
        Err(()) => { return CString::new("Filename was invalid.").unwrap().into_raw() },
    };
    let csv_str = match ptr_to_str(csv_str) {
        Ok(csv_str) => csv_str,
        Err(()) => { return CString::new("CSV String was invalid.").unwrap().into_raw() },
    };

    match parse_kaiko_csv_to_dtf_inner(symbol, fname, csv_str) {
        Some(err) => CString::new(err).unwrap().into_raw(),
        None => ptr::null(),
    }
}

/// decode a buffer of size `len` to Slice
#[no_mangle]
pub extern fn parse_stream(n: *mut c_uchar, len: u32) -> Slice {
    let byte_arr = unsafe {
        assert!(!n.is_null());
        slice::from_raw_parts(n, len as usize)
    };
    let mut rdr = Cursor::new(byte_arr);

    let mut v = decode_buffer(&mut rdr);

    let p = v.as_mut_ptr();
    let len = v.len();
    mem::forget(v);
    Slice { ptr: p, len: len }
}

/// free a c string
#[no_mangle]
pub extern fn str_free(s: *mut c_char) {
    unsafe {
        if s.is_null() { return }
        CString::from_raw(s)
    };
}
