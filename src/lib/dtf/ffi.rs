extern crate libc;

use std::{mem, slice, ptr};
use std::ffi::{CStr, CString};
use std::path::Path;

use csv::{DeserializeRecordsIntoIter, ReaderBuilder};
use dtf::{self, Update};
use self::libc::{c_char, c_uchar};

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

#[no_mangle]
pub extern fn read_dtf_to_csv(fname: *const c_char) -> *mut c_char {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let ups = dtf::decode(fname, None).unwrap();
    let data = dtf::update_vec_to_csv(&ups);

    let ret = String::from(data);
    let c_str_song = CString::new(ret).unwrap();
    c_str_song.into_raw()
}

#[no_mangle]
pub extern fn read_dtf_to_csv_with_limit(fname: *const c_char, num: u32) -> *mut c_char {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let ups = dtf::decode(fname, Some(num)).unwrap();
    let data = dtf::update_vec_to_csv(&ups);

    let ret = String::from(data);
    let c_str_song = CString::new(ret).unwrap();
    c_str_song.into_raw()
}

#[no_mangle]
pub extern fn read_dtf_to_arr(fname: *const c_char) -> Slice {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let mut ups = dtf::decode(fname, None).unwrap();

    let p = ups.as_mut_ptr();
    let len = ups.len();

    // so that no destructor is run on our vector
    mem::forget(ups);

    Slice { ptr: p, len: len }
}

#[no_mangle]
pub extern fn read_dtf_to_arr_with_limit(fname: *const c_char, num: u32) -> Slice {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let mut ups = dtf::decode(fname, Some(num)).unwrap();

    let p = ups.as_mut_ptr();
    let len = ups.len();

    // so that no destructor is run on our vector
    mem::forget(ups);

    Slice { ptr: p, len: len }
}

/// ```csv
/// id,exchange,symbol,date,price,amount,sell
/// 109797481,be,dashbtc,1498694478000,0.07154,0.40495999,false
/// ```
#[derive(Deserialize)]
struct KaikoCsvEntry {
    pub id: String,
    pub exchange: String,
    pub symbol: String,
    pub date: u64,
    pub price: f32,
    pub amount: f32,
    pub sell: Option<bool>,
}

impl Into<Update> for KaikoCsvEntry {
    fn into(self) -> Update {
        Update {
            ts: self.date,
            seq: self.id.parse().unwrap_or(0),
            is_trade: true,
            is_bid: !self.sell.unwrap_or(false),
            price: self.price,
            size: self.amount,
        }
    }
}

fn parse_kaiko_csv_to_dtf_inner(symbol: &str, filename: &str, csv_str: &str) -> Option<String> {
    let csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_str.as_bytes());

    // Parse the full CSV into a vector of `KaikoCsvEntry`s and make into `Update`s
    let iter: DeserializeRecordsIntoIter<_, KaikoCsvEntry> = csv_reader.into_deserialize();
    let size_hint = iter.size_hint().0;
    let mut updates: Vec<Update> = Vec::with_capacity(size_hint);

    for kaiko_entry_res in iter {
        match kaiko_entry_res {
            Ok(kaiko_entry) => updates.push(kaiko_entry.into()),
            Err(err) => { return Some(format!("{:?}", err)); }
        }
    }

    // Write or append the updates into the target DTF file
    let fpath = Path::new(&filename);
    let res = if fpath.exists() {
        dtf::append(filename, &updates)
    } else {
        dtf::encode(filename, symbol, &updates)
    };

    match res {
        Ok(_) => None,
        Err(err) => Some(format!("Error writing DTF to output file: {:?}", err)),
    }
}

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

#[no_mangle]
pub extern fn parse_stream(n: *mut c_uchar, len: u32) -> Slice {
    let mut byte_arr = unsafe {
        assert!(!n.is_null());
        slice::from_raw_parts(n, len as usize)
    };

    let mut v = dtf::decode_buffer(&mut byte_arr);

    let p = v.as_mut_ptr();
    let len = v.len();
    mem::forget(v);
    Slice { ptr: p, len: len }
}

#[no_mangle]
pub extern fn str_free(s: *mut c_char) {
    unsafe {
        if s.is_null() { return }
        CString::from_raw(s)
    };
}
