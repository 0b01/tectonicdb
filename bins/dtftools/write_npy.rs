use std::io::Write;

static MAGIC_VALUE: &[u8] = &[0x93, 0x4E, 0x55, 0x4D, 0x50, 0x59];

pub fn write_header(wtr: &mut dyn Write, numpy_type: &str, len: u64) {
    let _ = wtr.write(MAGIC_VALUE);
    let _ = wtr.write(&[0x01, 0x00]); // major version, minor version
    let header = format!(
        "{{'descr':[('data','{}')],'fortran_order':False,'shape':({},)}}",
        numpy_type, len
    );
    let header_len = header.len();
    let _ = wtr.write(&(header_len as u16).to_le_bytes());
    let _ = wtr.write(header.as_bytes()); // header


}
