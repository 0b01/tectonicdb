use byteorder::{BE, LE, WriteBytesExt};
use std::io::Write;
use crate::record::Record;

static MAGIC_VALUE: &[u8] = &[0x93, 0x4E, 0x55, 0x4D, 0x50, 0x59];

fn get_header(record: &Record) -> String {
    format!(
        "{{'descr': [('data', '>f4')],'fortran_order': False,'shape': ({},{},{})}}",
        record.batch_size,
        record.time_step,
        record.input_dim
    )
}

pub fn write(wtr: &mut Write, record: &Record) {
    let _ = wtr.write(MAGIC_VALUE);
    let _ = wtr.write_u8(0x01); // major version
    let _ = wtr.write_u8(0x00); // minor version
    let header = &get_header(record);
    let header_len = header.len();
    let _ = wtr.write_u16::<LE>(header_len as u16);
    let _ = wtr.write(header.as_bytes()); // header

    for batch in record.batches.iter() {
        for step in batch.iter() {
            for input in step.iter() {
                let _ = wtr.write_f32::<BE>(*input);
            }
        }
    }

}
