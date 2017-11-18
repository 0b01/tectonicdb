extern crate byteorder;
mod write_npy;
mod record;

use record::*;
use std::io::BufWriter;
use std::fs::File;

fn main() {
    println!("Start writing");

    let fname = "test.npy";
    let new_file = File::create(fname).unwrap();
    let mut wtr = BufWriter::new(new_file);

    let mut record = [[[ 0_f32 ; INPUT_DIM]; TIME_STEP]; BATCH_SIZE];
    for batch in 0..BATCH_SIZE {
        for step in 0..TIME_STEP {
            for dim in 0..INPUT_DIM {
                record[batch][step][dim] = (batch * 100 + 10 * step + 1* dim) as f32;
            }
        }
    }

    write_npy::write(&mut wtr, &record);
}
