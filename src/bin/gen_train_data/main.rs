extern crate byteorder;
extern crate libtectonic;

mod write_npy;
mod record;

use record::*;
use std::io::BufWriter;
use std::fs::File;
use libtectonic::dtf;
use libtectonic::postprocessing::orderbook::RebinnedOrderbook;

fn main() {
    let fname = "test.npy";
    let new_file = File::create(fname).unwrap();
    let mut wtr = BufWriter::new(new_file);

    let record = gen_dataset();

    write_npy::write(&mut wtr, &record);
}

/// dataset is a list of rebinned orderbook levels
/// then normalized to [-1,1]
fn gen_dataset() -> Record {
    let fname: &str = "test/test-data/bt_btceth.dtf";

    let batch_size = 1;
    let step_bins = 1000;
    let tick_bins = 100;

    let ups = dtf::decode(fname, Some(2000)).unwrap();
    let ob = RebinnedOrderbook::from(ups.as_slice(), step_bins, tick_bins, 1.);

    let mut record = Record::new(batch_size, step_bins, tick_bins);

    for (step, ref d_book) in ob.book.values().enumerate() {
        for (price, &size) in d_book.bids.iter() {
            let price = f64::from_bits(*price);
            let idx = ob.price_hist.index(price);
            record.batches[0][step][idx] = size;
        }
        for (price, &size) in d_book.asks.iter() {
            let price = f64::from_bits(*price);
            let idx = ob.price_hist.index(price);
            record.batches[0][step][idx] = -size;
        }
    }

    record
}