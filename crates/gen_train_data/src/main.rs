#![feature(rustc_private)]

#[macro_use]
extern crate log;
extern crate fern;

extern crate byteorder;
extern crate libtectonic;

mod write_npy;
mod record;

use std::io::{ErrorKind, Error};
use std::io::BufWriter;
use std::fs::File;
use libtectonic::dtf;
use libtectonic::postprocessing::orderbook::RebinnedOrderbook;
use libtectonic::postprocessing::candle::TickBars;
use libtectonic::dtf::update::Update;
use crate::record::Record;

// 0 - 49: ob levels
// 50: high
// 51: vol

static TICK_BINS: usize = 50; // granularity: 50 levels

static HIGH_IDX: usize = 50; // candle
static LOW_IDX: usize = 51; // candle
static VOL_IDX: usize = 52; // candle

static DIM: usize = 53; // last dimension, sum of channels
#[allow(unused)]
static ONE_HOUR : u64 = 60 *  60 * 1000 - 1000; // one hour in ms
static TWO_HOURS: u64 = 60 * 120 * 1000 - 1000; // two hours in ms

static PRICE_DECIMALS: u8 = 8;

fn preprare_logger() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}] {}",
                record.target(),
                record.level(),
                message
            ))
        })
        .chain(std::io::stdout())
        .apply()
        .unwrap();
}

fn main() {
    preprare_logger();

    let fname = "test.npy";
    let new_file = File::create(fname).unwrap();
    let mut wtr = BufWriter::new(new_file);

    let fname: &str = "./bnc_neo_btc.dtf";
    let meta = dtf::file_format::read_meta(fname).unwrap();
    let min_ts = meta.min_ts + TWO_HOURS;
    let max_ts = min_ts + TWO_HOURS;

    let ups = dtf::file_format::get_range_in_file(fname, min_ts, max_ts).unwrap();

    match gen_one_batch(&ups) {
        Ok(record) => { // movement is the target label
            write_npy::write(&mut wtr, &record);
        },
        Err(e) => {
            error!("{}", e);
        }
    }
}

fn write_ob_levels(rec: &mut Record, ups: &[Update], step_bins: usize, tick_bins: usize) {

    let ob = RebinnedOrderbook::from(PRICE_DECIMALS, ups, step_bins, tick_bins, 1.);

    // find max size
    let mut max_size = 0.;
    for d_book in ob.book.values() {
        for &size in d_book.bids.values() {
            if size > max_size {
                max_size = size;
            }
        }
        for &size in d_book.asks.values() {
            if size > max_size {
                max_size = size;
            }
        }
    }

    // scale size by dividing by max size
    for (step, ref d_book) in ob.book.values().enumerate() {
        for (price, &size) in d_book.bids.iter() {
            let price = f64::from_bits(*price);
            let idx = ob.price_hist.index(price);
            rec.batches[0][step][idx] = size as f32 / max_size as f32;
        }
        for (price, &size) in d_book.asks.iter() {
            let price = f64::from_bits(*price);
            let idx = ob.price_hist.index(price);
            rec.batches[0][step][idx] = size as f32 / max_size as f32;
        }
    }
}

fn write_candles(rec: &mut Record, candles: &TickBars) {
    let mut highest_high = 0.;
    let mut highest_vol  = 0.;

    for candle in candles.get_candles() {
        if candle.high > highest_high {
            highest_high = candle.high;
        }
        if candle.volume > highest_vol {
            highest_vol = candle.volume;
        }
    }

    for (idx, candle) in candles.get_candles().enumerate() {
        rec.batches[0][idx][HIGH_IDX] = candle.high / highest_high;
        rec.batches[0][idx][LOW_IDX] = candle.low / highest_high;
        rec.batches[0][idx][VOL_IDX] = candle.volume / highest_vol;
    }

}

/// dataset is a list of rebinned orderbook levels
/// then normalized to [-1,1]
fn gen_one_batch(ups: &[Update]) -> Result<Record, Error> {
    let batch_size = 1;

    let candles = TickBars::from(ups);
    // missing candles in minutes
    let missing_range_sum = candles.missing_ranges()
                                   .iter()
                                   .fold(0, |acc, &(s, f)| f - s + acc)
                                   / 60;

    // if there are more than a few minutes of zero trading activities or misssing data
    if missing_range_sum > 5 {
        Err(Error::new(ErrorKind::InvalidData, "missing range bigger than 5"))
    } else {
        // steps should coincide with candles
        // (hopefully, so no lookahead bias)
        let steps = candles.get_size();
        info!("There are {} candles.", steps - 1);

        let mut record = Record::new(batch_size, steps, DIM);

        write_ob_levels(&mut record, &ups, steps, TICK_BINS);
        write_candles(&mut record, &candles);
        Ok(record)
    }

}
