extern crate clap;
extern crate itertools;
extern crate byteorder;
extern crate dtf;

mod candle;

use candle::*;
use clap::{Arg, App};
use std::collections::HashMap;

///
/// converts orderbook updates to candles, and rebin 1 minute candles into 5min/ 12hour candles
/// 
fn main() {
        let matches = App::new("rebin")
                          .version("1.0.0")
                          .author("Ricky Han <tectonic@rickyhan.com>")
                          .about("rebin dtf files into candlesticks")
                          .arg(Arg::with_name("input")
                               .short("i")
                               .long("input")
                               .value_name("INPUT")
                               .help("file to read")
                               .required(true)
                               .takes_value(true))
                          .arg(Arg::with_name("aligned")
                               .short("a")
                               .long("aligned")
                               .help("Snap to grid"))
                          .arg(Arg::with_name("minutes")
                               .short("m")
                               .long("minutes")
                               .required(false)
                               .value_name("MINUTES")
                               .help("rebin into minutes. e.g. -m 60 # hour candles")
                               .takes_value(true))
                          .get_matches();

    let input = matches.value_of("input").unwrap();
    let aligned = matches.is_present("aligned");
    let minutes = matches.value_of("minutes").unwrap_or("1");

    let ups = dtf::decode(input);

    let candles = Candles::from_updates(true, &ups);

    let rebinned = candles
                    .rebin(aligned, minutes.parse::<u16>().unwrap())
                    .unwrap()
                    .to_csv();

    println!("{}", rebinned);
}

