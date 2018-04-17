extern crate clap;
extern crate byteorder;
extern crate libtectonic;
use libtectonic::dtf;
use libtectonic::storage::utils::{scan_files_for_range, total_folder_updates_len};
use libtectonic::postprocessing::candle::{Bar, TickBars};

use clap::{Arg, App};

fn main() {
    let matches = App::new("dtfcat")
        .version("1.0.0")
        .author("Ricky Han <tectonic@rickyhan.com>")
        .about("command line client for tectonic financial datastore
Examples:
    # filter for epoch and symbol in folder and output csv
    dtfcat --folder ./test/zrx --symbol bnc_zrx_btc --min 1514764800000 --max 1514851200000 -c > out
    # count number of numbers across files
    dtfcat --folder ./test/zrx -m
    # same as above but rebin into minute candle
    dtfcat --folder ./test/zrx --symbol bnc_zrx_btc --min 1514764800000 --max 1514851200000 -c -r > out
    # hour candle
    dtfcat --folder ./test/zrx --symbol bnc_zrx_btc --min 1514764800000 --max 1514851200000 -c -r -g 60 > out
    # read metadata of file
    dtfcat -m -i test.dtf
    # conver to csv
    dtfcat -i test.dtf -c
")
        .arg(
            Arg::with_name("input")
                .short("i")
                .long("input")
                .value_name("INPUT")
                .help("file to read")
                .required(false)
                .takes_value(true),
        )

        .arg(
            Arg::with_name("symbol")
            .long("symbol")
            .value_name("SYMBOL")
            .help("symbol too lookup")
            .required(false)
            .takes_value(true),
        )
        .arg(
            Arg::with_name("min")
            .long("min")
            .value_name("MIN")
            .help("minimum value to filter for")
            .required(false)
            .takes_value(true)
        )
        .arg(
            Arg::with_name("max")
            .long("max")
            .value_name("MAX")
            .help("maximum value to filter for")
            .required(false)
            .takes_value(true)
        )
        .arg(
            Arg::with_name("folder")
            .long("folder")
            .value_name("FOLDER")
            .help("folder to search")
            .required(false)
            .takes_value(true)
        )

        .arg(
            Arg::with_name("meta")
                .short("m")
                .long("show_metadata")
                .help("read only the metadata"),
        )
        .arg(Arg::with_name("csv").short("c").long("csv").help(
            "output csv (default is JSON)",
        ))

        // for batching into candles
        .arg(Arg::with_name("candle")
            .short("r")
            .long("candle")
            .help("output rebinned candles"))
        .arg(Arg::with_name("aligned")
            .short("a")
            .long("aligned")
            .help("Snap to grid"))
        .arg(Arg::with_name("minutes")
            .short("g")
            .long("minutes")
            .required(false)
            .value_name("MINUTES")
            .help("granularity in minute. e.g. -m 60 # hour candle")
            .takes_value(true))

        .get_matches();

    // single file
    let input = matches.value_of("input").unwrap_or("");

    // or range
    let symbol = matches.value_of("symbol").unwrap_or("");
    let min = matches.value_of("min").unwrap_or("");
    let max = matches.value_of("max").unwrap_or("");
    let folder = matches.value_of("folder").unwrap_or("./");

    // candle
    let candle = matches.is_present("candle");
    let aligned = matches.is_present("aligned");
    let granularity = matches.value_of("minutes").unwrap_or("1");

    // misc
    let print_metadata = matches.is_present("meta");
    let csv = matches.is_present("csv");

    if input == "" && (symbol == "" || min == "" || max == "") && (folder == "" && !print_metadata ){
        println!("Either supply a single file or construct a range query!");
        return;
    }

    let txt = if input != "" {
        if print_metadata {
            format!("{}", dtf::read_meta(input).unwrap())
        } else {
            let ups = dtf::decode(input, None).unwrap();
            if candle {
                let mut candles = TickBars::from(ups.as_slice());
                candles.insert_continuation_candles();
                let rebinned = candles
                    .rebin(aligned, granularity.parse().unwrap())
                    .unwrap()
                    .to_csv();
                format!("{}", rebinned)
            } else {
                if csv {
                    format!("{}", dtf::update_vec_to_csv(&ups))
                } else {
                    format!("[{}]", dtf::update_vec_to_json(&ups))
                }
            }

        }
    } else {
        if print_metadata {
            format!("total updates in folder: {}", total_folder_updates_len(folder).unwrap())
        } else {
            let ups = scan_files_for_range(folder, symbol, min.parse().unwrap(), max.parse().unwrap())
                .unwrap();
            if candle {
                let mut candles = TickBars::from(ups.as_slice());
                candles.insert_continuation_candles();
                let rebinned = candles
                    .rebin(aligned, granularity.parse().unwrap())
                    .unwrap()
                    .to_csv();
                format!("{}", rebinned)
            } else {
                if csv {
                    format!("{}", dtf::update_vec_to_csv(&ups))
                } else {
                    format!("[{}]", dtf::update_vec_to_json(&ups))
                }
            }
        }
    };

    println!("{}", txt);

}
