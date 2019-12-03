extern crate clap;
extern crate byteorder;
extern crate libtectonic;
use libtectonic::dtf::{self, update::UpdateVecConvert};
use libtectonic::storage::utils::{scan_files_for_range, total_folder_updates_len};
use libtectonic::postprocessing::candle::TickBars;
use indoc::indoc;

mod dtfconcat;
use clap::{Arg, App};

fn main() {
    let matches = App::new("dtftools")
        .version("1.0.0")
        .author("Ricky Han <tectonic@rickyhan.com>")

        .subcommand(clap::SubCommand::with_name("concat")
                .about(indoc!("
                    Concatenates two DTF files into a single output file.

                    Examples:
                    dtfconcat file1.dtf file2.dtf output.dtf
                    "))
                .arg(
                    Arg::with_name("input1")
                        .value_name("INPUT1")
                        .help("First file to read")
                        .required(true)
                        .takes_value(true)
                        .index(1)
                )
                .arg(
                    Arg::with_name("input2")
                        .value_name("INPUT2")
                        .help("Second file to read")
                        .required(true)
                        .takes_value(true)
                        .index(2)
                )
                .arg(
                    Arg::with_name("output")
                        .value_name("OUTPUT")
                        .help("Output file")
                        .required(true)
                        .takes_value(true)
                        .index(3)
                ))

        .subcommand(clap::SubCommand::with_name("split")
            .about(indoc!("
                Splits big dtf files into smaller ones

                Examples:
                dtfsplit -i test.dtf -f test-{}.dtf
                "))
            .arg(
                Arg::with_name("input")
                    .short("i")
                    .long("input")
                    .value_name("INPUT")
                    .help("file to read")
                    .required(true)
                    .takes_value(true))
            .arg(
                Arg::with_name("BATCH")
                    .short("b")
                    .long("batch_size")
                    .value_name("BATCH_SIZE")
                    .help("Specify the number of batches to read")
                    .required(true)
                    .takes_value(true),
            ))

        .subcommand(clap::SubCommand::with_name("cat")
            .about(indoc!("
                Print dense tick format files to plaintext

                Examples:
                # filter by symbol and epoch under given folder and output csv
                dtftools cat --folder ./test/zrx --symbol bnc_zrx_btc --min 1514764800000 --max 1514851200000 -c > out
                # count number of updates across files
                dtftools cat --folder ./test/zrx -m
                # same as above but rebin into minute candle
                dtftools cat --folder ./test/zrx --symbol bnc_zrx_btc --min 1514764800000 --max 1514851200000 -c -r > out
                # hour candle
                dtftools cat --folder ./test/zrx --symbol bnc_zrx_btc --min 1514764800000 --max 1514851200000 -c -r -g 60 > out
                # read metadata of file
                dtftools cat -m -i test.dtf
                # conver to csv
                dtftools cat -i test.dtf -c
                "))
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
                .help(indoc!("
                    align with minute mark (\"snap to grid\")
                    --|------|------|------|-->
                      |
                      ^ discard up to this point
                ")))
            .arg(Arg::with_name("minutes")
                .short("g")
                .long("minutes")
                .required(false)
                .value_name("MINUTES")
                .help("granularity in minute. e.g. -m 60 # hour candle")
                .takes_value(true)))
    .get_matches();

    if let Some(matches) = matches.subcommand_matches("cat") {
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
                format!("{}", dtf::file_format::read_meta(input).unwrap())
            } else {
                let ups = dtf::file_format::decode(input, None).unwrap();
                if candle {
                    let mut candles = TickBars::from(ups.as_slice());
                    candles.insert_continuation_candles();
                    let rebinned = candles
                        .rebin(aligned, granularity.parse().unwrap())
                        .unwrap()
                        .as_csv();
                    format!("{}", rebinned)
                } else {
                    if csv {
                        format!("{}", ups.as_csv())
                    } else {
                        format!("[{}]", ups.as_json())
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
                        .as_csv();
                    format!("{}", rebinned)
                } else {
                    if csv {
                        format!("{}", ups.as_csv())
                    } else {
                        format!("[{}]", ups.as_json())
                    }
                }
            }
        };
        println!("{}", txt);
    } else if let Some(matches) = matches.subcommand_matches("split") {
        // single file
        let fname = matches.value_of("input").expect("Must supply input");
        let batch_size = matches.value_of("BATCH").unwrap().parse().unwrap();
        let file_stem = std::path::Path::new(fname).file_stem().expect("Input not a valid file").to_str().unwrap();

        println!("Reading: {}", fname);
        let meta = dtf::file_format::read_meta(fname).unwrap();
        let rdr = dtf::file_format::DTFBufReader::new(fname, batch_size);
        for (i, batch) in rdr.enumerate() {
            let outname = format!("{}-{}.dtf", file_stem, i);
            println!("Writing to {}", outname);
            dtf::file_format::encode(&outname, &meta.symbol, &batch).unwrap();
        }
    } else if let Some(matches) = matches.subcommand_matches("concat") {
        dtfconcat::run(matches);
    } else {
        println!("No subcommand match. Use dtftools --help to view help information.");
        std::process::exit(1);
    }
}
