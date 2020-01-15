extern crate itertools;
extern crate clap;
extern crate byteorder;
extern crate libtectonic;
use itertools::Itertools;
use libtectonic::dtf;
use libtectonic::storage::utils::total_folder_updates_len;
use libtectonic::postprocessing::candle::TickBars;
use indoc::indoc;
use indicatif::{ProgressBar, ProgressStyle};
use memmap::MmapOptions;
use std::fs::File;

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
                Print dtf files to plaintext

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
                .default_value("0")
                .required(false)
                .takes_value(true)
            )
            .arg(
                Arg::with_name("max")
                .long("max")
                .value_name("MAX")
                .help("maximum value to filter for")
                .default_value("2147472000000")
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
        let min = matches.value_of("min").unwrap().parse().unwrap();
        let max = matches.value_of("max").unwrap().parse().unwrap();
        let folder = matches.value_of("folder").unwrap_or("./");
        // candle
        let candle = matches.is_present("candle");
        let aligned = matches.is_present("aligned");
        let granularity = matches.value_of("minutes").unwrap_or("1");
        // misc
        let print_metadata = matches.is_present("meta");
        let csv = matches.is_present("csv");
        if input == "" && symbol == "" && (folder == "" && !print_metadata ){
            println!("Either supply a single file with -i or specify range.");
            ::std::process::exit(1);
        }
        if input != "" {
            if print_metadata {
                println!("{}", dtf::file_format::read_meta(input).unwrap());
                let rdr = dtf::file_format::file_reader(input).expect("cannot open file");
                for meta in dtf::file_format::iterators::DTFMetadataReader::new(rdr) {
                    println!("{:?}", meta);
                }
            } else {
                if candle {
                    let ups = dtf::file_format::decode(input, None).unwrap();
                    let mut candles = TickBars::from(ups.as_slice());
                    candles.insert_continuation_candles();
                    let rebinned = candles
                        .rebin(aligned, granularity.parse().unwrap())
                        .unwrap()
                        .as_csv();
                    println!("{}", rebinned)
                } else {

                    let file = File::open(input).unwrap();
                    let rdr = unsafe { MmapOptions::new().map(&file).unwrap() };
                    let rdr = std::io::Cursor::new(rdr);

                    // let rdr = dtf::file_format::file_reader(input).expect("cannot open file");

                    let it = dtf::file_format::iterators::DTFBufReader::new(rdr);
                    let bar = ProgressBar::new(it.current_update_index().into());
                    bar.set_style(ProgressStyle::default_bar()
                        .template("[{elapsed_precise}, remaining: {eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                        .progress_chars("##-"));

                    for (i, up) in it.enumerate() {
                        if i != 0 && i % 10000 == 0 {
                            bar.inc(10000);
                        }
                        if csv {
                            println!("{}", up.as_csv()) // TODO: slooooow
                        } else {
                            println!("[{}]", up.as_json())
                        }
                    }
                    bar.finish();
                }
            }
        } else {
            if print_metadata {
                println!("total updates in folder: {}", total_folder_updates_len(folder).unwrap())
            } else {
                if candle {
                    let ups = libtectonic::dtf::file_format::scan_files_for_range(
                        folder,
                        symbol,
                        min,
                        max,
                        ).unwrap();
                    let mut candles = TickBars::from(ups.as_slice());
                    candles.insert_continuation_candles();
                    let rebinned = candles
                        .rebin(aligned, granularity.parse().unwrap())
                        .unwrap()
                        .as_csv();
                    println!("{}", rebinned)
                } else {
                    libtectonic::dtf::file_format::scan_files_for_range_for_each(
                        folder,
                        symbol,
                        min,
                        max,
                        &mut |up|{
                            if csv {
                                println!("{}", up.as_csv())
                            } else {
                                println!("[{}]", up.as_json())
                            }
                        }).unwrap();
                }
            }
        };
    } else if let Some(matches) = matches.subcommand_matches("split") {
        // single file
        let fname = matches.value_of("input").expect("Must supply input");
        let batch_size = matches.value_of("BATCH").unwrap().parse().unwrap();
        let file_stem = std::path::Path::new(fname).file_stem().expect("Input not a valid file").to_str().unwrap();

        println!("Reading: {}", fname);
        let meta = dtf::file_format::read_meta(fname).unwrap();
        let rdr = dtf::file_format::file_reader(fname).expect("cannot open file");
        let it = dtf::file_format::iterators::DTFBufReader::new(rdr);
        let mut i = 0;
        for batch in &it.chunks(batch_size) {
            let outname = format!("{}-{}.dtf", file_stem, i);
            println!("Writing to {}", outname);
            dtf::file_format::encode(&outname, &meta.symbol, &batch.collect::<Vec<_>>()).unwrap();
            i += 1;
        }
    } else if let Some(matches) = matches.subcommand_matches("concat") {
        dtfconcat::run(matches);
    } else {
        println!("No subcommand match. Use dtftools --help to view help information.");
        std::process::exit(1);
    }
}
