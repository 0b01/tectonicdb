extern crate itertools;
extern crate clap;
extern crate byteorder;
extern crate libtectonic;
#[macro_use]
extern crate indoc;


mod dtfcat;
mod dtfsplit;
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
        dtfcat::run(matches);
    } else if let Some(matches) = matches.subcommand_matches("split") {
        dtfsplit::run(matches);
    } else if let Some(matches) = matches.subcommand_matches("concat") {
        dtfconcat::run(matches);
    } else {
        println!("No subcommand match. Use dtftools --help to view help information.");
        std::process::exit(1);
    }
}
