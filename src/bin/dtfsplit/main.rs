extern crate clap;
extern crate byteorder;
extern crate libtectonic;
use libtectonic::dtf;
use libtectonic::storage::utils::{scan_files_for_range, total_folder_updates_len};

use std::path::Path;
use clap::{Arg, App};

fn main() {
    let matches = App::new("dtfsplit")
        .version("1.0.0")
        .author("Ricky Han <tectonic@rickyhan.com>")
        .about("Splits big dtf files into smaller ones
Examples:
    dtfsplit -i test.dtf -f test-{}.dtf
")
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
        )
        .get_matches();

    // single file
    let fname = matches.value_of("input").expect("Must supply input");
    let batch_size = matches.value_of("BATCH").unwrap().parse().unwrap();
    let file_stem = Path::new(fname).file_stem().expect("Input not a valid file").to_str().unwrap();

    println!("Reading: {}", fname);
    let meta = dtf::read_meta(fname).unwrap();
    let rdr = dtf::DTFBufReader::new(fname, batch_size);
    for (i, batch) in rdr.enumerate() {
        let outname = format!("{}-{}.dtf", file_stem, i);
        println!("Writing to {}", outname);
        dtf::encode(&outname, &meta.symbol, &batch).unwrap();
    }
}
