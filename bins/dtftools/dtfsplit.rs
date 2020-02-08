use tdb_core::dtf;
use itertools::Itertools;

pub fn run(matches: &clap::ArgMatches) {
    // single file
    let fname = matches.value_of("input").expect("Must supply input");
    let batch_size = matches.value_of("BATCH").unwrap().parse().unwrap();
    let file_stem = std::path::Path::new(fname).file_stem().expect("Input not a valid file").to_str().unwrap();

    println!("Reading: {}", fname);
    let meta = dtf::file_format::read_meta(fname).unwrap();
    let rdr = dtf::file_format::file_reader(fname).expect("cannot open file");
    let mut it = dtf::file_format::iterators::DTFBufReader::new(rdr);
    let mut i = 0;
    for batch in &(&mut it).chunks(batch_size) {
        let outname = format!("{}-{}.dtf", file_stem, i);
        println!("Writing to {}", outname);
        dtf::file_format::encode(&outname, &meta.symbol, &batch.collect::<Vec<_>>()).unwrap();
        i += 1;
    }
}