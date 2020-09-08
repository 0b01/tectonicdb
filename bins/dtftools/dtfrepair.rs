use tdb_core::dtf;

pub fn run(matches: &clap::ArgMatches) {
    let fname = matches.value_of("input").expect("Must supply input");
    let outname = matches.value_of("output").expect("Must supply output");
    let meta = dtf::file_format::read_meta(fname).unwrap();
    let rdr = dtf::file_format::file_reader(fname).expect("cannot open file");
    let mut it = dtf::file_format::iterators::DTFBufReader::new(rdr);
    let ups: Vec<dtf::update::Update> = (&mut it).collect();
    dtf::file_format::encode(&outname, &meta.symbol, &ups).unwrap();
}