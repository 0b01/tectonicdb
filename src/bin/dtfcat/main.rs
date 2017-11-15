extern crate clap;
extern crate byteorder;
extern crate dtf;

use clap::{Arg, App};

fn main() {
        let matches = App::new("dtfcat")
                          .version("1.0.0")
                          .author("Ricky Han <tectonic@rickyhan.com>")
                          .about("command line client for tectonic financial datastore")
                          .arg(Arg::with_name("input")
                               .short("i")
                               .long("input")
                               .value_name("INPUT")
                               .help("file to read")
                               .required(true)
                               .takes_value(true))
                          .arg(Arg::with_name("metadata")
                               .short("m")
                               .long("metadata")
                               .help("read only the metadata"))
                          .arg(Arg::with_name("csv")
                               .short("c")
                               .long("csv")
                               .help("output csv (default is JSON)"))
                          .get_matches();

    let input = matches.value_of("input").unwrap();
    let metadata = matches.is_present("metadata");
    let csv = matches.is_present("csv");

    if metadata {
        println!("{}", dtf::read_meta(input));
        return;
    } else if csv{
        println!("{}", dtf::update_vec_to_csv(&dtf::decode(input, None)));
        return;
    } else {
        println!("[{}]", dtf::update_vec_to_json(&dtf::decode(input, None)));
        return;
    }
}
