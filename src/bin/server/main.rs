extern crate dtf;
extern crate clap;
extern crate byteorder;

mod server;
mod conf;

use clap::{Arg, App};

fn main() {
    let matches = App::new("tectonic-server")
                        .version("0.0.1")
                        .author("Ricky Han <tectonic@rickyhan.com>")
                        .about("tectonic financial datastore")
                        .arg(Arg::with_name("host")
                            .short("h")
                            .long("host")
                            .value_name("HOST")
                            .help("Sets the host to connect to (default 0.0.0.0)")
                            .takes_value(true))
                        .arg(Arg::with_name("port")
                            .short("p")
                            .long("port")
                            .value_name("PORT")
                            .help("Sets the port to connect to (default 9001)")
                            .takes_value(true))
                        .arg(Arg::with_name("v")
                            .short("v")
                            .multiple(true)
                            .help("Sets the level of verbosity"))
                        .get_matches();

    let host = matches.value_of("host").unwrap_or("0.0.0.0");
    let port = matches.value_of("port").unwrap_or("9001");
    let verbosity = matches.occurrences_of("v");

    server::run_server(&host, &port, verbosity);
}
