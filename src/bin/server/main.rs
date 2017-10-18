extern crate dtf;
extern crate clap;
extern crate byteorder;

mod server;
mod state;
mod utils;
mod parser;
mod handler;
mod settings;

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
                        .arg(Arg::with_name("dtf_folder")
                            .short("f")
                            .long("dtf_folder")
                            .value_name("FOLDER")
                            .help("Sets the folder to serve dtf files")
                            .takes_value(true))
                        .arg(Arg::with_name("v")
                            .short("v")
                            .multiple(true)
                            .help("Sets the level of verbosity"))
                        .arg(Arg::with_name("autoflush")
                            .short("a")
                            .help("Sets autoflush (default is false)"))
                        .arg(Arg::with_name("flush_interval")
                            .short("i")
                            .long("flush_interval")
                            .value_name("INTERVAL")
                            .help("Sets autoflush interval (default every 1000 inserts)"))
                        .get_matches();

    let host = matches.value_of("host").unwrap_or("0.0.0.0");
    let port = matches.value_of("port").unwrap_or("9001");
    let dtf_folder = matches.value_of("dtf_folder").unwrap_or("db");
    let verbosity = matches.occurrences_of("v");
    let autoflush = matches.is_present("autoflush");
    let flush_interval = matches.value_of("flush_interval").unwrap_or("1000");

    let settings = settings::Settings {
        autoflush: autoflush,
        dtf_folder: dtf_folder.to_owned(),
        flush_interval: flush_interval.parse::<u32>().unwrap()
    };

    server::run_server(&host, &port, verbosity, &settings);
}
