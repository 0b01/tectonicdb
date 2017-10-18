extern crate dtf;
extern crate clap;
extern crate byteorder;

mod server;
mod state;
mod utils;
mod parser;
mod handler;
mod settings;
mod threadpool;

use clap::{Arg, App, ArgMatches};

fn main() {
    let matches = get_matches();

    let host = matches.value_of("host").unwrap_or("0.0.0.0");
    let port = matches.value_of("port").unwrap_or("9001");
    let dtf_folder = matches.value_of("dtf_folder").unwrap_or("db");
    let verbosity = matches.occurrences_of("v") as u8;
    let autoflush = matches.is_present("autoflush");
    let flush_interval = matches.value_of("flush_interval").unwrap_or("1000");
    let threads = matches.value_of("thread").unwrap_or("50");

    let settings = settings::Settings {
        autoflush: autoflush,
        dtf_folder: dtf_folder.to_owned(),
        flush_interval: flush_interval.parse::<u32>().unwrap(),
        threads: threads.parse::<usize>().unwrap(),
        verbosity
    };

    server::run_server(&host, &port, &settings);
}

fn get_matches<'a>() -> ArgMatches<'a> {
    App::new("tectonic-server")
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
    .arg(Arg::with_name("threads")
        .short("t")
        .long("threads")
        .value_name("THREAD")
        .help("Sets system thread count to handle the maximum number of client connection. (default 50)"))
    .get_matches()
}