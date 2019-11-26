extern crate libtdbcli;
extern crate clap;
extern crate fern;
extern crate chrono;
extern crate log;

use std::io::{stdin, stdout, Write};
use std::time::SystemTime;
use libtdbcli::client::TectonicClient;
use clap::{App, Arg};
use std::error::Error;
use libtectonic::dtf::update::Update;

fn init_logger() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S:%f]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(std::io::stdout())
        .chain(fern::log_file("tdb-cli.log").unwrap())
        .apply()
        .unwrap();
}


fn main() {
    init_logger();
    let matches = App::new("tectonic-cli")
        .version("0.0.1")
        .author("Ricky Han <tectonic@rickyhan.com>")
        .about("command line client for tectonic financial datastore")
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("HOST")
                .help("Sets the host to connect to (default 0.0.0.0)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("Sets the port to connect to (default 9001)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("s")
                .short("s")
                .long("subscription")
                .value_name("DBNAME")
                .help("subscribe to the datastore")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("b")
                .short("b")
                .value_name("ITERATION")
                .multiple(false)
                .help("Benchmark network latency")
                .takes_value(true),
        )
        .get_matches();

    let host = matches.value_of("host").unwrap_or("0.0.0.0");
    let port = matches.value_of("port").unwrap_or("9001");

    let mut cli = TectonicClient::new(host, port).unwrap();

    if matches.is_present("b") {
        let times = matches
            .value_of("b")
            .unwrap_or("10")
            .parse::<usize>()
            .unwrap_or(10);
        benchmark(cli, times);
    } else if matches.is_present("s") {
        let dbname = matches.value_of("s").unwrap_or("");
        subscribe(cli, dbname);
    } else {
        handle_query(&mut cli);
    }
}


fn benchmark(mut cli: TectonicClient, times: usize) {

    let mut t = SystemTime::now();

    let mut acc = vec![];
    let create = cli.cmd("CREATE benchmark\n");
    println!("{:?}", create);
    for i in 0..times {
        if i % 1_000 == 0 {
            dbg!(i);
        }
        let ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64 / 1000;

        let res = cli.insert(
            Some("benchmark"),
            &Update { ts, seq: 0, is_bid: true, is_trade: false, price: 0.001939,  size: 22.85 }
        );
        res.unwrap();
        acc.push(t.elapsed().unwrap().subsec_nanos() as usize);
        // info!("res: {:?}, latency: {:?}", res, t.elapsed());
        t = SystemTime::now();
    }

    ::std::thread::sleep(std::time::Duration::new(1, 0));
    cli.shutdown();

    let avg_ns = acc.iter().fold(0, |s, i| s + i) as f32 / acc.len() as f32;
    println!("AVG ns/insert: {}", avg_ns);
    println!("AVG inserts/s: {}", 1. / (avg_ns / 1_000_000_000.));
}


fn handle_query(cli: &mut TectonicClient) {
    loop {
        print!("--> ");
        stdout().flush().ok().expect("Could not flush stdout"); // manually flush stdout

        let mut cmd = String::new();
        stdin().read_line(&mut cmd).unwrap();
        match cli.cmd(&cmd) {
            Err(e) => {
                println!("{}", e.description());
            }
            Ok(msg) => {
                println!("{}", msg);
            }
        };
    }
}

fn subscribe(cli: TectonicClient, dbname: &str) {
    println!("Subscribing to {}", dbname);
    for up in cli.subscribe(dbname).unwrap() {
        println!("{:?}", up);
    }
}
