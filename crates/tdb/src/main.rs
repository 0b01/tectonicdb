extern crate libtdbcli;
extern crate clap;
extern crate fern;
extern crate chrono;
extern crate log;

use std::io::{stdin, stdout, Write};
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
        .chain(fern::log_file("bookkeeper.log").unwrap())
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

    let mut cxn = TectonicClient::new(host, port).unwrap();

    if matches.is_present("b") {
        let times = matches
            .value_of("b")
            .unwrap_or("10")
            .parse::<usize>()
            .unwrap_or(10) + 1;
        benchmark(&mut cxn, times);
    } else if matches.is_present("s") {
        let dbname = matches.value_of("s").unwrap_or("");
        subscribe(&mut cxn, dbname);
    } else {
        handle_query(&mut cxn);
    }
}


fn benchmark(cxn: &mut TectonicClient, times: usize) {

    let mut t = std::time::SystemTime::now();

    let mut acc = vec![];
    let create = cxn.cmd("CREATE benchmark\n");
    println!("{:?}", create);
    for _ in 1..times {
        let res = cxn.insert(
            Some("benchmark".to_owned()),
            &Update { ts: 1513922718770, seq: 0, is_bid: true, is_trade: false, price: 0.001939,  size: 22.85 }
        );
        res.unwrap();
        acc.push(t.elapsed().unwrap().subsec_nanos() as usize);
        // info!("res: {:?}, latency: {:?}", res, t.elapsed());
        t = std::time::SystemTime::now();
    }

    ::std::thread::sleep(std::time::Duration::new(1, 0));
    cxn.shutdown();

    let avg_ns = acc.iter().fold(0, |s, i| s + i) as f32 / acc.len() as f32;
    println!("AVG ns/insert: {}", avg_ns);
    println!("AVG inserts/s: {}", 1. / (avg_ns / 1_000_000_000.));
}


fn handle_query(cxn: &mut TectonicClient) {
    loop {
        print!("--> ");
        stdout().flush().ok().expect("Could not flush stdout"); // manually flush stdout

        let mut cmd = String::new();
        stdin().read_line(&mut cmd).unwrap();
        match cxn.cmd(&cmd) {
            Err(e) => {
                println!("{}", e.description());
            }
            Ok(msg) => {
                println!("{}", msg);
            }
        };
    }
}

fn subscribe(cxn: &mut TectonicClient, dbname: &str) {
    let _ = cxn.subscribe(dbname);
    let rx = cxn.subscription.clone();
    let rx = rx.unwrap();

    for msg in rx.lock().unwrap().recv() {
        println!("{:?}", msg);
    }
}
