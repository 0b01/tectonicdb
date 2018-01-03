extern crate clap;
extern crate byteorder;
extern crate libtectonic;

use clap::{Arg, App};
use std::{time, str};
use std::io::{self, Write};

mod db;


fn main() {
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

    let mut cxn = db::Cxn::new(host, port).unwrap();

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


fn benchmark(cxn: &mut db::Cxn, times: usize) {

    let mut t = time::SystemTime::now();

    let mut acc = vec![];
    let _create = cxn.cmd("CREATE bnc_gas_btc\n");
    for _ in 1..times {
        let _res = cxn.cmd(
            "ADD 1513922718770, 0, t, f, 0.001939, 22.85; INTO bnc_gas_btc\n",
        );
        acc.push(t.elapsed().unwrap().subsec_nanos());
        // println!("res: {:?}, latency: {:?}", res, t.elapsed());
        t = time::SystemTime::now();
    }

    let avg_ns = acc.iter().fold(0, |s, i| s + i) as f32 / acc.len() as f32;
    println!("AVG ns/insert: {}", avg_ns);
    println!("AVG inserts/s: {}", 1. / (avg_ns / 1_000_000_000.));
}


fn handle_query(cxn: &mut db::Cxn) {
    loop {
        print!("--> ");
        io::stdout().flush().ok().expect("Could not flush stdout"); // manually flush stdout

        let mut cmd = String::new();
        io::stdin().read_line(&mut cmd).unwrap();
        match cxn.cmd(&cmd) {
            Err(db::TectonicError::DecodeError) => {
                panic!("Decode Error");
            }
            Err(db::TectonicError::ConnectionError) => {
                panic!("Connection Error");
            }
            Err(db::TectonicError::ServerError(msg)) => {
                print!("{}", msg);
            }
            Ok(msg) => {
                print!("{}", msg);
            }
        };
    }
}

fn subscribe(cxn: &mut db::Cxn, dbname: &str) {
    let _ = cxn.subscribe(dbname);
    let rx = cxn.subscription.clone();
    let rx = rx.unwrap();

    for msg in rx.lock().unwrap().recv() {
        println!("{:?}", msg);
    }
}
