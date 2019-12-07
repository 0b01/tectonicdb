extern crate byteorder;
extern crate serde;
extern crate serde_json;
extern crate libtectonic;
#[macro_use] extern crate log;

pub mod error;
pub mod client;

use std::env;
use crate::client::TectonicClient;
use crate::error::TectonicError;
use std::time::SystemTime;
use libtectonic::dtf::update::Update;

fn key_or_default(key: &str, default: &str) -> String {
   match env::var(key) {
        Ok(val) => val,
        Err(_) => default.into(),
    }
}

fn get_tectonic_conf_from_env() -> (String, String) {
    let tectonic_hostname: String = key_or_default("TECTONICDB_HOSTNAME", "localhost");
    let tectonic_port: String     = key_or_default("TECTONICDB_PORT", "9001");

    (tectonic_hostname, tectonic_port)
}

/// Creates a new connection to TectonicDB, using configuration values from environment
/// or defaults to localhost:9001 if none are set.
///
/// "TECTONICDB_HOSTNAME", "localhost");
/// "TECTONICDB_PORT", "9001");
///
pub fn client_from_env() -> TectonicClient {
    let (tectonic_hostname, tectonic_port) = get_tectonic_conf_from_env();
    match TectonicClient::new(&tectonic_hostname, &tectonic_port) {
        Ok(cli) => cli,
        Err(TectonicError::ConnectionError) => {
            panic!("DB cannot be connected!");
        },
        _ => unreachable!(),
    }
}

pub fn benchmark(mut cli: TectonicClient, times: usize) {

    let mut t = SystemTime::now();

    let mut acc = vec![];
    let create = cli.cmd("CREATE benchmark\n");
    println!("{:?}", create);
    for i in 0..times {
        if i % 10_000 == 0 {
            dbg!(i);
        }
        let ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64 / 1000;

        let res = cli.insert(
            Some("benchmark"),
            &Update { ts, seq: 0, is_bid: true, is_trade: false, price: 0.001939,  size: 22.85 },
            true,
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
