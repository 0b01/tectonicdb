mod error;
#[allow(dead_code)]
mod circular_queue;
pub mod insert_command;
mod cxn;
mod pool;

pub use self::error::TectonicError;
pub use self::cxn::Cxn;
pub use self::pool::CxnPool;
pub use self::insert_command::InsertCommand;

use std::env;


fn key_or_default(key: &str, default: &str) -> String {
   match env::var(key) {
        Ok(val) => val,
        Err(_) => default.into(),
    }
}

fn get_tectonic_conf_from_env() -> (String, String, usize) {
    let tectonic_hostname: String = key_or_default("TECTONICDB_HOSTNAME", "localhost");
    let tectonic_port: String     = key_or_default("TECTONICDB_PORT", "9001");
    let q_capacity: usize         = key_or_default("QUEUE_CAPACITY", "70000000")
                                    .parse().unwrap(); // 70mm

    (tectonic_hostname, tectonic_port, q_capacity)
}

/// Creates a new connection to TectonicDB, using configuration values from environment values
/// or defaults to localhost:9001 if none are set.
pub fn get_cxn() -> Cxn {
    let (tectonic_hostname, tectonic_port, _capacity) = get_tectonic_conf_from_env();
    match Cxn::new(&tectonic_hostname, &tectonic_port) {
        Ok(cxn) => cxn,
        Err(TectonicError::ConnectionError) => {
            panic!("DB cannot be connected!");
        },
        _ => unreachable!(),
    }
}

pub fn get_cxn_pool() -> CxnPool {
    let (tectonic_hostname, tectonic_port, capacity) = get_tectonic_conf_from_env();

    match CxnPool::new(1, &tectonic_hostname, &tectonic_port, capacity) {
        Ok(pool) => pool,
        Err(TectonicError::ConnectionError) => {
            panic!("Connection Pool cannot be established!");
        },
        _ => unreachable!(),
    }
}

// #[cfg(test)]
// mod tests {
//     use dtf;
//     use super::*;
//     #[test]
//     fn should_err() {
//         let mut cxn = Cxn::new("localhost", "9001").unwrap();
//         let res = cxn.cmd("USE test\n");
//         assert!(res.is_err());
//     }
//
//     #[test]
//     fn should_cxnpool_work() {
//         let mut cxn = CxnPool::new(10, "localhost", "9001", 100).unwrap();
//         cxn.cmd("COUNT ALL\n").unwrap();
//
//         let res = cxn.insert(&InsertCommand::Add("default".to_owned(), dtf::Update {
//             ts: 0,
//             seq: 0,
//             is_bid: false,
//             is_trade: false,
//             price: 0.,
//             size: 0.,
//         }));
//         println!("{:?}", res);
//     }
// }
