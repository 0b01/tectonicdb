mod error;
pub mod insert_command;
mod cxn;

pub use self::error::TectonicError;
pub use self::cxn::Cxn;
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
