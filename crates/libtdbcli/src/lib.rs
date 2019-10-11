extern crate byteorder;
extern crate libtectonic;
#[macro_use] extern crate log;

pub mod error;
pub mod client;

use std::env;
use crate::client::TectonicClient;
use crate::error::TectonicError;

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
        Ok(cxn) => cxn,
        Err(TectonicError::ConnectionError) => {
            panic!("DB cannot be connected!");
        },
        _ => unreachable!(),
    }
}
