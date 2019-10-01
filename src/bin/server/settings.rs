use std::env;
use std::error::Error;
use std::str::FromStr;

pub fn key_or_default_parse<
    E: Into<Box<dyn Error>>,
    T: FromStr<Err=E>
>(key: &str, default: T) -> Result<T, Box<dyn Error>> {
    match env::var(key) {
        Ok(val) => val.parse::<T>().map_err(|err| err.into()),
        Err(_) => Ok(default),
    }
}

pub fn key_or_default(key: &str, default: &str) -> String {
   match env::var(key) {
        Ok(val) => val,
        Err(_) => default.into(),
    }
}

pub fn key_or_none(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(val) => Some(val),
        Err(_) => None,
    }
}

/// autoflush: boolean. Flush everything to disk at some interval.
/// dtf_folder: string. folder to save .dtf files
/// flush_interval: u32. flush at some regular interval.
#[derive(Clone, Debug, Default)]
pub struct Settings {
    pub autoflush: bool,
    pub dtf_folder: String,
    pub flush_interval: u32,
    pub hist_granularity: u64,
    pub hist_q_capacity: usize,
}
