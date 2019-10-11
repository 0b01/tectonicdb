use std::error;
use std::fmt;

#[derive(Debug)]
pub enum TectonicError {
    ServerError(String),
    DBNotFoundError(String),
    ConnectionError,
    SerialError,
}
use self::TectonicError::*;

impl error::Error for TectonicError {
    fn description(&self) -> &str {
        match *self {
            ServerError(ref msg) => &msg,
            DBNotFoundError(ref dbname) => &dbname,
            ConnectionError => "disconnection from tectonicdb",
            SerialError => "Unable to serialize/deserialize",
        }
    }
}

impl fmt::Display for TectonicError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ServerError(ref msg) => write!(f, "TectonicError: {}", msg),
            DBNotFoundError(ref dbname) => write!(f, "DBNotFoundError: {}", dbname),
            ConnectionError => write!(f, "ConnectionError"),
            SerialError => write!(f, "SerialError"),
        }
    }
}

impl From<std::io::Error> for TectonicError {
    fn from(_: std::io::Error) -> Self {
        TectonicError::SerialError
    }
}