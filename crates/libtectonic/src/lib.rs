//! libtectonic is a financial data storage library
#![deny(missing_docs)]

extern crate libc;
extern crate csv;
extern crate indexmap;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate uuid;
extern crate byteorder;
#[macro_use]
extern crate bitflags;
extern crate log;
#[macro_use]
extern crate lazy_static;

/// functions for histogram, event analytics
pub mod postprocessing;
/// data structures that describe data storage
pub mod storage;
/// helper functions
pub mod utils;
/// DTF(Dense Tick Format) implmentation
pub mod dtf;
