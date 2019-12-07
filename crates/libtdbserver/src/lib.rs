#![recursion_limit="1024"]
extern crate libtectonic;
extern crate clap;
extern crate chrono;
#[cfg(feature = "gcs")]
extern crate serde;
#[cfg(feature = "gcs")]
#[macro_use]
extern crate serde_derive;
extern crate openssl_probe;
extern crate lazy_static;

#[macro_use]
extern crate log;
extern crate fern;

extern crate byteorder;
extern crate uuid;
extern crate circular_queue;

#[macro_use]
extern crate futures;
pub extern crate async_std;
extern crate ctrlc;

#[cfg(feature = "count_alloc")]
use alloc_counter::AllocCounterSystem;
#[cfg(feature = "count_alloc")]
#[cfg_attr(feature = "count_alloc", global_allocator)]
static A: AllocCounterSystem = AllocCounterSystem;

pub mod plugins;
pub mod utils;
pub mod server;
pub mod state;
pub mod parser;
pub mod handler;
pub mod settings;
pub mod prelude;
